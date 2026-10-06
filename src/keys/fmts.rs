//! FMTS (AACS 2.1) forensic keys up front (KU §5). No public spec: the `IndividualSegment.tbl`
//! / `.fmts` model and the index-1 anchor contract are evidence (KS-25, KS-26).

use crate::aacs::content::ALIGNED_UNIT_LEN;
use crate::aacs::segment::{Segment, clip_byte_to_lba, parse_individual_segments};
use crate::consts::SECTOR_BYTES_U64;
use crate::decrypt::Phase;
use crate::disc::{ContentFormat, Extent};
use crate::error::{Error, Result};
use crate::halt::Halt;
use crate::sector::SectorSource;
use std::collections::HashMap;
use std::io;

/// Units per anchor or phase batch: the key service's sample floor.
const BATCH_UNITS: usize = crate::keysource::MIN_SAMPLE_UNITS;
/// Index-1 segments tried as the anchor, and same-index segments per phase probe (KU §2.3
/// step 11: "≤ 2 phases × the readable index-1 segments (≤ 16)").
const MAX_ANCHOR_ATTEMPTS: usize = 16;

/// The disc's forensic layout: its segments, the forensic clip and each segment's sectors.
pub(crate) struct Layout {
    pub(crate) segments: Vec<Segment>,
    pub(crate) clip: Vec<Extent>,
    /// `[start, end)` sectors and index of each addressable segment.
    pub(crate) ranges: Vec<(u32, u32, u16)>,
    /// A segment that straddles a clip extent: no defensible sector range.
    pub(crate) unresolved: bool,
}

// Bytes per source packet (the SPN unit).
const SPN_BYTES: u64 = 192;

/// Read the disc's forensic layout. `Ok(None)`: not FMTS (no or empty segment table); an unparseable table is refused.
pub(crate) fn layout(
    fs: &crate::udf::UdfFs,
    reader: &mut dyn SectorSource,
) -> Result<Option<Layout>> {
    let tbl = match fs.read_file(reader, "/AACS/IndividualSegment.tbl") {
        Ok(t) => t,
        Err(Error::UdfNotFound { .. }) => return Ok(None),
        Err(e) => return Err(e),
    };
    let Some(segments) = parse_individual_segments(&tbl) else {
        tracing::warn!(target: "freemkv::keys", "fmts: IndividualSegment.tbl present but unparseable");
        // Ok(None) would rip forensic units as base content: refuse.
        return Err(Error::FmtsKeyMissing);
    };
    if segments.is_empty() {
        return Ok(None);
    }
    let Some(clip) = forensic_clip_extents(fs, reader)? else {
        // No defensible anchor for the segment byte space: refuse rather than guess.
        tracing::warn!(target: "freemkv::keys", "fmts: forensic clip not identifiable");
        return Err(Error::FmtsKeyMissing);
    };
    let segments = filter_addressable_segments(segments, &clip);
    let mut ranges = Vec::with_capacity(segments.len());
    let mut unresolved = false;
    for seg in &segments {
        let (start, end) = (
            seg.start_spn as u64 * SPN_BYTES,
            (seg.end_spn as u64 + 1) * SPN_BYTES,
        );
        let (Some(a), Some(b)) = (
            clip_byte_to_lba(&clip, start),
            clip_byte_to_lba(&clip, end.saturating_sub(1)),
        ) else {
            unresolved = true;
            continue;
        };
        if seg.start_spn <= seg.end_spn
            && b >= a
            && (b - a) as u64 == (end - 1 - start) / SECTOR_BYTES_U64
        {
            ranges.push((a, b + 1, seg.index));
        } else {
            unresolved = true;
        }
    }
    Ok(Some(Layout {
        segments,
        clip,
        ranges,
        unresolved,
    }))
}

/// What the index-1 anchor produced.
pub(crate) enum Anchor {
    /// The source's complete ordered forensic set (element i = index i + 1).
    Keys(Vec<[u8; 16]>),
    /// Every anchor batch read faulted: nothing to ask with (KU §5.4 Pending).
    Pending,
    /// Anchors were read and asked with, and no source had the set; the first source
    /// failure, if any.
    Missing(Option<Error>),
}

// Aligned unit `index` of `seg`: clip byte `start_spn * 192 + index * 6144`.
fn read_unit(
    reader: &mut dyn SectorSource,
    clip: &[Extent],
    seg: &Segment,
    index: usize,
) -> Option<Vec<u8>> {
    let byte = seg.start_spn as u64 * SPN_BYTES + index as u64 * ALIGNED_UNIT_LEN as u64;
    let lba = clip_byte_to_lba(clip, byte)?;
    // Any failed read is a fault here, a Stop included: the anchor goes Pending.
    super::evidence::read_unit(reader, lba).ok().flatten()
}

/// Whether the forensic segments read in the clear: the first unit of each of up to
/// [`MAX_ANCHOR_ATTEMPTS`] segments is readable and unflagged. `false` when a segment has no
/// sector range or no unit was read.
pub(crate) fn segments_clear(
    reader: &mut dyn SectorSource,
    layout: &Layout,
    format: ContentFormat,
    halt: &Halt,
) -> Result<bool> {
    if layout.unresolved || layout.segments.is_empty() {
        return Ok(false);
    }
    for seg in layout.segments.iter().take(MAX_ANCHOR_ATTEMPTS) {
        halt.check()?;
        match read_unit(reader, &layout.clip, seg, 0) {
            Some(u) if !crate::aacs::content::aacs_unit_encrypted(&u, format) => {}
            _ => return Ok(false),
        }
    }
    Ok(true)
}

/// Sends one anchor batch to the sources: `Ok(None)` for an empty answer.
pub(crate) type AskFn<'a> = dyn FnMut(&[Vec<u8>]) -> Result<Option<Vec<[u8; 16]>>> + 'a;

/// Anchor the forensic set from one index-1 batch per phase (KU §5.1; evidence KS-26: the
/// decode server "returns the whole set only for an index-1 anchor"). `ask` sends one batch
/// and returns `Ok(None)` for an empty answer.
pub(crate) fn anchor(
    reader: &mut dyn SectorSource,
    layout: &Layout,
    halt: Option<&Halt>,
    ask: &mut AskFn,
) -> Result<Anchor> {
    let mut asked = false;
    let mut failure: Option<Error> = None;
    let anchors = layout.segments.iter().filter(|s| s.index == 1);
    for seg in anchors.take(MAX_ANCHOR_ATTEMPTS) {
        for phase in [0usize, 1] {
            if halt.is_some_and(|h| h.is_cancelled()) {
                return Err(Error::Halted);
            }
            let batch: Option<Vec<Vec<u8>>> = (0..BATCH_UNITS)
                .map(|p| read_unit(reader, &layout.clip, seg, p * 2 + phase))
                .collect();
            let Some(batch) = batch else {
                continue;
            };
            asked = true;
            match ask(&batch) {
                // KS-26 (evidence): a non-empty reply to an index-1 batch is the whole set.
                Ok(Some(keys)) if !keys.is_empty() => return Ok(Anchor::Keys(keys)),
                Ok(_) => {}
                Err(Error::Halted) => return Err(Error::Halted),
                Err(e) => {
                    failure.get_or_insert(e);
                }
            }
        }
    }
    Ok(if asked {
        Anchor::Missing(failure)
    } else {
        Anchor::Pending
    })
}

/// Each index's decrypt phase under its key (KU §5.1, §5.3). `Ok(None)`: an index whose
/// units read but decrypted clean in neither parity (a wrong key). An index whose every
/// probe faulted is `Phase::Verify`.
pub(crate) fn phases(
    reader: &mut dyn SectorSource,
    layout: &Layout,
    keys: &[[u8; 16]],
    format: ContentFormat,
    halt: Option<&Halt>,
) -> Result<Option<HashMap<u16, Phase>>> {
    let mut out = HashMap::new();
    for (i, key) in keys.iter().enumerate() {
        if halt.is_some_and(|h| h.is_cancelled()) {
            return Err(Error::Halted);
        }
        let tag = (i + 1) as u16;
        let probe = probe_index_phase(
            &layout.segments,
            tag,
            BATCH_UNITS,
            MAX_ANCHOR_ATTEMPTS,
            format,
            key,
            |seg, unit| read_unit(reader, &layout.clip, seg, unit),
        );
        match probe {
            IndexProbe::Phase(p) => {
                out.insert(tag, p);
            }
            IndexProbe::WrongKey => return Ok(None),
            // KS-25/KS-26 (evidence, no public spec): unknown phase → `Phase::Verify` (§5.3).
            IndexProbe::ReadFault => {
                out.insert(tag, Phase::Verify);
            }
        }
    }
    Ok(Some(out))
}

// Decide a forensic index's decrypt phase from clean-sample counts of its EVEN vs ODD aligned
// units under that index's key. Extracted for unit-testing.
fn resolve_tie_phase(even_clean: usize, odd_clean: usize) -> Option<Phase> {
    use std::cmp::Ordering;
    match even_clean.cmp(&odd_clean) {
        Ordering::Greater => Some(Phase::Even),
        Ordering::Less => Some(Phase::Odd),
        Ordering::Equal if even_clean == 0 => None,
        Ordering::Equal => Some(Phase::Even),
    }
}

// Outcome of probing ONE forensic index's decrypt phase; the load-bearing split is WrongKey vs
// ReadFault — only the former is a real FmtsKeyMissing.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum IndexProbe {
    /// A parity decrypted clean under this index's key (or a padding tie) → its phase.
    Phase(crate::decrypt::Phase),
    /// At least one unit was READ and decrypt-attempted, yet NEITHER parity came up
    /// clean under this index's key on any same-index segment → genuine wrong key.
    WrongKey,
    /// EVERY probe read of every same-index segment faulted (`read` returned `None`
    /// for all attempts) → zero decrypt evidence. A recoverable read fault, NOT a
    /// wrong key: there is no data to conclude the key is bad.
    ReadFault,
}

// Probe one forensic index's decrypt phase (EVEN vs ODD aligned units) under `key`, tolerating
// read faults without masking a genuine wrong key.
pub(crate) fn probe_index_phase(
    segments: &[crate::aacs::segment::Segment],
    tag: u16,
    batch_units: usize,
    max_segments: usize,
    format: ContentFormat,
    key: &[u8; 16],
    mut read: impl FnMut(&crate::aacs::segment::Segment, usize) -> Option<Vec<u8>>,
) -> IndexProbe {
    use crate::aacs::content::{aacs_unit_encrypted, decrypt_unit, is_clean};
    let mut any_read = false;
    for seg in segments
        .iter()
        .filter(|s| s.index == tag)
        .take(max_segments)
    {
        let (mut even, mut odd) = (0usize, 0usize);
        let mut seg_read = false;
        for p in 0..batch_units {
            for (phase_off, counter) in [(0usize, &mut even), (1usize, &mut odd)] {
                if let Some(mut c) = read(seg, p * 2 + phase_off) {
                    seg_read = true;
                    if aacs_unit_encrypted(&c, format) {
                        decrypt_unit(&mut c, key);
                        if is_clean(&c, format) {
                            *counter += 1;
                        }
                    }
                }
            }
        }
        if !seg_read {
            continue; // every read of this segment faulted — try the next same-index one
        }
        any_read = true;
        // A clean parity or padding tie (even == odd > 0) resolves the phase;
        // even == odd == 0 is this segment's wrong-key signature, but a
        // different same-index segment could still anchor, so keep trying.
        if let Some(phase) = resolve_tie_phase(even, odd) {
            return IndexProbe::Phase(phase);
        }
    }
    if any_read {
        IndexProbe::WrongKey
    } else {
        IndexProbe::ReadFault
    }
}

// Tag for an FMTS forensic index key banked into the key pool (base + slot); separates disc
// BASE CPS unit keys (< this) from forensic ones (>= this).
pub(crate) const FMTS_POOL_TAG_BASE: u32 = 1 << 24;

// FMTS forensic feature clip's own extents — the byte space every `IndividualSegment.tbl` SPN
// is relative to — or None if not exactly one such clip.
pub(crate) fn forensic_clip_extents(
    udf: &crate::udf::UdfFs,
    reader: &mut dyn SectorSource,
) -> io::Result<Option<Vec<crate::disc::Extent>>> {
    let Some(dir) = udf.find_dir("/BDMV/STREAM") else {
        return Ok(None);
    };
    let mut names = dir
        .entries
        .iter()
        .filter(|e| !e.is_dir && e.name.to_ascii_lowercase().ends_with(".fmts"))
        .map(|e| e.name.clone());
    let Some(name) = names.next() else {
        return Ok(None);
    };
    if names.next().is_some() {
        tracing::warn!(target: "freemkv::keys", "fmts: more than one forensic clip on the disc — segment byte space is ambiguous");
        return Ok(None);
    }
    // Addressing variant: these extents are a byte-space map for the forensic
    // segment table (`clip_byte_to_lba`), not a read plan — an unrecorded
    // extent must stay in place here or every later segment offset shifts.
    let exts: Vec<crate::disc::Extent> = udf
        .file_extents_addressing(reader, &format!("/BDMV/STREAM/{name}"))
        .map_err(io::Error::from)?
        .into_iter()
        .filter(|&(lba, sectors)| lba > 0 && sectors > 0)
        .map(|(start_lba, sector_count)| crate::disc::Extent {
            start_lba,
            sector_count,
        })
        .collect();
    Ok((!exts.is_empty()).then_some(exts))
}

// Keep only forensic segments addressable within the FORENSIC CLIP's extents; stale/foreign
// records past the clip's end are dropped. Split out of `layout` for direct testing.
pub(crate) fn filter_addressable_segments(
    segments: Vec<crate::aacs::segment::Segment>,
    extents: &[crate::disc::Extent],
) -> Vec<crate::aacs::segment::Segment> {
    segments
        .into_iter()
        .filter(|s| {
            crate::aacs::segment::clip_byte_to_lba(extents, s.start_spn as u64 * SPN_BYTES)
                .is_some()
        })
        .collect()
}

// Back-fill LBA gaps NOT covered by forensic segment ranges with the base Unit Key, so the map
// is a COMPLETE positive list over the title's content extents.
pub(crate) fn fill_base_key_gaps(
    extents: &[crate::disc::Extent],
    forensic_ranges: &[(u32, u32, usize, crate::decrypt::Phase)],
    base_idx: usize,
) -> Vec<(u32, u32, usize, crate::decrypt::Phase)> {
    let cuts: Vec<(u32, u32)> = {
        let mut c: Vec<(u32, u32)> = forensic_ranges.iter().map(|&(s, e, _, _)| (s, e)).collect();
        c.sort_unstable();
        c
    };
    let mut fills = Vec::new();
    for ext in extents {
        let end = ext.start_lba.saturating_add(ext.sector_count);
        let mut cur = ext.start_lba;
        for &(cs, ce) in &cuts {
            if ce <= cur || cs >= end {
                continue; // cut outside this extent
            }
            if cs > cur {
                fills.push((cur, cs, base_idx, crate::decrypt::Phase::All));
            }
            cur = cur.max(ce);
        }
        if cur < end {
            fills.push((cur, end, base_idx, crate::decrypt::Phase::All));
        }
    }
    fills
}

#[cfg(test)]
#[path = "fmts_probe_tests.rs"]
mod probe_tests;

#[cfg(test)]
#[path = "fmts_fmts_helper_tests.rs"]
mod fmts_helper_tests;
