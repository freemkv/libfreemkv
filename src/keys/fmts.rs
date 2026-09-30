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
use crate::whole_disc::UNIT;
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
    let mut unit = vec![0u8; ALIGNED_UNIT_LEN];
    match reader.read_sectors(lba, UNIT as u16, &mut unit, false) {
        Ok(n) if n == ALIGNED_UNIT_LEN => Some(unit),
        _ => None,
    }
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
// records past the clip's end are dropped. Extracted from resolve_fmts_key_map for direct
// testing.
pub(crate) fn filter_addressable_segments(
    segments: Vec<crate::aacs::segment::Segment>,
    extents: &[crate::disc::Extent],
) -> Vec<crate::aacs::segment::Segment> {
    segments
        .into_iter()
        .filter(|s| {
            crate::aacs::segment::clip_byte_to_lba(extents, s.start_spn as u64 * 192).is_some()
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
mod probe_tests {
    use crate::disc::ContentFormat;

    // BEHAVIOR 2 — phase-tie default: all four arms of the even/odd clean-count decision.
    #[test]
    fn resolve_tie_phase_covers_all_arms() {
        use crate::decrypt::Phase;
        // Non-tie: the clean half is the index's real variant.
        assert_eq!(
            super::resolve_tie_phase(5, 2).unwrap(),
            Phase::Even,
            "even majority → Even"
        );
        assert_eq!(
            super::resolve_tie_phase(2, 5).unwrap(),
            Phase::Odd,
            "odd majority → Odd"
        );
        // Padding tie (both halves clean, > 0): parity immaterial → default Even.
        assert_eq!(
            super::resolve_tie_phase(3, 3).unwrap(),
            Phase::Even,
            "even == odd > 0 → default Even"
        );
        assert_eq!(super::resolve_tie_phase(1, 1).unwrap(), Phase::Even);
        // Neither half clean (even == odd == 0): no evidence.
        assert_eq!(super::resolve_tie_phase(0, 0), None);
    }

    // ── Fix 1: FMTS phase-probe read-fault vs wrong-key distinction ─────────

    // Build a 6144-byte aligned unit of CLEAN MPEG-TS then AACS-encrypt it under key.
    fn encrypted_clean_unit(key: &[u8; 16]) -> Vec<u8> {
        use crate::aacs::content::ALIGNED_UNIT_LEN;
        let mut u = vec![0u8; ALIGNED_UNIT_LEN];
        let mut off = 0;
        while off + 192 <= ALIGNED_UNIT_LEN {
            u[off + 4] = 0x47; // TS sync at the BD-TS packet stride
            for b in &mut u[off + 5..off + 192] {
                *b = 0xAB; // non-zero payload so is_clean counts it as content
            }
            off += 192;
        }
        // Flag encrypted BEFORE encrypting: bytes 0..16 are the key seed.
        u[0] |= 0xC0;
        assert!(
            crate::aacs::content::encrypt_unit(&mut u, key),
            "a full-length unit must encrypt"
        );
        u
    }

    fn a_segment(index: u16) -> crate::aacs::segment::Segment {
        crate::aacs::segment::Segment {
            index,
            start_spn: 0,
            end_spn: 100,
        }
    }

    // A probe whose EVERY read faults must classify as ReadFault, NOT WrongKey.
    #[test]
    fn probe_index_phase_all_faults_is_read_fault_not_wrong_key() {
        let segs = vec![a_segment(1)];
        let key = [0x11u8; 16];
        let got = super::probe_index_phase(
            &segs,
            1,
            8,
            16,
            ContentFormat::BdTs,
            &key,
            |_seg, _unit| None, // every read faults
        );
        assert_eq!(
            got,
            super::IndexProbe::ReadFault,
            "all-faulted probe is a recoverable read fault, never a wrong key"
        );
    }

    /// Reads SUCCEED but decrypt to NEITHER clean parity (ciphertext under a key we
    /// do NOT hold) → [`IndexProbe::WrongKey`]. This is the genuine-missing-key path
    /// the caller MUST keep as a hard `FmtsKeyMissing`.
    #[test]
    fn probe_index_phase_reads_succeed_but_no_clean_phase_is_wrong_key() {
        let segs = vec![a_segment(1)];
        let cipher = encrypted_clean_unit(&[0xAAu8; 16]); // encrypted under key A
        let probe_key = [0xBBu8; 16]; // ... probed under the WRONG key B
        let got = super::probe_index_phase(
            &segs,
            1,
            8,
            16,
            ContentFormat::BdTs,
            &probe_key,
            |_seg, _unit| Some(cipher.clone()),
        );
        assert_eq!(
            got,
            super::IndexProbe::WrongKey,
            "reads that decrypt to no clean parity under the probed key are a wrong key"
        );
    }

    /// Reads succeed and the EVEN units decrypt clean under this index's key while
    /// the ODD units are (unencrypted) padding → [`IndexProbe::Phase`]`(Even)`.
    #[test]
    fn probe_index_phase_resolves_clean_even_phase() {
        use crate::aacs::content::ALIGNED_UNIT_LEN;
        use crate::decrypt::Phase;
        let segs = vec![a_segment(1)];
        let key = [0x33u8; 16];
        let even_unit = encrypted_clean_unit(&key);
        let got = super::probe_index_phase(
            &segs,
            1,
            8,
            16,
            ContentFormat::BdTs,
            &key,
            // even unit index → clean ciphertext under `key`; odd → zero padding
            // (aacs_unit_encrypted false → not counted).
            |_seg, unit| {
                if unit % 2 == 0 {
                    Some(even_unit.clone())
                } else {
                    Some(vec![0u8; ALIGNED_UNIT_LEN])
                }
            },
        );
        assert_eq!(
            got,
            super::IndexProbe::Phase(Phase::Even),
            "clean even units + padding odd → Even phase"
        );
    }

    // Read-fault TOLERANCE: first same-index segment faults every read, second decrypts clean
    // -> probe falls through.
    #[test]
    fn probe_index_phase_falls_through_faulting_segment_to_next() {
        use crate::decrypt::Phase;
        let mut faulting = a_segment(1);
        faulting.start_spn = 1; // distinguish the two same-index segments
        let good = a_segment(1);
        let segs = vec![faulting, good];
        let key = [0x44u8; 16];
        let clean = encrypted_clean_unit(&key);
        let got = super::probe_index_phase(
            &segs,
            1,
            8,
            16,
            ContentFormat::BdTs,
            &key,
            // The faulting segment (start_spn == 1) reads None; the good one reads a
            // clean even unit / padding odd.
            |seg, unit| {
                if seg.start_spn == 1 {
                    None
                } else if unit % 2 == 0 {
                    Some(clean.clone())
                } else {
                    Some(vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN])
                }
            },
        );
        assert_eq!(
            got,
            super::IndexProbe::Phase(Phase::Even),
            "a faulting first segment must not block resolving from the next same-index one"
        );
    }

    // A READABLE but unclean same-index segment (wrong-key signature) must not
    // stop the probe: a later same-index segment can still anchor the phase.
    #[test]
    fn probe_index_phase_falls_through_unclean_segment_to_next() {
        use crate::decrypt::Phase;
        let mut unclean = a_segment(1);
        unclean.start_spn = 1;
        let segs = vec![unclean, a_segment(1)];
        let key = [0x55u8; 16];
        let clean = encrypted_clean_unit(&key);
        let other = encrypted_clean_unit(&[0x66u8; 16]); // decrypts to junk under `key`
        let got = super::probe_index_phase(
            &segs,
            1,
            8,
            16,
            ContentFormat::BdTs,
            &key,
            |seg, unit| {
                if seg.start_spn == 1 {
                    Some(other.clone())
                } else if unit % 2 == 0 {
                    Some(clean.clone())
                } else {
                    Some(vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN])
                }
            },
        );
        assert_eq!(
            got,
            super::IndexProbe::Phase(Phase::Even),
            "an unclean first segment must not end the probe as WrongKey"
        );
    }
}

#[cfg(test)]
mod fmts_helper_tests {
    use super::*;

    // ── resolve_fmts_key_map decision helpers (behaviors flagged by audit) ──

    // BEHAVIOR 1 — segment filter: a segment mapping inside the title's extents is kept,
    // past-clip dropped, all-outside -> empty.
    #[test]
    fn filter_addressable_segments_keeps_only_in_title_segments() {
        use crate::aacs::segment::Segment;
        // One extent covering clip bytes [0, 60*2048) = [0, 122880).
        let extents = vec![Extent {
            start_lba: 500,
            sector_count: 60,
        }];
        // start_spn 100 → clip byte 19200 < 122880 → maps to an LBA → KEEP.
        let inside = Segment {
            index: 1,
            start_spn: 100,
            end_spn: 199,
        };
        // start_spn 1000 → clip byte 192000 >= 122880 → clip_byte_to_lba None → DROP.
        let outside = Segment {
            index: 2,
            start_spn: 1000,
            end_spn: 1099,
        };
        let kept = super::filter_addressable_segments(vec![inside, outside], &extents);
        assert_eq!(kept, vec![inside], "only the in-title segment survives");
        // All-outside → empty; `resolve_fmts_key_map` maps this to Ok(None).
        assert!(
            super::filter_addressable_segments(vec![outside], &extents).is_empty(),
            "no addressable segment → empty (→ resolver Ok(None))"
        );
        // Boundary: a segment whose start is the LAST clip byte still maps (Some);
        // one exactly at the clip end (122880) does not.
        let at_last = Segment {
            index: 3,
            start_spn: (122_879 / 192) as u32, // 639 → byte 122688 < 122880
            end_spn: 700,
        };
        let at_end = Segment {
            index: 4,
            start_spn: (122_880 / 192) as u32, // 640 → byte 122880 == clip end → None
            end_spn: 700,
        };
        assert_eq!(
            super::filter_addressable_segments(vec![at_last, at_end], &extents),
            vec![at_last],
            "start inside the clip is kept; start at/after the clip end is dropped"
        );
    }

    // forensic + fills must cover every LBA of every extent EXACTLY once: no gap, no overlap.
    fn assert_gapless(
        extents: &[Extent],
        forensic: &[(u32, u32, usize, crate::decrypt::Phase)],
        fills: &[(u32, u32, usize, crate::decrypt::Phase)],
    ) {
        let mut spans: Vec<(u32, u32)> = forensic.iter().map(|&(s, e, _, _)| (s, e)).collect();
        spans.extend(fills.iter().map(|&(s, e, _, _)| (s, e)));
        spans.sort_unstable();
        for w in spans.windows(2) {
            assert!(w[0].1 <= w[1].0, "spans overlap: {:?} vs {:?}", w[0], w[1]);
        }
        for ext in extents {
            let end = ext.start_lba + ext.sector_count;
            for lba in ext.start_lba..end {
                let covering = spans.iter().filter(|&&(s, e)| lba >= s && lba < e).count();
                assert_eq!(
                    covering, 1,
                    "LBA {lba} covered {covering}× (want exactly 1)"
                );
            }
        }
    }

    // BEHAVIOR 3 — gap-fill range arithmetic, exhaustive over segment positions.
    #[test]
    fn fill_base_key_gaps_is_gapless_over_every_extent() {
        use crate::decrypt::Phase::{All, Even, Odd};
        let base = 0usize;

        // No segments → the whole extent is base key.
        let ext = vec![Extent {
            start_lba: 100,
            sector_count: 60,
        }];
        let forensic: Vec<(u32, u32, usize, crate::decrypt::Phase)> = vec![];
        let fills = super::fill_base_key_gaps(&ext, &forensic, base);
        assert_eq!(fills, vec![(100, 160, base, All)], "no segments → all base");
        assert_gapless(&ext, &forensic, &fills);

        // One segment mid-extent → base | forensic | base, gapless.
        let forensic = vec![(120, 130, 5, Even)];
        let fills = super::fill_base_key_gaps(&ext, &forensic, base);
        assert_eq!(
            fills,
            vec![(100, 120, base, All), (130, 160, base, All)],
            "mid-extent segment → leading + trailing base"
        );
        assert_gapless(&ext, &forensic, &fills);

        // Segment at extent START → only a trailing base fill (no zero-length lead).
        let forensic = vec![(100, 130, 5, Even)];
        let fills = super::fill_base_key_gaps(&ext, &forensic, base);
        assert_eq!(
            fills,
            vec![(130, 160, base, All)],
            "segment at start → no leading base, one trailing"
        );
        assert_gapless(&ext, &forensic, &fills);

        // Segment at extent END → only a leading base fill (no zero-length trail).
        let forensic = vec![(130, 160, 5, Even)];
        let fills = super::fill_base_key_gaps(&ext, &forensic, base);
        assert_eq!(
            fills,
            vec![(100, 130, base, All)],
            "segment at end → one leading base, no trailing"
        );
        assert_gapless(&ext, &forensic, &fills);

        // Whole extent is one segment → no base fill at all, still gapless.
        let forensic = vec![(100, 160, 5, Even)];
        let fills = super::fill_base_key_gaps(&ext, &forensic, base);
        assert!(
            fills.is_empty(),
            "segment spans whole extent → no base fill"
        );
        assert_gapless(&ext, &forensic, &fills);

        // Adjacent segments (touching, no gap between) → NO zero-length base range
        // between them (guards the `cs > cur` off-by-one).
        let forensic = vec![(110, 120, 5, Even), (120, 130, 6, Odd)];
        let fills = super::fill_base_key_gaps(&ext, &forensic, base);
        assert_eq!(
            fills,
            vec![(100, 110, base, All), (130, 160, base, All)],
            "adjacent segments → no zero-length fill between them"
        );
        assert_gapless(&ext, &forensic, &fills);

        // Multi-extent: a segment mid-first-extent and one at the start of the
        // second. Fills are per-extent and the union is gapless across both.
        let exts = vec![
            Extent {
                start_lba: 100,
                sector_count: 60,
            }, // [100, 160)
            Extent {
                start_lba: 1000,
                sector_count: 40,
            }, // [1000, 1040)
        ];
        let forensic = vec![(120, 130, 5, Even), (1000, 1010, 7, Odd)];
        let fills = super::fill_base_key_gaps(&exts, &forensic, base);
        assert_eq!(
            fills,
            vec![
                (100, 120, base, All),
                (130, 160, base, All),
                (1010, 1040, base, All),
            ],
            "each extent filled independently"
        );
        assert_gapless(&exts, &forensic, &fills);
    }

    // Forensic ranges arrive in table RECORD order, not LBA order; the gap walk's forward sweep
    // needs them sorted.
    #[test]
    fn fill_base_key_gaps_sorts_cuts_that_arrive_in_table_order_not_lba_order() {
        use crate::decrypt::Phase::{All, Even, Odd};
        let ext = vec![Extent {
            start_lba: 100,
            sector_count: 60,
        }];
        // Table order: the HIGH segment recorded before the LOW one.
        let forensic = vec![(140, 150, 6, Odd), (110, 120, 5, Even)];
        let fills = super::fill_base_key_gaps(&ext, &forensic, 0);
        assert_eq!(
            fills,
            vec![(100, 110, 0, All), (120, 140, 0, All), (150, 160, 0, All),],
            "the gaps around BOTH forensic cuts must be filled, whatever order the \
             segment table listed them in"
        );
        // The load-bearing invariant: exactly one range covers every content LBA.
        assert_gapless(&ext, &forensic, &fills);
    }
}
