//! FMTS (AACS 2.1) forensic keys up front (KU §5). No public spec: the `IndividualSegment.tbl`
//! / `.fmts` model and the index-1 anchor contract are evidence (KS-25, KS-26).

use crate::aacs::content::ALIGNED_UNIT_LEN;
use crate::aacs::segment::{Segment, clip_byte_to_lba, parse_individual_segments};
use crate::decrypt::Phase;
use crate::disc::{ContentFormat, Extent};
use crate::error::{Error, Result};
use crate::halt::Halt;
use crate::sector::SectorSource;
use std::collections::HashMap;

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

/// Read the disc's forensic layout. `Ok(None)`: not FMTS (no or empty segment table).
pub(crate) fn layout(
    fs: &crate::udf::UdfFs,
    reader: &mut dyn SectorSource,
) -> Result<Option<Layout>> {
    let tbl = match fs.read_file(reader, "/AACS/IndividualSegment.tbl") {
        Ok(t) => t,
        Err(Error::UdfNotFound { .. }) => return Ok(None),
        Err(e) => return Err(e),
    };
    let Some(segments) = parse_individual_segments(&tbl).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let Some(clip) = crate::mux::resolve::forensic_clip_extents(fs, reader)? else {
        // No defensible anchor for the segment byte space: refuse rather than guess.
        tracing::warn!(target: "freemkv::keys", "fmts: forensic clip not identifiable");
        return Err(Error::FmtsKeyMissing);
    };
    let segments = crate::mux::resolve::filter_addressable_segments(segments, &clip);
    let mut ranges = Vec::with_capacity(segments.len());
    let mut unresolved = false;
    for seg in &segments {
        let (start, end) = (seg.start_spn as u64 * 192, (seg.end_spn as u64 + 1) * 192);
        let (Some(a), Some(b)) = (
            clip_byte_to_lba(&clip, start),
            clip_byte_to_lba(&clip, end.saturating_sub(1)),
        ) else {
            unresolved = true;
            continue;
        };
        if seg.start_spn <= seg.end_spn && b >= a && (b - a) as u64 == (end - 1 - start) / 2048 {
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
    let byte = seg.start_spn as u64 * 192 + index as u64 * ALIGNED_UNIT_LEN as u64;
    let lba = clip_byte_to_lba(clip, byte)?;
    let mut unit = vec![0u8; ALIGNED_UNIT_LEN];
    match reader.read_sectors(lba, 3, &mut unit, false) {
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
    use crate::mux::resolve::{IndexProbe, probe_index_phase};
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
