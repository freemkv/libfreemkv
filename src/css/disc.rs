//! DVD title-key selection for the shared sector decryption stage.

use crate::decrypt::DecryptKeys;
use crate::disc::Extent;
use crate::error::{Error, Result};
use crate::halt::Halt;
use crate::sector::SectorSource;
use crate::udf::{DirEntry, MAX_WALK_RANGES, UdfFs};
use std::collections::{BTreeMap, BTreeSet};

const CRACK_BATCH_SECTORS: u16 = 64;

struct Group {
    spans: Vec<Extent>,
    keys: DecryptKeys,
}

pub(crate) struct DiscKeys {
    groups: Vec<Group>,
}

impl DiscKeys {
    /// One key group per VOB file set, each cracked from its own scrambled sectors: a VTS's
    /// title VOBs (`VTS_xx_1..9`) share one, its menu VOB (`VTS_xx_0`) and `VIDEO_TS.VOB` each
    /// have their own (CSS keys each file separately). Title groups come first, so a span
    /// two files claim belongs to the title. A group whose crack finds scrambled sectors but
    /// no key refuses (`CssKeyMissing`): another file's key is never applied to it.
    pub(crate) fn new(
        fs: &UdfFs,
        reader: &mut dyn SectorSource,
        halt: Option<&Halt>,
    ) -> Result<Self> {
        fn collect<'a>(dir: &'a DirEntry, out: &mut BTreeMap<(u8, String), Vec<&'a DirEntry>>) {
            for entry in &dir.entries {
                if entry.is_dir {
                    collect(entry, out);
                } else if let Some(key) = vob_group_of(&entry.name) {
                    out.entry(key).or_default().push(entry);
                }
            }
        }
        let mut files = BTreeMap::new();
        collect(&fs.root, &mut files);
        let mut groups = Vec::new();
        let mut total_spans = 0;
        // ICBs already owned: an alias (any name, any group) adds no second owner.
        let mut seen = BTreeSet::new();
        for (_, mut files) in files {
            files.sort_by_key(|f| f.name.to_ascii_uppercase());
            let mut group = Group {
                spans: Vec::new(),
                keys: DecryptKeys::None,
            };
            let mut crack = Vec::new();
            for entry in files {
                if !seen.insert(entry.meta_lba) {
                    continue;
                }
                let start = group.spans.len();
                for extent in fs
                    .extents_abs_at(reader, entry.meta_lba)?
                    .into_iter()
                    .filter(|e| e.recorded)
                {
                    // Include other groups' retained spans and this group's
                    // temporary candidates in the existing UDF walk limit.
                    if total_spans + crack.len() >= MAX_WALK_RANGES {
                        return Err(budget_error(fs));
                    }
                    group.spans.push(Extent {
                        start_lba: extent.lba,
                        sector_count: u64::from(extent.len).div_ceil(2048) as u32,
                    });
                    total_spans += 1;
                }
                if total_spans + crack.len() + (group.spans.len() - start) > MAX_WALK_RANGES {
                    return Err(budget_error(fs));
                }
                crack.extend_from_slice(&group.spans[start..]);
            }
            if group.spans.is_empty() {
                continue;
            }
            if !crack.is_empty() {
                group.keys =
                    match super::crack_key_outcome(reader, &crack, CRACK_BATCH_SECTORS, halt) {
                        super::CrackOutcome::Cracked(state) => DecryptKeys::Css {
                            title_key: state.title_key,
                        },
                        // Clear so far: a scrambled sector read later is cracked from
                        // itself or refused (see `decrypt`), never keyed by another file.
                        super::CrackOutcome::Unencrypted => DecryptKeys::None,
                        super::CrackOutcome::Halted => return Err(Error::Halted),
                        super::CrackOutcome::ScrambledUncracked => {
                            return Err(Error::CssKeyMissing);
                        }
                        super::CrackOutcome::Unreadable(e) => return Err(e),
                    };
            }
            groups.push(group);
        }
        Ok(Self { groups })
    }

    /// Every VOB file's sectors as sorted, merged `(start_lba, sector_count)` ranges:
    /// the DVD whole-disc content map (menus included, unlike the title-only
    /// [`Disc::encrypted_content_ranges`](crate::Disc::encrypted_content_ranges)).
    pub(crate) fn content_ranges(&self) -> Vec<(u32, u32)> {
        let mut ranges: Vec<_> = self
            .groups
            .iter()
            .flat_map(|g| &g.spans)
            .filter(|e| e.sector_count > 0)
            .map(|e| (e.start_lba, e.sector_count))
            .collect();
        ranges.sort_by_key(|r| r.0);
        crate::udf::merge_ranges(&ranges)
    }

    // Preserve first-match ownership when damaged metadata has overlapping spans.
    fn group_at(&self, at: u64) -> Option<usize> {
        self.groups.iter().position(|g| {
            g.spans.iter().any(|e| {
                at >= u64::from(e.start_lba)
                    && at < u64::from(e.start_lba) + u64::from(e.sector_count)
            })
        })
    }

    // Owner of `at` and the first later LBA where it could change: the end of the
    // owner's covering span, or the start of any span a first-match owner would beat.
    fn owner_until(&self, at: u64) -> (Option<usize>, u64) {
        let owner = self.group_at(at);
        let mut limit = u64::MAX;
        for (i, g) in self.groups.iter().enumerate() {
            if owner.is_some_and(|o| i > o) {
                break;
            }
            for e in &g.spans {
                let (s, n) = (u64::from(e.start_lba), u64::from(e.sector_count));
                if s > at {
                    limit = limit.min(s);
                } else if owner == Some(i) && at < s + n {
                    limit = limit.min(s + n);
                }
            }
        }
        (owner, limit)
    }

    // Maximal runs `(start, end, owner)` of consecutive sectors with one owner.
    fn runs(&self, lba: u32, sectors: usize) -> Vec<(usize, usize, Option<usize>)> {
        let mut out = Vec::new();
        let mut start = 0;
        while start < sectors {
            let (owner, mut limit) = self.owner_until(u64::from(lba) + start as u64);
            let mut end;
            loop {
                end = (limit - u64::from(lba)).min(sectors as u64) as usize;
                if end >= sectors {
                    break;
                }
                let (o, l) = self.owner_until(u64::from(lba) + end as u64);
                if o != owner {
                    break;
                }
                limit = l;
            }
            out.push((start, end, owner));
            start = end;
        }
        out
    }

    pub(crate) fn decrypt(
        &mut self,
        lba: u32,
        buf: &mut [u8],
        fallback: &mut DecryptKeys,
    ) -> Result<()> {
        // Clear sectors belong to the same run: the CSS seam self-gates and
        // keeps its bounded retry cadence until the owner changes.
        for (start, end, owner) in self.runs(lba, buf.len() / 2048) {
            let keys = match owner {
                Some(index) => &mut self.groups[index].keys,
                None => &mut *fallback,
            };
            // A file whose crack saw only clear sectors: a scrambled pack now is keyed
            // from its own crib, else refused rather than passed through scrambled.
            if owner.is_some() && matches!(keys, DecryptKeys::None) {
                match super::crack_buffer(&buf[start * 2048..end * 2048]) {
                    None => {}
                    Some(Some(title_key)) => *keys = DecryptKeys::Css { title_key },
                    Some(None) => return Err(Error::CssKeyMissing),
                }
            }
            crate::decrypt::decrypt_sectors(&mut buf[start * 2048..end * 2048], keys, 0)?;
        }
        Ok(())
    }
}

// The UDF walk limit's refusal, as a metadata read failure.
fn budget_error(fs: &UdfFs) -> Error {
    Error::DiscRead {
        sector: u64::from(fs.metadata_start()),
        status: None,
        sense: None,
    }
}

// A VOB's key group, ordered titles, then menus, then `VIDEO_TS.VOB`; `None` for any
// other file.
fn vob_group_of(name: &str) -> Option<(u8, String)> {
    let up = name.to_ascii_uppercase();
    if !up.ends_with(".VOB") {
        return None;
    }
    if up == "VIDEO_TS.VOB" {
        return Some((2, up));
    }
    let vts = vts_group_of(&up)?;
    Some((u8::from(!is_title_vob(&up)), vts))
}

/// DVD VTS group key for a `VTS_xx_*` file name, else `None`. e.g.
/// `VTS_01_1.VOB` → `Some("VTS_01")`. Case-insensitive on the prefix.
pub(crate) fn vts_group_of(name: &str) -> Option<String> {
    let up = name.to_ascii_uppercase();
    let rest = up.strip_prefix("VTS_")?;
    // rest like "01_1.VOB" — take the 2-digit group number.
    let group = rest.split('_').next()?;
    if group.len() == 2 && group.bytes().all(|b| b.is_ascii_digit()) {
        Some(format!("VTS_{group}"))
    } else {
        None
    }
}

/// True for a title VOB (`VTS_xx_1.VOB`..`_9.VOB`) used in the initial
/// per-VTS key crack. Menu VOBs can also be scrambled, but are not initial
/// title-key crack candidates.
pub(crate) fn is_title_vob(name: &str) -> bool {
    let up = name.to_ascii_uppercase();
    if !up.ends_with(".VOB") {
        return false;
    }
    // VTS_xx_y.VOB → y is the part number; 0 = menu, 1..9 = title.
    let stem = up.trim_end_matches(".VOB");
    match stem.rsplit_once('_') {
        Some((_, part)) => part.len() == 1 && matches!(part.as_bytes()[0], b'1'..=b'9'),
        None => false,
    }
}

#[cfg(test)]
#[path = "disc_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "disc/budget_tests.rs"]
mod budget_tests;
