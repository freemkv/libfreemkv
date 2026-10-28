//! DVD title-key selection for the shared sector decryption stage.

use crate::decrypt::DecryptKeys;
use crate::disc::Extent;
use crate::error::{Error, Result};
use crate::halt::Halt;
use crate::sector::SectorSource;
use crate::udf::{DirEntry, MAX_WALK_RANGES, UdfFs};
use std::collections::{BTreeMap, btree_map::Entry};

const CRACK_BATCH_SECTORS: u16 = 64;

struct Group {
    spans: Vec<Extent>,
    keys: DecryptKeys,
}

pub(crate) struct DiscKeys {
    groups: Vec<Group>,
}

impl DiscKeys {
    pub(crate) fn new(
        fs: &UdfFs,
        reader: &mut dyn SectorSource,
        fallback: &DecryptKeys,
        halt: Option<&Halt>,
    ) -> Result<Self> {
        fn collect<'a>(dir: &'a DirEntry, out: &mut BTreeMap<String, Vec<&'a DirEntry>>) {
            for entry in &dir.entries {
                if entry.is_dir {
                    collect(entry, out);
                } else if entry.name.to_ascii_uppercase().ends_with(".VOB")
                    && let Some(vts) = vts_group_of(&entry.name)
                {
                    out.entry(vts).or_default().push(entry);
                }
            }
        }
        let mut files = BTreeMap::new();
        collect(&fs.root, &mut files);
        let mut groups = Vec::new();
        let mut total_spans = 0;
        for (_, mut files) in files {
            files.sort_by_key(|f| f.name.to_ascii_uppercase());
            let mut group = Group {
                spans: Vec::new(),
                keys: fallback.clone(),
            };
            let mut crack = Vec::new();
            let mut seen = BTreeMap::new();
            for entry in files {
                let (range, title_seen) = match seen.entry(entry.meta_lba) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(seen_entry) => {
                        let start = group.spans.len();
                        for extent in fs
                            .extents_abs_at(reader, entry.meta_lba)?
                            .into_iter()
                            .filter(|e| e.recorded)
                        {
                            // Include other groups' retained spans and this group's
                            // temporary candidates in the existing UDF walk limit.
                            if total_spans + crack.len() >= MAX_WALK_RANGES {
                                return Err(Error::DiscRead {
                                    sector: u64::from(fs.metadata_start()),
                                    status: None,
                                    sense: None,
                                });
                            }
                            group.spans.push(Extent {
                                start_lba: extent.lba,
                                sector_count: u64::from(extent.len).div_ceil(2048) as u32,
                            });
                            total_spans += 1;
                        }
                        seen_entry.insert((start..group.spans.len(), false))
                    }
                };
                // A menu alias may precede a title alias to the same ICB. Add
                // candidates at its first title alias, preserving crack order.
                if is_title_vob(&entry.name) && !*title_seen {
                    if total_spans + crack.len() + range.len() > MAX_WALK_RANGES {
                        return Err(Error::DiscRead {
                            sector: u64::from(fs.metadata_start()),
                            status: None,
                            sense: None,
                        });
                    }
                    crack.extend_from_slice(&group.spans[range.clone()]);
                    *title_seen = true;
                }
            }
            if !crack.is_empty() {
                group.keys =
                    match super::crack_key_outcome(reader, &crack, CRACK_BATCH_SECTORS, halt) {
                        super::CrackOutcome::Cracked(state) => DecryptKeys::Css {
                            title_key: state.title_key,
                        },
                        super::CrackOutcome::Unencrypted => fallback.clone(),
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
            crate::decrypt::decrypt_sectors(&mut buf[start * 2048..end * 2048], keys, 0)?;
        }
        Ok(())
    }
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
