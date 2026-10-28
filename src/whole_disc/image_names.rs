//! The decrypted-name rename for a finished ISO. Rescue passes copy the disc's own names;
//! the caller renames once at the end from [`plan_image_names`], so sectors written by
//! different passes never mix renamed and original metadata.

use super::DECRYPTED_NAME_RULES;
use crate::consts::SECTOR_BYTES;
use crate::error::{Error, Result};
use crate::sector::{FileSectorSource, SectorSource};
use std::path::Path;

/// What a decrypted copy did with `AACS/Unit_Key_RO.inf`.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecryptedNames {
    /// The copy lists `Unit_Key_RO.bak` and no `.inf`. `mirror_sectors_restored` counts
    /// metadata mirror sectors rebuilt from the main copy.
    Renamed { mirror_sectors_restored: u32 },
    /// The copy keeps `Unit_Key_RO.inf`; its content is still decrypted.
    Kept { cause: NamesKept },
}

/// Why a copy keeps `Unit_Key_RO.inf`.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamesKept {
    /// Its directory could not be read; a run that reads it can rename it.
    Damaged,
    /// The disc already has a `Unit_Key_RO.bak`.
    Collision,
    /// Metadata the rename does not handle, or a write that did not take.
    Unexpected,
}

impl NamesKept {
    /// The cause behind a [`plan_image_names`] error over `unreadable` `(start, count)` ranges.
    pub fn from_plan_error(e: &Error, unreadable: &[(u32, u32)]) -> Self {
        match e {
            Error::DirNameCollision { .. } => Self::Collision,
            Error::DiscRead { sector, .. } if masked(unreadable, *sector) => Self::Damaged,
            _ => Self::Unexpected,
        }
    }
}

/// The final bytes of every sector an ISO rename touches.
#[derive(Debug)]
pub struct ImageNames {
    /// `(lba, bytes)`, ascending: whole sectors to write.
    pub sectors: Vec<(u32, Box<[u8; SECTOR_BYTES]>)>,
    /// Metadata mirror sectors among `sectors` rebuilt from the main copy.
    pub restored: Vec<u32>,
}

/// A source that fails every sector in its `(start, count)` ranges, so an unrecovered
/// sector in an image reads as unreadable instead of as zeros.
pub struct Masked<S> {
    inner: S,
    unreadable: Vec<(u32, u32)>,
}

impl<S: SectorSource> Masked<S> {
    pub fn new(inner: S, unreadable: &[(u32, u32)]) -> Self {
        Self {
            inner,
            unreadable: super::merge_ranges(unreadable.to_vec()),
        }
    }
}

fn masked(unreadable: &[(u32, u32)], lba: u64) -> bool {
    unreadable
        .iter()
        .any(|&(s, n)| lba >= s as u64 && lba < s as u64 + n as u64)
}

impl<S: SectorSource> SectorSource for Masked<S> {
    fn capacity_sectors(&self) -> u32 {
        self.inner.capacity_sectors()
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        let end = lba as u64 + count as u64;
        if let Some(&(s, _)) = self
            .unreadable
            .iter()
            .find(|&&(s, n)| (s as u64) < end && s as u64 + n as u64 > lba as u64)
        {
            return Err(Error::DiscRead {
                sector: s.max(lba) as u64,
                status: None,
                sense: None,
            });
        }
        self.inner.read_sectors(lba, count, buf, recovery)
    }

    fn unmapped_stream_files(&self) -> &[crate::sector::bus_removal::UnmappedStreamFile] {
        self.inner.unmapped_stream_files()
    }

    fn random_access(&self) -> bool {
        self.inner.random_access()
    }
}

/// Plan the decrypted-name rename of the ISO at `path`, reading `unreadable` `(start, count)`
/// ranges as errors. `None` when there is nothing to rename. Never writes.
pub fn plan_image_names(path: &Path, unreadable: &[(u32, u32)]) -> Result<Option<ImageNames>> {
    let mut src = Masked::new(FileSectorSource::open(path)?, unreadable);
    let plan = match crate::udf::read_name_patches(&mut src, DECRYPTED_NAME_RULES)? {
        Some(plan) if !plan.is_empty() => plan,
        _ => return Ok(None),
    };
    let restored = plan.fills.iter().map(|(lba, _)| *lba).collect();
    let mut sectors = plan.fills;
    for patch in &plan.patches {
        let i = match sectors.iter().position(|(lba, _)| *lba == patch.lba) {
            Some(i) => i,
            None => {
                let mut bytes = Box::new([0u8; SECTOR_BYTES]);
                if src.read_sectors(patch.lba, 1, &mut bytes[..], true)? != SECTOR_BYTES {
                    return Err(Error::DiscRead {
                        sector: patch.lba as u64,
                        status: None,
                        sense: None,
                    });
                }
                sectors.push((patch.lba, bytes));
                sectors.len() - 1
            }
        };
        sectors[i].1[patch.offset] = patch.value;
    }
    sectors.sort_by_key(|(lba, _)| *lba);
    Ok(Some(ImageNames { sectors, restored }))
}

/// Whether the ISO at `path` lists `Unit_Key_RO.bak` or `.inf`; `None` when it has no
/// `/AACS` names. An unmountable image is `Kept { Damaged }` only for an `aacs` disc.
/// `Renamed` needs a readable metadata mirror to agree with main, and reports
/// `mirror_sectors_restored: 0`; the writer knows its own count.
pub fn check_image_names(
    path: &Path,
    unreadable: &[(u32, u32)],
    aacs: bool,
) -> Option<DecryptedNames> {
    let kept = |cause| Some(DecryptedNames::Kept { cause });
    let Ok(file) = FileSectorSource::open(path) else {
        return if aacs {
            kept(NamesKept::Unexpected)
        } else {
            None
        };
    };
    let mut src = Masked::new(file, unreadable);
    let Ok(fs) = crate::udf::read_filesystem_inner(&mut src, false) else {
        return if aacs { kept(NamesKept::Damaged) } else { None };
    };
    let rule = DECRYPTED_NAME_RULES[0];
    let entries = &fs.find_dir(rule.dir)?.entries;
    let listed = |name: &str| entries.iter().any(|e| e.name.eq_ignore_ascii_case(name));
    match (listed(rule.from), listed(rule.to)) {
        // Re-plan to learn why the original name stayed.
        (true, _) => match fs.name_plan(&mut src, DECRYPTED_NAME_RULES) {
            Err(e) => kept(NamesKept::from_plan_error(&e, unreadable)),
            Ok(_) => kept(NamesKept::Unexpected),
        },
        // Renamed on main: the reverse plan reads the mirror, and refuses two readable
        // copies that differ (a mirror still listing `.inf`).
        (false, true) => {
            let back = crate::udf::NameRule {
                dir: rule.dir,
                from: rule.to,
                to: rule.from,
            };
            match fs.name_plan(&mut src, &[back]) {
                Ok(_) => Some(DecryptedNames::Renamed {
                    mirror_sectors_restored: 0,
                }),
                Err(e) => kept(NamesKept::from_plan_error(&e, unreadable)),
            }
        }
        (false, false) => None,
    }
}

#[cfg(test)]
#[path = "image_names_tests.rs"]
mod tests;
