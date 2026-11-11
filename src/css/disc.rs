//! DVD title-key selection for the shared sector decryption stage.

use super::verify::{Verdict, verify_key};
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
    // The group's first file, as logged when its key is borrowed or refused.
    name: String,
    // VTS number (0 for `VIDEO_TS.VOB`): candidate keys are tried nearest VTS first.
    vts: u8,
    // The group whose key is the obvious one to test first: a menu's title set.
    relative: Option<usize>,
}

pub(crate) struct DiscKeys {
    groups: Vec<Group>,
}

impl DiscKeys {
    /// One key group per VOB file set: a VTS's title VOBs (`VTS_xx_1..9`) share one, its menu
    /// VOB (`VTS_xx_0`) and `VIDEO_TS.VOB` each have their own (CSS keys each file
    /// separately). Title groups come first, so a span two files claim belongs to the title,
    /// and title keys are known before the menus are keyed. Each group takes, in order:
    /// 1. its obvious key (a menu's title-set key), when that key verifies on its packs;
    /// 2. its own cracked key;
    /// 3. any other recovered key that verifies, nearest VTS first.
    ///
    /// Scrambled sectors none of these opens refuse the copy (`WholeDiscKeyMissing`).
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
        let mut groups: Vec<Group> = Vec::new();
        let mut titles = BTreeMap::new();
        let mut total_spans = 0;
        // ICBs already owned: an alias (any name, any group) adds no second owner.
        let mut seen = BTreeSet::new();
        for ((kind, set), mut files) in files {
            files.sort_by_key(|f| f.name.to_ascii_uppercase());
            let mut group = Group {
                spans: Vec::new(),
                keys: DecryptKeys::None,
                name: files
                    .first()
                    .map_or_else(String::new, |f| f.name.to_ascii_uppercase()),
                vts: set
                    .strip_prefix("VTS_")
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(0),
                relative: if kind == 1 {
                    titles.get(&set).copied()
                } else {
                    None
                },
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
                group.keys = key_group(&groups, &group, reader, &crack, halt)?;
            }
            if kind == 0 {
                titles.insert(set, groups.len());
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
            // A file whose crack saw only clear sectors: a scrambled pack now is keyed from
            // its own crib, else by a recovered key that verifies on the packs in hand, else
            // refused rather than passed through scrambled.
            if let Some(index) = owner
                && matches!(self.groups[index].keys, DecryptKeys::None)
            {
                let run = &buf[start * 2048..end * 2048];
                match super::crack_buffer(run) {
                    None => {}
                    Some(Some(title_key)) => {
                        self.groups[index].keys = DecryptKeys::Css { title_key };
                    }
                    Some(None) => {
                        let group = &self.groups[index];
                        let found = candidates(&self.groups, group, &[])
                            .into_iter()
                            .find(|k| verify_key(k, run.chunks(2048)) == Verdict::Pass);
                        let Some(title_key) = found else {
                            return Err(refuse(group));
                        };
                        tracing::info!(
                            target: "freemkv::css",
                            file = %group.name,
                            "css: scrambled packs keyed with a verified key from another file"
                        );
                        self.groups[index].keys = DecryptKeys::Css { title_key };
                    }
                }
            }
            let keys = match owner {
                Some(index) => &mut self.groups[index].keys,
                None => &mut *fallback,
            };
            crate::decrypt::decrypt_sectors(&mut buf[start * 2048..end * 2048], keys, 0)?;
        }
        Ok(())
    }
}

// Sample batches spread across a group's spans when verifying a borrowed key.
const SAMPLE_BATCHES: u32 = 8;

// A group's key: its obvious key if it verifies, its own crack, else another recovered key
// that verifies (see `DiscKeys::new`).
fn key_group(
    groups: &[Group],
    group: &Group,
    reader: &mut dyn SectorSource,
    crack: &[Extent],
    halt: Option<&Halt>,
) -> Result<DecryptKeys> {
    let obvious = group
        .relative
        .and_then(|r| css_key(&groups[r].keys))
        .into_iter()
        .collect::<Vec<_>>();
    let mut sample = None;
    if let Some(key) = obvious.first() {
        match sample_scrambled(reader, crack, halt) {
            Ok(s) if verify_key(key, s.chunks(2048)) == Verdict::Pass => {
                tracing::info!(
                    target: "freemkv::css",
                    file = %group.name,
                    from = %groups[group.relative.unwrap_or_default()].name,
                    "css: keyed with its title set's key, verified on its own sectors"
                );
                return Ok(DecryptKeys::Css { title_key: *key });
            }
            Ok(s) => sample = Some(s),
            Err(Error::Halted) => return Err(Error::Halted),
            // An unreadable sample proves no key: the crack below decides.
            Err(_) => {}
        }
    }
    match super::crack_key_outcome(reader, crack, CRACK_BATCH_SECTORS, halt) {
        super::CrackOutcome::Cracked(state) => Ok(DecryptKeys::Css {
            title_key: state.title_key,
        }),
        // Clear so far: a scrambled sector read later is keyed then (see `decrypt`).
        super::CrackOutcome::Unencrypted => Ok(DecryptKeys::None),
        super::CrackOutcome::Halted => Err(Error::Halted),
        // Too little read to prove a key: degrade like a clear verdict; damage stays a
        // counted hole, and `decrypt` still keys or refuses any scrambled pack it reads.
        super::CrackOutcome::Unreadable(e) => {
            tracing::warn!(
                target: "freemkv::css",
                file = %group.name,
                code = e.code(),
                "css: key sample unreadable; scrambled packs are keyed or refused when read"
            );
            Ok(DecryptKeys::None)
        }
        super::CrackOutcome::ScrambledUncracked => {
            let sample = match sample {
                Some(s) => s,
                None => sample_scrambled(reader, crack, halt)?,
            };
            for key in candidates(groups, group, &obvious) {
                if verify_key(&key, sample.chunks(2048)) == Verdict::Pass {
                    tracing::info!(
                        target: "freemkv::css",
                        file = %group.name,
                        "css: no key cracked; keyed with another file's key, verified on its own sectors"
                    );
                    return Ok(DecryptKeys::Css { title_key: key });
                }
            }
            Err(refuse(group))
        }
    }
}

fn css_key(keys: &DecryptKeys) -> Option<[u8; 5]> {
    match keys {
        DecryptKeys::Css { title_key } => Some(*title_key),
        _ => None,
    }
}

// Keys to test on `group`'s packs: its relative's, then every other group's nearest VTS
// first, once each, leaving out `tested`.
fn candidates(groups: &[Group], group: &Group, tested: &[[u8; 5]]) -> Vec<[u8; 5]> {
    let mut order: Vec<usize> = (0..groups.len()).collect();
    order.sort_by_key(|&i| {
        (
            Some(i) != group.relative,
            groups[i].vts.abs_diff(group.vts),
            groups[i].vts,
            i,
        )
    });
    let mut out: Vec<[u8; 5]> = Vec::new();
    for key in order.into_iter().filter_map(|i| css_key(&groups[i].keys)) {
        if !tested.contains(&key) && !out.contains(&key) {
            out.push(key);
        }
    }
    out
}

// The refusal for a file's scrambled sectors no recovered key opens.
fn refuse(group: &Group) -> Error {
    tracing::warn!(
        target: "freemkv::css",
        file = %group.name,
        "css: scrambled sectors yield no key and no recovered key verifies on them"
    );
    Error::WholeDiscKeyMissing
}

// Scrambled packs from every sector of a small group, span by span, else from
// `SAMPLE_BATCHES` batches spread evenly across `spans`. A Stop halts; a read error is
// propagated only when no batch could be read, as the crack does; a CSS-locked one is none.
fn sample_scrambled(
    reader: &mut dyn SectorSource,
    spans: &[Extent],
    halt: Option<&Halt>,
) -> Result<Vec<u8>> {
    let batch = u32::from(CRACK_BATCH_SECTORS);
    let total: u64 = spans.iter().map(|e| u64::from(e.sector_count)).sum();
    // `(lba, sectors)` reads, none crossing out of its span.
    let mut reads = Vec::new();
    if total <= u64::from(batch * SAMPLE_BATCHES) {
        for span in spans {
            for off in (0..span.sector_count).step_by(batch as usize) {
                let n = (span.sector_count - off).min(batch);
                reads.push((span.start_lba.saturating_add(off), n));
            }
        }
    } else {
        let room = total - u64::from(batch);
        for k in 0..u64::from(SAMPLE_BATCHES) {
            // The span holding offset `off`, and how far into it.
            let mut off = k * room / u64::from(SAMPLE_BATCHES - 1);
            let Some(span) = spans.iter().find(|e| {
                let hit = off < u64::from(e.sector_count);
                if !hit {
                    off -= u64::from(e.sector_count);
                }
                hit
            }) else {
                continue;
            };
            let n = (u64::from(span.sector_count) - off).min(u64::from(batch)) as u32;
            reads.push((span.start_lba.saturating_add(off as u32), n));
        }
    }
    let mut out = Vec::new();
    let mut buf = vec![0u8; batch as usize * 2048];
    let (mut read, mut first_err) = (false, None);
    for (lba, n) in reads {
        if halt.is_some_and(|h| h.is_cancelled()) {
            return Err(Error::Halted);
        }
        match reader.read_sectors(lba, n as u16, &mut buf[..n as usize * 2048], true) {
            Ok(got) => {
                read = true;
                let usable = (got / 2048).min(n as usize);
                for s in buf[..usable * 2048].chunks(2048) {
                    if super::scrambled_at(s).is_some() {
                        out.extend_from_slice(s);
                    }
                }
            }
            Err(Error::Halted) => return Err(Error::Halted),
            Err(e) if e.scsi_sense().is_some_and(|s| s.is_css_locked()) => {}
            Err(e) => {
                first_err.get_or_insert(e);
            }
        }
    }
    match first_err {
        Some(e) if !read => Err(e),
        _ => Ok(out),
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
