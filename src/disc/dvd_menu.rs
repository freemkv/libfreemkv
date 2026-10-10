//! Bounded static DVD menu evidence. Unsupported navigation remains unknown.
//!
//! Supported: unconditional First-Play to a single-language, single-PGC VMGM title
//! menu; one ordinary menu cell; stable connected PCI buttons with full JumpTT
//! targets; one VTS; complete single-PGC titles without VM actions. Every reachable
//! unique standalone title must exactly partition one reachable play-all title.
//! This proves an authored full-title program partition, NOT TV semantics: callers
//! must request Episodes from metadata/user intent, never infer TV from this alone.
//! VTS menus, menu chains, conditional/register commands, chapter jumps, angles,
//! multi-PGC titles, changing highlights and ambiguous cuts all require review.

use super::{DiscTitle, EpisodeEvidence};
use crate::{
    error::{Error, Result},
    halt::Halt,
    sector::SectorSource,
    udf::UdfFs,
};

const FILE_LIMIT: usize = 8 * 1024 * 1024;
type Cells = Vec<(u32, u32)>;
type Commands = Vec<[u8; 8]>;

#[path = "dvd_launch/mod.rs"]
mod launch;

fn u16_at(b: &[u8], at: usize) -> Option<usize> {
    Some(u16::from_be_bytes(b.get(at..at.checked_add(2)?)?.try_into().ok()?) as usize)
}
fn u32_at(b: &[u8], at: usize) -> Option<usize> {
    Some(u32::from_be_bytes(b.get(at..at.checked_add(4)?)?.try_into().ok()?) as usize)
}
fn need<T>(value: Option<T>) -> Result<T> {
    value.ok_or(Error::IfoParse)
}
fn table(b: &[u8], at: usize) -> Option<&[u8]> {
    if at == 0 {
        return None;
    }
    let tail = b.get(at..)?;
    let len = u32_at(tail, 4)?.checked_add(1)?;
    if len < 8 {
        return None;
    }
    tail.get(..len)
}
fn sector_table(b: &[u8], pointer: usize) -> Option<&[u8]> {
    table(b, u32_at(b, pointer)?.checked_mul(2048)?)
}

// Reject block/parental tables and overlapping or incomplete PGC records.
fn pgcs(table: &[u8]) -> Option<Vec<&[u8]>> {
    let count = u16_at(table, 0)?;
    if !(1..=99).contains(&count) {
        return None;
    }
    let header = 8 + count * 8;
    let mut offsets = Vec::new();
    for i in 0..count {
        let entry = table.get(8 + i * 8..16 + i * 8)?;
        if entry[1..4] != [0; 3] {
            return None;
        }
        let at = u32_at(entry, 4)?;
        if at < header || offsets.last().is_some_and(|&prev| prev >= at) {
            return None;
        }
        offsets.push(at);
    }
    offsets.push(table.len());
    offsets
        .windows(2)
        .map(|pair| {
            let pgc = table.get(pair[0]..pair[1])?;
            (pgc.len() >= 236).then_some(pgc)
        })
        .collect()
}

fn commands(pgc: &[u8]) -> Option<(Commands, Commands)> {
    let at = u16_at(pgc, 0xe4)?;
    if at == 0 {
        return Some((Vec::new(), Vec::new()));
    }
    if at < 236 {
        return None;
    }
    let pre = u16_at(pgc, at)?;
    let post = u16_at(pgc, at + 2)?;
    let cell = u16_at(pgc, at + 4)?;
    if pre + post > 128 || cell != 0 || u16_at(pgc, at + 6)? + 1 != 8 + (pre + post) * 8 {
        return None;
    }
    let bytes = pgc.get(at + 8..at + 8 + (pre + post) * 8)?;
    let cmds = bytes.as_chunks::<8>().0;
    Some((cmds[..pre].to_vec(), cmds[pre..].to_vec()))
}

fn cells(pgc: &[u8]) -> Option<Cells> {
    let programs = *pgc.get(2)? as usize;
    let count = *pgc.get(3)? as usize;
    // No random/shuffle playback, chained PGC, restricted operations or cell commands.
    if programs == 0
        || count == 0
        || programs > count
        || pgc.get(8..12)? != [0; 4]
        || pgc.get(0x9c..0xa2)? != [0; 6]
        || *pgc.get(0xa3)? != 0
    {
        return None;
    }
    let map_at = u16_at(pgc, 0xe6)?;
    let cell_at = u16_at(pgc, 0xe8)?;
    let pos_at = u16_at(pgc, 0xea)?;
    if map_at < 236 || cell_at < map_at + programs || pos_at < cell_at + count * 24 {
        return None;
    }
    let map = pgc.get(map_at..map_at + programs)?;
    if map[0] != 1
        || map.iter().any(|&n| n == 0 || n as usize > count)
        || map.windows(2).any(|p| p[0] >= p[1])
    {
        return None;
    }
    pgc.get(pos_at..pos_at + count * 4)?;
    let mut out = Vec::new();
    for c in pgc.get(cell_at..cell_at + count * 24)?.as_chunks::<24>().0 {
        // Ordinary cells only. Angle/interleaving, stills and cell commands need VM work.
        if c[0] & 0xf4 != 0 || c[1] != 0 || c[2] != 0 || c[3] != 0 {
            return None;
        }
        let first = u32_at(c, 8)? as u32;
        let last = u32_at(c, 20)? as u32;
        if first > last {
            return None;
        }
        out.push((first, last));
    }
    Some(out)
}

// Deliberately recognize exact unconditional encodings, not the forgiving VM decoder's
// unknown-opcode fallback. This slice starts directly in a single VMGM title menu.
fn menu_pgc(vmg: &[u8]) -> Option<&[u8]> {
    let fp = u32_at(vmg, 0x84)?;
    if fp < 0x100 {
        return None;
    }
    let (pre, post) = commands(vmg.get(fp..)?)?;
    if pre != [[0x30, 6, 0, 0, 0, 0x42, 0, 0]] || !post.is_empty() {
        return None;
    }
    let lu = sector_table(vmg, 0xc8)?;
    if u16_at(lu, 0)? != 1 || *lu.get(10)? != 0 {
        return None;
    }
    let pgcit = table(lu, u32_at(lu, 12)?)?;
    if u16_at(pgcit, 0)? != 1 || *pgcit.get(8)? != 0x82 {
        return None;
    }
    let menu = pgcs(pgcit)?.first().copied()?;
    let (pre, post) = commands(menu)?;
    // A repeat of this same menu is safe; every other automatic action needs review.
    if !pre.is_empty() || !(post.is_empty() || post == [[0x20, 4, 0, 0, 0, 0, 0, 1]]) {
        return None;
    }
    let ranges = cells(menu)?;
    (ranges.len() == 1).then_some(menu)
}

// A full fixed DVD navigation pack, not a byte-pattern search through video payload.
fn buttons(sector: &[u8]) -> Option<Option<Vec<u8>>> {
    if sector.len() != 2048 || sector[..4] != [0, 0, 1, 0xba] || sector[4] & 0xc0 != 0x40 {
        return None;
    }
    let next = 14 + (sector[13] & 7) as usize;
    if sector.get(next..next + 4)? != [0, 0, 1, 0xbb] {
        return Some(None);
    }
    if next != 14
        || u16_at(sector, 18)? != 18
        || sector[38..42] != [0, 0, 1, 0xbf]
        || u16_at(sector, 42)? != 0x3d4
        || sector[44] != 0
        || sector[1024..1028] != [0, 0, 1, 0xbf]
        || sector[1030] != 1
    {
        return None;
    }
    let pci = &sector[45..1024];
    // PCI prohibited-operations button-select bit. Other restrictions don't establish reachability.
    if u32_at(pci, 8)? & (1 << 17) != 0 {
        return None;
    }
    let hli = &pci[96..];
    let status = u16_at(hli, 0)?;
    if status == 0 {
        return Some(None);
    }
    if status != 1 || hli[14] & 0x30 != 0x10 || hli[16] != 0 || hli[20] != 0 || hli[21] != 0 {
        return None;
    }
    let start = u32_at(hli, 2)?;
    if u32_at(hli, 6)? <= start
        || u32_at(hli, 10)? <= start
        || start > u32_at(pci, 16)?
        || u32_at(hli, 6)? < u32_at(pci, 12)?
    {
        return None;
    }
    let count = hli[17] as usize;
    if !(1..=36).contains(&count) || hli[18] as usize > count {
        return None;
    }
    let mut targets = Vec::new();
    let mut edges = Vec::new();
    for i in 0..count {
        let button = hli.get(46 + i * 18..64 + i * 18)?;
        if button[3] & 0xc0 != 0 {
            return None;
        } // auto-action buttons
        let x0 = (usize::from(button[0] & 0x3f) << 4) | usize::from(button[1] >> 4);
        let x1 = (usize::from(button[1] & 3) << 8) | usize::from(button[2]);
        let y0 = (usize::from(button[3] & 0x3f) << 4) | usize::from(button[4] >> 4);
        let y1 = (usize::from(button[4] & 3) << 8) | usize::from(button[5]);
        if x0 >= x1 || y0 >= y1 {
            return None;
        }
        let neighbors: Vec<_> = button[6..10].iter().map(|n| usize::from(*n)).collect();
        if neighbors.iter().any(|&n| n == 0 || n > count) {
            return None;
        }
        edges.push(neighbors);
        let cmd = &button[10..];
        if cmd[..5] != [0x30, 2, 0, 0, 0] || cmd[5] == 0 || cmd[6..] != [0, 0] {
            return None;
        }
        targets.push(cmd[5]);
    }
    // Do not call an orphaned button reachable. With no forced selection, require
    // every possible starting highlight to reach every button via authored arrows.
    for start in 0..count {
        let mut seen = vec![false; count];
        let mut pending = vec![start];
        while let Some(i) = pending.pop() {
            if seen[i] {
                continue;
            }
            seen[i] = true;
            pending.extend(edges[i].iter().map(|n| n - 1).filter(|&n| !seen[n]));
        }
        if seen.iter().any(|s| !*s) {
            return None;
        }
    }
    Some(Some(targets))
}

struct CheckedReader<'a> {
    inner: &'a mut dyn SectorSource,
    halt: Option<&'a Halt>,
}
impl SectorSource for CheckedReader<'_> {
    fn capacity_sectors(&self) -> u32 {
        self.inner.capacity_sectors()
    }
    fn random_access(&self) -> bool {
        self.inner.random_access()
    }
    fn unmapped_stream_files(&self) -> &[crate::sector::bus_removal::UnmappedStreamFile] {
        self.inner.unmapped_stream_files()
    }
    fn set_speed(&mut self, kbs: u16) {
        self.inner.set_speed(kbs);
    }
    fn set_unit_base(&mut self, lba: u32) {
        self.inner.set_unit_base(lba);
    }
    fn read_sectors_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> Result<usize> {
        if self.halt.is_some_and(Halt::is_cancelled) {
            return Err(Error::Halted);
        }
        let n = self
            .inner
            .read_sectors_fua(lba, count, buf, recovery, fua)?;
        if self.halt.is_some_and(Halt::is_cancelled) {
            return Err(Error::Halted);
        }
        if n != usize::from(count) * 2048 {
            return Err(Error::IfoParse);
        }
        Ok(n)
    }
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        if self.halt.is_some_and(Halt::is_cancelled) {
            return Err(Error::Halted);
        }
        let n = self.inner.read_sectors(lba, count, buf, recovery)?;
        if self.halt.is_some_and(Halt::is_cancelled) {
            return Err(Error::Halted);
        }
        if n != usize::from(count) * 2048 {
            return Err(Error::IfoParse);
        }
        Ok(n)
    }
}

fn read_bounded(reader: &mut dyn SectorSource, fs: &UdfFs, name: &str) -> Result<Vec<u8>> {
    let entry = need(fs.find_dir("/VIDEO_TS").and_then(|dir| {
        dir.entries
            .iter()
            .find(|e| !e.is_dir && e.name.eq_ignore_ascii_case(name))
    }))?;
    if entry.size == 0 || entry.size > FILE_LIMIT as u64 {
        return Err(Error::IfoParse);
    }
    let bytes = fs.read_file_prefix(reader, &format!("/VIDEO_TS/{name}"), FILE_LIMIT)?;
    if bytes.len() as u64 != entry.size {
        return Err(Error::IfoParse);
    }
    Ok(bytes)
}

fn title_cells(vts: &[u8], ttn: u8) -> Option<Cells> {
    let ptt = sector_table(vts, 0xc8)?;
    let count = u16_at(ptt, 0)?;
    let n = usize::from(ttn).checked_sub(1)?;
    if count > 99 || n >= count {
        return None;
    }
    let start = u32_at(ptt, 8 + n * 4)?;
    let end = if n + 1 < count {
        u32_at(ptt, 12 + n * 4)?
    } else {
        ptt.len()
    };
    if start < 8 + count * 4 || end <= start || (end - start) % 4 != 0 {
        return None;
    }
    let parts = ptt.get(start..end)?;
    let pgcn = u16_at(parts, 0)?.checked_sub(1)?;
    // A title must start at program one and remain in exactly one complete PGC.
    if u16_at(parts, 2)? != 1 {
        return None;
    }
    let pgcit = sector_table(vts, 0xcc)?;
    let programs = pgcs(pgcit)?;
    let pgc = *programs.get(pgcn)?;
    let mut prev = 0;
    for p in parts.as_chunks::<4>().0 {
        let pgn = u16_at(p, 2)?;
        if u16_at(p, 0)? != pgcn + 1 || pgn <= prev || pgn > *pgc.get(2)? as usize {
            return None;
        }
        prev = pgn;
    }
    let (pre, post) = commands(pgc)?;
    if !pre.is_empty() || !post.is_empty() {
        return None;
    }
    cells(pgc)
}

// Exact ordered cell vectors only. Every other reachable unique standalone program must
// occur once in the play-all partition. Multiple decompositions or play-alls are review.
fn partition(groups: &[Cells]) -> Option<Vec<usize>> {
    let mut answer = None;
    for (whole, sequence) in groups.iter().enumerate() {
        let mut at = 0;
        let mut order = Vec::new();
        while at < sequence.len() {
            let matches: Vec<_> = groups
                .iter()
                .enumerate()
                .filter(|(i, c)| *i != whole && !order.contains(i) && sequence[at..].starts_with(c))
                .collect();
            // Prefix ambiguity is deliberately rejected instead of searching for a preferred cut.
            if matches.len() != 1 {
                break;
            }
            let (i, c) = matches[0];
            order.push(i);
            at += c.len();
        }
        if at == sequence.len() && order.len() >= 2 && order.len() + 1 == groups.len() {
            if answer.is_some() {
                return None;
            }
            answer = Some(order);
        }
    }
    answer
}

fn produce(
    reader: &mut dyn SectorSource,
    fs: &UdfFs,
    vmg: &[u8],
    addresses: &[(u8, u8)],
    titles: &mut [DiscTitle],
) -> Result<()> {
    if !reader.random_access() {
        return Err(Error::IfoParse);
    }
    let menu = need(menu_pgc(vmg))?;
    let range = need(cells(menu))?[0];
    let vob = read_bounded(reader, fs, "VIDEO_TS.VOB")?;
    let first = need((range.0 as usize).checked_mul(2048))?;
    let end = need(
        (range.1 as usize)
            .checked_add(1)
            .and_then(|n| n.checked_mul(2048)),
    )?;
    let packs = need(vob.get(first..end))?;
    let mut targets = None;
    for (offset, pack) in packs.as_chunks::<2048>().0.iter().enumerate() {
        if let Some(found) = need(buttons(pack))? {
            if need(u32_at(pack, 45))? != range.0 as usize + offset {
                return Err(Error::IfoParse);
            }
            if targets.as_ref().is_some_and(|old| old != &found) {
                return Err(Error::IfoParse);
            }
            targets = Some(found);
        }
    }
    let targets = need(targets)?;
    let tt = need(sector_table(vmg, 0xc4))?;
    let count = need(u16_at(tt, 0))?;
    if count != titles.len() || addresses.len() != count || count > 99 || tt.len() != 8 + count * 12
    {
        return Err(Error::IfoParse);
    }
    let entries = tt[8..].as_chunks::<12>().0;
    let mut declared = std::collections::HashSet::new();
    for entry in entries {
        let address = (entry[6], entry[7]);
        if !declared.insert(address) || addresses.iter().filter(|a| **a == address).count() != 1 {
            return Err(Error::IfoParse);
        }
    }
    let mut reachable = Vec::new();
    for target in targets {
        let entry = need(entries.get(usize::from(target) - 1))?;
        let address = (entry[6], entry[7]);
        let matches: Vec<_> = addresses
            .iter()
            .enumerate()
            .filter(|(_, a)| **a == address)
            .collect();
        if matches.len() != 1 {
            return Err(Error::IfoParse);
        }
        let i = matches[0].0;
        if !reachable.contains(&i) {
            reachable.push(i);
        }
    }
    for &i in &reachable {
        titles[i].selection_evidence.dvd_menu_reachable = true;
    }
    // This first slice deliberately excludes cross-VTS/title-chain navigation.
    let vtsn = need(addresses.first())?.0;
    if vtsn == 0 || addresses.iter().any(|a| a.0 != vtsn) {
        return Err(Error::IfoParse);
    }
    let name = format!("VTS_{vtsn:02}_0.IFO");
    let vts = read_bounded(reader, fs, &name)?;
    if vts.get(..12) != Some(b"DVDVIDEO-VTS") {
        return Err(Error::IfoParse);
    }
    let base = fs
        .file_start_lba(reader, &format!("/VIDEO_TS/{name}"))?
        .checked_add(need(u32_at(&vts, 0xc4))? as u32)
        .ok_or(Error::IfoParse)?;
    let mut groups: Vec<Cells> = Vec::new();
    let mut title_group = Vec::new();
    for &i in &reachable {
        let address = addresses[i];
        let entry = need(entries.iter().find(|e| (e[6], e[7]) == address))?;
        if entry[0] & 0x7f != 0 || entry[1] != 1 || entry[4..6] != [0, 0] {
            return Err(Error::IfoParse);
        }
        let raw = need(title_cells(&vts, address.1))?;
        let absolute: Cells = need(
            raw.into_iter()
                .map(|(a, b)| Some((base.checked_add(a)?, base.checked_add(b)?)))
                .collect(),
        )?;
        let scanned: Option<Cells> = titles[i]
            .extents
            .iter()
            .map(|e| {
                Some((
                    e.start_lba,
                    e.start_lba.checked_add(e.sector_count.checked_sub(1)?)?,
                ))
            })
            .collect();
        if scanned.as_ref() != Some(&absolute) {
            return Err(Error::IfoParse);
        }
        let group = groups
            .iter()
            .position(|g| g == &absolute)
            .unwrap_or_else(|| {
                groups.push(absolute);
                groups.len() - 1
            });
        title_group.push((i, group));
    }
    let order = need(partition(&groups))?;
    let title_count = titles.len();
    for (i, title) in titles.iter_mut().enumerate() {
        let ordinal = title_group
            .iter()
            .find(|(t, _)| *t == i)
            .and_then(|(_, g)| order.iter().position(|x| x == g));
        title.selection_evidence.episodes = EpisodeEvidence::Authored {
            roster: format!("dvd-vmgm-static-playall-partition-v1:vts-{vtsn}"),
            title_count,
            member: ordinal.is_some(),
            ordinal,
        };
    }
    Ok(())
}

pub(super) fn annotate(
    reader: &mut dyn SectorSource,
    fs: &UdfFs,
    vmg: &[u8],
    addresses: &[(u8, u8)],
    titles: &mut [DiscTitle],
    halt: Option<&Halt>,
) -> Result<()> {
    let mut reader = CheckedReader {
        inner: reader,
        halt,
    };
    match produce(&mut reader, fs, vmg, addresses, titles) {
        Err(Error::Halted) => return Err(Error::Halted),
        Err(_) => {
            tracing::debug!(target:"freemkv::scan", "dvd: static menu/full-title partition unproven; episode selection needs review")
        }
        Ok(()) => {}
    }
    if halt.is_some_and(Halt::is_cancelled) {
        return Err(Error::Halted);
    }
    launch::annotate(&mut reader, fs, vmg, addresses, titles)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_rejects_ambiguous_prefix_repeated_program_and_interval_variants() {
        let a = vec![(10, 20)];
        let b = vec![(30, 40)];
        assert_eq!(
            partition(&[a.clone(), b.clone(), vec![(30, 40), (10, 20)]]),
            Some(vec![1, 0])
        );
        assert_eq!(
            partition(&[
                a.clone(),
                vec![(10, 20), (30, 40)],
                vec![(10, 20), (30, 40), (50, 60)]
            ]),
            None
        );
        assert_eq!(
            partition(&[a.clone(), b.clone(), vec![(10, 20), (10, 20), (30, 40)]]),
            None
        );
        assert_eq!(partition(&[a, b, vec![(10, 19), (30, 40)]]), None);
    }
}
