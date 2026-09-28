//! The whole-disc decrypting reader behind every decrypted disc/image → ISO copy, so the
//! GUI, the CLI and the image path share one set of rules. AACS content is every content
//! file (`/BDMV/STREAM`, HD DVD `/HVDVD_TS/*.EVO`), not just the kept titles. Each file no
//! title plays is keyed on its own, probed at up to 32 units across it: a held key must
//! open TWO probed units before it keys the file (one chance TS-sync pass is no proof);
//! ciphertext no held key opens refuses up front ([`Error::WholeDiscKeyMissing`]); a file
//! with no proven key is keyed only on a provably single-CPS disc, else left unkeyed so an
//! encrypted unit there stops the pass with the same error (never ships as ciphertext).
//! Reads follow each file's own 3-sector unit grid ([`UnitAligned`]).

use crate::decrypt::{AacsKeyMap, DecryptKeys, Phase};
use crate::error::{Error, Result};
use crate::sector::{DecryptingSectorSource, KeyFetch, SectorSource};

/// Sectors in one AACS aligned unit (6144 bytes).
pub(crate) const UNIT: u64 = (crate::aacs::content::ALIGNED_UNIT_LEN / 2048) as u64;

/// Units probed per unplayed content file: its first unit, then evenly across it.
const PROBES: u64 = 32;

/// Probed units a key must open before it keys a file.
const PROOFS: u8 = 2;

/// The decrypting reader a whole-disc copy reads through.
pub type WholeDiscReader<S> = UnitAligned<DecryptingSectorSource<S>>;

/// The whole-disc reader for a raw (`--raw`) copy. It never decrypts: AACS ciphertext and
/// CSS-scrambled sectors pass through byte for byte. A decrypting copy reads through
/// [`ResolvedKeySet::whole_disc_reader`](crate::keys::ResolvedKeySet::whole_disc_reader).
pub fn raw_whole_disc_reader<S: SectorSource>(reader: S) -> WholeDiscReader<S> {
    UnitAligned::new(
        DecryptingSectorSource::new(reader, crate::decrypt::DecryptKeys::None),
        Vec::new(),
    )
}

/// Build the whole-disc reader over `reader`. For AACS every content file is keyed here, so
/// a refusal (or a stop during keying) comes before the caller creates any output; the one
/// exception is a file with no provable key on a multi-key disc, which stops the pass with
/// E7032 at its first encrypted unit. `fetch` goes to [`crate::mux::resolve_mux_key_map`],
/// which asks it only when the pool holds several base keys and none opens a file's
/// samples. CSS is descrambled; `decrypt == false` (raw copy) and clear discs pass through.
pub fn whole_disc_reader<S: SectorSource>(
    disc: &crate::Disc,
    mut reader: S,
    decrypt: bool,
    fetch: Option<&KeyFetch>,
    halt: Option<&crate::halt::Halt>,
) -> Result<WholeDiscReader<S>> {
    let mut keys = if decrypt {
        disc.decrypt_keys()
    } else {
        DecryptKeys::None
    };
    let mut content = disc.encrypted_content_ranges();
    let mut spans = Vec::new();
    let mut unproven = Vec::new();
    let mut key_map = None;
    if matches!(keys, DecryptKeys::Aacs { .. }) {
        let r: &mut dyn SectorSource = &mut reader;
        let map = disc.resolve_content_key_map(r, &mut keys, fetch, halt)?;
        // freemkv#55: the key map alone is no gate; clear UDF/nav outside it would refuse.
        let files = content_files(r)?;
        if files.is_empty() && !content.is_empty() {
            tracing::warn!(target: "freemkv::scan", "titles but no AACS content files");
            let path = match disc.format {
                crate::DiscFormat::HdDvd => "/HVDVD_TS",
                _ => "/BDMV/STREAM",
            };
            return Err(Error::UdfNotFound { path: path.into() });
        }
        spans = unit_spans(&files, &content);
        let rule = KeyRule {
            format: disc.content_format,
            single: single_cps_key_slot(disc, &keys, &map),
            fetch,
            halt,
        };
        let keyed = key_content_files(r, &rule, &mut keys, map, &files, &spans)?;
        key_map = Some(std::sync::Arc::new(keyed.map));
        unproven = keyed.unproven;
        content.extend(files.iter().flatten());
        content = merge_ranges(content);
        // A stop that landed during the last key lookup still ends before any output.
        if halt.is_some_and(|h| h.is_cancelled()) {
            return Err(Error::Halted);
        }
    }
    let mut dec = DecryptingSectorSource::new(reader, keys);
    if let Some(map) = key_map {
        dec = dec.with_key_map(map);
    }
    // CSS self-descrambles and `None` decrypts nothing: the gate only matters for AACS.
    if decrypt && !content.is_empty() {
        dec = dec.with_content_ranges(std::sync::Arc::from(content));
    }
    let mut out = UnitAligned::new(dec, spans);
    out.unproven = unproven;
    Ok(out)
}

// Every AACS content file's extents, one entry per file (contiguous extents joined):
// `/BDMV/STREAM` (m2ts before SSIF, which re-lists them) or else HD DVD `/HVDVD_TS/*.EVO`.
// An unreadable UDF or unmappable file fails loud: it would otherwise ship as ciphertext.
pub(crate) fn content_files(reader: &mut dyn SectorSource) -> Result<Vec<Vec<(u32, u32)>>> {
    let fs = crate::udf::read_filesystem(reader)?;
    content_files_in(&fs, reader)
}

// `content_files` over an already-read filesystem.
pub(crate) fn content_files_in(
    fs: &crate::udf::UdfFs,
    reader: &mut dyn SectorSource,
) -> Result<Vec<Vec<(u32, u32)>>> {
    let (top, evo_only) = if fs.find_dir("/BDMV/STREAM").is_some() {
        ("/BDMV/STREAM", false)
    } else if fs.find_dir("/HVDVD_TS").is_some() {
        ("/HVDVD_TS", true)
    } else {
        return Ok(Vec::new());
    };
    let mut paths = Vec::new();
    let mut stack: Vec<_> = fs
        .find_dir(top)
        .map(|d| (top.to_string(), d))
        .into_iter()
        .collect();
    while let Some((dir, entry)) = stack.pop() {
        for e in &entry.entries {
            let path = format!("{dir}/{}", e.name);
            if e.is_dir {
                stack.push((path, e));
            } else if !evo_only || e.name.to_ascii_uppercase().ends_with(".EVO") {
                paths.push(path);
            }
        }
    }
    paths.sort_by_key(|p| (p.to_ascii_uppercase().contains("/SSIF/"), p.clone()));
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let mut extents: Vec<(u32, u32)> = Vec::new();
        for (lba, n) in fs.file_extents(reader, &path)? {
            match extents.last_mut() {
                Some(last) if n > 0 && last.0 as u64 + last.1 as u64 == lba as u64 => last.1 += n,
                _ if n > 0 => extents.push((lba, n)),
                _ => {}
            }
        }
        if !extents.is_empty() {
            files.push(extents);
        }
    }
    Ok(files)
}

/// A content extent `(start, count)` and the LBA its unit grid is anchored at: the
/// owning file's first sector, carried across extents by file offset.
pub(crate) type UnitSpan = (u32, u32, u64);

// The unit grid of every content extent: per file by file offset (never a merged run:
// adjacent files each start their own grid), then title extents no file covers (anchored
// at the extent start). Sorted, disjoint; on overlap the earlier-listed source wins.
pub(crate) fn unit_spans(files: &[Vec<(u32, u32)>], title_extents: &[(u32, u32)]) -> Vec<UnitSpan> {
    let mut raw: Vec<(UnitSpan, usize)> = Vec::new();
    for (i, file) in files.iter().enumerate() {
        let mut off = 0u64;
        for &(lba, n) in file {
            raw.push(((lba, n, (lba as u64).saturating_sub(off % UNIT)), i));
            off += n as u64;
        }
    }
    for &(lba, n) in title_extents {
        raw.push(((lba, n, lba as u64), files.len()));
    }
    raw.retain(|&((_, n, _), _)| n > 0);
    raw.sort_by_key(|&((lba, _, _), i)| (lba, i));
    let mut spans: Vec<UnitSpan> = Vec::with_capacity(raw.len());
    let mut covered = 0u64;
    for ((lba, n, anchor), _) in raw {
        let end = lba as u64 + n as u64;
        if end <= covered {
            continue;
        }
        let start = (lba as u64).max(covered);
        spans.push((start as u32, (end - start) as u32, anchor));
        covered = end;
    }
    spans
}

// The span holding `lba`, if any.
pub(crate) fn span_at(spans: &[UnitSpan], lba: u64) -> Option<UnitSpan> {
    let i = spans
        .partition_point(|&(s, _, _)| s as u64 <= lba)
        .checked_sub(1)?;
    let span = spans[i];
    (lba < span.0 as u64 + span.1 as u64).then_some(span)
}

// The first unit head at or after `lba` on the grid anchored at `anchor`.
pub(crate) fn unit_head(lba: u32, anchor: u64) -> u64 {
    lba as u64 + (UNIT - (lba as u64).saturating_sub(anchor) % UNIT) % UNIT
}

// How content files are keyed: the disc's content format, the single-CPS slot an
// unproven file may take, and the key source + stop token.
struct KeyRule<'a> {
    format: crate::ContentFormat,
    single: Option<usize>,
    fetch: Option<&'a KeyFetch>,
    halt: Option<&'a crate::halt::Halt>,
}

// The keyed content map plus the `[start, end)` pieces left unkeyed (no proven key).
struct ContentKeys {
    map: AacsKeyMap,
    unproven: Vec<(u32, u32)>,
}

// Key every content file no kept title plays, ONE FILE AT A TIME (CPS units sit back to
// back), with a held key proven on its ciphertext; ciphertext none opens refuses now.
fn key_content_files(
    reader: &mut dyn SectorSource,
    rule: &KeyRule,
    keys: &mut DecryptKeys,
    map: AacsKeyMap,
    files: &[Vec<(u32, u32)>],
    spans: &[UnitSpan],
) -> Result<ContentKeys> {
    let mut keyed: Vec<(u32, u32)> = map.ranges().iter().map(|&(s, e, _, _)| (s, e)).collect();
    let mut ranges = map.ranges().to_vec();
    let mut unproven = Vec::new();
    for file in files {
        let orphans = subtract_ranges(file, &keyed);
        if orphans.is_empty() {
            continue;
        }
        // Each piece from its first unit head, so key resolution samples on the file grid.
        let heads: Vec<(u32, u32, u64)> = orphans
            .iter()
            .map(|&(s, e)| {
                let anchor = span_at(spans, s as u64).map_or(s as u64, |sp| sp.2);
                (unit_head(s, anchor).min(e as u64) as u32, e, anchor)
            })
            .collect();
        let mut title = crate::DiscTitle::empty();
        title.content_format = rule.format;
        title.extents = heads
            .iter()
            .filter(|&&(h, e, _)| h < e)
            .map(|&(h, e, _)| crate::Extent {
                start_lba: h,
                sector_count: e - h,
            })
            .collect();
        let fmap = crate::mux::resolve_mux_key_map(
            reader,
            &title,
            keys,
            rule.fetch,
            rule.format,
            rule.halt,
        )
        .map_err(|e| unkeyable(Error::from(e), file))?;
        for (&(s, e), &(_, _, anchor)) in orphans.iter().zip(&heads) {
            let probe = Probe {
                map: &fmap,
                keys,
                format: rule.format,
                halt: rule.halt,
            };
            match probe
                .run(reader, (s, e), anchor)
                .map_err(|e| unkeyable(e, file))?
            {
                Proof::Forensic => {
                    ranges.extend(fmap.ranges().iter().filter_map(|&(rs, re, i, p)| {
                        (rs < e && re > s).then_some((rs.max(s), re.min(e), i, p))
                    }));
                }
                Proof::Key(slot) => ranges.push((s, e, slot, Phase::All)),
                Proof::Unproven => match rule.single {
                    Some(slot) => ranges.push((s, e, slot, Phase::All)),
                    None => {
                        tracing::warn!(
                            target: "freemkv::scan",
                            start = s,
                            end = e,
                            probes = PROBES,
                            "unplayed stream file: no key opened two readable encrypted \
                             probes (damaged area, or too little ciphertext), so its key \
                             is unproven on this multi-key disc. Left unkeyed: if the copy \
                             meets an encrypted unit here it stops with E7032. An MKV rip \
                             or a raw copy avoids this."
                        );
                        unproven.push((s, e));
                    }
                },
            }
        }
        // Settled either way: a file re-listing these sectors (SSIF) is not re-keyed.
        keyed.extend(orphans);
        keyed.sort_unstable();
    }
    Ok(ContentKeys {
        map: AacsKeyMap::from_ranges_phased(merge_key_ranges(ranges)),
        unproven: merge_spans(unproven),
    })
}

// A key-resolution refusal for an unplayed file: "no held key opens it" is
// [`Error::WholeDiscKeyMissing`] (the fix is an MKV rip or a raw copy).
fn unkeyable(e: Error, file: &[(u32, u32)]) -> Error {
    match e {
        Error::DecryptFailed | Error::WholeDiscKeyMissing => {
            tracing::error!(
                target: "freemkv::scan",
                lba = file.first().map_or(0, |f| f.0),
                code = crate::error::E_WHOLE_DISC_KEY_MISSING,
                "unplayed stream file is encrypted and no held key opens it: a decrypted \
                 image would keep encrypted pieces. Refusing before the copy starts."
            );
            Error::WholeDiscKeyMissing
        }
        other => other,
    }
}

// What probing an unplayed file's ciphertext proved.
#[derive(Debug, PartialEq)]
enum Proof {
    /// A forensic (FMTS) segment entry: libfreemkv verifies each such unit as it decrypts.
    Forensic,
    /// This pool slot opened [`PROOFS`] probed units.
    Key(usize),
    /// No key opened enough readable encrypted units, and none was unopenable.
    Unproven,
}

// Inputs for proving an unplayed file's key.
struct Probe<'a> {
    map: &'a AacsKeyMap,
    keys: &'a DecryptKeys,
    format: crate::ContentFormat,
    halt: Option<&'a crate::halt::Halt>,
}

impl Probe<'_> {
    // Probe `[start, end)` on the file's grid (`anchor`): its first unit, then evenly
    // across it. Each encrypted unit is tried with the mapped key, then every held base
    // key (never forensic ones). Err(WholeDiscKeyMissing): a unit no held key opens.
    fn run(
        &self,
        reader: &mut dyn SectorSource,
        (start, end): (u32, u32),
        anchor: u64,
    ) -> Result<Proof> {
        use crate::aacs::content::{aacs_unit_encrypted, decrypt_unit, is_clean};
        let DecryptKeys::Aacs { unit_keys, .. } = self.keys else {
            return Ok(Proof::Unproven);
        };
        let base = crate::mux::resolve::base_key_slots(unit_keys);
        let head = unit_head(start, anchor);
        let units = (end as u64).saturating_sub(head) / UNIT;
        let mut opened = vec![0u8; unit_keys.len()];
        let mut unopened = false;
        let mut buf = vec![0u8; crate::aacs::content::ALIGNED_UNIT_LEN];
        let opens = |buf: &[u8], slot: usize| {
            unit_keys.get(slot).is_some_and(|(_, key)| {
                let mut u = buf.to_vec();
                decrypt_unit(&mut u, key);
                is_clean(&u, self.format)
            })
        };
        for unit in probe_units(units) {
            if self.halt.is_some_and(|h| h.is_cancelled()) {
                return Err(Error::Halted);
            }
            let Ok(lba) = u32::try_from(head + unit * UNIT) else {
                continue;
            };
            if !matches!(reader.read_sectors(lba, UNIT as u16, &mut buf, false),
                    Ok(n) if n == buf.len())
                || !aacs_unit_encrypted(&buf, self.format)
            {
                continue;
            }
            let mapped = match self.map.entry_for(lba) {
                Some((_, p, _)) if p != Phase::All => return Ok(Proof::Forensic),
                entry => entry.map(|(slot, _, _)| slot),
            };
            let mut candidates = mapped
                .into_iter()
                .chain(base.iter().copied().filter(|&s| Some(s) != mapped));
            match candidates.find(|&slot| opens(&buf, slot)) {
                Some(slot) => {
                    opened[slot] = opened[slot].saturating_add(1);
                    if opened[slot] >= PROOFS {
                        return Ok(Proof::Key(slot));
                    }
                }
                None => unopened = true,
            }
        }
        if unopened {
            return Err(Error::WholeDiscKeyMissing);
        }
        Ok(Proof::Unproven)
    }
}

// Which of a piece's `units` to probe, in order: all of them when few, else the
// first unit, then `PROBES - 1` more spread evenly to the end.
pub(crate) fn probe_units(units: u64) -> Vec<u64> {
    if units <= PROBES {
        return (0..units).collect();
    }
    (0..PROBES).map(|p| units * p / PROBES).collect()
}

/// The key-pool slot of the disc's only CPS unit, when provably single-CPS:
/// `Unit_Key_RO.inf` declares exactly one unit, the disc is not FMTS, and `map`
/// uses at most one key. Never the pool size: one held key on a two-unit disc is
/// not single-CPS.
fn single_cps_key_slot(disc: &crate::Disc, keys: &DecryptKeys, map: &AacsKeyMap) -> Option<usize> {
    if disc.format == crate::DiscFormat::Fmts || map.ranges().iter().any(|r| r.3 != Phase::All) {
        return None;
    }
    // KS-14 [BD] §3.9.3: "Num_of_CPS_Unit … indicates the number of CPS Units on the
    // disc"; K-8: via `parse_title_keys`, so an HD DVD VTKF counts too (KS-27, evidence).
    if disc.declared_cps_units()? != 1 {
        return None;
    }
    match (map.key_indices(), keys) {
        ([only], _) => Some(*only),
        ([], DecryptKeys::Aacs { unit_keys, .. }) if unit_keys.len() == 1 => Some(0),
        _ => None,
    }
}

// Sorted, disjoint key ranges (`AacsKeyMap::entry_for` checks one neighbour), merged as
// the per-disc map is: same key + phase overlap unions; a conflicting overlap drops.
fn merge_key_ranges(mut ranges: Vec<(u32, u32, usize, Phase)>) -> Vec<(u32, u32, usize, Phase)> {
    ranges.sort_by_key(|r| r.0);
    let mut merged: Vec<(u32, u32, usize, Phase)> = Vec::with_capacity(ranges.len());
    for r in ranges {
        match merged.last_mut() {
            Some(last) if r.0 < last.1 => {
                if r.2 == last.2 && r.3 == last.3 {
                    last.1 = last.1.max(r.1);
                }
            }
            _ => merged.push(r),
        }
    }
    merged
}

// `content` `(start, count)` ranges minus the sorted, disjoint `[start, end)` `keyed`
// ranges, as `[start, end)` pieces.
pub(crate) fn subtract_ranges(content: &[(u32, u32)], keyed: &[(u32, u32)]) -> Vec<(u32, u32)> {
    let mut out = Vec::new();
    for &(start, count) in content {
        let end = start.saturating_add(count);
        let mut pos = start;
        for &(ks, ke) in keyed {
            if ke <= pos || ks >= end {
                continue;
            }
            if ks > pos {
                out.push((pos, ks));
            }
            pos = pos.max(ke);
        }
        if pos < end {
            out.push((pos, end));
        }
    }
    out
}

// Sort + coalesce overlapping/adjacent `(start, count)` ranges (the content gate's
// binary search needs them sorted and disjoint); empty ranges are dropped.
pub(crate) fn merge_ranges(mut ranges: Vec<(u32, u32)>) -> Vec<(u32, u32)> {
    ranges.retain(|&(_, count)| count > 0);
    ranges.sort_unstable();
    let mut out: Vec<(u32, u32)> = Vec::with_capacity(ranges.len());
    for (start, count) in ranges {
        let end = start as u64 + count as u64;
        if let Some(last) = out.last_mut() {
            let last_end = last.0 as u64 + last.1 as u64;
            if start as u64 <= last_end {
                let merged = last_end.max(end) - last.0 as u64;
                last.1 = u32::try_from(merged).unwrap_or(u32::MAX);
                continue;
            }
        }
        out.push((start, count));
    }
    out
}

// Sort + coalesce `[start, end)` ranges.
fn merge_spans(mut v: Vec<(u32, u32)>) -> Vec<(u32, u32)> {
    v.sort_unstable();
    let mut out: Vec<(u32, u32)> = Vec::with_capacity(v.len());
    for (s, e) in v {
        match out.last_mut() {
            Some(last) if s <= last.1 => last.1 = last.1.max(e),
            _ => out.push((s, e)),
        }
    }
    out
}

/// Reads through `inner` on each content file's AACS unit grid: a read touching a span
/// is widened to whole units of that span (sweep batches, `write_image` batches and
/// patch's single-sector reads are not unit multiples). A unit whose head lies outside
/// its span fails loud; a decrypt refusal inside an unproven file is E7032.
pub struct UnitAligned<S> {
    inner: S,
    spans: Vec<UnitSpan>,
    scratch: Vec<u8>,
    /// Unplayed-file pieces left unkeyed (no proven key), `[start, end)`.
    unproven: Vec<(u32, u32)>,
}

impl<S: SectorSource> UnitAligned<S> {
    pub(crate) fn new(inner: S, spans: Vec<UnitSpan>) -> Self {
        Self {
            inner,
            spans,
            scratch: Vec::new(),
            unproven: Vec::new(),
        }
    }

    /// Where a block of sectors `[start, end)` should end so consecutive blocks tile each
    /// file's unit grid: `end` pulled back to its unit's head when that unit straddles it
    /// (else a bad sector there fails both neighbouring blocks). Never at or before `start`.
    pub fn unit_block_end(&self, start: u64, end: u64) -> u64 {
        match span_at(&self.spans, end) {
            Some((s, _, anchor)) if end > anchor => {
                let head = anchor + (end - anchor) / UNIT * UNIT;
                if head > start && head >= s as u64 {
                    head
                } else {
                    end
                }
            }
            _ => end,
        }
    }

    // A decrypt refusal inside an unproven piece is the key it could not prove up front.
    fn refusal(&self, err: Error, a0: u64, a1: u64) -> Error {
        let hit = self
            .unproven
            .iter()
            .any(|&(s, e)| (s as u64) < a1 && (e as u64) > a0);
        match err {
            Error::DecryptFailed if hit => {
                tracing::error!(
                    target: "freemkv::disc",
                    lba = a0,
                    code = crate::error::E_WHOLE_DISC_KEY_MISSING,
                    "encrypted unit in an unplayed stream file whose key could not be \
                     proven before the copy (no readable probe); stopping rather than \
                     writing ciphertext into a decrypted image"
                );
                Error::WholeDiscKeyMissing
            }
            other => other,
        }
    }
}

impl<S: SectorSource> UnitAligned<crate::sector::DecryptingSectorSource<S>> {
    /// Damaged AACS units the decrypting reader blanked so far (see
    /// [`DecryptingSectorSource::blanked_units`](crate::sector::DecryptingSectorSource::blanked_units)).
    pub fn blanked_units(&self) -> u64 {
        self.inner.blanked_units()
    }
}

impl<S: SectorSource> SectorSource for UnitAligned<S> {
    fn capacity_sectors(&self) -> u32 {
        self.inner.capacity_sectors()
    }

    fn unmapped_stream_files(&self) -> &[crate::sector::bus_removal::UnmappedStreamFile] {
        self.inner.unmapped_stream_files()
    }
    fn random_access(&self) -> bool {
        self.inner.random_access()
    }

    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        self.read_sectors_fua(lba, count, buf, recovery, false)
    }

    fn read_sectors_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> Result<usize> {
        const SECTOR: usize = 2048;
        let end = lba as u64 + count as u64;
        let mut cur = lba as u64;
        while cur < end {
            let off = (cur - lba as u64) as usize * SECTOR;
            let Some((s, n, anchor)) = span_at(&self.spans, cur) else {
                // Outside every content span: a plain read up to the next span.
                let i = self.spans.partition_point(|&(s, _, _)| s as u64 <= cur);
                let next = self
                    .spans
                    .get(i)
                    .map_or(end, |&(s, _, _)| end.min(s as u64));
                let want = (next - cur) as usize * SECTOR;
                self.inner.set_unit_base(cur as u32);
                let got = self.inner.read_sectors_fua(
                    cur as u32,
                    (next - cur) as u16,
                    &mut buf[off..off + want],
                    recovery,
                    fua,
                )?;
                if got < want {
                    return Ok(off + got);
                }
                cur = next;
                continue;
            };
            let (s, e) = (s as u64, s as u64 + n as u64);
            let a0 = anchor + (cur - anchor) / UNIT * UNIT;
            if a0 < s {
                // The unit straddles a non-contiguous extent boundary: undecryptable here.
                return Err(Error::DecryptFailed);
            }
            // Capped so the widened read still fits one u16-count request.
            let piece_end = end.min(e).min(a0 + (u16::MAX as u64 / UNIT - 1) * UNIT);
            let a1 = e.min(a0 + (piece_end - a0).div_ceil(UNIT) * UNIT);
            let len = (a1 - a0) as usize * SECTOR;
            self.scratch.resize(len, 0);
            self.inner.set_unit_base(a0 as u32);
            let got = self
                .inner
                .read_sectors_fua(
                    a0 as u32,
                    (a1 - a0) as u16,
                    &mut self.scratch[..len],
                    recovery,
                    fua,
                )
                .map_err(|e| self.refusal(e, a0, a1))?;
            let skip = (cur - a0) as usize * SECTOR;
            let want = (piece_end - cur) as usize * SECTOR;
            let have = got.saturating_sub(skip).min(want);
            buf[off..off + have].copy_from_slice(&self.scratch[skip..skip + have]);
            if have < want {
                return Ok(off + have);
            }
            cur = piece_end;
        }
        Ok(count as usize * SECTOR)
    }

    fn set_speed(&mut self, kbs: u16) {
        self.inner.set_speed(kbs)
    }
}

#[cfg(test)]
#[path = "whole_disc_tests.rs"]
mod tests;
