//! AACS bus-encryption removal for sector streams.
//!
//! The live `Drive` already removes bus encryption. Do not wrap it in this
//! adapter: applying the transform twice corrupts content. This adapter is for
//! other sources and tests, using the same [`BusMap`] gate.
//! [`BusStage::Passthrough`] leaves sectors untouched; [`BusStage::AacsHostKey`]
//! applies the host key obtained through the AACS handshake.
//!
//! A [`BusMap`] limits de-bussing to AACS stream files and, within them, to
//! aligned units whose copy-permission bits are set (libaacs `aacs_decrypt_bus`).
//! Without a map, the caller must supply content-only sectors.

use std::collections::HashMap;
use std::sync::Arc;

use crate::consts::SECTOR_BYTES;
use crate::error::{Error, Result};

use super::SectorSource;

// AACS aligned unit = 3 sectors; only its first sector's bus-clear bytes carry
// the TP_extra_header copy-permission indicator.
const UNIT_SECTORS: u64 = 3;
const CPI_MASK: u8 = 0xC0;
// Bound on remembered per-unit decisions (cleared when full).
const CPI_CACHE_MAX: usize = 1 << 16;

#[derive(Debug, Clone, Copy)]
struct Span {
    lba: u32,
    count: u32,
    /// `None` = content of unknown unit alignment: always de-bussed.
    file: Option<u32>,
    off: u64,
}

/// Where a mapped LBA sits: its file and aligned unit, plus the first sector of
/// that unit when it lies in the same span (no extent walk needed).
struct Located {
    file: u32,
    unit: u64,
    head_in_span: Option<u32>,
}

/// A Clip AV stream file whose File Entry extents could not be read, so a
/// [`BusMap`] cannot locate its sectors and they pass through still bus-encrypted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnmappedStreamFile {
    /// Disc path, e.g. `/BDMV/STREAM/00002.m2ts`.
    pub path: String,
    /// Why the File Entry could not be read, as `E{code}: {detail}`.
    pub cause: String,
    /// The file's ICB (File Entry) LBA in the metadata partition.
    pub(crate) icb: u32,
}

impl UnmappedStreamFile {
    /// `path` (a disc path), its File Entry `icb` and the read failure that lost it.
    pub fn new(path: String, icb: u32, cause: &Error) -> Self {
        Self {
            path,
            cause: cause.to_string(),
            icb,
        }
    }
}

/// Refuse a whole-disc image (`iso://`, sweep, patch) read through `reader`
/// when its bus map could not locate a bus-encrypted stream file: those sectors
/// would be written still bus-encrypted as if plaintext. Names every such file.
///
/// AACS BD Pre-recorded Book 0.953 §3.7 Note: "PC Host shall decrypt bus-encrypted
/// Clip AV stream file and hand it over to the application." An unlocated file cannot be.
/// Per spec; do not change without a spec citation proving otherwise.
pub fn ensure_image_debussable(reader: &dyn SectorSource) -> Result<()> {
    unmapped_error(reader.unmapped_stream_files())
}

/// `Err(`[`Error::BusStreamUnmapped`]`)` naming every file in `unmapped` as `path (cause)`,
/// or `Ok` when empty.
pub(crate) fn unmapped_error(unmapped: &[UnmappedStreamFile]) -> Result<()> {
    if unmapped.is_empty() {
        return Ok(());
    }
    let files: Vec<String> = unmapped
        .iter()
        .map(|u| format!("{} ({})", u.path, u.cause))
        .collect();
    Err(Error::BusStreamUnmapped {
        files: files.join(", "),
    })
}

/// Whole-disc host-key de-bus map: each AACS stream file's extents in file
/// order, so an LBA resolves to its aligned unit and that unit's first sector.
#[derive(Debug, Default, Clone)]
pub struct BusMap {
    /// Sorted, disjoint LBA spans, each tagged with its file and file offset.
    spans: Vec<Span>,
    /// Per-file extents in file order; `None` start = unrecorded (a hole).
    files: Vec<Vec<(Option<u32>, u32)>>,
    /// Stream files the map could not locate (their sectors stay bus-encrypted).
    unmapped: Vec<UnmappedStreamFile>,
}

impl BusMap {
    /// Build from per-file extent lists in file order. Where files overlap
    /// (SSIF re-lists m2ts extents), the earlier span keeps the sectors.
    pub fn from_files(files: Vec<Vec<(u32, u32)>>) -> Self {
        let files = files
            .into_iter()
            .map(|f| f.into_iter().map(|(l, c)| (Some(l), c)).collect())
            .collect();
        Self::new(files, &[])
    }

    /// Full constructor: per-file extents (`None` start = unrecorded hole that
    /// still advances the file offset) plus `unknown` content ranges whose unit
    /// alignment is unknown; those are de-bussed wherever no file covers them.
    pub fn new(files: Vec<Vec<(Option<u32>, u32)>>, unknown: &[(u32, u32)]) -> Self {
        let mut raw = Vec::new();
        for (fi, exts) in files.iter().enumerate() {
            let mut off = 0u64;
            for &(lba, count) in exts {
                if let Some(lba) = lba.filter(|_| count > 0) {
                    raw.push(Span {
                        lba,
                        count,
                        file: Some(fi as u32),
                        off,
                    });
                }
                off += count as u64;
            }
        }
        let mut spans = disjoint(raw);
        let mut gaps = Vec::new();
        for &(lba, count) in unknown {
            let (mut at, end) = (lba as u64, (lba as u64 + count as u64).min(1 << 32));
            for s in &spans {
                let (s0, s1) = (s.lba as u64, s.lba as u64 + s.count as u64);
                if s1 <= at || s0 >= end {
                    continue;
                }
                if s0 > at {
                    gaps.push(unknown_span(at, s0));
                }
                at = at.max(s1);
            }
            if at < end {
                gaps.push(unknown_span(at, end));
            }
        }
        spans.extend(gaps);
        spans = disjoint(spans);
        Self {
            spans,
            files,
            unmapped: Vec::new(),
        }
    }

    /// Record the stream files this map could not locate.
    pub(crate) fn with_unmapped(mut self, unmapped: Vec<UnmappedStreamFile>) -> Self {
        self.unmapped = unmapped;
        self
    }

    /// Stream files this map could not locate: whole-disc images must refuse them.
    pub fn unmapped(&self) -> &[UnmappedStreamFile] {
        &self.unmapped
    }

    /// Each range as its own unit-aligned file.
    pub fn from_ranges(ranges: &[(u32, u32)]) -> Self {
        Self::from_files(ranges.iter().map(|&r| vec![r]).collect())
    }

    /// Merged `(start_lba, sector_count)` coverage.
    pub fn covered_ranges(&self) -> Vec<(u32, u32)> {
        let r: Vec<(u32, u32)> = self.spans.iter().map(|s| (s.lba, s.count)).collect();
        crate::udf::merge_ranges(&r)
    }

    fn span_at(&self, lba: u32) -> Option<&Span> {
        let i = self.spans.partition_point(|s| s.lba <= lba);
        let s = self.spans.get(i.checked_sub(1)?)?;
        ((lba - s.lba) < s.count).then_some(s)
    }

    // Disc LBA of file-relative sector `off` in `file`; `None` in a hole.
    fn file_lba(&self, file: u32, mut off: u64) -> Option<u32> {
        for &(lba, count) in self.files.get(file as usize)? {
            if off < count as u64 {
                return u32::try_from(lba? as u64 + off).ok();
            }
            off -= count as u64;
        }
        None
    }
}

fn unknown_span(start: u64, end: u64) -> Span {
    Span {
        lba: start as u32,
        count: (end - start) as u32,
        file: None,
        off: 0,
    }
}

// Sort spans and clip overlaps so each sector belongs to the earliest span.
fn disjoint(mut raw: Vec<Span>) -> Vec<Span> {
    raw.sort_by_key(|s| (s.lba, s.file.is_none(), s.file));
    let mut spans: Vec<Span> = Vec::with_capacity(raw.len());
    let mut covered = 0u64;
    for mut s in raw {
        let end = (s.lba as u64 + s.count as u64).min(1 << 32);
        if end <= covered {
            continue;
        }
        if (s.lba as u64) < covered {
            s.off += covered - s.lba as u64;
            s.lba = covered as u32;
        }
        s.count = (end - s.lba as u64) as u32;
        covered = end;
        spans.push(s);
    }
    spans
}

impl BusMap {
    fn locate(&self, lba: u32) -> Option<Option<Located>> {
        let s = self.span_at(lba)?;
        let Some(file) = s.file else {
            return Some(None);
        };
        let off = s.off + (lba - s.lba) as u64;
        let unit = off / UNIT_SECTORS;
        let head = unit * UNIT_SECTORS;
        let head_in_span = (head >= s.off).then(|| s.lba + (head - s.off) as u32);
        Some(Some(Located {
            file,
            unit,
            head_in_span,
        }))
    }
}

/// A reader's [`BusMap`] plus its cache of per-unit copy-permission decisions.
#[derive(Debug, Clone)]
pub(crate) struct BusGate {
    map: Arc<BusMap>,
    cpi: HashMap<(u32, u64), bool>,
    /// Last decided unit, kept across reads (survives cache clears).
    last: Option<(u32, u64, bool)>,
}

impl BusGate {
    pub(crate) fn new(map: Arc<BusMap>) -> Self {
        Self {
            map,
            cpi: HashMap::new(),
            last: None,
        }
    }

    pub(crate) fn map(&self) -> &BusMap {
        &self.map
    }

    /// De-bus the whole sectors of `buf` (first at `base_lba`) that lie in
    /// mapped content AND whose aligned unit has copy-permission bits set. A
    /// unit whose first sector is outside `buf` and uncached is decided by
    /// `head_byte0` (a raw one-sector read); unknown or unreadable = encrypted.
    pub(crate) fn debus(
        &mut self,
        buf: &mut [u8],
        read_data_key: &[u8; 16],
        base_lba: u32,
        head_byte0: &mut dyn FnMut(u32) -> Option<u8>,
    ) {
        let n = buf.len() / SECTOR_BYTES;
        let cipher = crate::aacs::crypto::new_cipher_for(read_data_key);
        for i in 0..n {
            let Some(lba) = base_lba.checked_add(i as u32) else {
                break;
            };
            let encrypted = match self.map.locate(lba) {
                None => continue,
                Some(None) => true,
                Some(Some(loc)) => self.unit_encrypted(&loc, buf, base_lba, n, head_byte0),
            };
            if encrypted {
                let at = i * SECTOR_BYTES;
                // First 16 bytes of each sector are plaintext on the wire.
                crate::aacs::crypto::cbc_decrypt_blocks(
                    &cipher,
                    &mut buf[at + 16..at + SECTOR_BYTES],
                );
            }
        }
    }

    fn unit_encrypted(
        &mut self,
        loc: &Located,
        buf: &[u8],
        base_lba: u32,
        n: usize,
        head_byte0: &mut dyn FnMut(u32) -> Option<u8>,
    ) -> bool {
        let key = (loc.file, loc.unit);
        if let Some((f, u, e)) = self.last
            && (f, u) == key
        {
            return e;
        }
        let e = match self.cpi.get(&key).copied() {
            Some(e) => e,
            None => {
                let head = loc
                    .head_in_span
                    .or_else(|| self.map.file_lba(loc.file, loc.unit * UNIT_SECTORS));
                let byte0 = match head {
                    Some(h) if h >= base_lba && ((h - base_lba) as usize) < n => {
                        Some(buf[(h - base_lba) as usize * SECTOR_BYTES])
                    }
                    Some(h) => head_byte0(h),
                    None => None,
                };
                // An unreadable head is a guess, not a measurement: it is not
                // remembered, so a later read that has the head decides for real.
                // An unmapped head can never be measured, so that guess is stable.
                let e = byte0.is_none_or(|b| b & CPI_MASK != 0);
                if byte0.is_some() || head.is_none() {
                    if self.cpi.len() >= CPI_CACHE_MAX {
                        self.cpi.clear();
                    }
                    self.cpi.insert(key, e);
                } else {
                    return e;
                }
                e
            }
        };
        self.last = Some((loc.file, loc.unit, e));
        e
    }
}

// Host-key de-bus of `buf` read at `lba`: through `gate` when mapped, else every
// whole sector (content-only caller).
pub(crate) fn debus_read(
    gate: Option<&mut BusGate>,
    buf: &mut [u8],
    read_data_key: &[u8; 16],
    lba: u32,
    head_byte0: &mut dyn FnMut(u32) -> Option<u8>,
) {
    // KS-18 [BD] §3.7: BEF set for "the Aligned Unit with Copy_permission_indicator set to
    // 11₂" when BEE is 1₂. This gate diverges (KU §7.0.4), backlog: BEE is not read, and
    // unmapped or unknown-grid content is de-bussed by LBA range, whatever its CPI.
    match gate {
        Some(g) => g.debus(buf, read_data_key, lba, head_byte0),
        None => crate::aacs::content::decrypt_bus_sectors(buf, read_data_key),
    }
}

/// How this drive's AACS bus encryption is removed — decided once from the
/// unlock result, never re-examined downstream.
#[derive(Clone)]
pub enum BusStage {
    /// Bus encryption was removed at the drive by a firmware/vendor unlocker
    /// (`Bus=off`), or the disc carries none (DVD / clear BD). Reads pass
    /// through untouched.
    Passthrough,
    /// AACS cert route: the host holds the Read Data Key from the AKE and
    /// de-busses each content sector in software.
    AacsHostKey([u8; 16]),
}

impl BusStage {
    /// Map an unlock/handshake result to the bus stage: a cert-route Read Data
    /// Key (`Some`) means the host must de-bus content in software
    /// ([`BusStage::AacsHostKey`]); its absence (`None`) means bus encryption was
    /// removed at the drive by a firmware/vendor unlock, or the disc never had it
    /// ([`BusStage::Passthrough`]). This is the SINGLE decision point that wires
    /// [`crate::disc::Disc::scan`]'s handshake to the drive's de-bus.
    pub fn from_read_data_key(read_data_key: Option<[u8; 16]>) -> Self {
        match read_data_key {
            Some(rdk) => BusStage::AacsHostKey(rdk),
            None => BusStage::Passthrough,
        }
    }
}

// `BusStage` wraps a raw key; keep it out of logs.
impl std::fmt::Debug for BusStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BusStage::Passthrough => write!(f, "BusStage::Passthrough"),
            BusStage::AacsHostKey(_) => write!(f, "BusStage::AacsHostKey([redacted])"),
        }
    }
}

/// Decorator: read from `inner`, then remove AACS bus encryption from the bytes
/// that landed in `buf` when the [`BusStage`] carries a host key. A
/// [`BusStage::Passthrough`] stream is a zero-cost pass-through.
pub struct BusRemovalSectorSource<S: SectorSource> {
    inner: S,
    stage: BusStage,
    /// `None` = caller reads content-only, so every sector is de-bussed;
    /// `Some` = whole-disc reader gated by a [`BusMap`].
    gate: Option<BusGate>,
}

impl<S: SectorSource> BusRemovalSectorSource<S> {
    /// Wrap `inner` with the bus stage decided by the unlock result.
    pub fn new(inner: S, stage: BusStage) -> Self {
        Self {
            inner,
            stage,
            gate: None,
        }
    }

    /// Gate de-bussing with `map`; sectors outside it pass through untouched.
    pub fn with_bus_map(mut self, map: Arc<BusMap>) -> Self {
        self.gate = Some(BusGate::new(map));
        self
    }

    /// [`with_bus_map`](Self::with_bus_map) over plain ranges, each treated
    /// as one unit-aligned stream file.
    pub fn with_content_ranges(self, ranges: Arc<[(u32, u32)]>) -> Self {
        self.with_bus_map(Arc::new(BusMap::from_ranges(&ranges)))
    }

    /// `&mut` counterpart of [`with_content_ranges`](Self::with_content_ranges),
    /// for readers that build the stream before the content map is known.
    pub fn set_content_ranges(&mut self, ranges: Arc<[(u32, u32)]>) {
        self.gate = Some(BusGate::new(Arc::new(BusMap::from_ranges(&ranges))));
    }

    /// Borrow the inner source (tests / introspection).
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Mutable borrow of the inner source.
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Consume the decorator and return the underlying source.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: SectorSource> SectorSource for BusRemovalSectorSource<S> {
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
        let n = self
            .inner
            .read_sectors_fua(lba, count, buf, recovery, fua)?;
        if let BusStage::AacsHostKey(rdk) = self.stage.clone() {
            let mut gate = self.gate.take();
            let inner = &mut self.inner;
            debus_read(gate.as_mut(), &mut buf[..n], &rdk, lba, &mut |h| {
                let mut s = [0u8; SECTOR_BYTES];
                match inner.read_sectors_fua(h, 1, &mut s, false, false) {
                    Ok(got) if got >= SECTOR_BYTES => Some(s[0]),
                    _ => None,
                }
            });
            self.gate = gate;
        }
        Ok(n)
    }

    fn set_speed(&mut self, kbs: u16) {
        self.inner.set_speed(kbs)
    }

    fn unmapped_stream_files(&self) -> &[UnmappedStreamFile] {
        match (&self.stage, &self.gate) {
            (BusStage::AacsHostKey(_), Some(g)) => g.map().unmapped(),
            _ => self.inner.unmapped_stream_files(),
        }
    }

    fn random_access(&self) -> bool {
        self.inner.random_access()
    }
}

// Shared across every module's "wrappers forward `unmapped_stream_files`" test.
#[cfg(test)]
#[path = "bus_removal_test_support_tests.rs"]
pub(crate) mod test_support;

#[cfg(test)]
#[path = "bus_removal_tests.rs"]
mod tests;
