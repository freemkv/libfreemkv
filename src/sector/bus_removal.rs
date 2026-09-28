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
use crate::error::Result;

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

/// Whole-disc host-key de-bus map: each AACS stream file's extents in file
/// order, so an LBA resolves to its aligned unit and that unit's first sector.
#[derive(Debug, Default, Clone)]
pub struct BusMap {
    /// Sorted, disjoint LBA spans, each tagged with its file and file offset.
    spans: Vec<Span>,
    /// Per-file extents in file order; `None` start = unrecorded (a hole).
    files: Vec<Vec<(Option<u32>, u32)>>,
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
        Self { spans, files }
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
                // An unreadable or unmapped head is cached as encrypted: never re-read.
                let e = byte0.is_none_or(|b| b & CPI_MASK != 0);
                if self.cpi.len() >= CPI_CACHE_MAX {
                    self.cpi.clear();
                }
                self.cpi.insert(key, e);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aacs::content::encrypt_bus;

    /// A source returning a fixed buffer for any read (starting at whatever the
    /// fixture built), reporting the full span.
    struct FixedSource {
        bytes: Vec<u8>,
    }
    impl SectorSource for FixedSource {
        fn capacity_sectors(&self) -> u32 {
            (self.bytes.len() / SECTOR_BYTES) as u32
        }
        fn read_sectors(
            &mut self,
            _lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            let n = (count as usize * SECTOR_BYTES).min(self.bytes.len());
            buf[..n].copy_from_slice(&self.bytes[..n]);
            Ok(n)
        }
    }

    // A recognisable clear "content" unit: plaintext first 16 bytes of each
    // sector (bus enc leaves those clear on the wire), a known pattern in
    // 16..2048. Two sectors so cross-sector gating is exercised.
    fn clear_content(sectors: usize) -> Vec<u8> {
        let mut v = vec![0u8; sectors * SECTOR_BYTES];
        for (i, b) in v.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(31).wrapping_add(7);
        }
        // Copy-permission bits set on every sector head: encrypted AACS units.
        for sector in v.chunks_mut(2048) {
            sector[0] |= 0xC0;
        }
        v
    }

    // The single wiring decision: a cert-route Read Data Key becomes a host-key
    // stage; its absence (firmware/vendor unlock, or a non-bus disc) is Passthrough.
    #[test]
    fn from_read_data_key_maps_handshake_to_stage() {
        let rdk = [0xABu8; 16];
        match BusStage::from_read_data_key(Some(rdk)) {
            BusStage::AacsHostKey(k) => {
                assert_eq!(k, rdk, "cert RDK carries into the host-key stage")
            }
            BusStage::Passthrough => panic!("a Some(read_data_key) must map to AacsHostKey"),
        }
        assert!(
            matches!(BusStage::from_read_data_key(None), BusStage::Passthrough),
            "no read_data_key (firmware/vendor unlock or non-bus disc) must map to Passthrough"
        );
    }

    // Passthrough must hand bytes back byte-identical — even a stream fed
    // already-clear content never touches it.
    #[test]
    fn passthrough_returns_bytes_unchanged() {
        let clear = clear_content(2);
        let src = FixedSource {
            bytes: clear.clone(),
        };
        let mut s = BusRemovalSectorSource::new(src, BusStage::Passthrough);
        let mut got = vec![0u8; 2 * SECTOR_BYTES];
        let n = s.read_sectors(100, 2, &mut got, false).unwrap();
        assert_eq!(n, 2 * SECTOR_BYTES);
        assert_eq!(got, clear, "Passthrough must not alter any byte");
    }

    // The core contract: a bus-ENCRYPTED sector read through an AacsHostKey
    // stream comes back as the known plaintext. MUTATION: dropping the de-bus
    // call (or using Passthrough) leaves the ciphertext, so this goes red.
    #[test]
    fn host_key_removes_bus_encryption() {
        let rdk = [0x5Au8; 16];
        let clear = clear_content(2);
        let mut wire = clear.clone();
        encrypt_bus(&mut wire, &rdk); // model the drive's forward bus transform
        assert_ne!(wire, clear, "fixture must actually be bus-encrypted");

        let src = FixedSource { bytes: wire };
        let mut s = BusRemovalSectorSource::new(src, BusStage::AacsHostKey(rdk));
        let mut got = vec![0u8; 2 * SECTOR_BYTES];
        let n = s.read_sectors(0, 2, &mut got, false).unwrap();
        assert_eq!(n, 2 * SECTOR_BYTES);
        assert_eq!(
            got, clear,
            "AacsHostKey must recover the plaintext byte-for-byte"
        );
    }

    // A sector OUTSIDE the content map is clear filesystem and must pass through
    // untouched (de-bussing it would corrupt plaintext). LBA 0 is outside content
    // [300,303), so the unencrypted bytes come back verbatim.
    #[test]
    fn host_key_passes_through_sectors_outside_content() {
        let rdk = [0x5Au8; 16];
        let clear = clear_content(1); // clear filesystem sector on the wire
        let src = FixedSource {
            bytes: clear.clone(),
        };
        let ranges: Arc<[(u32, u32)]> = Arc::from(vec![(300u32, 3u32)].into_boxed_slice());
        let mut s = BusRemovalSectorSource::new(src, BusStage::AacsHostKey(rdk))
            .with_content_ranges(ranges);
        let mut got = vec![0u8; SECTOR_BYTES];
        // LBA 0 is outside content → no de-bus → bytes unchanged.
        let n = s.read_sectors(0, 1, &mut got, false).unwrap();
        assert_eq!(n, SECTOR_BYTES);
        assert_eq!(
            got, clear,
            "a sector outside content must pass through untouched"
        );
    }

    // The complement: a bus-encrypted sector INSIDE the content map is de-bussed.
    #[test]
    fn host_key_debusses_sectors_inside_content() {
        let rdk = [0x33u8; 16];
        let clear = clear_content(1);
        let mut wire = clear.clone();
        encrypt_bus(&mut wire, &rdk);
        let src = FixedSource { bytes: wire };
        let ranges: Arc<[(u32, u32)]> = Arc::from(vec![(300u32, 3u32)].into_boxed_slice());
        let mut s = BusRemovalSectorSource::new(src, BusStage::AacsHostKey(rdk))
            .with_content_ranges(ranges);
        let mut got = vec![0u8; SECTOR_BYTES];
        // LBA 300 is inside content → de-bus → plaintext recovered.
        let n = s.read_sectors(300, 1, &mut got, false).unwrap();
        assert_eq!(n, SECTOR_BYTES);
        assert_eq!(
            got, clear,
            "a content sector must be de-bussed to plaintext"
        );
    }

    // Per-sector gating WITHIN one multi-sector read at the decorator level:
    // content [301,302) covers only the middle sector of a 3-sector read at 300.
    // MUTATION: gating the whole buffer on the first sector's LBA goes red here.
    #[test]
    fn host_key_gates_per_sector_within_a_multi_sector_read() {
        let rdk = [0x44u8; 16];
        let clear = clear_content(3);
        let mut wire = clear.clone();
        // Encrypt ONLY the middle sector's body; 0 and 2 stay clear on the wire.
        let mut mid = clear[SECTOR_BYTES..2 * SECTOR_BYTES].to_vec();
        encrypt_bus(&mut mid, &rdk);
        wire[SECTOR_BYTES..2 * SECTOR_BYTES].copy_from_slice(&mid);

        let src = FixedSource { bytes: wire };
        let ranges: Arc<[(u32, u32)]> = Arc::from(vec![(301u32, 1u32)].into_boxed_slice());
        let mut s = BusRemovalSectorSource::new(src, BusStage::AacsHostKey(rdk))
            .with_content_ranges(ranges);
        let mut got = vec![0u8; 3 * SECTOR_BYTES];
        s.read_sectors(300, 3, &mut got, false).unwrap();
        assert_eq!(
            got, clear,
            "only the in-range middle sector is de-bussed; clear neighbours untouched"
        );
    }

    // Unit 0 encrypted (CPI set, bus-encrypted), unit 1 clear (CPI 0). libaacs
    // gates bus decrypt per unit on `buf[0] & 0xC0` (aacs.c aacs_decrypt_bus), so
    // the clear unit must pass through untouched.
    #[test]
    fn host_key_debusses_only_units_whose_cpi_is_set() {
        let rdk = [0x21u8; 16];
        let mut clear = clear_content(6);
        clear[0] |= 0xC0;
        clear[3 * SECTOR_BYTES] &= !0xC0;
        let mut wire = clear.clone();
        encrypt_bus(&mut wire[..3 * SECTOR_BYTES], &rdk);
        let ranges: Arc<[(u32, u32)]> = Arc::from(vec![(300u32, 6u32)].into_boxed_slice());
        let mut s =
            BusRemovalSectorSource::new(FixedSource { bytes: wire }, BusStage::AacsHostKey(rdk))
                .with_content_ranges(ranges);
        let mut got = vec![0u8; 6 * SECTOR_BYTES];
        s.read_sectors(300, 6, &mut got, false).unwrap();
        assert_eq!(
            got[..3 * SECTOR_BYTES],
            clear[..3 * SECTOR_BYTES],
            "encrypted unit de-bussed"
        );
        assert_eq!(
            got[3 * SECTOR_BYTES..],
            clear[3 * SECTOR_BYTES..],
            "CPI=0 unit must pass through untouched"
        );
    }

    // LBA-addressed source over `bytes` starting at `base`; logs every read.
    struct LbaSource {
        base: u32,
        bytes: Vec<u8>,
        reads: Vec<(u32, u16)>,
    }
    impl SectorSource for LbaSource {
        fn capacity_sectors(&self) -> u32 {
            self.base + (self.bytes.len() / SECTOR_BYTES) as u32
        }
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> Result<usize> {
            self.reads.push((lba, count));
            let at = (lba - self.base) as usize * SECTOR_BYTES;
            let n = count as usize * SECTOR_BYTES;
            buf[..n].copy_from_slice(&self.bytes[at..at + n]);
            Ok(n)
        }
    }

    // One sector of the wire image at `lba` (source based at `base`).
    fn sector(v: &[u8], base: u32, lba: u32) -> &[u8] {
        let at = (lba - base) as usize * SECTOR_BYTES;
        &v[at..at + SECTOR_BYTES]
    }

    // A read starting mid-unit: the clear unit's head (300) is outside the buffer,
    // so it is fetched raw once, decided CPI=0, cached (no second fetch).
    #[test]
    fn host_key_mid_unit_read_fetches_the_unit_head_once() {
        let rdk = [0x31u8; 16];
        let mut clear = clear_content(6); // LBAs 300..306
        clear[0] &= !0xC0; // unit 300..303 clear
        let mut wire = clear.clone();
        encrypt_bus(&mut wire[3 * SECTOR_BYTES..], &rdk); // unit 303..306 encrypted
        let src = LbaSource {
            base: 300,
            bytes: wire,
            reads: Vec::new(),
        };
        let map = Arc::new(BusMap::from_files(vec![vec![(300, 6)]]));
        let mut s = BusRemovalSectorSource::new(src, BusStage::AacsHostKey(rdk)).with_bus_map(map);
        let mut got = vec![0u8; 4 * SECTOR_BYTES];
        s.read_sectors(301, 4, &mut got, false).unwrap();
        assert_eq!(
            got,
            clear[SECTOR_BYTES..5 * SECTOR_BYTES],
            "301..305 plaintext"
        );
        let mut one = vec![0u8; SECTOR_BYTES];
        s.read_sectors(302, 1, &mut one, false).unwrap();
        assert_eq!(one, sector(&clear, 300, 302), "cached clear unit untouched");
        assert_eq!(
            s.inner().reads,
            vec![(301, 4), (300, 1), (302, 1)],
            "exactly one head fetch, then the cached decision"
        );
    }

    // A file whose extent boundary splits a unit: extents [500,2) + [700,4), so
    // unit 0 = 500,501,700 (encrypted, head 500) and unit 1 = 701..704 (clear).
    // Sector 700's own byte 0 says clear; only the file mapping finds head 500.
    #[test]
    fn host_key_unit_split_across_extents_uses_the_files_unit_head() {
        let rdk = [0x47u8; 16];
        let mut clear = clear_content(204); // LBAs 500..704
        let at = |l: u32| (l - 500) as usize * SECTOR_BYTES;
        clear[at(500)] |= 0xC0;
        clear[at(700)] &= !0xC0;
        clear[at(701)] &= !0xC0;
        let mut wire = clear.clone();
        for l in [500u32, 501, 700] {
            encrypt_bus(&mut wire[at(l)..at(l) + SECTOR_BYTES], &rdk);
        }
        let src = LbaSource {
            base: 500,
            bytes: wire.clone(),
            reads: Vec::new(),
        };
        let map = Arc::new(BusMap::from_files(vec![vec![(500, 2), (700, 4)]]));
        let mut s = BusRemovalSectorSource::new(src, BusStage::AacsHostKey(rdk)).with_bus_map(map);
        let mut got = vec![0u8; 4 * SECTOR_BYTES];
        s.read_sectors(700, 4, &mut got, false).unwrap();
        assert_eq!(
            got,
            clear[at(700)..at(704)],
            "700 de-bussed via head 500; 701..704 clear"
        );
        // Sectors between the extents are not stream content: untouched.
        let mut gap = vec![0u8; SECTOR_BYTES];
        s.read_sectors(600, 1, &mut gap, false).unwrap();
        assert_eq!(
            gap,
            sector(&wire, 500, 600),
            "gap sector outside the file untouched"
        );
    }

    // Overlapping files (SSIF re-listing an m2ts extent) resolve to one span; an
    // unreadable unit head defaults to de-bus (BEE disc norm).
    #[test]
    fn bus_map_overlap_and_unreadable_head() {
        let map = BusMap::from_files(vec![vec![(100, 6)], vec![(97, 12)]]);
        assert_eq!(map.covered_ranges(), vec![(97, 12)]);
        let at = |l| {
            map.locate(l)
                .map(|o| o.map(|x| (x.file, x.unit, x.head_in_span)))
        };
        assert_eq!(at(100), Some(Some((1, 1, Some(100)))), "SSIF span wins");
        assert_eq!(at(108), Some(Some((1, 3, Some(106)))));
        assert_eq!(at(109), None);

        let rdk = [0x52u8; 16];
        let mut clear = clear_content(1);
        clear[0] &= !0xC0;
        let mut wire = clear.clone();
        encrypt_bus(&mut wire, &rdk);
        let mut g = BusGate::new(Arc::new(BusMap::from_files(vec![vec![(10, 3)]])));
        let mut buf = wire.clone();
        g.debus(&mut buf, &rdk, 11, &mut |_| None);
        assert_eq!(buf, clear, "unknown head CPI must de-bus");
    }

    // A unit head that fails to read is fetched once, fast-path (recovery=false),
    // and cached as encrypted: neighbours never re-read it.
    #[test]
    fn host_key_failed_head_fetch_is_cached_as_encrypted() {
        struct BadHead {
            bytes: Vec<u8>,
            reads: Vec<(u32, bool)>,
        }
        impl SectorSource for BadHead {
            fn capacity_sectors(&self) -> u32 {
                1000
            }
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                recovery: bool,
            ) -> Result<usize> {
                self.reads.push((lba, recovery));
                if lba == 300 {
                    return Err(crate::error::Error::DiscRead {
                        sector: 300,
                        status: None,
                        sense: None,
                    });
                }
                let n = count as usize * SECTOR_BYTES;
                buf[..n].copy_from_slice(&self.bytes[..n]);
                Ok(n)
            }
        }
        let rdk = [0x61u8; 16];
        let clear = clear_content(1);
        let mut wire = clear.clone();
        encrypt_bus(&mut wire, &rdk);
        let src = BadHead {
            bytes: wire,
            reads: Vec::new(),
        };
        let map = Arc::new(BusMap::from_files(vec![vec![(300, 3)]]));
        let mut s = BusRemovalSectorSource::new(src, BusStage::AacsHostKey(rdk)).with_bus_map(map);
        let mut got = vec![0u8; SECTOR_BYTES];
        s.read_sectors(301, 1, &mut got, true).unwrap();
        assert_eq!(got, clear, "unreadable head: de-bus");
        s.read_sectors(302, 1, &mut got, true).unwrap();
        assert_eq!(got, clear);
        assert_eq!(
            s.inner().reads,
            vec![(301, true), (300, false), (302, true)],
            "one fast head fetch, then cached"
        );
    }

    // Unrecorded extents keep file offsets: [500,1) + hole(1) + [700,4) puts
    // 700 in unit 0 (head 500) and 701 at the head of unit 1.
    #[test]
    fn bus_map_holes_advance_the_file_offset() {
        let map = BusMap::new(vec![vec![(Some(500), 1), (None, 1), (Some(700), 4)]], &[]);
        let at = |l| {
            map.locate(l)
                .map(|o| o.map(|x| (x.file, x.unit, x.head_in_span)))
        };
        assert_eq!(at(700), Some(Some((0, 0, None))));
        assert_eq!(map.file_lba(0, 0), Some(500));
        assert_eq!(map.file_lba(0, 1), None, "hole has no LBA");
        assert_eq!(at(701), Some(Some((0, 1, Some(701)))));
    }

    // Content of unknown alignment (a title extent no stream file covers) is always
    // de-bussed with no head read; files win where they overlap it.
    #[test]
    fn bus_map_unknown_regions_always_debus_and_yield_to_files() {
        let map = BusMap::new(vec![vec![(Some(100), 6)]], &[(98, 10), (900, 3)]);
        assert_eq!(map.covered_ranges(), vec![(98, 10), (900, 3)]);
        assert!(matches!(map.locate(99), Some(None)));
        assert!(matches!(map.locate(100), Some(Some(_))), "file wins");
        let rdk = [0x73u8; 16];
        let mut clear = clear_content(1);
        clear[0] &= !0xC0; // own byte 0 says clear; must not matter
        let mut wire = clear.clone();
        encrypt_bus(&mut wire, &rdk);
        let mut g = BusGate::new(Arc::new(map));
        let mut buf = wire.clone();
        g.debus(&mut buf, &rdk, 901, &mut |_| {
            panic!("no head read for unknown")
        });
        assert_eq!(buf, clear);
    }

    // Only the reported `n` bytes are de-bussed; a short read must not touch
    // bytes beyond `n`. Also proves capacity delegates.
    #[test]
    fn debus_bounded_by_reported_n_and_capacity_delegates() {
        let rdk = [0x77u8; 16];
        // Two IDENTICAL bus-encrypted sectors on the wire, but the source reports
        // only the FIRST was read. Sector 0 must be de-bussed to plaintext; sector
        // 1 (beyond the reported n) must be left as ciphertext, untouched.
        let clear = clear_content(1);
        let mut enc = clear.clone();
        encrypt_bus(&mut enc, &rdk);
        let mut bytes = enc.clone();
        bytes.extend_from_slice(&enc); // two encrypted sectors

        struct ShortSource {
            bytes: Vec<u8>,
            report: usize,
        }
        impl SectorSource for ShortSource {
            fn capacity_sectors(&self) -> u32 {
                42
            }
            fn read_sectors(
                &mut self,
                _lba: u32,
                _count: u16,
                buf: &mut [u8],
                _recovery: bool,
            ) -> Result<usize> {
                buf[..self.bytes.len()].copy_from_slice(&self.bytes);
                Ok(self.report)
            }
        }
        let src = ShortSource {
            bytes,
            report: SECTOR_BYTES, // only sector 0 "read"
        };
        let mut s = BusRemovalSectorSource::new(src, BusStage::AacsHostKey(rdk));
        assert_eq!(s.capacity_sectors(), 42, "capacity must delegate to inner");
        let mut got = vec![0u8; 2 * SECTOR_BYTES];
        let n = s.read_sectors(0, 2, &mut got, false).unwrap();
        assert_eq!(n, SECTOR_BYTES);
        // Sector 0 was de-bussed to plaintext...
        assert_eq!(
            &got[..SECTOR_BYTES],
            &clear[..],
            "reported sector de-bussed"
        );
        // ...and sector 1 (beyond n) was NOT touched — still ciphertext.
        assert_eq!(
            &got[SECTOR_BYTES..],
            &enc[..],
            "bytes beyond the reported n must stay ciphertext"
        );
    }
}
