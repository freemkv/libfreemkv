use super::*;
use crate::aacs::content::encrypt_bus;

use super::test_support::{Reports, m2ts1, unmapped_paths};

// A wrapper that drops the list would reopen the image path to bus-encrypted bytes.
#[test]
fn unmapped_stream_files_forward_through_every_wrapper() {
    let want = ["/BDMV/STREAM/00001.m2ts"];
    let boxed: Box<dyn SectorSource> = Box::new(Reports(vec![m2ts1()]));
    assert_eq!(unmapped_paths(&boxed), want, "Box<dyn>");
    let mut r = Reports(vec![m2ts1()]);
    let dyn_ref: &mut dyn SectorSource = &mut r;
    assert_eq!(unmapped_paths(&dyn_ref), want, "&mut dyn");
    let mut r = Reports(vec![m2ts1()]);
    let buffered = crate::udf::BufferedSectorReader::new(&mut r, 1);
    assert_eq!(unmapped_paths(&buffered), want, "BufferedSectorReader");
    let pass = BusRemovalSectorSource::new(Reports(vec![m2ts1()]), BusStage::Passthrough);
    assert_eq!(
        unmapped_paths(&pass),
        want,
        "BusRemovalSectorSource defers to inner"
    );
}

// A host-key adapter answers from its own map, which is what it failed to de-bus.
#[test]
fn bus_removal_source_reports_its_own_maps_unmapped_files() {
    let map = BusMap::from_ranges(&[(300, 3)]).with_unmapped(vec![m2ts1()]);
    let s = BusRemovalSectorSource::new(Reports(Vec::new()), BusStage::AacsHostKey([1; 16]))
        .with_bus_map(Arc::new(map));
    assert_eq!(unmapped_paths(&s), ["/BDMV/STREAM/00001.m2ts"]);
}

/// Per spec; do not change without a spec citation proving otherwise. AACS BD Pre-recorded
/// 0.953 §3.7 Note: "PC Host shall decrypt bus-encrypted Clip AV stream file" — every
/// unlocated file is named, and none means the image may proceed.
#[test]
fn ensure_image_debussable_names_every_unmapped_file() {
    assert!(ensure_image_debussable(&Reports(Vec::new())).is_ok());
    let ssif = UnmappedStreamFile::new(
        "/BDMV/STREAM/SSIF/00003.ssif".into(),
        42,
        &Error::UdfAdChainTooLong,
    );
    let err = ensure_image_debussable(&Reports(vec![m2ts1(), ssif])).unwrap_err();
    assert_eq!(
        err.to_string(),
        "E6021: /BDMV/STREAM/00001.m2ts (E6016), /BDMV/STREAM/SSIF/00003.ssif (E6016)"
    );
}

// The Debug output wraps a Read Data Key: it must never print the bytes.
#[test]
fn bus_stage_debug_redacts_the_read_data_key() {
    let out = format!("{:?}", BusStage::AacsHostKey([0xAB; 16]));
    assert_eq!(out, "BusStage::AacsHostKey([redacted])");
    assert_eq!(
        format!("{:?}", BusStage::Passthrough),
        "BusStage::Passthrough"
    );
}

// Records the fua flag of every read; the default read_sectors_fua drops it.
struct FuaProbe {
    fua: Vec<bool>,
}
impl SectorSource for FuaProbe {
    fn capacity_sectors(&self) -> u32 {
        1000
    }
    fn read_sectors(&mut self, _: u32, count: u16, buf: &mut [u8], _: bool) -> Result<usize> {
        let n = count as usize * SECTOR_BYTES;
        buf[..n].fill(0);
        Ok(n)
    }
    fn read_sectors_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> Result<usize> {
        self.fua.push(fua);
        self.read_sectors(lba, count, buf, recovery)
    }
}

// Pass-N FUA recovery must reach the drive through the de-bus decorator.
#[test]
fn read_sectors_fua_forwards_fua_to_the_inner_source() {
    let mut s = BusRemovalSectorSource::new(FuaProbe { fua: vec![] }, BusStage::Passthrough);
    let mut buf = vec![0u8; SECTOR_BYTES];
    s.read_sectors_fua(5, 1, &mut buf, true, true).unwrap();
    s.read_sectors_fua(5, 1, &mut buf, true, false).unwrap();
    assert_eq!(s.inner().fua, [true, false]);
}

// The wrapper is transparent to the inner source's capabilities and speed control.
#[test]
fn bus_removal_forwards_random_access_and_set_speed() {
    struct Seq {
        speed: u16,
    }
    impl SectorSource for Seq {
        fn capacity_sectors(&self) -> u32 {
            0
        }
        fn read_sectors(&mut self, _: u32, _: u16, _: &mut [u8], _: bool) -> Result<usize> {
            Ok(0)
        }
        fn random_access(&self) -> bool {
            false
        }
        fn set_speed(&mut self, kbs: u16) {
            self.speed = kbs;
        }
    }
    let mut s = BusRemovalSectorSource::new(Seq { speed: 0 }, BusStage::Passthrough);
    assert!(!s.random_access());
    s.set_speed(4500);
    assert_eq!(s.inner().speed, 4500);
}

// An extent running past the 32-bit LBA space is clipped at it, never wrapped.
#[test]
fn bus_map_extent_past_the_lba_space_is_clipped() {
    let map = BusMap::from_files(vec![vec![(0xFFFF_FFF0, 100)]]);
    assert_eq!(map.covered_ranges(), vec![(0xFFFF_FFF0, 16)]);
}

// A partly overlapping second file keeps its file offset across the clip.
#[test]
fn bus_map_partial_overlap_clip_keeps_the_file_offset() {
    let map = BusMap::from_files(vec![vec![(100, 6)], vec![(103, 12)]]);
    let at = |l| {
        map.locate(l)
            .map(|o| o.map(|x| (x.file, x.unit, x.head_in_span)))
    };
    assert_eq!(at(105), Some(Some((0, 1, Some(103)))));
    assert_eq!(at(109), Some(Some((1, 2, Some(109)))));
}

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
    let mut s =
        BusRemovalSectorSource::new(src, BusStage::AacsHostKey(rdk)).with_content_ranges(ranges);
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
    let mut s =
        BusRemovalSectorSource::new(src, BusStage::AacsHostKey(rdk)).with_content_ranges(ranges);
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
    let mut s =
        BusRemovalSectorSource::new(src, BusStage::AacsHostKey(rdk)).with_content_ranges(ranges);
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

// A unit head that fails to read is fetched fast-path (recovery=false) and the
// unit guessed encrypted; the guess is not remembered, so each read retries.
#[test]
fn host_key_failed_head_fetch_guesses_encrypted_without_caching() {
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
        vec![(301, true), (300, false), (302, true), (300, false)],
        "the unreadable head is retried, never cached"
    );
}

// A head that was unreadable once and is clear when later read: the guess made
// meanwhile must not decide the whole unit.
#[test]
fn host_key_head_guess_is_replaced_by_the_measured_head() {
    let rdk = [0x62u8; 16];
    let mut clear = clear_content(3); // LBAs 300..303
    for l in 0..3 {
        clear[l * SECTOR_BYTES] &= !0xC0;
    }
    let mut wire = clear.clone();
    encrypt_bus(&mut wire, &rdk);
    let map = Arc::new(BusMap::from_files(vec![vec![(300, 3)]]));
    let mut g = BusGate::new(map);
    // First pass: sector 301 alone, head 300 unreadable -> guessed encrypted.
    let mut b = sector(&wire, 300, 301).to_vec();
    g.debus(&mut b, &rdk, 301, &mut |_| None);
    // Second pass: the whole unit with its real (clear) head.
    let mut all = clear.clone();
    g.debus(&mut all, &rdk, 300, &mut |_| {
        unreachable!("head is in the buffer")
    });
    assert_eq!(all, clear, "a clear unit must pass through untouched");
}

// Any copy-permission bit set means encrypted (CPI 01/10/11), not just the top bit.
#[test]
fn host_key_any_cpi_bit_marks_the_unit_encrypted() {
    let rdk = [0x63u8; 16];
    for (head, encrypted) in [(0x00u8, false), (0x40, true), (0x80, true), (0xC0, true)] {
        let mut clear = clear_content(3);
        clear[0] = (clear[0] & !0xC0) | head;
        let mut wire = clear.clone();
        if encrypted {
            encrypt_bus(&mut wire, &rdk);
        }
        let mut g = BusGate::new(Arc::new(BusMap::from_files(vec![vec![(300, 3)]])));
        g.debus(&mut wire, &rdk, 300, &mut |_| None);
        assert_eq!(wire, clear, "head byte0 {head:#x}");
    }
}

// The decision is kept per (file, unit): interleaved reads of two files' mid-unit
// sectors fetch each head once, and unit 0 of one file does not decide the other's.
#[test]
fn host_key_decisions_are_keyed_by_file_and_unit() {
    let rdk = [0x64u8; 16];
    let mut clear = clear_content(6); // file 0 = 300..303 (CPI set), file 1 = 303..306 (clear)
    clear[3 * SECTOR_BYTES] &= !0xC0;
    let mut wire = clear.clone();
    encrypt_bus(&mut wire[..3 * SECTOR_BYTES], &rdk);
    let map = Arc::new(BusMap::from_files(vec![vec![(300, 3)], vec![(303, 3)]]));
    let mut g = BusGate::new(map);
    let mut heads = Vec::new();
    for lba in [301u32, 304, 302, 305] {
        let mut b = sector(&wire, 300, lba).to_vec();
        g.debus(&mut b, &rdk, lba, &mut |h| {
            heads.push(h);
            Some(sector(&clear, 300, h)[0])
        });
        assert_eq!(b, sector(&clear, 300, lba), "lba {lba}");
    }
    assert_eq!(heads, vec![300, 303], "each head fetched once");
}

// The per-unit cache is bounded: it is cleared rather than growing with the disc.
#[test]
fn host_key_cpi_cache_stays_bounded() {
    let units = CPI_CACHE_MAX as u32 + 8;
    let map = Arc::new(BusMap::from_files(vec![vec![(0, units * 3)]]));
    let mut g = BusGate::new(map);
    let rdk = [0x65u8; 16];
    let mut buf = clear_content(1);
    buf[0] &= !0xC0;
    for u in 0..units {
        g.debus(&mut buf, &rdk, u * 3 + 1, &mut |_| Some(0));
        assert!(g.cpi.len() <= CPI_CACHE_MAX);
    }
    assert!(g.cpi.len() < units as usize);
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
