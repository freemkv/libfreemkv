//! Test fixtures for libfreemkv and its consumers (feature `test-util`; also built
//! for this crate's own unit tests). Never enabled in a release build.
//!
//! - [`unit_key_ro`]: a spec-laid-out `Unit_Key_RO.inf` (KS-12, KS-13, KS-14).
//! - [`aacs_state`]: the one sanctioned way to build an [`AacsState`] outside the
//!   library (consumer guard tests ban `AacsState { .. }` literals).
//! - [`encrypted_bd_image`]: a real UDF image whose stream files are AACS-encrypted
//!   per aligned unit on each file's own grid (KS-1, KS-2, KS-3).
//! - [`CountingSource`]: a [`SectorSource`] wrapper that logs every read.
//! - [`decrypt_unit`]: the aligned-unit decrypt, for tests that check ciphertext.

use crate::aacs::content::{ALIGNED_UNIT_LEN, encrypt_unit};
use crate::aacs::mkb::AacsVersion;
use crate::consts::{BD_SOURCE_PACKET_BYTES, SECTOR_BYTES};
use crate::disc::{AacsState, KeyOrigin};
use crate::error::Result;
use crate::sector::SectorSource;
use std::sync::{Arc, Mutex};

/// Bytes before `Unit_Key_Block()` that hold the start address and the reserved
/// field: KS-12, "Unit_Key_Block_start_address … 32" then "Reserved for future use 96".
const HEADER_START: usize = 16;

/// A `Unit_Key_RO.inf` holding `encrypted_keys` (CPS units 1..=n) and one Title per
/// `title_cps` entry (its 1-based CPS unit number, KS-11), laid out per KS-12/13/14:
/// the header at byte 16, First Playback and Top Menu in CPS unit 1, then
/// `Unit_Key_Block()` at a 16-byte-aligned start address with each key after its two
/// 16-byte MACs. `version` sets the key stride (48 for AACS 1.0; 64 for 2.x, KS-25).
pub fn unit_key_ro(
    version: AacsVersion,
    encrypted_keys: &[[u8; 16]],
    title_cps: &[u16],
) -> Vec<u8> {
    let n_keys = u16::try_from(encrypted_keys.len()).expect("at most 65535 CPS units");
    let n_titles = u16::try_from(title_cps.len()).expect("at most 65535 titles");
    let header_end = HEADER_START + 10 + 4 * title_cps.len();
    let start = header_end.next_multiple_of(16);
    let stride = match version {
        AacsVersion::V10 => 48,
        AacsVersion::V20 | AacsVersion::V21 => 64,
    };
    let mut v = vec![0u8; start + 48 + stride * encrypted_keys.len().max(1)];
    v[..4].copy_from_slice(&(start as u32).to_be_bytes());
    // KS-13: Application_Type (= 01₁₆), Num_of_BD_Directory (= 01₁₆); FP @20, TM @22.
    v[HEADER_START] = 1;
    v[HEADER_START + 1] = 1;
    v[HEADER_START + 4..HEADER_START + 6].copy_from_slice(&1u16.to_be_bytes());
    v[HEADER_START + 6..HEADER_START + 8].copy_from_slice(&1u16.to_be_bytes());
    v[HEADER_START + 8..HEADER_START + 10].copy_from_slice(&n_titles.to_be_bytes());
    for (i, cps) in title_cps.iter().enumerate() {
        // KS-13: "(reserved) 16 … CPS_Unit_number for Title#J in Directory #I 16".
        let at = HEADER_START + 10 + 4 * i + 2;
        v[at..at + 2].copy_from_slice(&cps.to_be_bytes());
    }
    v[start..start + 2].copy_from_slice(&n_keys.to_be_bytes());
    for (i, key) in encrypted_keys.iter().enumerate() {
        // KS-14: 16-byte block head, "MAC of PMSN#I 128 … MAC of Device Binding Nonce#I 128".
        let at = start + 48 + stride * i;
        v[at..at + 16].copy_from_slice(key);
    }
    v
}

/// Builds an [`AacsState`] for a test: start from [`aacs_state`], set what the test
/// needs, then [`AacsStateBuilder::build`]. Defaults: AACS 1.0, no bus encryption,
/// no MKB version, empty disc hash, [`KeyOrigin::ExternalUk`], no VUK, no unit keys,
/// a zero Volume ID, and empty `uk_ro` / `mkb`.
#[must_use]
pub struct AacsStateBuilder {
    state: AacsState,
}

/// Start an [`AacsStateBuilder`] with the defaults it documents.
pub fn aacs_state() -> AacsStateBuilder {
    AacsStateBuilder {
        state: AacsState {
            version: 1,
            bus_encryption: false,
            mkb_version: None,
            disc_hash: String::new(),
            key_source: KeyOrigin::ExternalUk,
            vuk: None,
            unit_keys: Vec::new(),
            volume_id: [0u8; 16],
            uk_ro: Vec::new(),
            mkb: Vec::new(),
        },
    }
}

impl AacsStateBuilder {
    pub fn version(mut self, version: u8) -> Self {
        self.state.version = version;
        self
    }
    pub fn bus_encryption(mut self, on: bool) -> Self {
        self.state.bus_encryption = on;
        self
    }
    pub fn mkb_version(mut self, v: Option<u32>) -> Self {
        self.state.mkb_version = v;
        self
    }
    pub fn disc_hash(mut self, hash: impl Into<String>) -> Self {
        self.state.disc_hash = hash.into();
        self
    }
    pub fn key_source(mut self, origin: KeyOrigin) -> Self {
        self.state.key_source = origin;
        self
    }
    pub fn vuk(mut self, vuk: Option<[u8; 16]>) -> Self {
        self.state.vuk = vuk;
        self
    }
    pub fn unit_keys(mut self, keys: Vec<(u32, [u8; 16])>) -> Self {
        self.state.unit_keys = keys;
        self
    }
    pub fn volume_id(mut self, vid: [u8; 16]) -> Self {
        self.state.volume_id = vid;
        self
    }
    pub fn uk_ro(mut self, bytes: Vec<u8>) -> Self {
        self.state.uk_ro = bytes;
        self
    }
    pub fn mkb(mut self, bytes: Vec<u8>) -> Self {
        self.state.mkb = bytes;
        self
    }
    pub fn build(self) -> AacsState {
        self.state
    }
}

/// One stream file of an [`encrypted_bd_image`]: its path under the image root
/// (e.g. `"BDMV/STREAM/00001.m2ts"`), its length in sectors (a multiple of 3 for
/// whole aligned units), and the CPS unit key its units are encrypted with
/// (`None`: clear content, every Copy_permission_indicator `00₂`).
#[derive(Debug, Clone)]
pub struct BdFile {
    pub path: String,
    pub sectors: u32,
    pub key: Option<[u8; 16]>,
}

impl BdFile {
    pub fn new(path: impl Into<String>, sectors: u32, key: Option<[u8; 16]>) -> Self {
        Self {
            path: path.into(),
            sectors,
            key,
        }
    }
}

/// An image built by [`encrypted_bd_image`]. `image` is what a drive serves; `plain`
/// is the same image before encryption (a keyed unit still carries CPI `11₂`, KS-5,
/// so compare a CPI-clearing decrypt with byte 0 of each 192-byte packet masked by
/// `0x3F`); `files[i]` is `(start_lba, sectors)` of the i-th [`BdFile`].
#[derive(Clone)]
pub struct EncryptedBdImage {
    pub image: Vec<u8>,
    pub plain: Vec<u8>,
    pub files: Vec<(u32, u32)>,
}

impl EncryptedBdImage {
    /// A [`SectorSource`] serving `image` (reads past the end fail).
    pub fn source(&self) -> MemSource {
        MemSource {
            data: Arc::new(self.image.clone()),
        }
    }
}

/// An in-memory [`SectorSource`] over a byte image.
#[derive(Clone)]
pub struct MemSource {
    data: Arc<Vec<u8>>,
}

impl MemSource {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data: Arc::new(data),
        }
    }
}

impl SectorSource for MemSource {
    fn capacity_sectors(&self) -> u32 {
        (self.data.len() / SECTOR_BYTES) as u32
    }
    fn read_sectors(&mut self, lba: u32, count: u16, buf: &mut [u8], _: bool) -> Result<usize> {
        let at = lba as usize * SECTOR_BYTES;
        let n = count as usize * SECTOR_BYTES;
        let Some(src) = self.data.get(at..at + n) else {
            return Err(crate::error::Error::DiscRead {
                sector: lba as u64,
                status: None,
                sense: None,
            });
        };
        buf[..n].copy_from_slice(src);
        Ok(n)
    }
}

/// A UDF image (laid out by [`crate::DirImage`]) holding `/AACS/Unit_Key_RO.inf` =
/// `uk_ro` and each of `files`. Every aligned unit of a file, counted from the file's
/// first sector (KS-1), is 32 source packets of TS (KS-2) with a varied payload; a
/// keyed file's units carry CPI `11₂` in every packet and are then encrypted
/// (KS-3, KS-4). Panics on a fixture error; test use only.
pub fn encrypted_bd_image(files: &[BdFile], uk_ro: &[u8]) -> EncryptedBdImage {
    let root = fixture_dir();
    write(&root.join("AACS/Unit_Key_RO.inf"), uk_ro);
    for f in files {
        write(
            &root.join(&f.path),
            &vec![0u8; f.sectors as usize * SECTOR_BYTES],
        );
    }
    let mut img = crate::DirImage::open(&root).expect("fixture DirImage");
    let fs = crate::udf::read_filesystem(&mut img).expect("fixture UDF");
    let extents: Vec<(u32, u32)> = files
        .iter()
        .map(|f| {
            let e = fs.file_extents(&mut img, &format!("/{}", f.path));
            let e = e.expect("fixture file extents");
            assert_eq!(e.len(), 1, "{}: one contiguous extent", f.path);
            e[0]
        })
        .collect();
    let cap = img.capacity_sectors();
    let mut image = vec![0u8; cap as usize * SECTOR_BYTES];
    for (lba, chunk) in image.chunks_mut(SECTOR_BYTES).enumerate() {
        img.read_sectors(lba as u32, 1, chunk, false)
            .expect("fixture read");
    }
    drop(img);
    let _ = std::fs::remove_dir_all(&root);
    let mut plain = image.clone();
    let unit_sectors = (ALIGNED_UNIT_LEN / SECTOR_BYTES) as u32;
    for (f, &(start, sectors)) in files.iter().zip(&extents) {
        for u in 0..sectors / unit_sectors {
            let lba = start + u * unit_sectors;
            let mut unit = content_unit(lba, f.key.is_some());
            let at = lba as usize * SECTOR_BYTES;
            plain[at..at + ALIGNED_UNIT_LEN].copy_from_slice(&unit);
            if let Some(key) = &f.key {
                assert!(encrypt_unit(&mut unit, key), "whole aligned unit");
            }
            image[at..at + ALIGNED_UNIT_LEN].copy_from_slice(&unit);
        }
    }
    EncryptedBdImage {
        image,
        plain,
        files: extents,
    }
}

// One aligned unit of clear TS: TP_extra_header + 0x47 sync per 192-byte packet, a
// payload varied by `lba`, and CPI 11₂ (encrypted) or 00₂ in every packet (KS-5).
fn content_unit(lba: u32, encrypted: bool) -> Vec<u8> {
    let mut u: Vec<u8> = (0..ALIGNED_UNIT_LEN as u32)
        .map(|i| (lba.wrapping_mul(7919).wrapping_add(i) % 251) as u8 | 1)
        .collect();
    for p in u.chunks_mut(BD_SOURCE_PACKET_BYTES) {
        p[0] = if encrypted { p[0] | 0xC0 } else { p[0] & 0x3F };
        p[4] = 0x47;
    }
    u
}

// A fresh, empty directory for one fixture (removed once the image is read).
fn fixture_dir() -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("libfreemkv-test-util-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("fixture dir");
    dir
}

fn write(path: &std::path::Path, bytes: &[u8]) {
    std::fs::create_dir_all(path.parent().expect("fixture path has a parent")).expect("mkdir");
    std::fs::write(path, bytes).expect("fixture write");
}

/// The shared read log of a [`CountingSource`]: `(lba, count)` per read, in order.
/// Clone it before the source moves into a reader; it sees every later read.
#[derive(Clone, Default)]
pub struct ReadLog(Arc<Mutex<Vec<(u32, u16)>>>);

impl ReadLog {
    pub fn reads(&self) -> Vec<(u32, u16)> {
        self.0.lock().expect("read log").clone()
    }
    pub fn count(&self) -> usize {
        self.0.lock().expect("read log").len()
    }
    /// Whether any logged read covered a sector in `[start, end)`.
    pub fn touched(&self, start: u32, end: u32) -> bool {
        self.reads()
            .iter()
            .any(|&(lba, n)| lba < end && lba as u64 + n as u64 > start as u64)
    }
    pub fn clear(&self) {
        self.0.lock().expect("read log").clear();
    }
}

/// A [`SectorSource`] that forwards to `inner` and logs every read into [`Self::log`].
pub struct CountingSource<S> {
    inner: S,
    log: ReadLog,
}

impl<S: SectorSource> CountingSource<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            log: ReadLog::default(),
        }
    }
    pub fn log(&self) -> ReadLog {
        self.log.clone()
    }
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: SectorSource> SectorSource for CountingSource<S> {
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
        self.log.0.lock().expect("read log").push((lba, count));
        self.inner.read_sectors(lba, count, buf, recovery)
    }
    fn read_sectors_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> Result<usize> {
        self.log.0.lock().expect("read log").push((lba, count));
        self.inner.read_sectors_fua(lba, count, buf, recovery, fua)
    }
    fn set_speed(&mut self, kbs: u16) {
        self.inner.set_speed(kbs)
    }
    fn set_unit_base(&mut self, lba: u32) {
        self.inner.set_unit_base(lba)
    }
}

/// Decrypt one 6144-byte aligned unit in place with a CPS unit key (KS-3, KS-4). The
/// test-side door to the unit decrypt, so no consumer test needs the internal one.
pub fn decrypt_unit(unit: &mut [u8], unit_key: &[u8; 16]) {
    crate::aacs::content::decrypt_unit(unit, unit_key)
}

#[cfg(test)]
#[path = "test_util_tests.rs"]
mod tests;
