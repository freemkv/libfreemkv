//! `BusRemovalSectorSource` — a stream form of AACS bus-encryption removal.
//!
//! # Where bus removal actually happens
//! AACS 2.0 "bus encryption" (BEE) wraps content on the drive↔host transport,
//! and removing it is the UNLOCKER's job. The single de-bus TRANSFORM is
//! [`decrypt_bus_in_content`]; in the PRODUCTION path it is applied once, at the
//! very bottom of the read chain, inside the live drive's own `SectorSource`
//! impl (`Drive::remove_bus_encryption`, gated by the drive's `BusStage` +
//! content ranges). Every reader above it (key sampler, mux, unit-key
//! validation, whole-disc sweep) reads the `Drive` as `&mut dyn SectorSource`
//! and so transparently sees bus-removed sectors and never reasons about bus
//! encryption.
//!
//! This decorator is an equivalent, independently-tested STREAM form of that
//! same transform (it also calls [`decrypt_bus_in_content`]), kept for tests and
//! for any future non-`Drive` source that needs de-bussing. It is NOT wired into
//! the production chain today. DANGER: never wrap the live `Drive` in this
//! decorator — the `Drive` already de-busses, so a second pass would DOUBLE-
//! de-bus and corrupt content. There must only ever be ONE de-bus in a chain.
//!
//! # How the stage is chosen
//! [`BusStage`] is computed ONCE from the unlock result:
//!   * a firmware/vendor unlocker (freemkv / LibreDrive / Renesas) removes bus
//!     encryption AT THE DRIVE (`Bus=off`), and a non-bus disc (DVD / clear BD)
//!     has none → [`BusStage::Passthrough`] (reads pass through untouched);
//!   * the AACS cert route yields the Read Data Key from the AKE, so the host
//!     must de-bus each content sector → [`BusStage::AacsHostKey`].
//!
//! # Content gating
//! De-bussing a clear UDF/nav sector would corrupt plaintext, so the host-key
//! path is gated to the disc's encrypted-content extents via
//! [`with_content_ranges`](BusRemovalSectorSource::with_content_ranges) — the
//! same `(start_lba, sector_count)` map [`DecryptingSectorSource`] uses. Absent
//! a map, the caller reads content-only (title extents) and every sector is
//! treated as content — matching the pre-overhaul behaviour where `decrypt_bus`
//! ran on content units only.
//!
//! [`decrypt_bus_in_content`]: crate::aacs::content::decrypt_bus_in_content
//! [`DecryptingSectorSource`]: crate::sector::decrypting::DecryptingSectorSource

use std::sync::Arc;

use crate::error::Result;

use super::SectorSource;

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
    /// Encrypted-content extent map (sorted/merged `(start_lba, sector_count)`).
    /// `None` = caller reads content-only, so every sector is de-bussed;
    /// `Some` = a whole-disc reader, so clear sectors outside these ranges pass
    /// through untouched. Ignored entirely by [`BusStage::Passthrough`].
    content_ranges: Option<Arc<[(u32, u32)]>>,
}

impl<S: SectorSource> BusRemovalSectorSource<S> {
    /// Wrap `inner` with the bus stage decided by the unlock result.
    pub fn new(inner: S, stage: BusStage) -> Self {
        Self {
            inner,
            stage,
            content_ranges: None,
        }
    }

    /// Restrict host-key de-bussing to the disc's encrypted-content extents;
    /// clear sectors outside them pass through untouched. Whole-disc readers
    /// (sweep / patch) set this; content-only readers (mux title extents,
    /// sampler) leave it unset.
    pub fn with_content_ranges(mut self, ranges: Arc<[(u32, u32)]>) -> Self {
        self.content_ranges = Some(ranges);
        self
    }

    /// `&mut` counterpart of [`with_content_ranges`](Self::with_content_ranges),
    /// for readers that build the stream before the content map is known.
    pub fn set_content_ranges(&mut self, ranges: Arc<[(u32, u32)]>) {
        self.content_ranges = Some(ranges);
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
        if let BusStage::AacsHostKey(rdk) = &self.stage {
            crate::aacs::content::decrypt_bus_in_content(
                &mut buf[..n],
                rdk,
                lba,
                self.content_ranges.as_deref(),
            );
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
    use crate::consts::SECTOR_BYTES;

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
