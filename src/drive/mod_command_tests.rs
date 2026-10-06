use super::*;
use crate::scsi::{DataDirection, ScsiResult, ScsiTransport};
use std::sync::atomic::{AtomicBool, Ordering};

// Minimal MODE SENSE(10) reply carrying the Error Recovery page (0x01)
// with the given flags/retry, no block descriptors. `ps` sets the page's
// PS bit (SENSE-only), which the SELECT payload must clear.
fn mode_sense_error_recovery(flags: u8, retry: u8, ps: bool) -> Vec<u8> {
    let mut v = vec![0u8; MODE10_HEADER_LEN + 12];
    // Header: nonzero mode-data-length (must be zeroed on SELECT); no block
    // descriptors.
    v[0] = 0x00;
    v[1] = 0x22;
    v[6] = 0x00;
    v[7] = 0x00; // block descriptor length = 0
    let po = MODE10_HEADER_LEN;
    v[po] = MODE_PAGE_ERROR_RECOVERY | if ps { MODE_PAGE_PS_BIT } else { 0 };
    v[po + 1] = 0x0A; // page length
    v[po + 2] = flags; // error-recovery flags
    v[po + 3] = retry; // read retry count
    v
}

#[test]
fn error_recovery_payload_sets_per_tb_clears_dte_ps_preserves_retry() {
    // Start with PER off, DTE on, PS on, a specific retry count. The SELECT
    // payload must flip PER on, TB on, DTE off, clear PS, zero the header
    // mode-data-length, and leave the retry count untouched.
    let sense = mode_sense_error_recovery(ERP_FLAG_DTE, 0x2C, true);
    let out = build_error_recovery_select_payload(&sense).expect("valid page");
    let po = MODE10_HEADER_LEN;
    assert_eq!(out[0], 0, "header mode-data-length zeroed for SELECT");
    assert_eq!(out[1], 0);
    assert_eq!(out[po] & MODE_PAGE_PS_BIT, 0, "PS cleared for SELECT");
    assert_eq!(out[po] & 0x3F, MODE_PAGE_ERROR_RECOVERY, "still page 0x01");
    assert_eq!(out[po + 2] & ERP_FLAG_PER, ERP_FLAG_PER, "PER set");
    assert_eq!(
        out[po + 2] & ERP_FLAG_TB,
        ERP_FLAG_TB,
        "TB set (still get data)"
    );
    assert_eq!(
        out[po + 2] & ERP_FLAG_DTE,
        0,
        "DTE cleared (don't terminate)"
    );
    assert_eq!(out[po + 3], 0x2C, "read retry count preserved");
}

#[test]
fn error_recovery_payload_honors_block_descriptor_offset() {
    // With an 8-byte block descriptor between header and page, the function
    // must locate the page at header+desc, not a fixed offset.
    let mut sense = vec![0u8; MODE10_HEADER_LEN + 8 + 12];
    sense[1] = (MODE10_HEADER_LEN + 8 + 12 - 2) as u8; // mode data length
    sense[7] = 8; // block descriptor length
    let po = MODE10_HEADER_LEN + 8;
    sense[po] = MODE_PAGE_ERROR_RECOVERY;
    sense[po + 1] = 0x0A;
    sense[po + 2] = 0x00;
    let out = build_error_recovery_select_payload(&sense).expect("valid");
    assert_eq!(
        out[po + 2] & ERP_FLAG_PER,
        ERP_FLAG_PER,
        "PER set at the descriptor-offset page"
    );
}

#[test]
fn error_recovery_payload_rejects_wrong_or_short_page() {
    // Wrong page code → None (leave drive at defaults).
    let mut wrong = mode_sense_error_recovery(0, 0, false);
    wrong[MODE10_HEADER_LEN] = 0x08; // page 0x08 (caching), not 0x01
    assert!(build_error_recovery_select_payload(&wrong).is_none());
    // Too short to hold the header → None, no panic.
    assert!(build_error_recovery_select_payload(&[0u8; 4]).is_none());
    // Header claims a block descriptor that runs off the buffer → None.
    let mut bad = mode_sense_error_recovery(0, 0, false);
    bad[7] = 0xF0; // descriptor length way past the buffer
    assert!(build_error_recovery_select_payload(&bad).is_none());
}

// A bridge that ignores residue reports the whole 252-byte allocation; the
// SELECT parameter list must stop at Mode Data Length + 2 (SPC-4 §7.5.5), or
// the trailing zeros are sent as bogus page-0 descriptors and rejected.
#[test]
fn error_recovery_payload_stops_at_mode_data_length() {
    let mut sense = mode_sense_error_recovery(0, 0x05, false);
    sense[1] = (MODE10_HEADER_LEN + 12 - 2) as u8;
    sense.resize(252, 0);
    let out = build_error_recovery_select_payload(&sense).expect("valid page");
    assert_eq!(out.len(), MODE10_HEADER_LEN + 12, "parameter list length");
    // A Mode Data Length that cuts the page header short is malformed.
    sense[1] = (MODE10_HEADER_LEN + 2 - 2) as u8;
    assert!(build_error_recovery_select_payload(&sense).is_none());
}

// Boundary for the 3-byte page header guard (page_off + 3 > sense.len()):
// exactly-enough length must be accepted, not rejected by a `>=` mutation.
#[test]
fn error_recovery_payload_accepts_exact_minimum_length() {
    // No block descriptors: page starts right after the 8-byte header.
    // Page needs exactly 3 bytes (code, length, flags) -> total 11.
    let mut sense = vec![0u8; MODE10_HEADER_LEN + 3];
    sense[1] = (MODE10_HEADER_LEN + 3 - 2) as u8; // mode data length
    let po = MODE10_HEADER_LEN;
    sense[po] = MODE_PAGE_ERROR_RECOVERY;
    sense[po + 1] = 0x00; // page length field (unused by this function)
    sense[po + 2] = ERP_FLAG_DTE; // flags: DTE on, PER/TB off
    let out = build_error_recovery_select_payload(&sense)
        .expect("page_off + 3 == len is exactly enough room, must be accepted");
    assert_eq!(out[po + 2] & ERP_FLAG_PER, ERP_FLAG_PER, "PER set");
    assert_eq!(out[po + 2] & ERP_FLAG_DTE, 0, "DTE cleared");
}

/// Mock transport: returns a fixed data payload (copied into the
/// caller's buffer, truncated to fit) on every `execute()`.
struct FixedTransport {
    payload: Vec<u8>,
}

impl ScsiTransport for FixedTransport {
    fn execute(
        &mut self,
        _cdb: &[u8],
        _direction: DataDirection,
        data: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        let n = self.payload.len().min(data.len());
        data[..n].copy_from_slice(&self.payload[..n]);
        Ok(ScsiResult {
            status: 0,
            bytes_transferred: n,
            sense: [0u8; 32],
        })
    }
}

fn drive_with(payload: Vec<u8>) -> Drive {
    Drive::from_transport_for_test(Box::new(FixedTransport { payload }))
}

// AACS bus-removal tests (the drive's single de-bus point): a clear "content"
// buffer with plaintext first-16-of-each-sector (bus enc leaves those clear on
// the wire) and a known 16..2048 pattern; `sectors` exercises cross-sector gating.
fn clear_bus_content(sectors: usize) -> Vec<u8> {
    let mut v = vec![0u8; sectors * 2048];
    for (i, b) in v.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(31).wrapping_add(7);
    }
    // Copy-permission bits set on every sector head: encrypted AACS units.
    for sector in v.chunks_mut(2048) {
        sector[0] |= 0xC0;
    }
    v
}

// Passthrough (the default) is a no-op: every read byte comes back verbatim,
// even when the wire happens to be clear content.
#[test]
fn bus_passthrough_read_returns_bytes_unchanged() {
    use crate::sector::SectorSource;
    let clear = clear_bus_content(3);
    let mut d = drive_with(clear.clone());
    let mut got = vec![0u8; clear.len()];
    let n = d.read_sectors(0, 3, &mut got, false).unwrap();
    assert_eq!(n, clear.len());
    assert_eq!(got, clear, "default Passthrough must not alter any byte");
}

// Host-key stage de-busses at read time: a bus-ENCRYPTED wire comes back as
// the known plaintext. MUTATION: leaving `bus_stage` Passthrough (dropping the
// de-bus call) returns ciphertext, so this goes red.
#[test]
fn bus_host_key_removes_bus_encryption_at_read() {
    use crate::sector::SectorSource;
    let rdk = [0x5Au8; 16];
    let clear = clear_bus_content(3);
    let mut wire = clear.clone();
    crate::aacs::content::encrypt_bus(&mut wire, &rdk); // model the drive's forward bus transform
    assert_ne!(wire, clear, "fixture must actually be bus-encrypted");

    let mut d = drive_with(wire);
    d.set_bus_stage(crate::sector::bus_removal::BusStage::AacsHostKey(rdk));
    let mut got = vec![0u8; clear.len()];
    let n = d.read_sectors(0, 3, &mut got, false).unwrap();
    assert_eq!(n, clear.len());
    assert_eq!(
        got, clear,
        "AacsHostKey must recover the plaintext byte-for-byte"
    );
}

// The FUA read path applies the same single de-bus step (the per-sector
// recovery lever must not ship ciphertext).
#[test]
fn bus_host_key_removes_bus_encryption_on_fua_read() {
    use crate::sector::SectorSource;
    let rdk = [0x5Au8; 16];
    let clear = clear_bus_content(3);
    let mut wire = clear.clone();
    crate::aacs::content::encrypt_bus(&mut wire, &rdk);

    let mut d = drive_with(wire);
    d.set_bus_stage(crate::sector::bus_removal::BusStage::AacsHostKey(rdk));
    let mut got = vec![0u8; clear.len()];
    let n = d.read_sectors_fua(0, 3, &mut got, false, true).unwrap();
    assert_eq!(n, clear.len());
    assert_eq!(got, clear, "the FUA path must de-bus too");
}

// Content-range gating: a sector OUTSIDE the encrypted-content extents is
// clear filesystem/nav and must pass through untouched — de-bussing it would
// corrupt plaintext. LBA 0 is outside content [300,303).
#[test]
fn bus_content_ranges_pass_through_sectors_outside_content() {
    use crate::sector::SectorSource;
    let rdk = [0x5Au8; 16];
    let clear = clear_bus_content(1); // clear filesystem sector on the wire
    let mut d = drive_with(clear.clone());
    d.set_bus_stage(crate::sector::bus_removal::BusStage::AacsHostKey(rdk));
    d.set_bus_content_ranges(std::sync::Arc::from(
        vec![(300u32, 3u32)].into_boxed_slice(),
    ));
    let mut got = vec![0u8; clear.len()];
    // LBA 0 is outside content → no de-bus → bytes unchanged.
    let n = d.read_sectors(0, 1, &mut got, false).unwrap();
    assert_eq!(n, clear.len());
    assert_eq!(
        got, clear,
        "a sector outside content must pass through untouched"
    );
}

// The complement: a bus-encrypted sector INSIDE the content map is de-bussed.
#[test]
fn bus_content_ranges_debus_sectors_inside_content() {
    use crate::sector::SectorSource;
    let rdk = [0x33u8; 16];
    let clear = clear_bus_content(3);
    let mut wire = clear.clone();
    crate::aacs::content::encrypt_bus(&mut wire, &rdk);
    let mut d = drive_with(wire);
    d.set_bus_stage(crate::sector::bus_removal::BusStage::AacsHostKey(rdk));
    d.set_bus_content_ranges(std::sync::Arc::from(
        vec![(300u32, 3u32)].into_boxed_slice(),
    ));
    let mut got = vec![0u8; clear.len()];
    // Read the 3 content sectors at LBA 300 → all inside → de-bussed to plaintext.
    let n = d.read_sectors(300, 3, &mut got, false).unwrap();
    assert_eq!(n, clear.len());
    assert_eq!(got, clear, "content sectors must be de-bussed to plaintext");
}

// Partial gating within one read: content [301,302) covers only the middle
// sector of a 3-sector read at LBA 300. Only that sector is de-bussed; the
// clear neighbours (300, 302) pass through untouched.
#[test]
fn bus_content_ranges_gate_per_sector_within_a_read() {
    use crate::sector::SectorSource;
    let rdk = [0x44u8; 16];
    // Sector 1 is bus-encrypted content; sectors 0 and 2 are clear on the wire.
    let clear = clear_bus_content(3);
    let mut wire = clear.clone();
    let mut mid = clear[2048..4096].to_vec();
    crate::aacs::content::encrypt_bus(&mut mid, &rdk); // encrypt only the middle sector's body
    wire[2048..4096].copy_from_slice(&mid);

    let mut d = drive_with(wire);
    d.set_bus_stage(crate::sector::bus_removal::BusStage::AacsHostKey(rdk));
    d.set_bus_content_ranges(std::sync::Arc::from(
        vec![(301u32, 1u32)].into_boxed_slice(),
    ));
    let mut got = vec![0u8; clear.len()];
    d.read_sectors(300, 3, &mut got, false).unwrap();
    // All three sectors come back as the known plaintext: 0 and 2 were clear
    // and untouched; 1 was de-bussed back to clear.
    assert_eq!(
        got, clear,
        "only the in-range sector is de-bussed; clear neighbours untouched"
    );
}

// F1 fail-safe: an AacsHostKey stage with an EMPTY content map must de-bus
// NOTHING (empty `Some([])`), not everything — Disc::scan always installs the
// map now, so a host-key disc that parsed no titles leaves clear sectors alone.
#[test]
fn bus_empty_content_ranges_debus_nothing() {
    use crate::sector::SectorSource;
    let rdk = [0x7Eu8; 16];
    let clear = clear_bus_content(2);
    let mut wire = clear.clone();
    crate::aacs::content::encrypt_bus(&mut wire, &rdk);
    let mut d = drive_with(wire.clone());
    d.set_bus_stage(crate::sector::bus_removal::BusStage::AacsHostKey(rdk));
    // Empty map = "no content anywhere" → de-bus nothing (fail-safe).
    d.set_bus_content_ranges(std::sync::Arc::from(
        Vec::<(u32, u32)>::new().into_boxed_slice(),
    ));
    let mut got = vec![0u8; clear.len()];
    d.read_sectors(300, 2, &mut got, false).unwrap();
    assert_eq!(
        got, wire,
        "empty content map must leave every sector untouched (still bus-encrypted), \
             never de-bus clear bytes into garbage"
    );
}

// Per-unit CPI gate at the drive: a read starting mid-unit fetches the unit's
// first sector raw (the mock serves the same CPI=0 sector) and leaves the clear
// unit untouched. MUTATION: skipping the fetch de-busses it into garbage.
#[test]
fn bus_gate_fetches_unit_head_raw_for_a_mid_unit_read() {
    use crate::sector::SectorSource;
    let rdk = [0x19u8; 16];
    let mut wire = clear_bus_content(1);
    wire[0] &= !0xC0; // CPI=0: a clear unit, never bus-encrypted
    let mut d = drive_with(wire.clone());
    d.set_bus_stage(crate::sector::bus_removal::BusStage::AacsHostKey(rdk));
    d.set_bus_content_ranges(std::sync::Arc::from(
        vec![(299u32, 3u32)].into_boxed_slice(),
    ));
    let mut got = vec![0u8; wire.len()];
    d.read_sectors(300, 1, &mut got, false).unwrap();
    assert_eq!(got, wire, "CPI=0 unit read mid-unit must pass through");
}

// scan()'s handshake→bus_stage wiring, via the `wire_bus_removal` seam. CERT
// route: a Some(rdk) handshake arms AacsHostKey + installs the content map, so
// an in-range bus-encrypted sector is de-bussed to plaintext.
#[test]
fn wire_bus_removal_cert_route_arms_host_key_and_ranges() {
    use crate::sector::SectorSource;
    let rdk = [0x6Bu8; 16];
    let clear = clear_bus_content(1);
    let mut wire = clear.clone();
    crate::aacs::content::encrypt_bus(&mut wire, &rdk);
    let mut d = drive_with(wire);
    crate::disc::Disc::wire_bus_removal(
        &mut d,
        Some(rdk),
        crate::sector::bus_removal::BusMap::from_ranges(&[(300, 3)]),
    );
    let mut got = vec![0u8; clear.len()];
    d.read_sectors(300, 1, &mut got, false).unwrap();
    assert_eq!(
        got, clear,
        "Some(read_data_key) must arm AacsHostKey + ranges so content is de-bussed"
    );
}

// FIRMWARE/vendor route: a None handshake maps to Passthrough, so even an
// in-range sector is NOT de-bussed. MUTATION: mapping None to AacsHostKey
// would corrupt these bytes here.
#[test]
fn wire_bus_removal_firmware_route_is_passthrough() {
    use crate::sector::SectorSource;
    let rdk = [0x6Bu8; 16];
    let clear = clear_bus_content(1);
    let mut wire = clear.clone();
    crate::aacs::content::encrypt_bus(&mut wire, &rdk); // still bus-encrypted on the wire
    let mut d = drive_with(wire.clone());
    crate::disc::Disc::wire_bus_removal(
        &mut d,
        None,
        crate::sector::bus_removal::BusMap::from_ranges(&[(300, 3)]),
    );
    let mut got = vec![0u8; clear.len()];
    d.read_sectors(300, 1, &mut got, false).unwrap();
    assert_eq!(
        got, wire,
        "None must map to Passthrough — no host-side de-bus, bytes returned verbatim"
    );
}

// Only a host-key stage leaves sectors bus-encrypted, so only it reports unmapped files.
#[test]
fn unmapped_stream_files_are_reported_only_under_a_host_key_stage() {
    use crate::sector::SectorSource;
    let file = crate::sector::bus_removal::UnmappedStreamFile::new(
        "/BDMV/STREAM/00001.m2ts".into(),
        40,
        &crate::error::Error::UdfAdChainTooLong,
    );
    let map = || {
        crate::sector::bus_removal::BusMap::from_ranges(&[(300, 3)])
            .with_unmapped(vec![file.clone()])
    };
    let mut d = drive_with(Vec::new());
    assert!(d.unmapped_stream_files().is_empty(), "no stage wired");
    crate::disc::Disc::wire_bus_removal(&mut d, Some([0x6B; 16]), map());
    assert_eq!(d.unmapped_stream_files(), std::slice::from_ref(&file));
    crate::disc::Disc::wire_bus_removal(&mut d, None, map());
    assert!(
        d.unmapped_stream_files().is_empty(),
        "firmware route de-busses at the drive"
    );
}

#[test]
fn read_capacity_normal_adds_one() {
    // last_lba = 0x0000_0063 (99) → capacity 100 sectors.
    let mut d = drive_with(vec![0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x08, 0x00]);
    assert_eq!(d.read_capacity().unwrap(), 100);
}

#[test]
fn read_capacity_sentinel_does_not_overflow() {
    // last_lba = 0xFFFF_FFFF is the "capacity exceeds 32-bit" sentinel;
    // +1 would overflow. Must surface DiscCapacityOverflow, not panic
    // (debug) or wrap to 0 (release).
    let mut d = drive_with(vec![0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x08, 0x00]);
    assert!(matches!(
        d.read_capacity(),
        Err(Error::DiscCapacityOverflow)
    ));
}

// disc_is_dvd() must match ONLY the DVD profile family (0x0010..=0x001F plus
// the DVD+ DL profiles 0x002A / 0x002B).
#[test]
fn disc_is_dvd_matches_only_dvd_profile_family() {
    let probe = |profile: u16| {
        let mut hdr = vec![0u8; 8];
        hdr[6] = (profile >> 8) as u8;
        hdr[7] = profile as u8;
        drive_with(hdr).disc_is_dvd()
    };
    // DVD family → DVD (skip drive unlock, run stock for CSS).
    assert!(probe(0x0010), "DVD-ROM");
    assert!(probe(0x0011), "DVD-R");
    assert!(probe(0x001B), "DVD+R");
    assert!(probe(0x002A), "DVD+RW DL");
    assert!(probe(0x002B), "DVD+R DL");
    // BD/UHD family → NOT DVD (must keep today's unlock path).
    assert!(!probe(0x0040), "BD-ROM (UHD) must NOT be classed as DVD");
    assert!(!probe(0x0041), "BD-R");
    assert!(!probe(0x0008), "CD-ROM");
    assert!(!probe(0x0000), "no/unknown profile");
    // Short / failed GET CONFIGURATION → no Current Profile → NOT DVD,
    // so the drive unlock still runs (safe default).
    assert!(
        !drive_with(vec![0u8; 4]).disc_is_dvd(),
        "short GET CONFIGURATION must default to not-DVD (unlock still runs)"
    );
}

// A conformant GET EVENT STATUS NOTIFICATION reply with one Media Event
// Descriptor (MMC-6 §6.7): Event Header (bytes 0-3) + Media Event
// Descriptor whose byte 1 (reply byte 5) is the Media Status.
fn media_event_reply(media_status: u8) -> Vec<u8> {
    let mut buf = vec![0u8; 8];
    buf[0..2].copy_from_slice(&6u16.to_be_bytes()); // 6 bytes follow
    buf[2] = 0x04; // NEA = 0, Notification Class 4 = Media
    buf[3] = 0x10; // Supported Event Classes: media
    buf[4] = 0x00; // Event Code: NoChg
    buf[5] = media_status;
    buf
}

#[test]
fn drive_status_tray_open_and_media_present_is_not_ready_to_rip() {
    // Media Status low bits = 0b11 (tray-open AND media-present,
    // contradictory). Must NOT report DiscPresent.
    let mut d = drive_with(media_event_reply(0x03));
    assert_eq!(d.drive_status(), DriveStatus::TrayOpen);
}

#[test]
fn drive_status_disc_present_maps_correctly() {
    // Media Status 0x02 = media present, tray closed.
    let mut d = drive_with(media_event_reply(0x02));
    assert_eq!(d.drive_status(), DriveStatus::DiscPresent);
}

// Byte 5 is a Media Status only when NEA is clear AND class == Media; otherwise it must
// fall back to TUR, not decode a reserved byte.
#[test]
fn drive_status_rejects_a_reply_carrying_no_media_event_descriptor() {
    // NEA = 1: "No Event Available" — no descriptor was returned, so the
    // bytes after the header are not a Media Event Descriptor.
    let mut nea = media_event_reply(0x00);
    nea[2] = 0x80 | 0x04;
    let mut d = drive_with(nea);
    assert_ne!(
        d.drive_status(),
        DriveStatus::NoDisc,
        "NEA=1 means no event descriptor — byte 5 is not a Media Status"
    );
    assert_eq!(d.drive_status(), DriveStatus::DiscPresent);

    // Notification Class 1 (Operational Change), not 4 (Media): a real
    // descriptor, but of a class whose byte 5 means something else.
    let mut other_class = media_event_reply(0x00);
    other_class[2] = 0x01;
    let mut d = drive_with(other_class);
    assert_ne!(
        d.drive_status(),
        DriveStatus::NoDisc,
        "a non-Media notification class carries no media status"
    );
    assert_eq!(d.drive_status(), DriveStatus::DiscPresent);

    // Control: the same 8 bytes WITH a valid media event header really do
    // decode Media Status 0 as NoDisc, so the two asserts above are about
    // the header and not about byte 5.
    let mut d = drive_with(media_event_reply(0x00));
    assert_eq!(d.drive_status(), DriveStatus::NoDisc);
}

// ── Mocks for Drive::read single-shot semantics + CDB encoding ──

use std::sync::{Arc, Mutex};

/// Records the CDB of every execute() and returns a programmable
/// outcome. Lets a test assert both the bytes sent to the drive and
/// how the driver translates the transport result.
struct RecordingTransport {
    last_cdb: Arc<Mutex<Vec<u8>>>,
    last_timeout: Arc<Mutex<u32>>,
    outcome: TransportOutcome,
}
enum TransportOutcome {
    /// Report this many bytes transferred (data left as-is).
    Ok(usize),
    /// Fail with a ScsiError carrying this status + optional sense.
    Scsi(u8, Option<crate::scsi::ScsiSense>),
}
impl ScsiTransport for RecordingTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        _dir: DataDirection,
        _data: &mut [u8],
        timeout_ms: u32,
    ) -> Result<ScsiResult> {
        *self.last_cdb.lock().unwrap() = cdb.to_vec();
        *self.last_timeout.lock().unwrap() = timeout_ms;
        match self.outcome {
            TransportOutcome::Ok(n) => Ok(ScsiResult {
                status: 0,
                bytes_transferred: n,
                sense: [0u8; 32],
            }),
            TransportOutcome::Scsi(status, sense) => Err(Error::ScsiError {
                opcode: cdb[0],
                status,
                sense,
            }),
        }
    }
}

/// A drive under test plus the handles that observe it: captured CDB bytes
/// and the timeout counter.
struct RecordingHarness {
    drive: Drive,
    cdb: Arc<Mutex<Vec<u8>>>,
    timeouts: Arc<Mutex<u32>>,
}

fn recording(outcome: TransportOutcome) -> RecordingHarness {
    let cdb = Arc::new(Mutex::new(Vec::new()));
    let to = Arc::new(Mutex::new(0u32));
    let t = RecordingTransport {
        last_cdb: cdb.clone(),
        last_timeout: to.clone(),
        outcome,
    };
    RecordingHarness {
        drive: Drive::from_transport_for_test(Box::new(t)),
        cdb,
        timeouts: to,
    }
}

#[test]
fn read_builds_read10_cdb_with_be_lba_and_count() {
    // READ(10) (0x28): LBA bytes 2..5 BE, length bytes 7..8 BE (MMC-6). FUA is
    // DISABLED (byte 1 == 0x00) — forcing FUA on the bulk sweep collapsed
    // throughput ~10x. Distinct nibbles catch a swapped shift.
    let RecordingHarness {
        drive: mut d,
        cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Ok(4096));
    let mut buf = vec![0u8; 4096];
    let n = d.read(0x00AB_CDEF, 2, &mut buf, false).unwrap();
    assert_eq!(n, 4096, "returns transport bytes_transferred");
    let c = cdb.lock().unwrap();
    assert_eq!(c[0], crate::scsi::SCSI_READ_10);
    assert_eq!(
        c[1], 0x00,
        "FUA disabled — cache/readahead allowed on the bulk read path"
    );
    assert_eq!(&c[2..6], &[0x00, 0xAB, 0xCD, 0xEF], "LBA big-endian");
    assert_eq!(&c[7..9], &[0x00, 0x02], "transfer length big-endian");
}

#[test]
fn read_fua_sets_the_force_unit_access_bit() {
    // The Pass-N FuaRetry lever: read_fua(.., fua=true) sets READ(10) byte-1
    // bit 0x08 so the drive re-fetches the medium past its cache; fua=false
    // leaves it clear (the bulk path).
    let RecordingHarness {
        drive: mut d,
        cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Ok(2048));
    let mut buf = vec![0u8; 2048];
    d.read_fua(0, 1, &mut buf, false, true).unwrap();
    assert_eq!(
        cdb.lock().unwrap()[1],
        0x08,
        "FUA requested — byte-1 bit 0x08 set so the drive bypasses its cache"
    );
}

// Uses a NONZERO top byte for LBA/count so a `>>` mutated to `<<` is
// observable (a zero top byte can't tell the two apart).
#[test]
fn read_cdb_shifts_are_not_masked_by_a_zero_top_byte() {
    let RecordingHarness {
        drive: mut d,
        cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Ok(300 * 2048));
    let mut buf = vec![0u8; 300 * 2048];
    // count = 300 (0x012C): count >> 8 == 0x01, nonzero — a `<<`
    // mutation would instead yield 0x00.
    d.read(0xAABB_CCDD, 300, &mut buf, false).unwrap();
    let c = cdb.lock().unwrap();
    assert_eq!(
        &c[2..6],
        &[0xAA, 0xBB, 0xCC, 0xDD],
        "LBA bytes, including the >>24 top byte, must be big-endian verbatim"
    );
    assert_eq!(
        &c[7..9],
        &[0x01, 0x2C],
        "count bytes, including the >>8 top byte"
    );
}

#[test]
fn read_recovery_flag_selects_60s_timeout() {
    // recovery=true must use READ_RECOVERY_TIMEOUT_MS (60 s); false
    // uses READ_TIMEOUT_MS (10 s). Doc: patch pass vs copy sweep.
    let RecordingHarness {
        drive: mut d,
        cdb: _cdb,
        timeouts: to,
    } = recording(TransportOutcome::Ok(2048));
    let mut buf = vec![0u8; 2048];
    d.read(0, 1, &mut buf, true).unwrap();
    assert_eq!(*to.lock().unwrap(), crate::scsi::READ_RECOVERY_TIMEOUT_MS);

    let RecordingHarness {
        drive: mut d2,
        cdb: _c2,
        timeouts: to2,
    } = recording(TransportOutcome::Ok(2048));
    d2.read(0, 1, &mut buf, false).unwrap();
    assert_eq!(*to2.lock().unwrap(), crate::scsi::READ_TIMEOUT_MS);
}

#[test]
fn read_maps_scsi_error_to_discread_preserving_status_and_sense() {
    // On a non-Halted failure, Drive::read returns Error::DiscRead
    // with sector=lba and the transport's status+sense carried
    // through (extract_scsi_context). A 03/11/05 MEDIUM ERROR.
    let sense = crate::scsi::ScsiSense {
        sense_key: 3,
        asc: 0x11,
        ascq: 0x05,
    };
    let RecordingHarness {
        drive: mut d,
        cdb: _cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Scsi(0x02, Some(sense)));
    let mut buf = vec![0u8; 2048];
    let err = d.read(0x1234, 1, &mut buf, false).unwrap_err();
    match err {
        Error::DiscRead {
            sector,
            status,
            sense: s,
        } => {
            assert_eq!(sector, 0x1234, "sector must be the requested LBA");
            assert_eq!(status, Some(0x02));
            assert_eq!(s, Some(sense), "sense triple preserved");
        }
        other => panic!("expected DiscRead, got {other:?}"),
    }
}

#[test]
fn read_transport_failure_status_preserved_for_marginal_routing() {
    // Status 0xFF (TRANSPORT_FAILURE) with no sense must surface in
    // DiscRead.status so is_scsi_transport_failure() routes it.
    let RecordingHarness {
        drive: mut d,
        cdb: _cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Scsi(
        crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
        None,
    ));
    let mut buf = vec![0u8; 2048];
    let err = d.read(7, 1, &mut buf, false).unwrap_err();
    assert!(err.is_scsi_transport_failure());
    assert!(err.scsi_sense().is_none());
}

// extract_scsi_context must map the two dead-bus faults (IoError,
// DeviceNotFound) to the 0xFF TRANSPORT_FAILURE sentinel, not 0x00 — a
// wedged bus must abort the pass, not zero-fill it.
#[test]
fn extract_scsi_context_maps_dead_bus_faults_to_transport_failure() {
    let (status, sense) = extract_scsi_context(&Error::IoError {
        source: std::io::Error::from(std::io::ErrorKind::NotConnected),
    });
    assert_eq!(
        status,
        crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
        "a failed ioctl(SG_IO) is a transport-layer fault"
    );
    assert!(sense.is_none(), "no SCSI reply means no sense data");

    let (status, sense) = extract_scsi_context(&Error::DeviceNotFound {
        path: "/dev/sg9".into(),
    });
    assert_eq!(
        status,
        crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
        "a vanished device is a transport-layer fault"
    );
    assert!(sense.is_none());

    // Control: the catch-all still yields (0, None) for unrelated errors,
    // so the two asserts above are about these variants specifically.
    assert_eq!(extract_scsi_context(&Error::Halted), (0, None));

    // Control: real SCSI replies still pass their own status through.
    let s = crate::scsi::ScsiSense {
        sense_key: 3,
        asc: 0x11,
        ascq: 0x00,
    };
    assert_eq!(
        extract_scsi_context(&Error::ScsiError {
            opcode: 0x28,
            status: 0x02,
            sense: Some(s),
        }),
        (0x02, Some(s))
    );
}

// End-to-end: a transport IoError must still classify as a transport
// failure after Drive::read flattens it into DiscRead.
#[test]
fn read_io_error_surfaces_as_transport_failure_not_a_bad_sector() {
    let mut d = Drive::from_transport_for_test(Box::new(AlwaysErr {
        err: || Error::IoError {
            source: std::io::Error::from(std::io::ErrorKind::NotConnected),
        },
    }));
    let mut buf = vec![0u8; 2048];
    let err = d.read(42, 1, &mut buf, false).unwrap_err();
    assert!(
        err.is_scsi_transport_failure(),
        "a wedged bus must abort the pass, not zero-fill: got {err:?}"
    );

    // The same for a device that vanished mid-read.
    let mut d = Drive::from_transport_for_test(Box::new(AlwaysErr {
        err: || Error::DeviceNotFound {
            path: "/dev/sg9".into(),
        },
    }));
    let err = d.read(42, 1, &mut buf, false).unwrap_err();
    assert!(
        err.is_scsi_transport_failure(),
        "a vanished device must abort the pass: got {err:?}"
    );
}

#[test]
fn read_returns_halted_before_dispatch_without_touching_transport() {
    // When the halt flag is set, checked_exec returns Halted BEFORE
    // execute(); the error must be Halted (not DiscRead), so the
    // recovery loop distinguishes user-stop from a read failure.
    let RecordingHarness {
        drive: mut d,
        cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Ok(2048));
    d.halt();
    let mut buf = vec![0u8; 2048];
    let err = d.read(0, 1, &mut buf, false).unwrap_err();
    assert!(matches!(err, Error::Halted));
    assert!(
        cdb.lock().unwrap().is_empty(),
        "transport execute must not run when pre-halted"
    );
}

#[test]
fn a_fresh_token_reenables_reads_after_a_stop() {
    // A cancel is one-way (`clear_halt` is gone, §2.2): the next op attaches a
    // fresh token, and reads work again under it.
    let RecordingHarness {
        drive: mut d,
        cdb: _cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Ok(2048));
    d.halt();
    d.attach(&Halt::new());
    let mut buf = vec![0u8; 2048];
    assert!(d.read(0, 1, &mut buf, false).is_ok());
}

#[test]
fn read_does_not_truncate_reported_bytes() {
    // Single-shot contract: Drive::read returns exactly what the
    // transport reported, never a smaller count silently. Transport
    // says a full 32-sector batch (65536 bytes) succeeded.
    let RecordingHarness {
        drive: mut d,
        cdb: _cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Ok(65536));
    let mut buf = vec![0u8; 65536];
    assert_eq!(d.read(0, 32, &mut buf, false).unwrap(), 65536);
}

// ── Drive::read chunking against a capped transport ─────────────

// Transport with a small max_transfer_bytes that records each READ(10)'s
// (lba, count), can fail the Nth read, for chunk-decomposition tests.
struct ChunkingTransport {
    max_bytes: usize,
    /// Recorded (lba, transfer_length_sectors) per READ(10).
    reads: Arc<Mutex<Vec<(u32, u16)>>>,
    /// If Some(i), the i-th READ(10) (0-based) fails with a SCSI error.
    fail_on: Option<usize>,
    seen: usize,
}
impl ScsiTransport for ChunkingTransport {
    fn max_transfer_bytes(&self) -> usize {
        self.max_bytes
    }
    fn execute(
        &mut self,
        cdb: &[u8],
        _dir: DataDirection,
        data: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        // Only track READ(10); ignore other CDBs (e.g. the 6-byte
        // PREVENT ALLOW MEDIUM REMOVAL the Drive sends on Drop).
        if cdb.first() != Some(&crate::scsi::SCSI_READ_10) || cdb.len() < 10 {
            return Ok(ScsiResult {
                status: 0,
                bytes_transferred: data.len(),
                sense: [0u8; 32],
            });
        }
        let lba = u32::from_be_bytes([cdb[2], cdb[3], cdb[4], cdb[5]]);
        let count = u16::from_be_bytes([cdb[7], cdb[8]]);
        self.reads.lock().unwrap().push((lba, count));
        let idx = self.seen;
        self.seen += 1;
        if self.fail_on == Some(idx) {
            return Err(Error::ScsiError {
                opcode: cdb[0],
                status: 0x02,
                sense: Some(crate::scsi::ScsiSense {
                    sense_key: 3,
                    asc: 0x11,
                    ascq: 0x05,
                }),
            });
        }
        Ok(ScsiResult {
            status: 0,
            bytes_transferred: data.len(),
            sense: [0u8; 32],
        })
    }
}

/// A drive under test plus the handle recording each `(lba, count)` read.
struct ChunkingHarness {
    drive: Drive,
    reads: Arc<Mutex<Vec<(u32, u16)>>>,
}

fn chunking(max_bytes: usize, fail_on: Option<usize>) -> ChunkingHarness {
    let reads = Arc::new(Mutex::new(Vec::new()));
    let t = ChunkingTransport {
        max_bytes,
        reads: reads.clone(),
        fail_on,
        seen: 0,
    };
    ChunkingHarness {
        drive: Drive::from_transport_for_test(Box::new(t)),
        reads,
    }
}

// An undersized caller buffer must error, not panic, whether the request fits in one
// transfer or has to be chunked.
#[test]
fn an_undersized_buffer_errors_on_the_chunked_path_just_like_the_single_one() {
    // max_transfer = 4 sectors, so a 10-sector read must chunk.
    let mut h = chunking(4 * 2048, None);
    let mut small = vec![0u8; 4096]; // 2 sectors' worth for a 10-sector read

    let chunked = h.drive.read(0, 10, &mut small, false);
    assert!(
        matches!(chunked, Err(Error::DiscRead { .. })),
        "an undersized buffer on the chunked path must be an error, not a panic"
    );

    // The single-chunk path, same undersized buffer, same verdict.
    let single = h.drive.read(0, 3, &mut small, false);
    assert!(
        matches!(single, Err(Error::DiscRead { .. })),
        "the single-chunk path must agree"
    );

    // Exactly-sized still works, so the guard is not simply rejecting
    // everything on the chunked path.
    let mut exact = vec![0u8; 10 * 2048];
    assert!(h.drive.read(0, 10, &mut exact, false).is_ok());
}

#[test]
fn read_chunks_large_request_to_max_transfer() {
    // max_transfer = 4 sectors (4 * 2048 = 8192 bytes). A read of 10
    // sectors at LBA 0 must split into 3 READ(10) CDBs: (0,4), (4,4),
    // (8,2). The assembled buffer is the full 10*2048 bytes.
    let ChunkingHarness {
        drive: mut d,
        reads,
    } = chunking(4 * 2048, None);
    let mut buf = vec![0u8; 10 * 2048];
    let n = d.read(0, 10, &mut buf, false).unwrap();
    assert_eq!(n, 10 * 2048, "returns total bytes across all chunks");
    let r = reads.lock().unwrap();
    assert_eq!(
        *r,
        vec![(0, 4), (4, 4), (8, 2)],
        "must chunk into 4+4+2 sectors at advancing LBAs"
    );
}

#[test]
fn read_chunk_failure_reports_failing_chunk_lba() {
    // Same 4-sector cap; fail the 2nd chunk (index 1), which covers
    // LBA 4. The error must be DiscRead with sector = 4 (the failing
    // chunk's LBA), NOT the request base LBA 0.
    let ChunkingHarness {
        drive: mut d,
        reads,
    } = chunking(4 * 2048, Some(1));
    let mut buf = vec![0u8; 10 * 2048];
    let err = d.read(0, 10, &mut buf, false).unwrap_err();
    match err {
        Error::DiscRead { sector, status, .. } => {
            assert_eq!(sector, 4, "failing chunk's LBA, not the request base");
            assert_eq!(status, Some(0x02));
        }
        other => panic!("expected DiscRead, got {other:?}"),
    }
    // Reads 0 (LBA 0) succeeded and 1 (LBA 4) failed; the loop stops on
    // the error so LBA 8 is never issued.
    let r = reads.lock().unwrap();
    assert_eq!(*r, vec![(0, 4), (4, 4)], "stops at the failing chunk");
}

#[test]
fn read_small_request_is_single_unchunked_read() {
    // count <= max_sectors must take the single-read path unchanged: a
    // 3-sector read under a 4-sector cap is exactly one READ(10).
    let ChunkingHarness {
        drive: mut d,
        reads,
    } = chunking(4 * 2048, None);
    let mut buf = vec![0u8; 3 * 2048];
    assert_eq!(d.read(0, 3, &mut buf, false).unwrap(), 3 * 2048);
    assert_eq!(*reads.lock().unwrap(), vec![(0, 3)], "single CDB, no split");
}

/// Transport that fills whatever slice of `data` it's given with a
/// marker byte derived from the CDB's LBA, so a test can verify BYTE
/// POSITION, not just which (lba, count) pairs were issued.
struct PlacementTransport;
impl ScsiTransport for PlacementTransport {
    fn max_transfer_bytes(&self) -> usize {
        4 * 2048
    }
    fn execute(
        &mut self,
        cdb: &[u8],
        _dir: DataDirection,
        data: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        if cdb.first() != Some(&crate::scsi::SCSI_READ_10) || cdb.len() < 10 {
            return Ok(ScsiResult {
                status: 0,
                bytes_transferred: data.len(),
                sense: [0u8; 32],
            });
        }
        let lba = u32::from_be_bytes([cdb[2], cdb[3], cdb[4], cdb[5]]);
        data.fill((lba + 1) as u8);
        Ok(ScsiResult {
            status: 0,
            bytes_transferred: data.len(),
            sense: [0u8; 32],
        })
    }
}

// Writes a distinct marker per chunk and checks the BYTE offset, catching a `*` -> `+`/`/`
// mutation in the chunk destination-slice arithmetic.
#[test]
fn read_chunks_write_into_correctly_offset_buffer_regions() {
    // max_transfer = 4 sectors (8192 bytes): a 10-sector read at LBA 0
    // splits into (lba=0,4), (lba=4,4), (lba=8,2).
    let mut d = Drive::from_transport_for_test(Box::new(PlacementTransport));
    let mut buf = vec![0u8; 10 * 2048];
    d.read(0, 10, &mut buf, false).unwrap();
    assert!(
        buf[0..8192].iter().all(|&b| b == 1),
        "chunk at LBA 0 (marker 1) must fill bytes [0, 8192)"
    );
    assert!(
        buf[8192..16384].iter().all(|&b| b == 5),
        "chunk at LBA 4 (marker 5) must fill bytes [8192, 16384), not overlap the first chunk"
    );
    assert!(
        buf[16384..20480].iter().all(|&b| b == 9),
        "chunk at LBA 8 (marker 9) must fill bytes [16384, 20480)"
    );
}

// Multi-chunk path with an undersized buffer must error, not panic, same as the
// single-chunk path.
#[test]
fn undersized_buffer_multi_chunk_errors_not_panics() {
    let ChunkingHarness {
        drive: mut d,
        reads: _reads,
    } = chunking(4 * 2048, None);
    // count (10) > max_sectors (4) → the chunk loop; buf holds only 1 sector.
    let mut buf = vec![0u8; 2048];
    assert!(
        matches!(d.read(0, 10, &mut buf, false), Err(Error::DiscRead { .. })),
        "an undersized buffer must error, not panic"
    );
    // The single-chunk path with the SAME undersized buffer already errored;
    // the two paths must now agree.
    let mut buf = vec![0u8; 2048];
    assert!(
        matches!(d.read(0, 3, &mut buf, false), Err(Error::DiscRead { .. })),
        "single-chunk path errors on an undersized buffer (unchanged)"
    );
}

// The chunk loop's `lba + done` is checked: a request crossing u32::MAX
// must error, not debug-panic or release-wrap to a low LBA.
#[test]
fn chunk_lba_near_u32_max_errors_not_overflows() {
    let ChunkingHarness {
        drive: mut d,
        reads: _reads,
    } = chunking(4 * 2048, None);
    let mut buf = vec![0u8; 10 * 2048];
    // 0xFFFF_FFFE + 4 overflows on the second chunk.
    assert!(
        matches!(
            d.read(0xFFFF_FFFE, 10, &mut buf, false),
            Err(Error::DiscRead { .. })
        ),
        "an LBA range past u32::MAX must error, not overflow"
    );
}

// ── find_drive media-preference selection policy ────────────────

// Fake drive whose GET EVENT STATUS reply reports the given media_status
// byte (0x02 = DiscPresent, 0x00 = NoDisc), for hardware-free tests.
fn drive_with_media_byte(media_status: u8) -> Drive {
    drive_with(media_event_reply(media_status))
}

#[test]
fn select_drive_prefers_drive_with_media() {
    // Drive #1 has no disc (0x00), drive #2 has a disc (0x02). The
    // selection must skip the empty first drive and pick the one with
    // media — the Windows multi-drive bug fix.
    let picked = select_drive_with_media([0x00u8, 0x02], |m| Some(drive_with_media_byte(*m)));
    let mut picked = picked.expect("a drive");
    assert_eq!(
        picked.drive_status(),
        DriveStatus::DiscPresent,
        "must pick the drive reporting DiscPresent, not the empty first drive"
    );
}

#[test]
fn select_drive_falls_back_to_first_when_none_have_media() {
    // No drive reports a disc → fall back to the FIRST opened drive so single-drive
    // / quirky setups still get one (historical behavior). Tag drive #1 distinctly
    // (TrayOpen 0x01) and confirm it, not #2 (NoDisc 0x00), is returned.
    let picked = select_drive_with_media([0x01u8, 0x00], |m| Some(drive_with_media_byte(*m)));
    let mut picked = picked.expect("a fallback drive");
    assert_eq!(
        picked.drive_status(),
        DriveStatus::TrayOpen,
        "fallback must be the first drive yielded"
    );
}

#[test]
fn select_drive_none_when_no_drives() {
    // No candidates at all → None.
    let empty: [u8; 0] = [];
    assert!(select_drive_with_media(empty, |m| Some(drive_with_media_byte(*m))).is_none());
}

// Models macOS `MacScsiTransport`: only one instance may be alive per
// process; construction fails while another is open, Drop releases it.
struct ExclusiveTransport {
    inner: FixedTransport,
    live: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl ScsiTransport for ExclusiveTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        direction: DataDirection,
        data: &mut [u8],
        timeout_ms: u32,
    ) -> Result<ScsiResult> {
        self.inner.execute(cdb, direction, data, timeout_ms)
    }
}

impl Drop for ExclusiveTransport {
    fn drop(&mut self) {
        self.live.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

fn open_exclusive(
    live: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    media_status: u8,
) -> Option<Drive> {
    if live.swap(true, std::sync::atomic::Ordering::SeqCst) {
        return None; // DeviceLocked: another handle is still open
    }
    Some(Drive::from_transport_for_test(Box::new(
        ExclusiveTransport {
            inner: FixedTransport {
                payload: media_event_reply(media_status),
            },
            live: live.clone(),
        },
    )))
}

#[test]
fn select_drive_finds_media_behind_empty_drive_with_single_open_transport() {
    // Two drives, first empty (TrayOpen), second has a disc, on a
    // transport that allows one live handle. The empty drive must not
    // be held open while the second is tried.
    let live = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let picked = select_drive_with_media([0x01u8, 0x02], |m| open_exclusive(&live, *m));
    let mut picked = picked.expect("a drive");
    assert_eq!(
        picked.drive_status(),
        DriveStatus::DiscPresent,
        "must open the second drive and pick its disc"
    );
}

// ── drive_status branch coverage (GET EVENT STATUS byte 5) ──────

#[test]
fn drive_status_no_disc_maps_correctly() {
    // media_status low bits 0b00 = tray closed, no disc.
    let mut d = drive_with(media_event_reply(0x00));
    assert_eq!(d.drive_status(), DriveStatus::NoDisc);
}

#[test]
fn drive_status_tray_open_maps_correctly() {
    // media_status low bits 0b01 = tray open, no media.
    let mut d = drive_with(media_event_reply(0x01));
    assert_eq!(d.drive_status(), DriveStatus::TrayOpen);
}

#[test]
fn drive_status_high_bits_in_media_status_ignored() {
    // MMC-6 §6.7: only the low 2 bits of the Media Event Descriptor's
    // Media Status are the door/media state; the reserved upper bits must
    // be masked. 0xFE has low bits 0b10 = DiscPresent.
    let mut d = drive_with(media_event_reply(0xFE));
    assert_eq!(d.drive_status(), DriveStatus::DiscPresent);
}

// A 5-byte transfer (byte 5 undelivered) must fall back to TUR, not
// decode a zero-initialised byte 5 as a real Media Status of NoDisc.
#[test]
fn drive_status_rejects_media_status_from_an_undelivered_byte() {
    let short = vec![0x00, 0x06, 0x04, 0x00, 0x00]; // 5 bytes: descriptor_len=6, NEA=0, class=Media
    let mut d = drive_with(short);
    assert_eq!(
        d.drive_status(),
        DriveStatus::DiscPresent,
        "a transfer too short to include byte 5 must fall back to TUR, \
             not decode a media status the drive never sent"
    );
}

// An event length too short to hold a Media Event Descriptor means byte 5 is not a
// Media Status, even with NEA clear and class Media: fall back to TUR.
#[test]
fn drive_status_rejects_a_too_short_descriptor_length() {
    for len in [0u16, 2, 5] {
        let mut reply = media_event_reply(0x00);
        reply[0..2].copy_from_slice(&len.to_be_bytes());
        let mut d = drive_with(reply);
        assert_eq!(d.drive_status(), DriveStatus::DiscPresent, "len {len}");
    }
}

#[test]
fn drive_status_short_transfer_falls_back_to_tur() {
    // bytes_transferred < 6 (buffer len 8, payload 4) means the GET EVENT reply
    // is unusable, so code falls back to a TUR; FixedTransport always returns
    // Ok, so the TUR "succeeds" → DiscPresent.
    let mut d = drive_with(vec![0u8; 4]);
    assert_eq!(d.drive_status(), DriveStatus::DiscPresent);
}

/// Transport that fails every command with a programmable error —
/// drives the TUR-fallback NotReady/Unknown branches of drive_status.
struct AlwaysErr {
    err: fn() -> Error,
}
impl ScsiTransport for AlwaysErr {
    fn execute(
        &mut self,
        _cdb: &[u8],
        _dir: DataDirection,
        _data: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        Err((self.err)())
    }
}

#[test]
fn drive_status_tur_not_ready_sense_maps_not_ready() {
    // GET EVENT fails, fallback TUR fails with NOT READY sense →
    // DriveStatus::NotReady (drive spinning up). Doc: drive_status
    // fallback branch.
    let mut d = Drive::from_transport_for_test(Box::new(AlwaysErr {
        err: || Error::ScsiError {
            opcode: 0,
            status: 0x02,
            sense: Some(crate::scsi::ScsiSense {
                sense_key: 2, // NOT READY
                asc: 0x04,
                ascq: 0x01,
            }),
        },
    }));
    assert_eq!(d.drive_status(), DriveStatus::NotReady);
}

#[test]
fn drive_status_tur_unit_attention_maps_not_ready() {
    // UNIT ATTENTION (media changed) on the fallback TUR also maps to
    // NotReady per the is_unit_attention() arm.
    let mut d = Drive::from_transport_for_test(Box::new(AlwaysErr {
        err: || Error::ScsiError {
            opcode: 0,
            status: 0x02,
            sense: Some(crate::scsi::ScsiSense {
                sense_key: 6, // UNIT ATTENTION
                asc: 0x28,
                ascq: 0x00,
            }),
        },
    }));
    assert_eq!(d.drive_status(), DriveStatus::NotReady);
}

#[test]
fn drive_status_tur_other_error_maps_unknown() {
    // A fallback TUR failure that is neither NOT READY nor UNIT
    // ATTENTION (e.g. transport failure, no sense) → Unknown.
    let mut d = Drive::from_transport_for_test(Box::new(AlwaysErr {
        err: || Error::ScsiError {
            opcode: 0,
            status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
            sense: None,
        },
    }));
    assert_eq!(d.drive_status(), DriveStatus::Unknown);
}

// ── get_config_feature: header-strip threshold + clamp ──────────

#[test]
fn get_config_feature_strips_8_byte_header() {
    // GET CONFIGURATION reply: 8-byte Feature Header (MMC-6 §5.3.1), then the
    // descriptor. Returns the descriptor only, bounded by its Additional
    // Length even when the transport reports the whole zero-padded buffer.
    let mut payload = vec![0, 0, 0, 12, 0, 0, 0, 0];
    payload.extend_from_slice(&[0x01, 0x0D, 0x00, 0x04, 0xDE, 0xAD, 0xBE, 0xEF]);
    payload.resize(256, 0);
    let mut d = drive_with(payload);
    assert_eq!(
        d.get_config_feature(0x010D),
        Some(vec![0x01, 0x0D, 0x00, 0x04, 0xDE, 0xAD, 0xBE, 0xEF])
    );
}

#[test]
fn get_config_feature_absent_feature_returns_none() {
    // RT=10b for an unsupported feature: header only (Data Length 4), even if
    // the transport over-reports the transfer as the full 256 bytes.
    let mut header_only = vec![0, 0, 0, 4, 0, 0, 0, 0];
    header_only.resize(256, 0);
    let mut d = drive_with(header_only);
    assert_eq!(d.get_config_feature(0x010D), None);
    // Exactly 8 bytes transferred is also header-only.
    let mut d = drive_with(vec![0u8; 8]);
    assert_eq!(d.get_config_feature(0x0000), None);
}

#[test]
fn get_config_feature_encodes_feature_code_be_in_cdb() {
    // GET CONFIGURATION (0x46), RT byte 1 = 0x02, feature code big-endian in
    // bytes 2..4. 0x010D has a nonzero high byte, so a swapped shift (>> vs <<)
    // or byte order bug would ask the drive for the wrong feature.
    let RecordingHarness {
        drive: mut d,
        cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Ok(0));
    let _ = d.get_config_feature(0x010D);
    let c = cdb.lock().unwrap();
    assert_eq!(
        &c[..4],
        &[crate::scsi::SCSI_GET_CONFIGURATION, 0x02, 0x01, 0x0D],
        "feature code must be big-endian in CDB bytes 2..4"
    );
}

// ── spin_cycle / wait_ready: recovery entry points (LOW finding 8) ──────

/// Records EVERY CDB issued, in order — unlike `RecordingTransport`
/// (used above), which only keeps the last one. Needed to assert a
/// multi-command sequence like `spin_cycle`'s STOP-then-START.
struct SequenceTransport {
    cdbs: Arc<Mutex<Vec<Vec<u8>>>>,
    ok: bool,
}
impl ScsiTransport for SequenceTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        _dir: DataDirection,
        _data: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        self.cdbs.lock().unwrap().push(cdb.to_vec());
        if self.ok {
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: 0,
                sense: [0u8; 32],
            })
        } else {
            Err(Error::ScsiError {
                opcode: cdb[0],
                status: 2,
                sense: None,
            })
        }
    }
}

// BU40N/Initio wedge recovery: exactly STOP (START=0) then START
// (START=1), both LOEJ=0 — a slot-loading drive must never eject.
#[test]
fn spin_cycle_issues_stop_then_start_without_ejecting() {
    let cdbs = Arc::new(Mutex::new(Vec::new()));
    let t = SequenceTransport {
        cdbs: cdbs.clone(),
        ok: true,
    };
    let mut d = Drive::from_transport_for_test(Box::new(t));
    d.spin_cycle()
        .expect("spin_cycle must succeed when both SCSI commands succeed");
    let seq = cdbs.lock().unwrap();
    assert_eq!(
        seq.len(),
        2,
        "spin_cycle must issue exactly two commands: {seq:?}"
    );
    assert_eq!(seq[0][0], SCSI_START_STOP_UNIT);
    assert_eq!(seq[0][4], 0x00, "first command: START=0 (spin down)");
    assert_eq!(seq[1][0], SCSI_START_STOP_UNIT);
    assert_eq!(seq[1][4], 0x01, "second command: START=1 (spin up)");
    for (i, c) in seq.iter().enumerate() {
        assert_eq!(
            c[4] & 0x02,
            0,
            "LOEJ bit must be clear on command {i} — spin_cycle must never eject"
        );
    }
}

// A drive that never answers TUR successfully must surface
// Err(DeviceNotReady), not silently report ready.
#[test]
fn wait_ready_returns_err_when_drive_never_becomes_ready() {
    struct NeverReady;
    impl ScsiTransport for NeverReady {
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: DataDirection,
            _data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            Err(Error::ScsiError {
                opcode: cdb[0],
                status: 2,
                sense: None,
            })
        }
    }
    let mut d = Drive::from_transport_for_test(Box::new(NeverReady));
    // T6 scaled: 60 s → 300 ms, polls 500 ms → 10 ms.
    let r = d.wait_ready_with(WaitReadyTiming {
        poll: std::time::Duration::from_millis(10),
        window: std::time::Duration::from_millis(300),
        dead_bus: WAIT_READY_DEAD_BUS_BUDGET,
        ceiling: std::time::Duration::from_secs(600),
    });
    assert!(
        matches!(r, Err(Error::DeviceNotReady { .. })),
        "a drive that never answers TUR successfully must be DeviceNotReady, got {r:?}"
    );
}

// A drive that keeps answering with a fresh sense triple every poll re-arms the
// no-progress window forever; the absolute ceiling must still end the wait.
#[test]
fn wait_ready_ceiling_bounds_a_cycling_drive() {
    struct Cycling(u16);
    impl ScsiTransport for Cycling {
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: DataDirection,
            _data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            self.0 = self.0.wrapping_add(1);
            let mut sense = [0u8; 32];
            sense[0] = 0x70;
            sense[2] = 0x02;
            sense[12] = (self.0 >> 8) as u8;
            sense[13] = self.0 as u8;
            Err(Error::ScsiError {
                opcode: cdb[0],
                status: 2,
                sense: Some(crate::scsi::ScsiSense {
                    sense_key: 2,
                    asc: sense[12],
                    ascq: sense[13],
                }),
            })
        }
    }
    let mut d = Drive::from_transport_for_test(Box::new(Cycling(0x0500)));
    let t0 = std::time::Instant::now();
    let r = d.wait_ready_with(WaitReadyTiming {
        poll: std::time::Duration::from_millis(5),
        window: std::time::Duration::from_secs(30),
        dead_bus: WAIT_READY_DEAD_BUS_BUDGET,
        ceiling: std::time::Duration::from_millis(300),
    });
    assert!(matches!(r, Err(Error::DeviceNotReady { .. })), "{r:?}");
    assert!(t0.elapsed() < std::time::Duration::from_secs(5));
}

// A dead bus (transport failures in a row) will never spin up, so wait_ready
// must surface it once the ~5 s budget is spent, not poll a phantom for ~30 s.
#[test]
fn wait_ready_breaks_out_on_a_dead_bus() {
    struct DeadBus;
    impl ScsiTransport for DeadBus {
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: DataDirection,
            _data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            Err(Error::ScsiError {
                opcode: cdb[0],
                status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            })
        }
    }
    let mut d = Drive::from_transport_for_test(Box::new(DeadBus));
    let t0 = std::time::Instant::now();
    let r = d.wait_ready();
    assert!(
        matches!(&r, Err(e) if e.is_scsi_transport_failure()),
        "a dead bus must surface the transport failure, not DeviceNotReady: {r:?}"
    );
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(8),
        "a dead bus must break out after the budget, not run the ~30 s poll"
    );
}

// One bus hiccup (DID_TIME_OUT, then the Linux fd-reopen gap) during spin-up
// must not abort the poll: the next TUR on the recovered fd succeeds.
#[test]
fn wait_ready_rides_out_a_transient_transport_failure() {
    struct Hiccup(usize);
    impl ScsiTransport for Hiccup {
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: DataDirection,
            _data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            self.0 += 1;
            match self.0 {
                1 => Err(Error::ScsiError {
                    opcode: cdb[0],
                    status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
                    sense: None,
                }),
                2 => Err(Error::DeviceNotFound {
                    path: "/dev/sg0".into(),
                }),
                _ => Ok(ScsiResult {
                    status: 0,
                    bytes_transferred: 0,
                    sense: [0u8; 32],
                }),
            }
        }
    }
    let mut d = Drive::from_transport_for_test(Box::new(Hiccup(0)));
    let r = d.wait_ready();
    assert!(
        r.is_ok(),
        "a transient transport failure must not abort wait_ready: {r:?}"
    );
}

// Fails with a transport error for the first `fails` TURs, each taking
// `delay`, then answers GOOD.
struct FlakyBus {
    fails: usize,
    delay: std::time::Duration,
}
impl ScsiTransport for FlakyBus {
    fn execute(
        &mut self,
        cdb: &[u8],
        _dir: DataDirection,
        _data: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        std::thread::sleep(self.delay);
        if self.fails == 0 {
            return Ok(ScsiResult {
                status: 0,
                bytes_transferred: 0,
                sense: [0u8; 32],
            });
        }
        self.fails -= 1;
        Err(Error::ScsiError {
            opcode: cdb[0],
            status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
            sense: None,
        })
    }
}

// The dead-bus budget is TIME, not a count: several quick transport
// failures within it (a bridge reset storm) must still ride through.
#[test]
fn wait_ready_tolerates_quick_transport_failures_within_the_budget() {
    let mut d = Drive::from_transport_for_test(Box::new(FlakyBus {
        fails: 4,
        delay: std::time::Duration::ZERO,
    }));
    let r = d.wait_ready();
    assert!(r.is_ok(), "4 fast failures (~2 s) are within budget: {r:?}");
}

// TUR answers NOT READY with `sense` until a START STOP UNIT (START=1)
// arrives, then `after_start` more times, then GOOD. Counts the STARTs.
struct NeedsStart {
    sense: crate::scsi::ScsiSense,
    after_start: usize,
    started: bool,
    starts: Arc<Mutex<Vec<Vec<u8>>>>,
}
impl ScsiTransport for NeedsStart {
    fn execute(
        &mut self,
        cdb: &[u8],
        _dir: DataDirection,
        _data: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        let good = Ok(ScsiResult {
            status: 0,
            bytes_transferred: 0,
            sense: [0u8; 32],
        });
        if cdb[0] == SCSI_START_STOP_UNIT {
            self.starts.lock().unwrap().push(cdb.to_vec());
            self.started = true;
            return good;
        }
        if self.started && self.after_start == 0 {
            return good;
        }
        if self.started {
            self.after_start -= 1;
        }
        Err(Error::ScsiError {
            opcode: cdb[0],
            status: crate::scsi::SCSI_STATUS_CHECK_CONDITION,
            sense: Some(self.sense),
        })
    }
}

fn needs_start(asc: u8, ascq: u8, after_start: usize) -> (Drive, Arc<Mutex<Vec<Vec<u8>>>>) {
    let starts = Arc::new(Mutex::new(Vec::new()));
    let t = NeedsStart {
        sense: crate::scsi::ScsiSense {
            sense_key: crate::scsi::SENSE_KEY_NOT_READY,
            asc,
            ascq,
        },
        after_start,
        started: false,
        starts: starts.clone(),
    };
    (Drive::from_transport_for_test(Box::new(t)), starts)
}

// 02/04/02 (initializing command required): nothing spins the unit up unless
// wait_ready sends START UNIT, once, never ejecting, then keeps polling.
#[test]
fn wait_ready_sends_one_start_unit_on_initializing_command_required() {
    let (mut d, starts) = needs_start(0x04, 0x02, 2);
    let r = d.wait_ready();
    assert!(
        r.is_ok(),
        "START UNIT must bring a stopped unit ready: {r:?}"
    );
    let starts = starts.lock().unwrap();
    assert_eq!(starts.len(), 1, "exactly one START UNIT: {starts:?}");
    assert_eq!(starts[0][4], 0x01, "START=1, LoEj=0");
}

// 02/30/xx (incompatible or blank medium) never becomes ready: surface it at
// once with its sense, not DeviceNotReady after ~30 s of polling.
#[test]
fn wait_ready_fails_fast_on_incompatible_medium() {
    let (mut d, starts) = needs_start(0x30, 0x00, usize::MAX);
    let t0 = std::time::Instant::now();
    let r = d.wait_ready();
    assert!(
        matches!(&r, Err(e) if e.scsi_sense().is_some_and(|s| s.asc == 0x30)),
        "incompatible medium must surface its sense: {r:?}"
    );
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(2),
        "{:?}",
        t0.elapsed()
    );
    assert!(starts.lock().unwrap().is_empty(), "no START for 30h");
}

// Answers each TUR from a script of NOT READY (asc, ascq) pairs, then GOOD.
struct ScriptedNotReady(
    std::collections::VecDeque<(u8, u8)>,
    Arc<std::sync::atomic::AtomicUsize>,
);
impl ScsiTransport for ScriptedNotReady {
    fn execute(
        &mut self,
        cdb: &[u8],
        _dir: DataDirection,
        _data: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        self.1.fetch_add(1, Ordering::Relaxed);
        let Some((asc, ascq)) = self.0.pop_front() else {
            return Ok(ScsiResult {
                status: 0,
                bytes_transferred: 0,
                sense: [0u8; 32],
            });
        };
        Err(Error::ScsiError {
            opcode: cdb[0],
            status: crate::scsi::SCSI_STATUS_CHECK_CONDITION,
            sense: Some(crate::scsi::ScsiSense {
                sense_key: crate::scsi::SENSE_KEY_NOT_READY,
                asc,
                ascq,
            }),
        })
    }
}

fn scripted(script: &[(u8, u8)]) -> (Drive, Arc<std::sync::atomic::AtomicUsize>) {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let t = ScriptedNotReady(script.iter().copied().collect(), calls.clone());
    (Drive::from_transport_for_test(Box::new(t)), calls)
}

// An empty drive (3Ah on every TUR, never 04/01) will not become ready:
// surface MEDIUM NOT PRESENT after a bounded run, not after ~30 s.
#[test]
fn wait_ready_fails_fast_on_an_empty_drive() {
    let (mut d, calls) = scripted(&[(0x3A, 0x00); 100]);
    let r = d.wait_ready();
    assert!(
        matches!(&r, Err(e) if e.scsi_sense().is_some_and(|s| s.asc == 0x3A)),
        "{r:?}"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 10, "bounded 3Ah run");
}

// Once the drive has said 04/01 (a disc is being identified), 3Ah is not
// final: keep polling rather than give up on the 3Ah run.
#[test]
fn wait_ready_keeps_polling_3a_after_becoming_ready() {
    let mut script = vec![(0x04, 0x01)];
    script.extend([(0x3A, 0x00); 11]);
    let (mut d, _calls) = scripted(&script);
    let r = d.wait_ready();
    assert!(r.is_ok(), "{r:?}");
}

// One DID_TIME_OUT TUR eats its whole 5 s timeout; that single hiccup must
// not spend the dead-bus budget before the next TUR can succeed.
#[test]
fn wait_ready_rides_out_one_slow_transport_failure() {
    struct SlowHiccup(bool);
    impl ScsiTransport for SlowHiccup {
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: DataDirection,
            _data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            if std::mem::replace(&mut self.0, false) {
                std::thread::sleep(std::time::Duration::from_millis(5_100));
                return Err(Error::ScsiError {
                    opcode: cdb[0],
                    status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
                    sense: None,
                });
            }
            Ok(ScsiResult {
                status: 0,
                bytes_transferred: 0,
                sense: [0u8; 32],
            })
        }
    }
    let mut d = Drive::from_transport_for_test(Box::new(SlowHiccup(true)));
    let r = d.wait_ready();
    assert!(r.is_ok(), "one timed-out TUR is a hiccup: {r:?}");
}

// An unplugged drive (DeviceNotFound on every TUR, the Linux fd<0 state once
// the reopen fails) is a dead bus and must fail fast, not poll for ~30 s.
#[test]
fn wait_ready_fails_fast_on_an_unplugged_drive() {
    struct Unplugged;
    impl ScsiTransport for Unplugged {
        fn execute(
            &mut self,
            _cdb: &[u8],
            _dir: DataDirection,
            _data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            Err(Error::DeviceNotFound {
                path: "/dev/sg0".into(),
            })
        }
    }
    let mut d = Drive::from_transport_for_test(Box::new(Unplugged));
    let t0 = std::time::Instant::now();
    let r = d.wait_ready();
    assert!(matches!(r, Err(Error::DeviceNotFound { .. })), "{r:?}");
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(8),
        "unplug must fail fast, took {:?}",
        t0.elapsed()
    );
}

// Slow failing TURs: the verdict comes when the run has lasted the 5 s budget
// past the first failure's completion. With 1.2 s failures + 0.5 s backoff
// that is exactly the 4th failure, so any count rule (2, 3, ...) shows up.
#[test]
fn wait_ready_bounds_slow_transport_failures_by_elapsed_time() {
    struct SlowDeadBus(Arc<std::sync::atomic::AtomicUsize>);
    impl ScsiTransport for SlowDeadBus {
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: DataDirection,
            _data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            self.0.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(std::time::Duration::from_millis(1_200));
            Err(Error::ScsiError {
                opcode: cdb[0],
                status: crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE,
                sense: None,
            })
        }
    }
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut d = Drive::from_transport_for_test(Box::new(SlowDeadBus(calls.clone())));
    let r = d.wait_ready();
    assert!(
        matches!(&r, Err(e) if e.is_scsi_transport_failure()),
        "{r:?}"
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        4,
        "gives up on the first failure completing >= 5 s after the first"
    );
}

// A Stop pressed before the poll starts must be answered at once, not after the ~30 s poll.
#[test]
fn wait_ready_returns_halted_when_stopped_before_the_poll() {
    struct NeverReady;
    impl ScsiTransport for NeverReady {
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: DataDirection,
            _data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            Err(Error::ScsiError {
                opcode: cdb[0],
                status: 2,
                sense: None,
            })
        }
    }
    let mut d = Drive::from_transport_for_test(Box::new(NeverReady));
    d.halt();
    let t0 = std::time::Instant::now();
    let r = d.wait_ready();
    assert!(
        matches!(r, Err(Error::Halted)),
        "a Stop is the operator, not a drive that failed to spin up: {r:?}"
    );
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(5),
        "the poll must abandon immediately, not run its ~30 s course"
    );
}

// A Stop pressed part way through the poll must be answered at the next command boundary.
#[test]
fn wait_ready_returns_halted_when_stopped_during_the_poll() {
    struct StopsOnThirdPoll {
        halt: Arc<AtomicBool>,
        seen: usize,
    }
    impl ScsiTransport for StopsOnThirdPoll {
        fn execute(
            &mut self,
            cdb: &[u8],
            _dir: DataDirection,
            _data: &mut [u8],
            _timeout_ms: u32,
        ) -> Result<ScsiResult> {
            self.seen += 1;
            if self.seen == 3 {
                self.halt.store(true, Ordering::Relaxed);
            }
            Err(Error::ScsiError {
                opcode: cdb[0],
                status: 2,
                sense: None,
            })
        }
    }
    let mut d = Drive::from_transport_for_test(Box::new(StopsOnThirdPoll {
        halt: Arc::new(AtomicBool::new(false)),
        seen: 0,
    }));
    // Hand the transport the drive's OWN flag, so setting it is exactly
    // what an operator's Stop does.
    let flag = d.halt_flag();
    d.scsi = Box::new(StopsOnThirdPoll {
        halt: flag,
        seen: 0,
    });
    let t0 = std::time::Instant::now();
    let r = d.wait_ready();
    assert!(
        matches!(r, Err(Error::Halted)),
        "a mid-poll Stop must surface as Halted, not as DeviceNotReady \
             after the full 30 s: {r:?}"
    );
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(5),
        "must exit on the third poll, not the sixtieth"
    );
}

// spin_cycle must not be deaf to Stop for its ~15 s of deliberate waiting.
#[test]
fn spin_cycle_returns_halted_when_stopped_before_it_starts() {
    let RecordingHarness {
        drive: mut d,
        cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Ok(0));
    d.halt();
    let t0 = std::time::Instant::now();
    let r = d.spin_cycle();
    assert!(
        matches!(r, Err(Error::Halted)),
        "a Stop must abort the spin cycle: {r:?}"
    );
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(2),
        "no sleep may run after the flag is set"
    );
    assert!(
        cdb.lock().unwrap().is_empty(),
        "not one START STOP UNIT may be issued after Stop"
    );
}

// A Stop that lands DURING the spin-down pause must wake it (halt-aware sleep, ~100 ms).
#[test]
fn spin_cycle_wakes_from_its_spin_down_pause_when_stopped() {
    let RecordingHarness {
        drive: mut d,
        cdb: _cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Ok(0));
    let flag = d.halt_flag();
    let stopper = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(200));
        flag.store(true, Ordering::Relaxed);
    });
    let t0 = std::time::Instant::now();
    let r = d.spin_cycle();
    stopper.join().expect("stopper thread");
    assert!(
        matches!(r, Err(Error::Halted)),
        "a Stop during the spin-down pause must surface as Halted: {r:?}"
    );
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(2),
        "the pause must be halt-aware, not a blind {SPIN_DOWN_IDLE_SECS}s \
             thread::sleep; took {:?}",
        t0.elapsed()
    );
}

// A Stop that lands DURING the spin-up settle (the second pause) must wake it too.
#[test]
fn spin_cycle_wakes_from_its_spin_up_settle_when_stopped() {
    let RecordingHarness {
        drive: mut d,
        cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Ok(0));
    let flag = d.halt_flag();
    let stopper = std::thread::spawn(move || {
        // Wait for the START (spin-up) CDB, then stop inside the settle pause.
        while cdb.lock().unwrap().get(4) != Some(&0x01) {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
        flag.store(true, Ordering::Relaxed);
    });
    let t0 = std::time::Instant::now();
    let r = d.spin_cycle();
    let took = t0.elapsed();
    stopper.join().expect("stopper thread");
    assert!(
        matches!(r, Err(Error::Halted)),
        "a Stop during the spin-up settle must surface as Halted: {r:?}"
    );
    assert!(
        took < std::time::Duration::from_secs(SPIN_DOWN_IDLE_SECS + 3),
        "the settle must be halt-aware, not a blind {SPIN_UP_SETTLE_SECS}s \
             thread::sleep; took {took:?}"
    );
}

// A READ(10) with GOOD status but a residual underrun must be refused AND logged.
#[test]
fn read_logs_a_good_status_short_transfer() {
    let RecordingHarness {
        drive: mut d,
        cdb: _cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Ok(1024)); // half of one sector
    let mut buf = vec![0u8; 2048];
    let (r, events) = crate::testlog::capture(|| d.read(7, 1, &mut buf, false));
    assert!(
        matches!(r, Err(Error::DiscRead { sector: 7, .. })),
        "a short transfer stays a failed read: {r:?}"
    );
    let line = events
        .iter()
        .find(|e| e.target == "freemkv::drive" && e.field("transferred").is_some())
        .unwrap_or_else(|| {
            panic!("a silently refused short transfer is the defect; got {events:?}")
        });
    assert_eq!(line.level, tracing::Level::WARN);
    assert_eq!(line.field("lba"), Some("7"));
    assert_eq!(line.field("transferred"), Some("1024"));
    assert_eq!(
        line.field("expected"),
        Some("2048"),
        "the underrun is only legible as transferred-vs-expected"
    );
    assert_eq!(
        line.field("code"),
        Some(crate::error::E_DISC_READ.to_string().as_str())
    );
}

// ── report_key / mode_sense / read_buffer empty-vs-some ─────────

#[test]
fn report_key_rpc_state_returns_transferred_prefix() {
    // Returns buf[..end] where end = bytes_transferred. An 8-byte
    // reply yields all 8 bytes.
    let mut d = drive_with(vec![1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(d.report_key_rpc_state(), Some(vec![1, 2, 3, 4, 5, 6, 7, 8]));
}

#[test]
fn report_key_rpc_state_zero_transfer_returns_none() {
    // end == 0 → None (the `end > 0` guard), never Some(empty).
    let mut d = drive_with(vec![]);
    assert_eq!(d.report_key_rpc_state(), None);
}

#[test]
fn mode_sense_zero_transfer_returns_none() {
    let mut d = drive_with(vec![]);
    assert_eq!(d.mode_sense_page(0x2A), None);
}

#[test]
fn mode_sense_page_positive_transfer_returns_prefix() {
    // Guard is `end > 0`; without a positive-transfer case the guard
    // could be flipped to `end < 0` (always false for a usize) and
    // every call would silently return None.
    let mut d = drive_with(vec![0xAA, 0xBB, 0xCC]);
    assert_eq!(d.mode_sense_page(0x01), Some(vec![0xAA, 0xBB, 0xCC]));
}

#[test]
fn read_buffer_returns_prefix_and_clamps() {
    // read_buffer allocates `length` bytes; FixedTransport returns
    // min(payload, length). Request 16 with a 4-byte payload → 4 bytes.
    let mut d = drive_with(vec![9, 9, 9, 9]);
    assert_eq!(d.read_buffer(0x02, 0xF1, 16), Some(vec![9, 9, 9, 9]));
}

#[test]
fn read_buffer_zero_transfer_returns_none() {
    let mut d = drive_with(vec![]);
    assert_eq!(d.read_buffer(0x02, 0xF1, 16), None);
}

// ── No-unlocker paths: init/probe succeed (OEM fallback) ────────

#[test]
fn init_without_unlocker_is_ok_oem_fallback() {
    // No registered unlocker matches, so route_unlock returns None. init() must
    // still succeed (stock mode for the host-cert handshake) — no-match is the
    // OEM fallback, not a failure.
    let mut d = drive_with(vec![]);
    assert!(
        d.init().is_ok(),
        "no-match init must succeed (OEM fallback)"
    );
    assert!(
        !d.is_ready(),
        "no unlocker ran → not in unlocked-ready state"
    );
}

#[test]
fn probe_disc_without_unlocker_is_ok_noop() {
    // Disc-speed calibration moved into the unlocker (run at init).
    // With no unlocker, probe_disc is a successful no-op.
    let mut d = drive_with(vec![]);
    assert!(d.probe_disc().is_ok());
}

// ── Tray/speed control CDBs (thin wrappers; verify they actually send) ──

#[test]
fn set_speed_sends_set_cd_speed_cdb_with_be_speed() {
    let RecordingHarness {
        drive: mut d,
        cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Ok(0));
    d.set_speed(0x1234);
    let c = cdb.lock().unwrap();
    assert_eq!(c[0], crate::scsi::SCSI_SET_CD_SPEED);
    assert_eq!(&c[2..4], &[0x12, 0x34], "read speed big-endian");
}

// Dropping a drive must not clear a tray lock it never took: find_drive opens
// and drops candidates, and another process may hold the PREVENT.
#[test]
fn drop_unlocks_the_tray_only_if_this_drive_locked_it() {
    let seq = |f: &dyn Fn(&mut Drive)| {
        let cdbs = Arc::new(Mutex::new(Vec::new()));
        let mut d = Drive::from_transport_for_test(Box::new(SequenceTransport {
            cdbs: cdbs.clone(),
            ok: true,
        }));
        f(&mut d);
        cdbs.lock().unwrap().clear();
        drop(d);
        cdbs.lock().unwrap().clone()
    };
    assert!(seq(&|_| {}).is_empty(), "never locked: Drop sends nothing");
    let after_lock = seq(&|d| d.lock_tray());
    assert_eq!(after_lock.len(), 1, "locked: Drop unlocks {after_lock:?}");
    assert_eq!(after_lock[0][0], SCSI_PREVENT_ALLOW_MEDIUM_REMOVAL);
    assert_eq!(after_lock[0][4], 0x00, "ALLOW");
    let unlocked = seq(&|d| {
        d.lock_tray();
        d.unlock_tray();
    });
    assert!(unlocked.is_empty(), "already unlocked: {unlocked:?}");
}

#[test]
fn lock_tray_sends_prevent_with_removal_bit_set() {
    let RecordingHarness {
        drive: mut d,
        cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Ok(0));
    d.lock_tray();
    let c = cdb.lock().unwrap();
    assert_eq!(c[0], SCSI_PREVENT_ALLOW_MEDIUM_REMOVAL);
    assert_eq!(c[4], 0x01, "PREVENT bit set (locked)");
}

#[test]
fn unlock_tray_sends_prevent_with_removal_bit_clear() {
    let RecordingHarness {
        drive: mut d,
        cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Ok(0));
    d.unlock_tray();
    let c = cdb.lock().unwrap();
    assert_eq!(c[0], SCSI_PREVENT_ALLOW_MEDIUM_REMOVAL);
    assert_eq!(c[4], 0x00, "PREVENT bit clear (unlocked)");
}

// SET CD SPEED and PREVENT/ALLOW MEDIUM REMOVAL are best-effort: a CHECK
// CONDITION or a transport failure still issues the CDB, never fails the
// rip, and a failed unlock is warned about (a stuck PREVENT is a real symptom).
#[test]
fn set_speed_and_tray_control_swallow_rejection_and_transport_failure() {
    for status in [0x02, crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE] {
        let RecordingHarness {
            drive: mut d,
            cdb,
            timeouts: _to,
        } = recording(TransportOutcome::Scsi(status, None));
        d.set_speed(0x1234);
        assert_eq!(
            cdb.lock().unwrap()[0],
            crate::scsi::SCSI_SET_CD_SPEED,
            "SET CD SPEED still issued despite status {status:#x}"
        );
        d.lock_tray();
        assert_eq!(cdb.lock().unwrap()[0], SCSI_PREVENT_ALLOW_MEDIUM_REMOVAL);
        assert_eq!(cdb.lock().unwrap()[4], 0x01, "PREVENT bit still set");
        let ((), events) = crate::testlog::capture(|| d.unlock_tray());
        assert_eq!(cdb.lock().unwrap()[0], SCSI_PREVENT_ALLOW_MEDIUM_REMOVAL);
        assert_eq!(cdb.lock().unwrap()[4], 0x00, "ALLOW bit still clear");
        let warned = events
            .iter()
            .any(|e| e.level == tracing::Level::WARN && e.field("phase") == Some("unlock_tray"));
        assert!(
            warned,
            "failed unlock must warn (status {status:#x}): {events:?}"
        );
    }
}

/// Logs every CDB (unlike `RecordingTransport`, which keeps the last) and answers
/// each command with `payload`.
struct LogTransport {
    log: Arc<Mutex<Vec<Vec<u8>>>>,
    payload: Vec<u8>,
}
impl ScsiTransport for LogTransport {
    fn execute(
        &mut self,
        cdb: &[u8],
        _dir: DataDirection,
        data: &mut [u8],
        _timeout_ms: u32,
    ) -> Result<ScsiResult> {
        self.log.lock().unwrap().push(cdb.to_vec());
        let n = self.payload.len().min(data.len());
        data[..n].copy_from_slice(&self.payload[..n]);
        Ok(ScsiResult {
            status: 0,
            bytes_transferred: n,
            sense: [0u8; 32],
        })
    }
}

fn logging(payload: Vec<u8>) -> (Drive, Arc<Mutex<Vec<Vec<u8>>>>) {
    let log = Arc::new(Mutex::new(Vec::new()));
    let t = LogTransport {
        log: log.clone(),
        payload,
    };
    (Drive::from_transport_for_test(Box::new(t)), log)
}

#[test]
fn eject_unlocks_then_sends_start_stop_with_loej() {
    let (mut d, log) = logging(Vec::new());
    d.eject().unwrap();
    let log = log.lock().unwrap();
    let first = &log[0];
    assert_eq!(first[0], SCSI_PREVENT_ALLOW_MEDIUM_REMOVAL, "unlock first");
    assert_eq!(first[4], 0x00, "ALLOW");
    let last = log.last().unwrap();
    assert_eq!(last[0], SCSI_START_STOP_UNIT);
    assert_eq!(last[4], 0x02, "START=0, LOEJ=1 -> eject");
}

#[test]
fn enable_recovered_error_reporting_sends_mode_select10_with_payload_length() {
    // 8-byte MODE(10) header, no block descriptor, 12-byte error-recovery page.
    let mut sense = vec![0u8; 20];
    sense[1] = 18; // mode data length
    sense[8] = MODE_PAGE_ERROR_RECOVERY;
    sense[9] = 0x0A;
    let (mut d, log) = logging(sense);
    assert!(d.enable_recovered_error_reporting());
    let log = log.lock().unwrap();
    let c = log.last().unwrap();
    assert_eq!(c[0], SCSI_MODE_SELECT);
    assert_eq!(c[1], 0x10, "PF=1, SP=0");
    assert_eq!(&c[7..9], &[0x00, 20], "parameter list length");
}

/// `SectorSource for Drive` must actually forward to `Drive`'s own
/// methods, not silently become a no-op / stub return.
#[test]
fn sector_source_impl_forwards_to_drive_methods() {
    let RecordingHarness {
        drive: mut d,
        cdb,
        timeouts: _to,
    } = recording(TransportOutcome::Ok(2048));
    let mut buf = vec![0u8; 2048];
    let n = SectorSource::read_sectors(&mut d, 0, 1, &mut buf, false).unwrap();
    assert_eq!(n, 2048, "read_sectors must forward to Drive::read");
    assert_eq!(cdb.lock().unwrap()[0], crate::scsi::SCSI_READ_10);

    let n2 = SectorSource::read_sectors_fua(&mut d, 0, 1, &mut buf, false, true).unwrap();
    assert_eq!(n2, 2048, "read_sectors_fua must forward to Drive::read_fua");
    assert_eq!(
        cdb.lock().unwrap()[1],
        0x08,
        "fua=true must reach the CDB via the trait method"
    );

    SectorSource::set_speed(&mut d, 0xFFFF);
    assert_eq!(
        cdb.lock().unwrap()[0],
        crate::scsi::SCSI_SET_CD_SPEED,
        "SectorSource::set_speed must forward to Drive::set_speed"
    );
}

// ── decode_read_capacity additional boundaries ──────────────────

#[test]
fn read_capacity_exactly_4_bytes_decodes() {
    // bytes_transferred == 4 is the minimum that decodes (the guard
    // is `< 4`). last_lba in bytes 0..4 big-endian.
    let buf = [0x00, 0x00, 0x00, 0x05, 0, 0, 0, 0];
    assert_eq!(decode_read_capacity(&buf, 4).unwrap(), 6);
}

#[test]
fn read_capacity_zero_last_lba_is_one_sector() {
    // last_lba 0 → capacity 1 (a single-sector medium), distinct from
    // the malformed/short-transfer rejection.
    let buf = [0, 0, 0, 0, 0, 0, 0, 0];
    assert_eq!(decode_read_capacity(&buf, 8).unwrap(), 1);
}
