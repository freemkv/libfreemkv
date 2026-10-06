//! Drive session — open, identify, and read from optical drives.
//!
//! A `Drive` is opened from a device path, identifies itself via INQUIRY,
//! optionally unlocks/initializes via the `freemkv-unlock` dispatch
//! (through [`crate::unlock_bridge`]), and reads sectors.

pub fn extract_scsi_context(e: &Error) -> (u8, Option<crate::scsi::ScsiSense>) {
    match e {
        Error::ScsiError { status, sense, .. } => (*status, *sense),
        Error::DiscRead { status, sense, .. } => (status.unwrap_or(0), *sense),
        // A failed `ioctl(SG_IO)` or vanished device is a dead-bus fault, not a
        // recoverable bad sector — map to SCSI_STATUS_TRANSPORT_FAILURE so sweep /
        // patch / fill_extents abort the pass instead of zero-filling a wedged device.
        Error::IoError { .. } | Error::DeviceNotFound { .. } => {
            (crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE, None)
        }
        _ => (0, None),
    }
}

// Per-platform discovery helpers (the `pub(crate)` `find_drives` /
// equivalents). Crate-public so `scsi/{linux,macos,windows}.rs` can
// reuse the existing enumeration logic when shaping `DriveInfo`.
#[cfg(target_os = "linux")]
pub(crate) mod linux;
#[cfg(target_os = "macos")]
pub(crate) mod macos;
#[cfg(windows)]
pub(crate) mod windows;

pub(crate) mod allow;
#[cfg(test)]
mod stop_tests;

// Pick the platform module ONCE, here, so the cross-platform entry points below
// dispatch through `platform::…` with no per-function `#[cfg]` in their bodies.
#[cfg(target_os = "linux")]
pub(crate) use linux as platform;
#[cfg(target_os = "macos")]
pub(crate) use macos as platform;
#[cfg(windows)]
pub(crate) use windows as platform;

use crate::error::{Error, Result};
use crate::halt::{Halt, Liveness};
use crate::identity::DriveId;
use crate::scsi::ScsiTransport;
use crate::sector::SectorSource;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub(crate) use allow::CleanupCtx;

/// Physical state of the drive tray and disc.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DriveStatus {
    /// Tray is open
    TrayOpen,
    /// Tray closed, no disc
    NoDisc,
    /// Tray closed, disc present and ready
    DiscPresent,
    /// Drive is loading or spinning up
    NotReady,
    /// Could not determine status
    Unknown,
}

// SCSI opcodes used in drive control
const SCSI_TEST_UNIT_READY: u8 = 0x00;
const SCSI_START_STOP_UNIT: u8 = allow::START_STOP_UNIT;
/// Idle time the disc sits spun-down during [`Drive::spin_cycle`] before it's
/// spun back up — long enough for the mechanism's fast-fail wedge state to
/// clear. Validated at 5–6 s live.
const SPIN_DOWN_IDLE_SECS: u64 = 5;
/// Settle time after spin-up in [`Drive::spin_cycle`] before the caller reads
/// again, so the first post-cycle read doesn't hit a transient NOT_READY.
const SPIN_UP_SETTLE_SECS: u64 = 10;
const SCSI_PREVENT_ALLOW_MEDIUM_REMOVAL: u8 = allow::PREVENT_ALLOW;
const SCSI_GET_EVENT_STATUS: u8 = 0x4A;
const SCSI_MODE_SENSE: u8 = 0x5A;
const SCSI_MODE_SELECT: u8 = 0x55;
const SCSI_REPORT_KEY: u8 = allow::REPORT_KEY;

// SBC/MMC Read-Write Error Recovery mode page. Flipping PER makes the drive REPORT recovered
// reads instead of silently returning best-effort GOOD data.
const MODE_PAGE_ERROR_RECOVERY: u8 = 0x01;
/// Bit masks in the Read-Write Error Recovery flags byte (page byte 2).
const ERP_FLAG_TB: u8 = 0x20; // Transfer Block: still deliver the recovered data
const ERP_FLAG_PER: u8 = 0x04; // Post Error: report recovered errors
const ERP_FLAG_DTE: u8 = 0x02; // Data Terminate on Error: MUST be off (we want the data)
/// `Parameters Saveable` bit in a mode page's byte 0 — valid only on MODE SENSE;
/// must be cleared before echoing the page back in a MODE SELECT.
const MODE_PAGE_PS_BIT: u8 = 0x80;
/// MODE SENSE(10) parameter header length (bytes), preceding any block
/// descriptors and the mode pages.
const MODE10_HEADER_LEN: usize = 8;

/// Optical disc drive session -- open, identify, unlock, and read.
pub struct Drive {
    scsi: Box<dyn ScsiTransport>,
    /// Name of the unlocker that handled this drive at `init()`, if any matched.
    /// `None` means no unlocker matched and the drive runs in stock mode
    /// (host-cert AACS handshake carries discs).
    unlocker_name: Option<String>,
    /// The OEM Volume ID the matching unlocker returned from `unlock()` at
    /// `init()`, stashed for the AACS handshake phase (which reads it via
    /// [`Drive::oem_vid`] instead of a separate VID read). `None` when no
    /// unlocker matched or the matching unlocker produced no VID — the cert
    /// handshake then acquires the VID.
    oem_vid: Option<[u8; 16]>,
    /// True once `init()` has run (whether or not an unlocker matched).
    init_ran: bool,
    /// `lock_tray` was called and no `unlock_tray` since: only then does Drop
    /// send ALLOW (never clear a lock another process holds).
    tray_locked: bool,
    /// Lazily-computed registry-match name for `platform_name()`'s `&str`
    /// return before `init()` has run.
    matched_name_cache: std::sync::OnceLock<String>,
    pub drive_id: DriveId,
    device_path: String,
    /// The op token every CDB is checked against (§2.2).
    slot: Slot,
    /// Bumped on every `exec` completion, busy while one is in flight (T29 feed).
    progress: Option<Liveness>,
    /// AGIDs allocated through this Drive and not yet invalidated (§2.2 ledger).
    agids: u8,
    /// The SINGLE AACS bus-encryption removal point. Decided ONCE from the
    /// unlock/handshake result during [`crate::disc::Disc::scan`] and applied on
    /// every read below this drive, so every reader above (sampler, mux, sweep,
    /// validation) sees already-de-bussed content and never threads a bus key.
    /// Defaults to [`BusStage::Passthrough`](crate::sector::bus_removal::BusStage)
    /// (firmware/vendor unlock de-busses at the drive, or the disc carries no bus
    /// encryption).
    bus_stage: crate::sector::bus_removal::BusStage,
    /// Stream-file bus map gating the host-key de-bus per AACS aligned unit — clear
    /// UDF/nav sectors and CPI=0 units are left untouched. `None` = de-bus every read
    /// sector (a content-only reader). Ignored entirely under `BusStage::Passthrough`.
    bus_gate: Option<crate::sector::bus_removal::BusGate>,
    /// Linux only: raw fd for the corresponding block device (`/dev/sr*`)
    /// used as a recovery fallback when SCSI READ via `/dev/sg*` returns
    /// an error. The kernel `sr_mod` driver auto-retries failed reads
    /// (~5× per command) — historically the reason `dd if=/dev/sr0`
    /// recovers ~50% of bad sectors that single-shot `SG_IO` READ
    /// misses on the same drive. `None` when the block device couldn't
    /// be resolved or opened (no fallback in that case; SCSI read
    /// errors propagate as before).
    #[cfg(target_os = "linux")]
    block_dev_fd: Option<std::os::unix::io::RawFd>,
}

/// Which token a Drive checks every CDB against (stop design §2.2).
#[derive(Debug)]
enum Slot {
    /// `own`: the Drive made the token itself ([`Drive::open`]); `false`: the caller's
    /// op token ([`Drive::open_with`], [`Drive::attach`]).
    Attached { token: Halt, own: bool },
    /// No token: `exec` panics in debug, and in release warns once and runs
    /// uncancellably.
    Detached,
}

impl Slot {
    fn own() -> Self {
        Slot::Attached {
            token: Halt::new(),
            own: true,
        }
    }

    fn foreign(halt: &Halt) -> Self {
        Slot::Attached {
            token: halt.clone(),
            own: false,
        }
    }

    fn token(&self) -> Option<&Halt> {
        match self {
            Slot::Attached { token, .. } => Some(token),
            Slot::Detached => None,
        }
    }
}

/// While alive, the Drive checks a `ScanOptions.halt` alias; dropping it (on every
/// exit, unwind included) restores the slot it replaced (§2.2).
pub(crate) struct AliasGuard<'a> {
    drive: &'a mut Drive,
    saved: Option<Slot>,
}

impl std::ops::Deref for AliasGuard<'_> {
    type Target = Drive;
    fn deref(&self) -> &Drive {
        self.drive
    }
}

impl std::ops::DerefMut for AliasGuard<'_> {
    fn deref_mut(&mut self) -> &mut Drive {
        self.drive
    }
}

impl Drop for AliasGuard<'_> {
    fn drop(&mut self) {
        if let Some(slot) = self.saved.take() {
            self.drive.slot = slot;
        }
    }
}

impl Drive {
    /// Open the drive with its own token, cancelled by [`halt`](Self::halt).
    pub fn open(device: &Path) -> Result<Self> {
        Self::open_slot(device, Slot::own())
    }

    /// Open the drive checking the caller's op token: identification and every later
    /// CDB are refused once `halt` is cancelled.
    pub fn open_with(device: &Path, halt: &Halt) -> Result<Self> {
        Self::open_slot(device, Slot::foreign(halt))
    }

    fn open_slot(device: &Path, slot: Slot) -> Result<Self> {
        let t0 = std::time::Instant::now();
        tracing::info!(target: "freemkv::drive", phase = "open", device = %device.display(), "begin");
        let transport = match slot.token() {
            Some(halt) => crate::scsi::open_with(device, halt)?,
            None => crate::scsi::open(device)?,
        };
        let mut drive = Self::bare(transport, device.to_string_lossy().to_string(), slot);
        drive.drive_id = DriveId::identify(&mut |cdb, dir, buf, t| drive.exec(cdb, dir, buf, t))?;
        tracing::info!(
            target: "freemkv::drive",
            phase = "open",
            device = %device.display(),
            vendor = %drive.drive_id.vendor_id.trim(),
            product = %drive.drive_id.product_id.trim(),
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "end"
        );
        #[cfg(target_os = "linux")]
        {
            drive.block_dev_fd = open_block_device_for_sg(device);
        }
        Ok(drive)
    }

    // A Drive over `scsi` with a blank identity (filled in by the caller).
    fn bare(scsi: Box<dyn ScsiTransport>, device_path: String, slot: Slot) -> Self {
        Drive {
            scsi,
            unlocker_name: None,
            oem_vid: None,
            init_ran: false,
            tray_locked: false,
            matched_name_cache: std::sync::OnceLock::new(),
            drive_id: DriveId {
                vendor_id: String::new(),
                product_id: String::new(),
                product_revision: String::new(),
                vendor_specific: String::new(),
                firmware_date: String::new(),
                serial_number: String::new(),
                raw_inquiry: Vec::new(),
                raw_gc_010c: Vec::new(),
            },
            device_path,
            slot,
            progress: None,
            agids: 0,
            bus_stage: crate::sector::bus_removal::BusStage::Passthrough,
            bus_gate: None,
            #[cfg(target_os = "linux")]
            block_dev_fd: None,
        }
    }

    /// Set the SINGLE AACS bus-removal stage for this drive, decided once from
    /// the unlock/handshake result. `AacsHostKey(rdk)` de-busses each content
    /// read in software; `Passthrough` (a firmware/vendor unlock, or a non-bus
    /// disc) is a no-op. Every reader above the drive inherits the result.
    pub fn set_bus_stage(&mut self, stage: crate::sector::bus_removal::BusStage) {
        self.bus_stage = stage;
    }

    /// Gate host-key de-bussing with the disc's stream-file bus map (built by
    /// [`crate::disc::Disc::scan`]); unset means every read sector is content.
    pub fn set_bus_map(&mut self, map: Arc<crate::sector::bus_removal::BusMap>) {
        self.bus_gate = Some(crate::sector::bus_removal::BusGate::new(map));
    }

    /// [`set_bus_map`](Self::set_bus_map) over plain ranges, each treated as
    /// one unit-aligned stream file.
    pub fn set_bus_content_ranges(&mut self, ranges: Arc<[(u32, u32)]>) {
        self.set_bus_map(Arc::new(crate::sector::bus_removal::BusMap::from_ranges(
            &ranges,
        )));
    }

    /// The SOLE application of AACS bus decryption in the read path: under a
    /// host-key stage, de-bus the `data` that a raw read landed at `lba`, gated
    /// by the bus map. `Passthrough` is a no-op, so the raw
    /// `read`/`read_fua` used by internal callers stay bus-encrypted while
    /// `SectorSource` readers above see plaintext content.
    fn remove_bus_encryption(&mut self, lba: u32, data: &mut [u8]) {
        let crate::sector::bus_removal::BusStage::AacsHostKey(rdk) = self.bus_stage.clone() else {
            return;
        };
        let mut gate = self.bus_gate.take();
        // Head fetch is fast-timeout (never recovery): a failure caches as encrypted.
        crate::sector::bus_removal::debus_read(gate.as_mut(), data, &rdk, lba, &mut |h| {
            let mut s = [0u8; 2048];
            match self.read_fua(h, 1, &mut s, false, false) {
                Ok(n) if n >= 2048 => Some(s[0]),
                _ => None,
            }
        });
        self.bus_gate = gate;
    }

    // Test-only constructor: build a `Drive` over an arbitrary `ScsiTransport`
    // (no profile, no platform driver, no block-device fallback) so
    // command-builder/response-parser logic can be exercised against a mock.
    #[cfg(test)]
    pub(crate) fn from_transport_for_test(scsi: Box<dyn ScsiTransport>) -> Self {
        Self::bare(scsi, "test".to_string(), Slot::own())
    }

    /// Test fixture (feature `test-util`): a Drive over any transport (e.g.
    /// [`crate::test_util::FakeTransport`]) with its own token and a blank identity.
    #[cfg(any(test, feature = "test-util"))]
    pub fn from_transport(scsi: Box<dyn ScsiTransport>) -> Self {
        Self::bare(scsi, "test".to_string(), Slot::own())
    }

    /// Test fixture (feature `test-util`): [`from_transport`](Self::from_transport)
    /// checking the caller's op token, as [`open_with`](Self::open_with) does.
    #[cfg(any(test, feature = "test-util"))]
    pub fn from_transport_with(scsi: Box<dyn ScsiTransport>, halt: &Halt) -> Self {
        Self::bare(scsi, "test".to_string(), Slot::foreign(halt))
    }

    /// Test-only: mark the drive as claimed by a named firmware unlocker at
    /// init(), so the do_handshake_cert anti-poison guard can be exercised.
    #[cfg(test)]
    pub(crate) fn set_unlocker_name_for_test(&mut self, name: &str) {
        self.unlocker_name = Some(name.to_string());
    }

    /// Test-only: stash the OEM Volume ID a matching unlocker would have
    /// returned at init(), so `do_handshake_cert`'s OEM-VID short-circuit
    /// (skip the cert handshake, credit `drive_unlocked`) can be exercised.
    #[cfg(test)]
    pub(crate) fn set_oem_vid_for_test(&mut self, vid: [u8; 16]) {
        self.oem_vid = Some(vid);
    }

    /// The attached token as a raw flag (a view over the same bit, so setting it
    /// is a Stop). A detached Drive hands out a fresh, never-read flag.
    pub fn halt_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        match self.slot.token() {
            Some(t) => t.as_arc().clone(),
            None => Arc::default(),
        }
    }

    /// Cancel the attached token: the next CDB is refused, and a READ in flight is
    /// discarded when it completes.
    pub fn halt(&self) {
        if let Some(t) = self.slot.token() {
            t.cancel();
        }
    }

    /// The token every CDB is checked against, or `None` once [`detach`](Self::detach)ed.
    pub fn token(&self) -> Option<&Halt> {
        self.slot.token()
    }

    /// Check the caller's op token from now on (it replaces the Drive's own).
    pub fn attach(&mut self, halt: &Halt) {
        self.slot = Slot::foreign(halt);
    }

    /// Stop checking any token, handing back the one that was attached. Until the
    /// next [`attach`](Self::attach), `exec` panics in debug builds and in release
    /// warns once and runs uncancellably.
    pub fn detach(&mut self) -> Option<Halt> {
        match std::mem::replace(&mut self.slot, Slot::Detached) {
            Slot::Attached { token, .. } => Some(token),
            Slot::Detached => None,
        }
    }

    /// Report forward progress to `p`: bumped on every CDB completion, and
    /// [`busy`](Liveness::busy) while a CDB (or a scan bus step) is in flight.
    pub fn attach_progress(&mut self, p: &Liveness) {
        self.progress = Some(p.clone());
    }

    pub(crate) fn progress(&self) -> Option<&Liveness> {
        self.progress.as_ref()
    }

    /// The `ScanOptions.halt` alias rule (§2.2): over the Drive's own token (or none),
    /// check `alias` until the guard drops, then restore; over a caller's attached
    /// token, that token wins and a different alias is logged.
    pub(crate) fn alias(&mut self, alias: Option<&Halt>) -> AliasGuard<'_> {
        let saved = match (alias, &self.slot) {
            (None, _) => None,
            (Some(a), Slot::Attached { token, own: false }) => {
                if !Arc::ptr_eq(a.as_arc(), token.as_arc()) {
                    tracing::warn!(
                        target: "freemkv::drive",
                        phase = "halt_alias",
                        "ScanOptions.halt differs from the op token attached to this drive; the attached token wins"
                    );
                }
                None
            }
            (Some(a), _) => Some(std::mem::replace(&mut self.slot, Slot::foreign(a))),
        };
        AliasGuard { drive: self, saved }
    }

    pub(crate) fn is_halted(&self) -> bool {
        self.slot.token().is_some_and(Halt::is_cancelled)
    }

    /// `Err(Halted)` once the attached token is cancelled.
    pub(crate) fn check_token(&self) -> Result<()> {
        self.slot.token().map_or(Ok(()), Halt::check)
    }

    /// Wait `d` on the attached token: `Err(Halted)` within one slice of a Stop.
    pub(crate) fn pause(&self, d: Duration) -> Result<()> {
        match self.slot.token() {
            Some(t) => t.wait(d),
            None => Halt::new().wait(d),
        }
    }

    /// The §2.2 ledger: what a post-cancel clean-up CDB may release.
    pub(crate) fn ledger(&self) -> allow::Ledger {
        allow::Ledger {
            tray_locked: self.tray_locked,
            agids: self.agids,
        }
    }

    // A Detached exec: a bug in debug; in release, one warning, then uncancellable.
    fn on_detached_exec(&self) {
        static WARNED: std::sync::Once = std::sync::Once::new();
        debug_assert!(false, "Drive::exec on a detached drive");
        WARNED.call_once(|| {
            tracing::warn!(target: "freemkv::drive", phase = "exec", "CDB on a detached drive runs uncancellably");
        });
    }

    /// Every CDB this Drive issues goes through here (§2.2): the token is checked
    /// before dispatch (a cancelled op issues nothing), and a READ-class CDB is
    /// checked again on completion so its buffer is never returned as good.
    pub(crate) fn exec(
        &mut self,
        cdb: &[u8],
        dir: crate::scsi::DataDirection,
        buf: &mut [u8],
        timeout_ms: u32,
    ) -> Result<crate::scsi::ScsiResult> {
        crate::halt::diag::assert_may_block("Drive::exec", Duration::MAX);
        match self.slot.token() {
            None => self.on_detached_exec(),
            Some(t) => t.check()?,
        }
        let r = self.dispatch(cdb, dir, buf, timeout_ms);
        if is_read_class(cdb) {
            self.check_token()?;
        }
        r
    }

    /// A CDB inside a critical section entered before any cancel (§2.4 row 4): not
    /// checked against the token.
    pub(crate) fn exec_uncancellable(
        &mut self,
        cdb: &[u8],
        dir: crate::scsi::DataDirection,
        buf: &mut [u8],
        timeout_ms: u32,
    ) -> Result<crate::scsi::ScsiResult> {
        crate::halt::diag::assert_may_block("Drive::exec_uncancellable", Duration::MAX);
        self.dispatch(cdb, dir, buf, timeout_ms)
    }

    /// A clean-up CDB (§2.4): runs as usual until a cancel, then only if the
    /// allow-list admits it from `ctx` with this Drive's ledger; refused with
    /// `Halted` (and not sent) otherwise.
    pub(crate) fn exec_cleanup(
        &mut self,
        cdb: &[u8],
        dir: crate::scsi::DataDirection,
        buf: &mut [u8],
        timeout_ms: u32,
        ctx: CleanupCtx,
    ) -> Result<crate::scsi::ScsiResult> {
        crate::halt::diag::assert_may_block("Drive::exec_cleanup", Duration::MAX);
        if self.is_halted() && !allow::allowed_after_cancel(cdb, self.ledger(), ctx) {
            return Err(Error::Halted);
        }
        self.dispatch(cdb, dir, buf, timeout_ms)
    }

    // The one call into the transport: progress is busy while the CDB is in flight
    // and bumped on its completion, and the AGID ledger follows REPORT KEY.
    fn dispatch(
        &mut self,
        cdb: &[u8],
        dir: crate::scsi::DataDirection,
        buf: &mut [u8],
        timeout_ms: u32,
    ) -> Result<crate::scsi::ScsiResult> {
        let busy = self.progress.as_ref().map(Liveness::busy);
        let r = self.scsi.as_mut().execute(cdb, dir, buf, timeout_ms);
        drop(busy);
        if let Some(p) = &self.progress {
            p.bump();
        }
        self.note_ledger(cdb, buf, r.is_ok());
        r
    }

    // §2.2 ledger: an ALLOW that reached the drive ends this Drive's claim on the tray
    // (so it is sent once); a successful REPORT KEY key format 0 allocates the AGID in
    // response byte 7 bits 7-6; key format 3Fh invalidates the AGID in byte 10 bits 7-6.
    fn note_ledger(&mut self, cdb: &[u8], resp: &[u8], ok: bool) {
        if cdb.first() == Some(&allow::PREVENT_ALLOW) && cdb.get(4).is_some_and(|b| b & 1 == 0) {
            self.tray_locked = false;
        }
        // SS-7 (evidence, not spec) libaacs mmc.c: "*agid = (buf[7] & 0xff) >> 6;" on
        // allocation, and "cmd[10] = (agid << 6) | (format & 0x3f);" names the AGID.
        if cdb.first() != Some(&allow::REPORT_KEY) || cdb.len() < 11 {
            return;
        }
        if let Some(agid) = allow::invalidated_agid(cdb) {
            self.agids &= !(1 << agid);
        } else if cdb[10] & 0x3F == 0 && ok && resp.len() > 7 {
            self.agids |= 1 << (resp[7] >> 6);
        }
    }

    /// Close the drive cleanly. Unlocks the tray and closes the fd.
    /// Also runs automatically on Drop as a safety net.
    pub fn close(self) {
        // cleanup() runs here via Drop
    }

    /// Shared cleanup — called by Drop (and thus by close).
    fn cleanup(&mut self) {
        if self.tray_locked {
            self.unlock_tray();
        }
    }

    /// Whether an unlocker claims this drive by identity (i.e. it can be
    /// unlocked at drive-prep). Queried via `freemkv-unlock`; does not require
    /// `init()` to have run.
    pub fn has_profile(&self) -> bool {
        crate::unlock_bridge::unlocker_name(&self.drive_id).is_some()
    }

    /// The name of the drive unlocker that ACTUALLY unlocked this drive at
    /// `init()`, or `None` if none applied (unsupported drive, or the unlock
    /// failed). Distinct from [`has_profile`](Self::has_profile), which reports
    /// only an identity match: this is the runtime outcome. Apps render it in the
    /// user-facing unlocker report.
    pub fn unlocker_name(&self) -> Option<&str> {
        self.unlocker_name.as_deref()
    }

    // The OEM Volume ID a matching unlocker returned at Drive::init, if any.
    // AACS uses it to skip the cert handshake. None if no unlocker matched.
    pub(crate) fn oem_vid(&self) -> Option<[u8; 16]> {
        self.oem_vid
    }

    /// Wait for the drive to become ready: TEST UNIT READY every 500 ms until it
    /// answers GOOD. Fails with `DeviceNotReady` after 60 s without progress
    /// (§2.11): an answer not yet seen in this wait, or a rising progress indicator;
    /// or after an absolute 10 min ceiling, however much progress it shows.
    /// A Stop interrupts between polls; a dead bus fails after 5 s of failures.
    pub fn wait_ready(&mut self) -> Result<()> {
        self.wait_ready_with(WaitReadyTiming::PRODUCTION)
    }

    pub(crate) fn wait_ready_with(&mut self, timing: WaitReadyTiming) -> Result<()> {
        // SS-4 TEST UNIT READY: "provides a means to check if the logical unit is ready".
        let tur = [SCSI_TEST_UNIT_READY, 0x00, 0x00, 0x00, 0x00, 0x00];
        let t0 = std::time::Instant::now();
        tracing::info!(target: "freemkv::drive", phase = "wait_ready", "begin");
        let mut hb = crate::progress::Heartbeat::new("wait_ready");
        let ceiling_hit: bool;
        // T6: the answers seen so far and the highest progress indicator; either
        // growing re-arms the no-progress window.
        let moved = Liveness::new();
        let mut stall = crate::halt::StallTimer::new(timing.window, &moved);
        let mut seen: Vec<Option<(u8, u8, u8)>> = Vec::new();
        let mut best: Option<u16> = None;
        // (when the first failure of the current run completed, failures in it)
        let mut failing: Option<(std::time::Instant, u32)> = None;
        let mut start_sent = false;
        // Consecutive 3Ah answers, and whether 04/01 (a disc being identified) was seen.
        let (mut empty_run, mut becoming_ready_seen) = (0u32, false);
        let mut attempt = 0u64;
        loop {
            attempt += 1;
            hb.tick(t0.elapsed().as_secs(), timing.window.as_secs());
            let mut buf = [0u8; 0];
            match self.exec(
                &tur,
                crate::scsi::DataDirection::None,
                &mut buf,
                crate::scsi::TUR_TIMEOUT_MS,
            ) {
                Ok(_) => {
                    tracing::info!(
                        target: "freemkv::drive",
                        phase = "wait_ready",
                        attempts = attempt,
                        elapsed_ms = t0.elapsed().as_millis() as u64,
                        "end"
                    );
                    return Ok(());
                }
                Err(Error::Halted) => return Err(Error::Halted),
                // T5: transport failures (DID_TIME_OUT/DID_RESET, fd<0 DeviceNotFound) may be
                // a hiccup; a run of 2+ lasting the budget past the first one's completion
                // is a dead bus. Any drive answer ends the run.
                Err(e) if e.is_scsi_transport_failure() => {
                    empty_run = 0;
                    let (since, n) = failing.get_or_insert((std::time::Instant::now(), 0));
                    *n += 1;
                    if *n >= 2 && since.elapsed() >= timing.dead_bus {
                        return Err(e);
                    }
                }
                Err(e) => {
                    failing = None;
                    let sense = e.scsi_sense().map(|s| (s.sense_key, s.asc, s.ascq));
                    // §2.11: a new answer in this wait is progress; the same one, or two
                    // known ones alternating, is not.
                    if !seen.contains(&sense) {
                        seen.push(sense);
                        moved.bump();
                    }
                    // SS-1: "The PROGRESS INDICATION field is a percent complete indication";
                    // a value above the best seen so far is progress.
                    if let Some(p) = self.scsi.last_sense_progress()
                        && best.is_none_or(|b| p > b)
                    {
                        best = Some(p);
                        moved.bump();
                    }
                    let empty = matches!(sense, Some((crate::scsi::SENSE_KEY_NOT_READY, 0x3A, _)));
                    empty_run = if empty { empty_run + 1 } else { 0 };
                    becoming_ready_seen |=
                        sense == Some((crate::scsi::SENSE_KEY_NOT_READY, 0x04, 0x01));
                    // SS-3 MMC-6 Table F.3 "2 3A 00 MEDIUM NOT PRESENT": an empty drive that
                    // never said 04/01 will not become ready.
                    if !becoming_ready_seen && empty_run >= WAIT_READY_MAX_EMPTY_POLLS {
                        return Err(e);
                    }
                    match sense {
                        // SS-3 "2 30 00 INCOMPATIBLE MEDIUM INSTALLED": never becomes
                        // ready, so surface its sense now.
                        Some((crate::scsi::SENSE_KEY_NOT_READY, 0x30, _)) => return Err(e),
                        // SS-3 "2 04 02 LOGICAL UNIT NOT READY, INITIALIZING CMD. REQUIRED":
                        // nothing else spins the unit up, so START UNIT once.
                        Some((crate::scsi::SENSE_KEY_NOT_READY, 0x04, 0x02)) if !start_sent => {
                            start_sent = true;
                            self.start_unit()?;
                        }
                        _ => {}
                    }
                }
            }
            let stalled = stall.poll(&moved) == crate::halt::Stall::Expired;
            if stalled || t0.elapsed() >= timing.ceiling {
                ceiling_hit = !stalled;
                break;
            }
            self.pause(timing.poll)?;
        }
        tracing::warn!(
            target: "freemkv::drive",
            phase = "wait_ready",
            elapsed_ms = t0.elapsed().as_millis() as u64,
            window_ms = timing.window.as_millis() as u64,
            ceiling_hit,
            "device never became ready: no progress for the window, or the ceiling was hit"
        );
        Err(Error::DeviceNotReady {
            path: self.device_path.clone(),
        })
    }

    /// Query the physical state of the drive — disc present, tray open, etc.
    /// Uses GET EVENT STATUS NOTIFICATION which works regardless of firmware state.
    pub fn drive_status(&mut self) -> DriveStatus {
        // GET EVENT STATUS NOTIFICATION: polled, media event class (0x10)
        let cdb = [
            SCSI_GET_EVENT_STATUS,
            0x01,
            0x00,
            0x00,
            0x10,
            0x00,
            0x00,
            0x00,
            0x08,
            0x00,
        ];
        let mut buf = [0u8; 8];
        let reply = self.exec(
            &cdb,
            crate::scsi::DataDirection::FromDevice,
            &mut buf,
            5_000,
        );

        // MMC-6 §6.7: byte 5 is Media Status only when the Event Header (NEA flag +
        // Notification Class) announces a Media Event Descriptor. Otherwise decoding
        // it reads as "no disc" for untrusted input, so fall back to TUR instead.
        const NEA: u8 = 0x80;
        const NOTIFICATION_CLASS_MASK: u8 = 0x07;
        const NOTIFICATION_CLASS_MEDIA: u8 = 0x04;
        // Bytes 2..7: the 2 remaining header bytes plus the 4-byte Media Event
        // Descriptor — the shortest reply in which byte 5 exists and is a Media
        // Status.
        const MIN_DESCRIPTOR_LENGTH: u16 = 6;

        let media_status = match reply {
            Ok(r) if r.bytes_transferred >= 6 => {
                let descriptor_len = u16::from_be_bytes([buf[0], buf[1]]);
                let class = buf[2] & NOTIFICATION_CLASS_MASK;
                if buf[2] & NEA == 0
                    && class == NOTIFICATION_CLASS_MEDIA
                    && descriptor_len >= MIN_DESCRIPTOR_LENGTH
                {
                    Some(buf[5])
                } else {
                    tracing::debug!(
                        target: "freemkv::drive",
                        nea = buf[2] & NEA != 0,
                        class,
                        descriptor_len,
                        "get event status carried no media event descriptor"
                    );
                    None
                }
            }
            _ => None,
        };

        match media_status {
            Some(media_status) => {
                // Bits 1-0: door/tray state
                // Bit 1: media present, Bit 0: tray open
                match media_status & 0x03 {
                    0x00 => DriveStatus::NoDisc,      // tray closed, no disc
                    0x01 => DriveStatus::TrayOpen,    // tray open, no media
                    0x02 => DriveStatus::DiscPresent, // tray closed, disc present
                    // 0x03 = tray-open AND media-present both set: a contradictory,
                    // transient state. Don't report ready — autorip must not start on
                    // a drive that's still settling — so treat as tray-open.
                    0x03 => DriveStatus::TrayOpen,
                    _ => DriveStatus::Unknown,
                }
            }
            None => {
                // Fallback: try TUR
                let tur = [SCSI_TEST_UNIT_READY, 0x00, 0x00, 0x00, 0x00, 0x00];
                let mut empty = [0u8; 0];
                match self.exec(&tur, crate::scsi::DataDirection::None, &mut empty, 5_000) {
                    Ok(_) => DriveStatus::DiscPresent,
                    Err(ref e)
                        if e.scsi_sense()
                            .is_some_and(|s| s.is_not_ready() || s.is_unit_attention()) =>
                    {
                        DriveStatus::NotReady
                    }
                    _ => DriveStatus::Unknown,
                }
            }
        }
    }

    /// Name of the unlocker handling this drive. After `init()` this is the
    /// unlocker that ran; before `init()` it reflects the unlocker match by
    /// identity. `"Unknown"` when no unlocker matches.
    pub fn platform_name(&self) -> &str {
        if let Some(ref n) = self.unlocker_name {
            return n;
        }
        // Cache the unlocker match so we can hand out a `&str` borrow.
        self.matched_name_cache.get_or_init(|| {
            crate::unlock_bridge::unlocker_name(&self.drive_id)
                .map(str::to_string)
                .unwrap_or_else(|| "Unknown".to_string())
        })
    }

    pub fn device_path(&self) -> &str {
        &self.device_path
    }

    // Current mounted-disc profile from GET CONFIGURATION (Current Profile,
    // bytes 6-7). DVD is 0x0010..=0x001F, BD 0x0040..=0x0043. Stock MMC
    // command, works before any drive unlock. None if unreadable.
    fn current_profile(&mut self) -> Option<u16> {
        let cdb = [
            crate::scsi::SCSI_GET_CONFIGURATION,
            0x00, // RT=0: header carries the Current Profile
            0x00,
            0x00, // starting feature 0
            0x00,
            0x00,
            0x00,
            0x00,
            0x08, // allocation length = 8 (header only)
            0x00,
        ];
        let mut buf = [0u8; 8];
        let r = self
            .exec(
                &cdb,
                crate::scsi::DataDirection::FromDevice,
                &mut buf,
                5_000,
            )
            .ok()?;
        if r.bytes_transferred >= 8 {
            Some(((buf[6] as u16) << 8) | buf[7] as u16)
        } else {
            None
        }
    }

    /// True when the mounted disc is a DVD (profile family `0x0010..=0x001F`,
    /// plus DVD+RW DL `0x002A` and DVD+R DL `0x002B`).
    pub(crate) fn disc_is_dvd(&mut self) -> bool {
        matches!(self.current_profile(), Some(p) if (0x0010..=0x001F).contains(&p) || p == 0x002A || p == 0x002B)
    }

    /// Initialize drive — drive-prep unlock + init.
    /// Optional. Adds features: removes riplock, enables UHD reads, speed control.
    ///
    /// Drive-prep runs for every disc, DVD included: drive features are
    /// disc-independent, and the AACS/CSS handshakes run later, gated on disc kind.
    /// A transport fault aborts init; other errors fall through to stock mode.
    pub fn init(&mut self) -> Result<()> {
        let t0 = std::time::Instant::now();
        tracing::info!(target: "freemkv::drive", phase = "init", "begin");
        // Drive-prep runs for EVERY disc, DVD INCLUDED — drive features are
        // disc-independent; AACS/CSS handshakes run LATER, gated on disc kind. A
        // transport fault aborts init (v1.1.0 invariant); other errors fall through.
        self.init_ran = true;
        let drive_id = self.drive_id.clone();
        let (matched, unlock_res) = crate::unlock_bridge::run_features(self, &drive_id);
        // §2.3 reclassification: a Stop is `Halted` before any other result mapping, so
        // the refused CDBs it caused never read as a dead bus.
        self.check_token()?;
        let r: Result<()> = match unlock_res {
            Ok(Some(unlocked)) => {
                // Record which firmware unlocker ran, not the id-only lookup.
                self.unlocker_name = Some(matched.to_string());
                // Stash the OEM Volume ID (best-effort: a transient miss leaves
                // the drive unlocked with no VID). do_handshake_cert reads `oem_vid()`.
                if let Some(vid) = unlocked.vid {
                    self.oem_vid = Some(vid);
                }
                Ok(())
            }
            // No firmware unlocker claimed the drive — a stock/cert-only drive;
            // the AACS cert route runs later at the handshake phase.
            Ok(None) => Ok(()),
            Err(freemkv_unlock::UnlockError::Transport) => {
                Err(crate::unlock_bridge::unlock_transport_error())
            }
            Err(_) => Ok(()),
        };
        // Raise to max read speed UNCONDITIONALLY whenever the bus is alive, for
        // ANY disc type — DVD included, and even a stock-mode drive with no
        // firmware unlocker still wants it. Best-effort: failure must NOT fail the rip.
        if r.is_ok() {
            self.set_speed(Self::SPEED_MAX_KBPS);
            // Ask the drive to REPORT recovered/marginal reads instead of silently
            // committing best-effort data as GOOD (the dirty-disc "passed-clean-but-
            // decodes-with-errors" trap). Best-effort: unsupported drives keep defaults.
            self.enable_recovered_error_reporting();
        }
        tracing::info!(
            target: "freemkv::drive",
            phase = "init",
            ok = r.is_ok(),
            unlocker = self.unlocker_name.as_deref().unwrap_or("none"),
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "end"
        );
        r
    }

    /// No-op retained for API stability. Disc-speed calibration is now
    /// unlocker-specific and runs inside the unlocker's `unlock()` at `init()`
    /// (for every disc, including DVD), so there is nothing to do here — this
    /// only emits the begin/end trace span. Kept so existing callers and the
    /// `probe_disc_without_unlocker_is_ok_noop` test need not change.
    pub fn probe_disc(&mut self) -> Result<()> {
        let t0 = std::time::Instant::now();
        tracing::info!(target: "freemkv::drive", phase = "probe_disc", "begin");
        // Disc-speed calibration is unlocker-specific and now lives inside
        // the unlocker's `unlock()` (run at `init()`, for every disc including
        // DVD). Nothing to do here — no disc-type branch.
        tracing::info!(
            target: "freemkv::drive",
            phase = "probe_disc",
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "end (calibration handled by unlocker at init)"
        );
        Ok(())
    }

    /// Query a specific GET CONFIGURATION feature by code.
    /// Returns the feature descriptor (without the 8-byte header), or None if the
    /// drive does not report that feature.
    pub fn get_config_feature(&mut self, feature_code: u16) -> Option<Vec<u8>> {
        let cdb = [
            crate::scsi::SCSI_GET_CONFIGURATION,
            0x02,
            (feature_code >> 8) as u8,
            feature_code as u8,
            0x00,
            0x00,
            0x00,
            0x01,
            0x00,
            0x00,
        ];
        let mut buf = vec![0u8; 256];
        let r = self
            .exec(
                &cdb,
                crate::scsi::DataDirection::FromDevice,
                &mut buf,
                5_000,
            )
            .ok()?;
        crate::scsi::gc_feature_descriptor(&buf, r.bytes_transferred, feature_code)
            .map(<[u8]>::to_vec)
    }

    /// Read REPORT KEY RPC state (region playback control).
    pub fn report_key_rpc_state(&mut self) -> Option<Vec<u8>> {
        let cdb = [
            SCSI_REPORT_KEY,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x08,
            0x08,
            0x00,
        ];
        let mut buf = vec![0u8; 8];
        let r = self
            .exec(
                &cdb,
                crate::scsi::DataDirection::FromDevice,
                &mut buf,
                5_000,
            )
            .ok()?;
        let end = r.bytes_transferred.min(buf.len());
        if end > 0 {
            Some(buf[..end].to_vec())
        } else {
            None
        }
    }

    /// Read MODE SENSE page data.
    pub fn mode_sense_page(&mut self, page: u8) -> Option<Vec<u8>> {
        let cdb = [
            SCSI_MODE_SENSE,
            0x00,
            page,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0xFC,
            0x00,
        ];
        let mut buf = vec![0u8; 252];
        let r = self
            .exec(
                &cdb,
                crate::scsi::DataDirection::FromDevice,
                &mut buf,
                5_000,
            )
            .ok()?;
        let end = r.bytes_transferred.min(buf.len());
        if end > 0 {
            Some(buf[..end].to_vec())
        } else {
            None
        }
    }

    /// Ask the drive to REPORT recovered/marginal reads instead of silently
    /// returning best-effort data as GOOD status. MODE SENSE the Read-Write
    /// Error Recovery page, flip `PER` (and `TB` on / `DTE` off so we still get
    /// the data), and MODE SELECT it back — preserving the drive's own retry
    /// count and other bits.
    ///
    /// Best-effort: a drive that doesn't support the page, or rejects the
    /// SELECT, simply keeps its default behaviour. Returns whether the page
    /// was successfully written.
    pub fn enable_recovered_error_reporting(&mut self) -> bool {
        let Some(sense) = self.mode_sense_page(MODE_PAGE_ERROR_RECOVERY) else {
            tracing::debug!(target: "freemkv::drive", "MODE SENSE error-recovery page unavailable; leaving drive defaults");
            return false;
        };
        let Some(payload) = build_error_recovery_select_payload(&sense) else {
            tracing::debug!(target: "freemkv::drive", "error-recovery page malformed/short; leaving drive defaults");
            return false;
        };
        // MODE SELECT(10): PF=1 (page format), parameter list length = payload.
        let len = payload.len() as u16;
        let cdb = [
            SCSI_MODE_SELECT,
            0x10, // PF=1, SP=0 (don't persist across power cycles)
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            (len >> 8) as u8,
            len as u8,
            0x00,
        ];
        let mut buf = payload;
        match self.exec(&cdb, crate::scsi::DataDirection::ToDevice, &mut buf, 5_000) {
            Ok(_) => {
                tracing::info!(target: "freemkv::drive", phase = "error_recovery", "recovered-error reporting enabled (PER=1) — marginal reads will surface instead of committing silently");
                true
            }
            Err(e) => {
                tracing::debug!(target: "freemkv::drive", error = %e, "MODE SELECT error-recovery page rejected; leaving drive defaults");
                false
            }
        }
    }

    /// Read vendor-specific READ BUFFER data.
    pub fn read_buffer(&mut self, mode: u8, buffer_id: u8, length: u16) -> Option<Vec<u8>> {
        let cdb = crate::scsi::build_read_buffer(mode, buffer_id, 0, length as u32);
        let mut buf = vec![0u8; length as usize];
        let r = self
            .exec(
                &cdb,
                crate::scsi::DataDirection::FromDevice,
                &mut buf,
                5_000,
            )
            .ok()?;
        let end = r.bytes_transferred.min(buf.len());
        if end > 0 {
            Some(buf[..end].to_vec())
        } else {
            None
        }
    }

    pub fn is_ready(&self) -> bool {
        // Ready once init() has run and an unlocker handled the drive.
        self.init_ran && self.unlocker_name.is_some()
    }

    /// Whether libfreemkv should take the OEM extended-access read path.
    ///
    /// True when an unlocker claims this drive by identity. Such an unlocker
    /// unlocks *drive functionality* — drive unlock, OEM VID retrieval, and other
    /// vendor capabilities. When one matches, libfreemkv routes both `unlock` and
    /// OEM VID through it (VID via the OEM path is decoupled from the host cert +
    /// HRL). This mirrors [`Self::has_profile`] — the honest signal is "an
    /// unlocker claims this drive" — rather than the old const `false`.
    pub fn has_unlocker(&self) -> bool {
        crate::unlock_bridge::unlocker_name(&self.drive_id).is_some()
    }

    /// Deprecated alias of [`Self::has_unlocker`].
    #[deprecated(note = "use has_unlocker")]
    pub fn is_unlocked(&self) -> bool {
        self.has_unlocker()
    }

    /// Read sectors from the disc. Single-shot — no inline retries, no
    /// SCSI reset.
    ///
    /// `recovery=true` uses [`crate::scsi::READ_RECOVERY_TIMEOUT_MS`] (60 s, matches sg_dd) for
    /// the `freemkv_engine::recovery::patch` pass; `recovery=false` uses
    /// [`crate::scsi::READ_TIMEOUT_MS`] (10 s) for `freemkv_engine::recovery::copy`'s fast
    /// skip-forward sweep. On any failure returns `Err(DiscRead)` immediately; orchestration
    /// handles retry policy.
    pub fn read(&mut self, lba: u32, count: u16, buf: &mut [u8], recovery: bool) -> Result<usize> {
        // Bulk path: FUA off (the drive cache IS the streaming throughput).
        self.read_fua(lba, count, buf, recovery, false)
    }

    /// [`read`], but with an explicit Force Unit Access request: `fua = true`
    /// sets the READ(10) FUA bit so the drive re-fetches the medium instead of
    /// returning a cached copy — the Pass-N marginal-sector lever (see
    /// [`crate::sector::SectorSource::read_sectors_fua`]). The bulk sweep always
    /// passes `false`; only a per-sector recovery handler asks for FUA.
    ///
    /// [`read`]: Drive::read
    pub fn read_fua(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
        fua: bool,
    ) -> Result<usize> {
        let timeout_ms = if recovery {
            crate::scsi::READ_RECOVERY_TIMEOUT_MS
        } else {
            crate::scsi::READ_TIMEOUT_MS
        };
        // TRACE, not DEBUG: this fires on every read (hundreds of thousands per
        // rip). At DEBUG it floods a bug-report log and buries the real events.
        tracing::trace!(
            target: "freemkv::drive",
            lba,
            count,
            recovery,
            timeout_ms,
            "Drive::read enter"
        );

        // Cap each CDB to the transport's max data-in transfer: a single READ past the
        // adapter limit fails outright on some backends (e.g. Windows SPTI, where a
        // 16 MiB read exceeds MaximumTransferLength and we'd mis-read it as transport failure).
        let max_sectors = (self.scsi.max_transfer_bytes() / 2048).max(1) as u32;
        if count as u32 <= max_sectors {
            return self.read_one(lba, count, buf, timeout_ms, recovery, fua);
        }

        // Large read: split into `max_sectors`-sized chunks, each a self-contained
        // READ(10). Any chunk error reports that chunk's LBA, not the base LBA.
        let count = count as u32;
        // Check the caller's buffer ONCE, up front: the chunk loop slices `buf` by
        // `count * 2048`, and without this an undersized buffer PANICKED out of the
        // public `read`/`read_fua` instead of returning `Err(DiscRead)` like the single-chunk path.
        if buf.len() < count as usize * 2048 {
            return Err(Error::DiscRead {
                sector: lba as u64,
                status: None,
                sense: None,
            });
        }
        // The whole range must be addressable: READ(10) carries a 32-bit LBA, so a
        // request crossing `u32::MAX` has no valid CDB. `lba + done` was unchecked —
        // a debug panic, or in release a wrap to a low LBA read and returned as requested.
        if lba.checked_add(count.saturating_sub(1)).is_none() {
            return Err(Error::DiscRead {
                sector: lba as u64,
                status: None,
                sense: None,
            });
        }
        let mut done: u32 = 0;
        let mut total: usize = 0;
        while done < count {
            let chunk = (count - done).min(max_sectors);
            let cur_lba = lba + done;
            let byte_off = done as usize * 2048;
            let byte_len = chunk as usize * 2048;
            let slice = &mut buf[byte_off..byte_off + byte_len];
            let n = self.read_one(cur_lba, chunk as u16, slice, timeout_ms, recovery, fua)?;
            total += n;
            done += chunk;
        }
        Ok(total)
    }

    // Single READ(10) for up to `count` sectors at `lba`, timeout already
    // resolved by the caller. On failure returns `Err(DiscRead)` with
    // `sector = lba`; a short transfer is treated as a failed read.
    fn read_one(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        timeout_ms: u32,
        // `recovery` gates only the Linux /dev/sr0 pread fallback below; on
        // other platforms it is intentionally unused.
        #[cfg_attr(not(target_os = "linux"), allow(unused_variables))] recovery: bool,
        // FUA (Force Unit Access): when set, byte-1 bit 0x08 forces the drive to
        // re-fetch the medium past its cache.
        fua: bool,
    ) -> Result<usize> {
        // FUA is OFF on the bulk path: forcing every READ(10) past the cache disabled
        // readahead on the sequential sweep and collapsed throughput ~10x (UHD → ~2
        // MB/s, DVD → ~0.5 MB/s). Set ONLY per marginal-sector re-read by FuaRetry (#55).
        let cdb = [
            crate::scsi::SCSI_READ_10,
            if fua { 0x08 } else { 0x00 },
            (lba >> 24) as u8,
            (lba >> 16) as u8,
            (lba >> 8) as u8,
            lba as u8,
            0x00,
            (count >> 8) as u8,
            count as u8,
            0x00,
        ];

        match self.exec(
            &cdb,
            crate::scsi::DataDirection::FromDevice,
            buf,
            timeout_ms,
        ) {
            Ok(result) if result.bytes_transferred == count as usize * 2048 => {
                Ok(result.bytes_transferred)
            }
            // GOOD status with a residual underrun is a SHORT transfer: `buf`'s tail
            // holds stale bytes, so fail (NonTrimmed, retried) rather than commit stale
            // data — and log it, so it isn't indistinguishable from a scratched disc.
            Ok(result) => {
                tracing::warn!(
                    target: "freemkv::drive",
                    lba,
                    count,
                    transferred = result.bytes_transferred,
                    expected = count as usize * 2048,
                    code = crate::error::E_DISC_READ,
                    "READ(10) returned GOOD status with a residual underrun; refusing the short transfer"
                );
                Err(Error::DiscRead {
                    sector: lba as u64,
                    status: None,
                    sense: None,
                })
            }
            Err(Error::Halted) => Err(Error::Halted),
            Err(e) => {
                let (status, sense) = extract_scsi_context(&e);
                tracing::warn!(
                    target: "freemkv::drive",
                    lba,
                    count,
                    inner_error = %e,
                    scsi_status = status,
                    "Drive::read checked_exec failed"
                );

                // /dev/sr0 pread fallback (Linux only): sr_mod auto-retries reads
                // (~5x). Empirically (BU40N + UHD disc, 2026-05-08) this recovers
                // ~50% of bad sectors that a single-shot SG_IO READ misses.
                #[cfg(target_os = "linux")]
                if recovery
                    && let Some(fd) = self.block_dev_fd
                    && buf.len() >= count as usize * 2048
                {
                    let len = count as usize * 2048;
                    let offset = lba as i64 * 2048;
                    // A Stop that landed since exec's check issues no blocking read.
                    self.check_token()?;
                    let n = crate::scsi::linux::pread_uncached(fd, &mut buf[..len], offset);
                    if n == len as isize {
                        // A Stop during the blocking read discards the data, as `exec` does.
                        self.check_token()?;
                        tracing::info!(
                            target: "freemkv::drive",
                            lba,
                            count,
                            bytes = len,
                            "Drive::read recovered via /dev/sr0 pread fallback"
                        );
                        return Ok(len);
                    }
                    tracing::debug!(
                        target: "freemkv::drive",
                        lba,
                        count,
                        pread_ret = n as i64,
                        errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
                        "/dev/sr0 pread fallback also failed"
                    );
                }

                Err(Error::DiscRead {
                    sector: lba as u64,
                    status: Some(status),
                    sense,
                })
            }
        }
    }

    /// Read the disc capacity in sectors (2048 bytes each).
    pub fn read_capacity(&mut self) -> Result<u32> {
        let cdb = [
            crate::scsi::SCSI_READ_CAPACITY,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
        ];
        let mut buf = [0u8; 8];
        let result = self.exec(
            &cdb,
            crate::scsi::DataDirection::FromDevice,
            &mut buf,
            5_000,
        )?;
        decode_read_capacity(&buf, result.bytes_transferred)
    }

    /// SET CD SPEED "use the drive's maximum" sentinel (0xFFFF KB/s per MMC).
    pub const SPEED_MAX_KBPS: u16 = 0xFFFF;

    pub fn set_speed(&mut self, speed_kbs: u16) {
        let cdb = crate::scsi::build_set_cd_speed(speed_kbs);
        let mut dummy = [0u8; 0];
        if let Err(e) = self.scsi_execute(&cdb, crate::scsi::DataDirection::None, &mut dummy, 5_000)
        {
            tracing::warn!(target: "freemkv::drive", error = %e, "SET CD SPEED failed");
        }
    }

    /// Lock the tray so the disc cannot be ejected during a rip. After a Stop it
    /// sends nothing and leaves the tray unlocked.
    pub fn lock_tray(&mut self) {
        // SS-5 MMC-6 Table 329: Persistent 0, Prevent 1 = "Prevent State shall be set (Locked)".
        let prevent = [
            SCSI_PREVENT_ALLOW_MEDIUM_REMOVAL,
            0x00,
            0x00,
            0x00,
            0x01,
            0x00,
        ];
        let mut buf = [0u8; 0];
        // Pre-check: a stopped op must not lock a tray it will never unlock.
        if self.is_halted() {
            return;
        }
        self.tray_locked = true;
        match self.exec(&prevent, crate::scsi::DataDirection::None, &mut buf, 5_000) {
            Err(Error::Halted) => self.tray_locked = false,
            Err(e) => {
                tracing::warn!(target: "freemkv::drive", error = %e, "PREVENT MEDIUM REMOVAL failed")
            }
            Ok(_) => {}
        }
    }

    /// Unlock the tray so the user can manually eject the disc. A clean-up CDB: after
    /// a Stop it is still sent if this Drive locked the tray (§2.4).
    pub fn unlock_tray(&mut self) {
        // SS-5 MMC-6 Table 329: Persistent 0, Prevent 0 = "Prevent State shall be cleared (Unlocked)".
        let allow = [
            SCSI_PREVENT_ALLOW_MEDIUM_REMOVAL,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
        ];
        let mut buf = [0u8; 0];
        // Checked against the ledger as it stands, then cleared: exactly one ALLOW.
        let r = self.exec_cleanup(
            &allow,
            crate::scsi::DataDirection::None,
            &mut buf,
            5_000,
            CleanupCtx::Plain,
        );
        self.tray_locked = false;
        // Best-effort (the tray-unlock is advisory), but a failure is worth a warn!
        // rather than a silent `let _`: a stuck PREVENT lock is a real symptom the
        // operator otherwise never sees until the tray won't open.
        match r {
            Ok(_) | Err(Error::Halted) => {}
            Err(e) => tracing::warn!(
                target: "freemkv::drive",
                phase = "unlock_tray",
                error_code = e.code(),
                "Failed to clear the medium-removal PREVENT lock; the tray may stay locked until the drive is power-cycled."
            ),
        }
    }

    /// Eject the disc tray. Unlocks first, then ejects.
    pub fn eject(&mut self) -> Result<()> {
        self.unlock_tray();
        // SS-6 MMC-6 Table 633: LoEj 1, Start 0 = "Eject the disc if permitted".
        let eject_cdb = allow::EJECT_CDB;
        let mut buf = [0u8; 0];
        self.exec(
            &eject_cdb,
            crate::scsi::DataDirection::None,
            &mut buf,
            30_000,
        )?;
        Ok(())
    }

    /// `DiscSession::finish(Finish::Eject)` (§2.4 row 2): the ALLOW (after a Stop only
    /// for a tray this Drive locked), then the eject. Both are clean-up CDBs, so a Stop
    /// never refuses them; the eject is the one START STOP UNIT a Stop still admits.
    pub(crate) fn finish_eject(&mut self) -> Result<()> {
        self.unlock_tray();
        // SS-6 MMC-6 Table 633: LoEj 1, Start 0 = "Eject the disc if permitted".
        let eject_cdb = allow::EJECT_CDB;
        let mut buf = [0u8; 0];
        let dir = crate::scsi::DataDirection::None;
        self.exec_cleanup(&eject_cdb, dir, &mut buf, 30_000, CleanupCtx::FinishEject)?;
        Ok(())
    }

    // START STOP UNIT with START=1, LoEj=0 (never ejects). Only Halted propagates;
    // a rejected START just leaves wait_ready polling.
    fn start_unit(&mut self) -> Result<()> {
        // SS-6 MMC-6 Table 633: LoEj 0, Start 1 = "Start the disc and make ready for access".
        let start = [SCSI_START_STOP_UNIT, 0, 0, 0, 0x01, 0];
        let mut buf = [0u8; 0];
        match self.exec(&start, crate::scsi::DataDirection::None, &mut buf, 30_000) {
            Err(Error::Halted) => Err(Error::Halted),
            Err(e) => {
                tracing::warn!(target: "freemkv::drive", phase = "wait_ready", error_code = e.code(), "START UNIT rejected");
                Ok(())
            }
            Ok(_) => Ok(()),
        }
    }

    /// Soft power-cycle the drive mechanism WITHOUT ejecting: spin the disc
    /// down (`START STOP UNIT`, START=0, **LOEJ=0**) then back up (START=1).
    /// This clears the BU40N/Initio fast-fail *wedge* state that a run of
    /// `HARDWARE_ERROR` reads leaves the drive in — the non-eject equivalent of
    /// the power-cycle our notes say the wedge needs. The disc stays loaded (the
    /// BU40N is slot-loading; we NEVER eject to recover — a hands-on eject is a
    /// failure for an unattended service). Validated live 2026-07-01: took the
    /// drive from failing-every-read back to reading at MB/s.
    pub fn spin_cycle(&mut self) -> Result<()> {
        let stop = [SCSI_START_STOP_UNIT, 0, 0, 0, 0x00, 0]; // START=0, LOEJ=0 → spin down
        let start = [SCSI_START_STOP_UNIT, 0, 0, 0, 0x01, 0]; // START=1, LOEJ=0 → spin up
        let mut buf = [0u8; 0];
        // `exec` + `pause`, not blind `thread::sleep`: this ~15 s wait runs from the
        // recovery path, exactly when Stop is likely pressed. A half-finished spin
        // cycle is fine — the next command spins the drive back up.
        self.exec(&stop, crate::scsi::DataDirection::None, &mut buf, 30_000)?;
        self.pause(Duration::from_secs(SPIN_DOWN_IDLE_SECS))?;
        self.exec(&start, crate::scsi::DataDirection::None, &mut buf, 30_000)?;
        self.pause(Duration::from_secs(SPIN_UP_SETTLE_SECS))?;
        Ok(())
    }

    pub fn scsi_execute(
        &mut self,
        cdb: &[u8],
        direction: crate::scsi::DataDirection,
        buf: &mut [u8],
        timeout_ms: u32,
    ) -> Result<crate::scsi::ScsiResult> {
        self.exec(cdb, direction, buf, timeout_ms)
    }
}

impl Drop for Drive {
    fn drop(&mut self) {
        self.cleanup();
        // SgIoTransport::drop() runs next, calling libc::close(fd)
        #[cfg(target_os = "linux")]
        if let Some(fd) = self.block_dev_fd.take() {
            crate::scsi::linux::close_block_fd(fd);
        }
    }
}

// Resolve a `/dev/sg*` path to its `/dev/sr*` block device via sysfs, open for read (no
// O_DIRECT). None on any error ("no fallback available").
#[cfg(target_os = "linux")]
fn open_block_device_for_sg(sg_path: &Path) -> Option<std::os::unix::io::RawFd> {
    let basename = sg_path.file_name()?.to_str()?;
    if !basename.starts_with("sg") {
        return None;
    }
    let sysfs_dir = format!("/sys/class/scsi_generic/{}/device/block", basename);
    let entries = std::fs::read_dir(&sysfs_dir).ok()?;
    let block_name = entries
        .flatten()
        .find_map(|e| e.file_name().into_string().ok())?;
    let block_path = format!("/dev/{}", block_name);

    let fd = crate::scsi::linux::open_block_ro(&block_path);
    if fd < 0 {
        tracing::debug!(
            target: "freemkv::drive",
            sg = basename,
            block_path,
            errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            "Failed to open block device for fallback; sr0 fallback disabled"
        );
        None
    } else {
        tracing::info!(
            target: "freemkv::drive",
            sg = basename,
            block_path,
            fd,
            "Opened /dev/sr* as recovery fallback for failed SCSI reads"
        );
        Some(fd)
    }
}

impl SectorSource for Drive {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> Result<usize> {
        let n = self.read(lba, count, buf, recovery)?;
        self.remove_bus_encryption(lba, &mut buf[..n]);
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
        let n = self.read_fua(lba, count, buf, recovery, fua)?;
        self.remove_bus_encryption(lba, &mut buf[..n]);
        Ok(n)
    }

    fn set_speed(&mut self, kbs: u16) {
        Drive::set_speed(self, kbs);
    }

    // Only a host-key stage leaves sectors bus-encrypted; firmware de-busses at the drive.
    fn unmapped_stream_files(&self) -> &[crate::sector::bus_removal::UnmappedStreamFile] {
        match (&self.bus_stage, &self.bus_gate) {
            (crate::sector::bus_removal::BusStage::AacsHostKey(_), Some(g)) => g.map().unmapped(),
            _ => &[],
        }
    }
}

/// Find an optical drive on this system and open it, **preferring a drive
/// that currently has media**.
///
/// Opens each candidate in enumeration order and returns the first reporting
/// [`DriveStatus::DiscPresent`] via [`Drive::drive_status`]; falls back to the first drive that
/// opened if none report a disc. Only one drive is held open at a time. For
/// just listing drives without opening, use `scsi::list_drives()` instead.
pub fn find_drive() -> Option<Drive> {
    #[cfg(target_os = "macos")]
    let candidates = crate::scsi::list_drives()
        .into_iter()
        .map(|info| info.path)
        .collect::<Vec<_>>();

    #[cfg(not(target_os = "macos"))]
    let candidates = platform::candidate_paths();

    select_drive_with_media(candidates, |path| {
        match Drive::open(std::path::Path::new(path)) {
            Ok(drive) if drive.drive_id.is_optical() => Some(drive),
            Ok(drive) => {
                tracing::debug!(
                    target: "freemkv::drive",
                    path,
                    "skipping non-optical SCSI device during drive selection"
                );
                drop(drive);
                None
            }
            Err(error) => {
                tracing::warn!(
                    target: "freemkv::drive",
                    path,
                    error = %error,
                    "drive open failed during autodetection"
                );
                None
            }
        }
    })
}

// Prefer a drive with media, else the first that opened. Only one drive is open
// at a time (macOS allows one live handle): the fallback is released before the
// next open and reopened by path. Split from find_drive for unit tests.
fn select_drive_with_media<P>(
    candidates: impl IntoIterator<Item = P>,
    mut open: impl FnMut(&P) -> Option<Drive>,
) -> Option<Drive> {
    let mut fallback: Option<(P, Option<Drive>)> = None;
    for path in candidates {
        if let Some((_, held)) = fallback.as_mut() {
            *held = None;
        }
        let Some(mut drive) = open(&path) else {
            continue;
        };
        if drive.drive_status() == DriveStatus::DiscPresent {
            return Some(drive);
        }
        if fallback.is_none() {
            fallback = Some((path, Some(drive)));
        }
    }
    match fallback {
        Some((_, Some(drive))) => Some(drive),
        // Accepted edge: if the reopen fails (e.g. another process took the
        // device in the gap) there is no drive to return.
        Some((path, None)) => open(&path),
        None => None,
    }
}

// MODE SENSE(10) Error Recovery page -> MODE SELECT(10) payload enabling
// recovered-error reporting (PER/TB set, DTE/PS cleared), other bits kept.
// None if too short or not the error-recovery page.
fn build_error_recovery_select_payload(sense: &[u8]) -> Option<Vec<u8>> {
    // SPC-4 §7.5.5: the mode parameter list is Mode Data Length + 2 bytes; the
    // transfer count may over-report (ignored residue) and pad with zeros.
    let mode_data_len = usize::from(u16::from_be_bytes([*sense.first()?, *sense.get(1)?]));
    let sense = &sense[..sense.len().min(mode_data_len + 2)];
    if sense.len() < MODE10_HEADER_LEN {
        return None;
    }
    let block_desc_len = u16::from_be_bytes([sense[6], sense[7]]) as usize;
    let page_off = MODE10_HEADER_LEN.checked_add(block_desc_len)?;
    // Need page byte 0 (code), byte 1 (length), byte 2 (flags).
    if page_off.checked_add(3)? > sense.len() {
        return None;
    }
    if sense[page_off] & 0x3F != MODE_PAGE_ERROR_RECOVERY {
        return None;
    }
    let mut payload = sense.to_vec();
    // Header: mode-data-length is reserved on SELECT — zero it.
    payload[0] = 0;
    payload[1] = 0;
    // Page byte 0: clear PS (SENSE-only).
    payload[page_off] &= !MODE_PAGE_PS_BIT;
    // Flags byte: PER on, TB on, DTE off. Retry count (next byte) untouched.
    payload[page_off + 2] |= ERP_FLAG_PER | ERP_FLAG_TB;
    payload[page_off + 2] &= !ERP_FLAG_DTE;
    Some(payload)
}

// Decode a READ CAPACITY(10) response into a sector count. A short transfer
// (<4 bytes, would decode a bogus 1-sector disc) is DiscCapacityMalformed;
// the 0xFFFF_FFFF sentinel (last_lba+1 overflows u32) is DiscCapacityOverflow.
pub(crate) fn decode_read_capacity(buf: &[u8; 8], bytes_transferred: usize) -> Result<u32> {
    if bytes_transferred < 4 {
        return Err(Error::DiscCapacityMalformed);
    }
    let last_lba = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    last_lba.checked_add(1).ok_or(Error::DiscCapacityOverflow)
}

// Consecutive MEDIUM NOT PRESENT (3Ah) TURs, with no 04/01 seen, after which
// wait_ready reports the empty drive (~5 s of polling).
const WAIT_READY_MAX_EMPTY_POLLS: u32 = 10;

// How long an unbroken run of transport-class TUR failures may last, from the
// first one's completion, before wait_ready calls the bus dead.
const WAIT_READY_DEAD_BUS_BUDGET: Duration = Duration::from_secs(5);

// Absolute wait_ready ceiling: a drive cycling fresh answers can't re-arm the window forever.
const WAIT_READY_CEILING: Duration = Duration::from_secs(600);

/// `wait_ready`'s durations (§2.11, T3/T5/T6), parameters so tests run in ms.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WaitReadyTiming {
    /// The wait between two TEST UNIT READYs.
    pub poll: Duration,
    /// T6: give up after this long with no progress.
    pub window: Duration,
    /// T5: an unbroken run of transport failures this long is a dead bus.
    pub dead_bus: Duration,
    /// Absolute cap on the whole wait, however the drive's answers vary.
    pub ceiling: Duration,
}

impl WaitReadyTiming {
    /// 500 ms polls, 60 s without progress (user: "60s"), the 5 s dead-bus budget.
    pub(crate) const PRODUCTION: Self = Self {
        poll: Duration::from_millis(500),
        window: Duration::from_secs(60),
        dead_bus: WAIT_READY_DEAD_BUS_BUDGET,
        ceiling: WAIT_READY_CEILING,
    };
}

// READ-class opcodes: their data is checked against the token again on completion
// (§2.2), so a READ that finishes after a Stop is discarded, never returned as good.
fn is_read_class(cdb: &[u8]) -> bool {
    matches!(
        cdb.first(),
        Some(&(crate::scsi::SCSI_READ_10 | 0xA8 | 0x88 | 0xBE | 0xB9))
    )
}

/// Structured outcome of [`resolve_device`] — a machine-readable signal
/// (no English prose) the application layer can render however it likes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceResolution {
    /// Path resolved directly to a SCSI-generic device; no substitution.
    Direct,
    /// A `/dev/sr*` block path was substituted with the matching
    /// `/dev/sg*` SCSI-generic device for raw access (Linux only).
    SrToSg,
    /// A `/dev/sr*` block path was given but no matching `/dev/sg*`
    /// device could be found; the original path is returned (Linux only).
    SrNoSgMatch,
}

// Resolve a device path to its raw SCSI device; returns the resolved path plus a
// DeviceResolution signal. Staged, not yet wired — allow(dead_code) is deliberate.
#[allow(dead_code)]
pub(crate) fn resolve_device(path: &str) -> Result<(String, DeviceResolution)> {
    platform::resolve_device(path)
}

#[cfg(test)]
#[path = "mod_halt_tests.rs"]
mod halt_tests;

#[cfg(test)]
#[path = "mod_command_tests.rs"]
mod command_tests;
