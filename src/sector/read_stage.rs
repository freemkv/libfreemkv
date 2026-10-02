//! The Read stage (pipeline design §2.2, slice 6): walks a title's extents over a
//! (decrypting) sector source and yields chunks, under one [`ReadPolicy`]. The prefetch
//! producer runs it for every sector input — a live drive and an image alike — so there is
//! one read loop and one demux behind it.
//!
//! The policy is the transport's (X-1): an image reads fixed batches and stops on the first
//! error; a live drive shrinks its batch on a failing zone, regrows it after a clean streak,
//! gives a unit that still fails one full ECC recovery read, then zero-fills and counts it
//! (`skip_errors`) or fails the read.

use crate::ctx::Ctx;
use crate::disc::Extent;
use crate::drive::extract_scsi_context;
use crate::error::{Error, Result};
use crate::event::{BatchSizeReason, Event};
use crate::sector::SectorSource;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// Ramp back to the preferred batch size after this many clean sectors (100
// MiB = 51,200 sectors) — long enough that noisy zones can't trigger a
// premature probe, short enough an isolated failure doesn't lock size 1.
const PROBE_THRESHOLD_SECTORS: u32 = (100 * 1024 * 1024 / SECTOR) as u32;

const SECTOR: usize = crate::consts::SECTOR_BYTES;

/// How the Read stage reads a source (the transport's policy, X-1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadPolicy {
    /// An image or file: `batch`-sector reads in whole decrypt units (an extent tail shorter
    /// than one unit is one read); any read error ends the run.
    Image { batch: u16 },
    /// A live drive: adaptive batches from `batch` down, one ECC recovery read for a unit
    /// that still fails, then a zero-filled, counted unit (`skip_errors`) or a `DiscRead`.
    Live { batch: u16, skip_errors: bool },
}

impl ReadPolicy {
    /// The preferred read batch, in sectors.
    pub fn batch(&self) -> u16 {
        match *self {
            ReadPolicy::Image { batch } | ReadPolicy::Live { batch, .. } => batch,
        }
    }
}

/// Read loss the stage counted (skip events and zero-filled bytes), shared with the PES
/// stream that reports it as `errors()` / `lost_bytes()`.
#[derive(Debug, Default)]
pub struct ReadLoss {
    skips: AtomicU64,
    bytes: AtomicU64,
}

impl ReadLoss {
    /// Read errors skipped past.
    pub fn skips(&self) -> u64 {
        self.skips.load(Ordering::Relaxed)
    }

    /// Bytes zero-filled in their place.
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    fn add(&self, bytes: u64) {
        self.skips.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Halve a batch size, keeping 3-sector alignment when >= 6
/// (3-sector alignment = one AACS unit). At sizes < 6 we descend
/// through 3 → 1 without intermediate unaligned sizes.
fn halve_batch_size(size: u16) -> u16 {
    let h = (size / 2).max(1);
    if h >= 6 { h - (h % 3) } else { h }
}

/// Double a batch size toward a preferred max, keeping 3-sector alignment
/// when the result is >= 6.
fn double_batch_size(size: u16, preferred: u16) -> u16 {
    let d = size.saturating_mul(2).min(preferred);
    if d >= 6 { d - (d % 3) } else { d }
}

/// Adaptive batch sizer. Shrinks on read failure, grows after a sustained
/// clean streak. Amortizes the cost of entering a bad zone — descent happens
/// once, not once per bad sector.
#[derive(Debug)]
struct AdaptiveBatch {
    preferred: u16,
    current: u16,
    streak_sectors: u32,
}

impl AdaptiveBatch {
    fn new(preferred: u16) -> Self {
        Self {
            preferred,
            current: preferred,
            streak_sectors: 0,
        }
    }

    fn current(&self) -> u16 {
        self.current
    }

    /// Record a successful read of `sectors`. Returns an event if the
    /// sizer probed up to a larger batch size.
    fn on_success(&mut self, sectors: u16) -> Option<Event<'static>> {
        self.streak_sectors = self.streak_sectors.saturating_add(sectors as u32);
        if self.current < self.preferred && self.streak_sectors >= PROBE_THRESHOLD_SECTORS {
            let new_size = double_batch_size(self.current, self.preferred);
            if new_size != self.current {
                self.current = new_size;
                self.streak_sectors = 0;
                return Some(Event::BatchSizeChanged {
                    new_size,
                    reason: BatchSizeReason::Probed,
                });
            }
        }
        None
    }

    /// Record a read failure. Returns an event if the sizer shrank.
    /// Does nothing at size 1 (caller handles skip/error).
    fn on_failure(&mut self) -> Option<Event<'static>> {
        self.streak_sectors = 0;
        if self.current <= 1 {
            return None;
        }
        let new_size = halve_batch_size(self.current);
        self.current = new_size;
        Some(Event::BatchSizeChanged {
            new_size,
            reason: BatchSizeReason::Shrunk,
        })
    }
}

// The key set's on-arrival loud stop: E7022 (a title) or E7032 (an image or folder).
fn is_key_stop(e: &Error) -> bool {
    matches!(e, Error::NoDiscKey { .. } | Error::WholeDiscKeyMissing)
}

/// One title's extent walk under a [`ReadPolicy`]: each [`next`](Self::next) fills a buffer
/// with the next chunk (today's read batch for the source kind).
pub struct ExtentWalk {
    extents: Vec<Extent>,
    idx: usize,
    offset: u32,
    unit_align: u32,
    policy: ReadPolicy,
    adaptive: AdaptiveBatch,
    bytes_read: u64,
    bytes_total: u64,
    loss: Arc<ReadLoss>,
    ctx: Ctx,
}

impl ExtentWalk {
    /// Walk `extents` in `unit_align`-sector decrypt units (3 for AACS, 1 for CSS/clear).
    /// A zero batch or alignment is refused (it would never advance).
    pub fn new(
        extents: Vec<Extent>,
        policy: ReadPolicy,
        unit_align: u16,
        ctx: &Ctx,
    ) -> Result<Self> {
        if policy.batch() == 0 {
            return Err(Error::MuxBatchSectorsZero);
        }
        if unit_align == 0 {
            return Err(Error::IoError {
                source: std::io::Error::from(std::io::ErrorKind::InvalidInput),
            });
        }
        // Extents come from untrusted nav/MPLS/UDF data: sum in u64.
        let bytes_total = extents
            .iter()
            .map(|e| e.sector_count as u64 * crate::consts::SECTOR_BYTES_U64)
            .sum();
        Ok(ExtentWalk {
            extents,
            idx: 0,
            offset: 0,
            unit_align: unit_align as u32,
            policy,
            adaptive: AdaptiveBatch::new(policy.batch()),
            bytes_read: 0,
            bytes_total,
            loss: Arc::default(),
            ctx: ctx.clone(),
        })
    }

    /// The loss this walk counts, for the stream that reports it.
    pub fn loss(&self) -> Arc<ReadLoss> {
        self.loss.clone()
    }

    /// Total sectors across the extents (advisory; clamped to `u32`).
    pub fn total_sectors(&self) -> u32 {
        self.extents
            .iter()
            .map(|e| e.sector_count as u64)
            .sum::<u64>()
            .min(u32::MAX as u64) as u32
    }

    // The next extent with sectors left, as `(start, sectors, remaining)`; `None` at the end.
    // Iterative, not recursive: a malformed disc can declare thousands of empty extents.
    fn current(&mut self) -> Option<(u32, u32, u32)> {
        loop {
            let ext = self.extents.get(self.idx)?;
            let remaining = ext.sector_count.saturating_sub(self.offset);
            if remaining > 0 {
                return Some((ext.start_lba, ext.sector_count, remaining));
            }
            self.idx += 1;
            self.offset = 0;
        }
    }

    fn advance(&mut self, sectors: u32, ext_sectors: u32) {
        self.offset = self.offset.saturating_add(sectors);
        if self.offset >= ext_sectors {
            self.idx += 1;
            self.offset = 0;
        }
    }

    /// Fill `buf` with the next chunk. `Ok(false)` once every extent is read. A Stop is
    /// `Err(Halted)`, never a skipped sector.
    pub fn next(&mut self, reader: &mut dyn SectorSource, buf: &mut Vec<u8>) -> Result<bool> {
        let Some((start, ext_sectors, remaining)) = self.current() else {
            return Ok(false);
        };
        // AACS aligned units anchor at this extent's start LBA, not absolute disc LBA 0.
        // No-op for CSS / clear sources.
        reader.set_unit_base(start);
        // `start + offset` derives from untrusted extent data: saturate.
        let lba = start.saturating_add(self.offset);
        match self.policy {
            ReadPolicy::Image { batch } => {
                self.next_image(reader, buf, lba, remaining, ext_sectors, batch)
            }
            ReadPolicy::Live { skip_errors, .. } => {
                self.next_live(reader, buf, lba, remaining, ext_sectors, skip_errors)
            }
        }
    }

    fn read_progress(&mut self, got: usize) {
        self.bytes_read = self.bytes_read.saturating_add(got as u64);
        self.ctx.emit(Event::BytesRead {
            bytes: self.bytes_read,
            total: self.bytes_total,
        });
    }

    fn next_image(
        &mut self,
        reader: &mut dyn SectorSource,
        buf: &mut Vec<u8>,
        lba: u32,
        remaining: u32,
        ext_sectors: u32,
        batch: u16,
    ) -> Result<bool> {
        self.ctx.halt.check()?;
        let align = self.unit_align;
        let mut sectors = remaining.min(batch as u32);
        // Trim to whole units; a window landing on a sub-unit boundary reads one unit. An
        // extent tail below one unit is read as is: the decrypt stage passes it when clear
        // and refuses it when it is the head of an encrypted unit.
        sectors = if remaining < align {
            remaining
        } else if sectors >= align {
            sectors - sectors % align
        } else {
            align
        };
        let bytes = sectors as usize * SECTOR;
        buf.resize(bytes, 0);
        let n = reader.read_sectors(lba, sectors as u16, &mut buf[..bytes], false)?;
        // A short read must not desync the stream: advance by sectors actually read, and
        // reject a non-whole-sector count.
        if n % SECTOR != 0 {
            return Err(Error::ExtentNotUnitAligned);
        }
        let read = (n / SECTOR) as u32;
        // A zero-byte read with extents left is not EOF and would spin forever.
        if read == 0 {
            return Err(Error::SourceTerminated);
        }
        buf.truncate(n);
        self.read_progress(n);
        self.advance(read, ext_sectors);
        Ok(true)
    }

    // A failed read that Stop caused: the reader reports Halted, or the token fired.
    fn stopped(&self, e: &Error) -> bool {
        matches!(e, Error::Halted) || self.ctx.halt.is_cancelled()
    }

    // The read error to report for a unit that failed at `lba`.
    fn disc_read(lba: u32, e: Option<&Error>) -> Error {
        let (status, sense) = e.map(extract_scsi_context).unwrap_or((0, None));
        Error::DiscRead {
            sector: lba as u64,
            status: Some(status),
            sense,
        }
    }

    // A read failure no policy may skip or retry past (`Err`): Stop, the key set's loud
    // stop, a wedged transport, a dead source, or an image that ends before the read.
    // `Ok` hands a soft failure back.
    fn soft(&self, lba: u32, e: Error) -> std::result::Result<Error, Error> {
        if is_key_stop(&e) || matches!(e, Error::ImageEndsBeforeRead { .. }) {
            return Err(e);
        }
        if self.stopped(&e) {
            return Err(Error::Halted);
        }
        // A transport failure (status 0xFF: bridge crash/disconnect) is NOT a skippable bad
        // sector: every later read fails too, so skipping marches the disc producing nothing.
        if e.is_scsi_transport_failure() {
            return Err(Self::disc_read(lba, Some(&e)));
        }
        // The read SOURCE is gone, not bad media: skipping would zero-fill the rest.
        if e.is_source_terminated() {
            return Err(Error::SourceTerminated);
        }
        Ok(e)
    }

    // Commit a read of `got` of `bytes`: a short read is a `DiscRead`, or zero-filled and
    // counted under `skip_errors` (never a silent hole).
    fn commit(
        &mut self,
        buf: &mut [u8],
        lba: u32,
        got: usize,
        bytes: usize,
        skip_errors: bool,
    ) -> Result<()> {
        if got < bytes {
            if !skip_errors {
                return Err(Error::DiscRead {
                    sector: lba as u64,
                    status: None,
                    sense: None,
                });
            }
            // Clear the tail the source did not write: it holds stale bytes from a
            // previous batch, which would mux as plausible garbage.
            buf[got..bytes].fill(0);
            self.skip(lba, (bytes - got) as u64);
        }
        // Only the bytes the source delivered count as read; the zero-filled tail is loss.
        self.read_progress(got);
        Ok(())
    }

    fn skip(&mut self, lba: u32, bytes: u64) {
        self.loss.add(bytes);
        self.ctx.stats.add_skip(bytes);
        self.ctx.emit(Event::SectorSkipped { lba: lba as u64 });
    }

    fn next_live(
        &mut self,
        reader: &mut dyn SectorSource,
        buf: &mut Vec<u8>,
        lba: u32,
        remaining: u32,
        ext_sectors: u32,
        skip_errors: bool,
    ) -> Result<bool> {
        let align = self.unit_align;
        // Shrink on failure, grow on success; one attempt per size, no retry loops. Stop
        // is checked every iteration: a bad zone can spend minutes here.
        loop {
            if self.ctx.halt.is_cancelled() {
                return Err(Error::Halted);
            }
            // Every read buffer starts on a real on-disc unit boundary: AACS decrypts whole
            // units keyed off the buffer's first bytes.
            let want = remaining.min(self.adaptive.current() as u32);
            let sectors: u16 = if align <= 1 {
                want as u16
            } else if remaining < align {
                remaining as u16
            } else if want < align {
                align as u16
            } else {
                (want - want % align) as u16
            };
            let bytes = sectors as usize * SECTOR;
            buf.resize(bytes, 0);
            let res = reader.read_sectors(lba, sectors, &mut buf[..bytes], false);
            match res {
                Ok(got) => {
                    debug_assert!(got <= bytes, "read_sectors over-reported byte count");
                    if let Some(ev) = self.adaptive.on_success(sectors) {
                        self.ctx.emit(ev);
                    }
                    self.commit(buf, lba, got.min(bytes), bytes, skip_errors)?;
                    self.advance(sectors as u32, ext_sectors);
                    return Ok(true);
                }
                Err(e) => {
                    self.soft(lba, e)?;
                    if (sectors as u32) > align {
                        // Shrink and retry at the same LBA with a smaller batch.
                        if let Some(ev) = self.adaptive.on_failure() {
                            self.ctx.emit(ev);
                        }
                        continue;
                    }
                    // Bottomed out with no later pass to recover: one full ECC recovery
                    // read (~60 s) before skip/fail — a single bounded read, never a loop.
                    tracing::debug!(
                        target: "mux",
                        "last-chance recovery read at LBA {lba} ({sectors} sectors, 60s ECC)"
                    );
                    let rec = reader.read_sectors(lba, sectors, &mut buf[..bytes], true);
                    match rec {
                        Ok(got) => {
                            debug_assert!(got <= bytes, "recovery read over-reported byte count");
                            if let Some(ev) = self.adaptive.on_success(sectors) {
                                self.ctx.emit(ev);
                            }
                            self.commit(buf, lba, got.min(bytes), bytes, skip_errors)?;
                            self.advance(sectors as u32, ext_sectors);
                            return Ok(true);
                        }
                        Err(rec) => {
                            let rec = self.soft(lba, rec)?;
                            if !skip_errors {
                                return Err(Self::disc_read(lba, Some(&rec)));
                            }
                            // Skip the WHOLE failed unit: advancing by the full unit keeps
                            // the cursor AACS-aligned.
                            buf[..bytes].fill(0);
                            self.skip(lba, bytes as u64);
                            self.advance(sectors as u32, ext_sectors);
                            return Ok(true);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "read_stage_tests.rs"]
mod live_tests;
