//! DiscStream — read any disc (physical drive or ISO file) → PES frames.
//!
//! One stream type for all disc sources. The source is a SectorSource —
//! Drive (hardware) or FileSectorSource (file). DiscStream doesn't care.
//!
//! Read-only. For disc→ISO (raw sector copy), use `Disc::copy()`.

use crate::disc::{DiscTitle, Extent};
use crate::drive::extract_scsi_context;
use crate::event::{BatchSizeReason, Event, EventKind};
use crate::halt::Halt;
use crate::sector::{DecryptingSectorSource, SectorSource};
use std::io;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicBool;

/// Ramp back up to the preferred batch size after this many sectors
/// of clean reading at the current (reduced) size. 100 MiB = 51,200 sectors.
///
/// Chosen so that an isolated transient failure doesn't lock the rip at
/// size 1: once past the bad zone, we probe up after ~100 ms of good reads.
/// And so that noisy zones with occasional successes can't trigger a
/// premature probe — we need a sustained clean run.
const PROBE_THRESHOLD_SECTORS: u32 = 100 * 1024 * 1024 / 2048;

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
    fn on_success(&mut self, sectors: u16) -> Option<EventKind> {
        self.streak_sectors = self.streak_sectors.saturating_add(sectors as u32);
        if self.current < self.preferred && self.streak_sectors >= PROBE_THRESHOLD_SECTORS {
            let new_size = double_batch_size(self.current, self.preferred);
            if new_size != self.current {
                self.current = new_size;
                self.streak_sectors = 0;
                return Some(EventKind::BatchSizeChanged {
                    new_size,
                    reason: BatchSizeReason::Probed,
                });
            }
        }
        None
    }

    /// Record a read failure. Returns an event if the sizer shrank.
    /// Does nothing at size 1 (caller handles skip/error).
    fn on_failure(&mut self) -> Option<EventKind> {
        self.streak_sectors = 0;
        if self.current <= 1 {
            return None;
        }
        let new_size = halve_batch_size(self.current);
        self.current = new_size;
        Some(EventKind::BatchSizeChanged {
            new_size,
            reason: BatchSizeReason::Shrunk,
        })
    }
}

/// Disc stream. Reads sectors from any source → PES frames.
///
/// Sources: physical drive, ISO file, or any SectorSource.
/// Decrypt, demux, and codec parsing happen internally.
pub struct DiscStream {
    /// Underlying sector source wrapped in the 0.18
    /// [`DecryptingSectorSource`] decorator. Every `read_sectors`
    /// call yields plaintext, so `fill_extents` no longer needs an
    /// inline `decrypt::decrypt_sectors` step. `DecryptKeys::None`
    /// (raw / unencrypted disc) makes the decorator a pass-through.
    reader: DecryptingSectorSource<Box<dyn SectorSource>>,
    /// Shared decrypt-loss counter, cloned once at construction from
    /// `reader.decrypt_loss()`. `lost_bytes()` loads it directly so the
    /// per-frame hot path performs no per-call `Arc::clone` (matching the
    /// `PipelinedPesStream` pattern).
    decrypt_loss: std::sync::Arc<std::sync::atomic::AtomicU64>,
    title: DiscTitle,
    /// Mirror of the keys handed in at construction. The decorator
    /// owns the cryptographic state; this field is kept for
    /// metadata-side callers (`info()` and friends) that want to
    /// know whether the disc was encrypted, without reaching through
    /// the wrapper.
    decrypt_keys: crate::decrypt::DecryptKeys,

    /// Sector granularity the decrypt step requires each read buffer to start
    /// on and span a multiple of. AACS decrypts whole 6144-byte (3-sector)
    /// units keyed off the buffer's first 16 bytes, so every `read_sectors`
    /// buffer must begin on a real on-disc unit boundary — hence reads and
    /// error-skips must stay aligned to this. `3` for AACS, `1` for CSS /
    /// unencrypted (per-sector, self-synchronizing). Mirrors the file-backed
    /// highway's `PrefetchedSectorSource` guard; this is the inline live path.
    unit_align: u16,

    // Extents to read
    extents: Vec<Extent>,

    // Position
    current_extent: usize,
    current_offset: u32,

    // Buffer
    read_buf: Vec<u8>,
    buf_valid: usize,

    // Adaptive batch sizer — preferred comes from the caller
    // (detect_max_batch_sectors), shrinks/grows based on read outcomes.
    adaptive: AdaptiveBatch,
    pub errors: u64,
    /// Cumulative bytes actually skipped (zero-filled) on read error.
    /// Distinct from `errors`, which counts skip *events*: one event can
    /// cover a whole AACS unit (`unit_align` sectors = 6144 bytes), so
    /// `errors * 2048` understates real loss by the alignment factor.
    /// Consumers estimating lost video time must scale by this, not by
    /// the event count.
    pub lost_bytes: u64,
    pub skip_errors: bool,
    /// When set and the token is cancelled, fill_extents returns Err(Halted)
    /// at the next retry boundary. Unlike skip_errors, this propagates the
    /// error up so the rip terminates cleanly. Construct with
    /// [`DiscStream::with_halt`], passing the same `Halt` clone handed to
    /// sweep / patch / mux so every phase observes one Stop signal.
    halt: Option<Halt>,
    event_fn: Option<Box<dyn Fn(Event) + Send>>,
    eof: bool,
    /// Count of dropped DVD navigation packets (private_stream_2, 0xBF) — these
    /// are expected on every disc; tallied and summarised once at EOF instead of
    /// a per-packet WARN.
    dropped_nav_packets: u64,

    // Cumulative bytes successfully read from the source. Drives
    // EventKind::BytesRead emission and autorip's per-device progress.
    bytes_read_total: u64,
    // Pre-computed total of all extents in bytes (or 0 if extents are
    // empty). Carried in EventKind::BytesRead.total so consumers can show
    // a percent without a separate API call.
    bytes_total_extents: u64,

    // PES output — single-threaded inline demux + codec parse. The
    // pipeline-mode mux (3-stage threaded) lives in
    // [`super::pipelined_stream::PipelinedPesStream`]; this type is
    // the legacy in-thread path for live-disc reads where adaptive
    // batch retry on bad sectors lives in `fill_extents`.
    ts_demuxer: Option<super::ts::TsDemuxer>,
    ps_demuxer: Option<super::ps::PsDemuxer>,
    parsers: Vec<(u16, Box<dyn super::codec::CodecParser>)>,
    pending_frames: std::collections::VecDeque<crate::pes::PesFrame>,
    pid_to_track: Vec<(u16, usize)>,
    /// Cached `FREEMKV_SKIP_PARSE` profiling flag. The env var cannot
    /// change at runtime, and `std::env::var_os` takes a process-wide
    /// lock; reading it once at construction keeps it out of the
    /// per-batch read() hot loop.
    skip_parse: bool,
    /// Cached `FREEMKV_PROFILE` presence, read once at construction. When
    /// false, the read() loop skips the four `Instant::now()` captures and the
    /// `prof_tick` calls entirely, so profiling-off runs pay no per-iteration
    /// timestamp cost or `prof_active()` env-var lookup (which takes a
    /// process-wide lock).
    profiling: bool,
}

impl DiscStream {
    /// Create a disc stream from any sector reader.
    ///
    /// Works with physical drives and ISO files — both implement SectorSource.
    /// The caller opens the source, scans for titles/keys, and passes them in.
    /// The stream handles demuxing, decryption, and codec parsing internally.
    pub fn new(
        reader: Box<dyn SectorSource>,
        title: DiscTitle,
        decrypt_keys: crate::decrypt::DecryptKeys,
        batch_sectors: u16,
        content_format: crate::disc::ContentFormat,
    ) -> Self {
        let mut title = title;
        let extents = title.extents.clone();
        let bytes_total_extents: u64 = extents.iter().map(|e| e.sector_count as u64 * 2048).sum();

        // Debug log reader type at construction — critical for diagnosing mux
        // reading from drive instead of ISO. `type_name_of_val(&*reader)`
        // resolves the CONCRETE type behind the box (Drive / FileSectorSource),
        // unlike `type_name::<dyn SectorSource>()` which always prints the
        // trait-object name regardless of the underlying source.
        tracing::debug!(
            target: "mux",
            "DiscStream constructed with reader type: {}",
            std::any::type_name_of_val(&*reader)
        );

        // CSS/unencrypted content needs a decrypting wrapper to yield plaintext
        // VOB bytes before the AC-3 sub-stream probe can read real `acmod`s.
        // MUX path: tolerate decrypt loss — conceal an undecryptable unit (NULL TS
        // fill) + tally + log rather than abort the stream (P3). DiscStream is a
        // decode/mux stream (live-drive single-pass / direct), never the
        // ciphertext-preserving sweep, so concealment is always correct here.
        let mut reader =
            DecryptingSectorSource::new(reader, decrypt_keys.clone()).tolerate_decrypt_loss();

        // Wrong-substream fix (Silence-of-the-Lambs): re-route the title's
        // declared AC-3 audio onto the physically-correct `0x8x` sub-streams by
        // probing their real channel counts off the head of the feature. No-op
        // for non-DVD or when the probe yields nothing.
        crate::disc::dvd_audio_probe::probe_and_remap(&mut reader, &mut title);

        let mut pids = Vec::new();
        let mut parsers = Vec::new();
        let mut pid_to_track = Vec::new();
        for (idx, s) in title.streams.iter().enumerate() {
            let (pid, codec) = match s {
                crate::disc::Stream::Video(v) => (v.pid, v.codec),
                crate::disc::Stream::Audio(a) => (a.pid, a.codec),
                crate::disc::Stream::Subtitle(s) => (s.pid, s.codec),
            };
            pids.push(pid);
            pid_to_track.push((pid, idx));
            let is_dvd_ps = matches!(content_format, crate::disc::ContentFormat::MpegPs);
            parsers.push((pid, super::codec::parser_for_codec(codec, None, is_dvd_ps)));
        }

        let mut ts_demuxer = None;
        let mut ps_demuxer = None;
        match content_format {
            crate::disc::ContentFormat::MpegPs => {
                ps_demuxer = Some(super::ps::PsDemuxer::new());
            }
            crate::disc::ContentFormat::BdTs => {
                let ts_pids: Vec<u16> = pids.clone();
                if !ts_pids.is_empty() {
                    ts_demuxer = Some(super::ts::TsDemuxer::new(&ts_pids));
                }
            }
        }

        // AACS decrypts whole 6144-byte (3-sector) units keyed off each read
        // buffer's first 16 bytes, so reads/skips must stay 3-sector aligned.
        // CSS and unencrypted content are per-2048-byte and self-synchronizing
        // (align 1). Same rule the file-backed highway applies in resolve.rs.
        let unit_align: u16 = match &decrypt_keys {
            crate::decrypt::DecryptKeys::Aacs { .. } => 3,
            _ => 1,
        };

        // `reader` is already wrapped in DecryptingSectorSource above (so the
        // internal fill_extents path sees plaintext bytes; for DecryptKeys::None
        // the decorator is a pass-through). Reset the unit base the probe read
        // advanced so the first fill_extents read starts cleanly.
        reader.set_unit_base(0);
        // Clone the shared loss counter once here so `lost_bytes()` never
        // clones an Arc per frame on the mux hot path.
        let decrypt_loss = reader.decrypt_loss();

        Self {
            reader,
            decrypt_loss,
            title,
            decrypt_keys,
            unit_align,
            extents,
            current_extent: 0,
            current_offset: 0,
            read_buf: Vec::with_capacity(batch_sectors as usize * 2048),
            buf_valid: 0,
            adaptive: AdaptiveBatch::new(batch_sectors),
            errors: 0,
            lost_bytes: 0,
            skip_errors: false,
            halt: None,
            event_fn: None,
            eof: false,
            dropped_nav_packets: 0,
            bytes_read_total: 0,
            bytes_total_extents,
            ts_demuxer,
            ps_demuxer,
            parsers,
            pending_frames: std::collections::VecDeque::new(),
            pid_to_track,
            skip_parse: std::env::var_os("FREEMKV_SKIP_PARSE").is_some(),
            profiling: std::env::var_os("FREEMKV_PROFILE").is_some(),
        }
    }

    /// Set event handler for sector-level events (binary search, skip, recover).
    pub fn on_event(&mut self, f: impl Fn(Event) + Send + 'static) {
        self.event_fn = Some(Box::new(f));
    }

    /// Constructor-time builder: attach a [`Halt`] token so that when
    /// any clone is cancelled, the next read-retry boundary inside
    /// `fill_extents` returns `Err(Halted)`. Required for Stop to work
    /// during dense bad-sector regions (where the outer PES read() loop
    /// can spend minutes inside fill_extents before emitting a frame).
    ///
    /// Pass the same `Halt` clone you hand to sweep / patch / mux so every
    /// phase observes a single Stop signal.
    pub fn with_halt(mut self, halt: Halt) -> Self {
        self.halt = Some(halt);
        self
    }

    fn is_halted(&self) -> bool {
        self.halt
            .as_ref()
            .map(|h| h.is_cancelled())
            .unwrap_or(false)
    }

    fn emit(&self, kind: EventKind) {
        if let Some(ref f) = self.event_fn {
            f(Event { kind });
        }
    }

    /// Skip decryption — return raw encrypted bytes. Updates both
    /// the metadata-side key field and the wrapped reader's keys so
    /// subsequent `read_sectors` calls become a pass-through.
    pub fn set_raw(&mut self) {
        self.decrypt_keys = crate::decrypt::DecryptKeys::None;
        self.reader.set_keys(crate::decrypt::DecryptKeys::None);
    }

    fn fill_extents(&mut self) -> io::Result<bool> {
        if self.current_extent >= self.extents.len() {
            return Ok(false);
        }
        let ext_start = self.extents[self.current_extent].start_lba;
        let ext_sectors = self.extents[self.current_extent].sector_count;

        let remaining = ext_sectors.saturating_sub(self.current_offset);
        if remaining == 0 {
            self.current_extent += 1;
            self.current_offset = 0;
            return self.fill_extents();
        }

        // start_lba comes from UDF/MPLS extents; a malformed extent near
        // u32::MAX would overflow (debug panic / release wrap to a wrong LBA).
        // Saturate for consistency with the rest of the file's arithmetic.
        let lba = ext_start.saturating_add(self.current_offset);

        // AACS aligned units are anchored at this extent's start LBA — gate the
        // decrypt-on-read source relative to it (clip-anchored), not absolute
        // disc LBA 0. No-op for CSS / None.
        self.reader.set_unit_base(ext_start);

        // Adaptive sizer: start at current (preferred until a failure), shrink
        // on failure, advance on success. One 5s read attempt per try — no
        // retry loops, no sleeps. On size-1 failure, skip or error.
        //
        // Halt is checked at the top of every iteration — in a dense bad zone
        // this loop can spend minutes shrinking and skipping sectors; without
        // the check, Stop wouldn't take effect until the outer PES read() loop
        // finally emits a frame, which may never happen.

        let start_time = std::time::Instant::now();

        loop {
            if self.is_halted() {
                return Err(crate::error::Error::Halted.into());
            }

            // Debug: log slow reads during mux — helps diagnose stalls
            if cfg!(debug_assertions) && start_time.elapsed().as_secs() > 5 {
                tracing::debug!(target: "mux", "fill_extents waiting at LBA {} ({}s elapsed, sectors={})", lba, start_time.elapsed().as_secs(), remaining);
            }

            // Keep every read buffer starting on a real on-disc unit boundary.
            // AACS (unit_align=3) decrypts whole 6144-byte units keyed off the
            // buffer's first bytes, so a sub-unit read mid-extent desyncs the
            // rest of the title; always read at least one full unit. Only the
            // final partial unit at the extent tail (remaining < align) is read
            // short — nothing follows it to desync. CSS/raw (align=1) is
            // per-sector and self-synchronizing, so this is a no-op there.
            let align = self.unit_align.max(1) as u32;
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
            let bytes = sectors as usize * 2048;
            self.read_buf.resize(bytes, 0);

            let res = self
                .reader
                .read_sectors(lba, sectors, &mut self.read_buf[..bytes], false);

            if let Ok(&got) = res.as_ref() {
                // SectorSource::read_sectors returns the number of bytes
                // written into buf. All in-tree sources return full-or-error,
                // but a short count would leave the stale/zeroed tail of
                // read_buf in place; trust the returned count, not `bytes`.
                debug_assert!(got <= bytes, "read_sectors over-reported byte count");
                if let Some(ev) = self.adaptive.on_success(sectors) {
                    self.emit(ev);
                }
                let bytes = got.min(bytes);
                self.buf_valid = bytes;
                self.current_offset += sectors as u32;
                self.bytes_read_total = self.bytes_read_total.saturating_add(bytes as u64);
                self.emit(EventKind::BytesRead {
                    bytes: self.bytes_read_total,
                    total: self.bytes_total_extents,
                });
                break;
            }

            // Transport failure (status=0xFF: USB-bridge crash / disconnect) is
            // NOT a skippable bad sector. The bridge is wedged and every
            // subsequent read fails identically, so shrinking + skipping past it
            // — even under `skip_errors` — just marches the whole disc at one
            // ~15s bridge-recovery per probe, producing no usable output (the
            // "runs forever, no MKV" report). Abort immediately, highest
            // priority, mirroring the multipass sweep's transport-failure rule
            // in `read_error::handle_read_error`. The CLI/UX surfaces this so the
            // user power-cycles the drive (or switches to multipass recovery).
            if let Some(e) = res.as_ref().err() {
                if e.is_scsi_transport_failure() {
                    let (status, sense) = extract_scsi_context(e);
                    return Err(crate::error::Error::DiscRead {
                        sector: lba as u64,
                        status: Some(status),
                        sense,
                    }
                    .into());
                }
            }

            if (sectors as u32) <= align {
                // Bottomed out at one unit (AACS) / one sector (CSS) / the
                // extent tail. This is single-pass disc→MKV, which has NO Pass N
                // to come back and recover later — so before we skip or bail,
                // give the drive its full ECC recovery budget ONCE
                // (`recovery=true` → READ_RECOVERY_TIMEOUT_MS, ~60s), exactly as
                // the multipass patch does on its bad ranges. A single bounded
                // read, never a loop (hard rule #2: tight retry loops on one LBA
                // push the BU40N into fast-fail). On success we USE the recovered
                // data, so the old "transient retry returns a bogus status for
                // readable data" hole cannot reopen; the earlier 10s-timeout read
                // gave the drive no chance to recover a marginal sector that a
                // 60s ECC read can.
                tracing::debug!(
                    target: "mux",
                    "fill_extents last-chance recovery read at LBA {} ({} sectors, 60s ECC)",
                    lba,
                    sectors
                );
                let rec = self
                    .reader
                    .read_sectors(lba, sectors, &mut self.read_buf[..bytes], true);
                if let Ok(&got) = rec.as_ref() {
                    debug_assert!(got <= bytes, "recovery read over-reported byte count");
                    if let Some(ev) = self.adaptive.on_success(sectors) {
                        self.emit(ev);
                    }
                    let got = got.min(bytes);
                    self.buf_valid = got;
                    self.current_offset += sectors as u32;
                    self.bytes_read_total = self.bytes_read_total.saturating_add(got as u64);
                    self.emit(EventKind::BytesRead {
                        bytes: self.bytes_read_total,
                        total: self.bytes_total_extents,
                    });
                    break;
                }

                // Recovery read also failed. A transport failure here (status
                // 0xFF: USB-bridge crash / disconnect) is NOT a skippable bad
                // unit — same as the original 10s read above. The line-442
                // short-circuit only inspected `res`; the 60s recovery read
                // (`rec`) can wedge the bridge on its own, and falling into the
                // `skip_errors` branch below would zero-fill + advance, treating
                // a dead bridge as a skippable unit and marching the whole disc
                // at one bridge-recovery per probe (hard rule #2, "runs forever,
                // no MKV"). Re-check `rec` and abort, mirroring line 442.
                if let Some(e) = rec.as_ref().err() {
                    if e.is_scsi_transport_failure() {
                        let (status, sense) = extract_scsi_context(e);
                        return Err(crate::error::Error::DiscRead {
                            sector: lba as u64,
                            status: Some(status),
                            sense,
                        }
                        .into());
                    }
                }

                // Recovery read also failed. Skip the WHOLE failed unit or bail.
                // Zero-filling and advancing by the full unit keeps
                // current_offset unit-aligned, so the next read still begins on a
                // real AACS unit boundary (a 1-sector skip here would desync the
                // rest of the title — the bug this guards).
                if self.skip_errors {
                    let zb = sectors as usize * 2048;
                    self.read_buf.resize(zb, 0);
                    self.read_buf[..zb].fill(0);
                    self.buf_valid = zb;
                    self.errors += 1;
                    // `errors` counts skip events; `lost_bytes` counts the
                    // bytes actually zero-filled. For AACS (unit_align=3) a
                    // single event skips a whole 6144-byte unit, so loss
                    // estimates must use this, not `errors * 2048`.
                    self.lost_bytes = self.lost_bytes.saturating_add(zb as u64);
                    self.emit(EventKind::SectorSkipped { sector: lba as u64 });
                    self.current_offset += sectors as u32;
                    break;
                } else {
                    // Build the error from the recovery failure we now hold
                    // (falling back to the original 10s-read failure).
                    let err = rec.err().or(res.err());
                    let (status, sense) =
                        err.as_ref().map(extract_scsi_context).unwrap_or((0, None));
                    return Err(crate::error::Error::DiscRead {
                        sector: lba as u64,
                        status: Some(status),
                        sense,
                    }
                    .into());
                }
            }

            // Shrink and retry at the same LBA with a smaller batch.
            if let Some(ev) = self.adaptive.on_failure() {
                self.emit(ev);
            }
        }

        if self.current_offset >= ext_sectors {
            self.current_extent += 1;
            self.current_offset = 0;
        }
        Ok(true)
    }
}

/// Per-stage profiling state — populated only when `FREEMKV_PROFILE`
/// is set. Logs a percentage breakdown via `tracing` (target "mux")
/// every [`PROFILE_INTERVAL`]. Zero overhead in normal runs (the
/// `DiscStream::profiling` check is the only added cost).
struct StageProf {
    started: std::time::Instant,
    last_dump: std::time::Instant,
    fill_ns: u128,
    feed_ns: u128,
    consume_ns: u128,
    bytes_in: u64,
}

const PROFILE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

thread_local! {
    static STAGE_PROF: std::cell::RefCell<Option<StageProf>> = const { std::cell::RefCell::new(None) };
}

fn prof_active() -> bool {
    std::env::var_os("FREEMKV_PROFILE").is_some()
}

fn prof_tick(stage: &str, ns: u128, bytes: u64) {
    STAGE_PROF.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            if !prof_active() {
                return;
            }
            let now = std::time::Instant::now();
            *slot = Some(StageProf {
                started: now,
                last_dump: now,
                fill_ns: 0,
                feed_ns: 0,
                consume_ns: 0,
                bytes_in: 0,
            });
        }
        let p = slot.as_mut().unwrap();
        match stage {
            "fill" => p.fill_ns += ns,
            "feed" => p.feed_ns += ns,
            "consume" => p.consume_ns += ns,
            _ => {}
        }
        p.bytes_in += bytes;
        let now = std::time::Instant::now();
        if now.duration_since(p.last_dump) < PROFILE_INTERVAL {
            return;
        }
        let elapsed_ms = now.duration_since(p.started).as_millis().max(1);
        let fill_pct = p.fill_ns / 10_000 / elapsed_ms;
        let feed_pct = p.feed_ns / 10_000 / elapsed_ms;
        let consume_pct = p.consume_ns / 10_000 / elapsed_ms;
        let mbps = p.bytes_in as u128 * 1000 / 1_000_000 / elapsed_ms;
        tracing::debug!(
            target: "mux",
            "[profile] elapsed={}ms in={}MB/s fill={}% feed={}% consume={}%",
            elapsed_ms, mbps, fill_pct, feed_pct, consume_pct,
        );
        p.last_dump = now;
    });
}

impl crate::pes::Stream for DiscStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        if let Some(frame) = self.pending_frames.pop_front() {
            return Ok(Some(frame));
        }

        if self.eof {
            return Ok(None);
        }

        loop {
            // Profiling timestamps only when FREEMKV_PROFILE is set; otherwise
            // these stay None and no Instant::now() is taken in the hot loop.
            let t0 = self.profiling.then(std::time::Instant::now);
            if !self.fill_extents()? {
                self.eof = true;
                if self.dropped_nav_packets > 0 {
                    tracing::debug!(
                        target: "mux",
                        "dropped {} DVD navigation packets (private_stream_2/0xBF) — expected, carry no elementary stream",
                        self.dropped_nav_packets
                    );
                }
                // Flush demuxer — last PES packet may still be in the assembler
                if let Some(ref mut demuxer) = self.ts_demuxer {
                    for pes in &demuxer.flush() {
                        if let Some((_, track)) =
                            self.pid_to_track.iter().find(|(pid, _)| *pid == pes.pid)
                        {
                            if let Some((_, parser)) =
                                self.parsers.iter_mut().find(|(pid, _)| *pid == pes.pid)
                            {
                                for frame in parser.parse(pes) {
                                    self.pending_frames.push_back(
                                        crate::pes::PesFrame::from_codec_frame(*track, frame),
                                    );
                                }
                            }
                        }
                    }
                }
                // PS demuxer flush (DVD)
                if let Some(ref mut demuxer) = self.ps_demuxer {
                    for ps in &demuxer.flush() {
                        // Route by the REAL DVD PID (see consume_ps in
                        // pipelined_stream.rs); the old (sub_id & 0x1F)+1
                        // heuristic mis-routed VobSub into the AC-3 parser.
                        let Some(pid) = ps.dvd_pid() else {
                            if ps.is_nav() {
                                // Expected DVD navigation packet (PCI/DSI) —
                                // tally, no WARN.
                                self.dropped_nav_packets += 1;
                            } else {
                                // Unexpected unmappable stream_id (a
                                // possibly-dropped real stream). Keep the WARN.
                                tracing::warn!(
                                    target: "mux",
                                    "dropping unmappable PS packet (stream_id={:#04x}, sub_stream_id={:?})",
                                    ps.stream_id,
                                    ps.sub_stream_id,
                                );
                            }
                            continue;
                        };
                        let Some((_, track)) =
                            self.pid_to_track.iter().find(|(p, _)| *p == pid).copied()
                        else {
                            tracing::warn!(
                                target: "mux",
                                "dropping PS packet for unmapped PID {:#06x} (stream_id={:#04x}, sub_stream_id={:?})",
                                pid,
                                ps.stream_id,
                                ps.sub_stream_id,
                            );
                            continue;
                        };
                        let pes = super::ts::PesPacket {
                            source: None,
                            pid,
                            pts: ps.pts.map(|p| p as i64),
                            dts: ps.dts.map(|d| d as i64),
                            data: ps.data.clone(),
                        };
                        if let Some((_, parser)) = self.parsers.iter_mut().find(|(p, _)| *p == pid)
                        {
                            for frame in parser.parse(&pes) {
                                self.pending_frames.push_back(
                                    crate::pes::PesFrame::from_codec_frame(track, frame),
                                );
                            }
                        }
                    }
                }
                // Drain any access unit a codec parser buffered past the last
                // PES (DTS-HD's final core+extension unit, assembled across
                // PES boundaries).
                let pid_to_track = &self.pid_to_track;
                let pending = &mut self.pending_frames;
                for (pid, parser) in self.parsers.iter_mut() {
                    let Some(&(_, track)) = pid_to_track.iter().find(|(p, _)| p == pid) else {
                        continue;
                    };
                    for frame in parser.flush() {
                        pending.push_back(crate::pes::PesFrame::from_codec_frame(track, frame));
                    }
                }
                return Ok(self.pending_frames.pop_front());
            }

            let bytes = self.buf_valid;
            let t1 = self.profiling.then(std::time::Instant::now);
            if let (Some(t0), Some(t1)) = (t0, t1) {
                prof_tick("fill", t1.duration_since(t0).as_nanos(), bytes as u64);
            }
            // Plaintext: the wrapped reader (DecryptingSectorSource)
            // applied AACS / CSS in-place during fill_extents'
            // read_sectors call. The pre-0.18 inline decrypt step
            // lived here.

            if let Some(ref mut demuxer) = self.ts_demuxer {
                let packets = demuxer.feed(&self.read_buf[..bytes]);
                let t2 = self.profiling.then(std::time::Instant::now);
                if let (Some(t1), Some(t2)) = (t1, t2) {
                    prof_tick("feed", t2.duration_since(t1).as_nanos(), 0);
                }
                let skip_parse = self.skip_parse;
                for pes in packets {
                    if let Some((_, track)) = self
                        .pid_to_track
                        .iter()
                        .find(|(pid, _)| *pid == pes.pid)
                        .copied()
                    {
                        if skip_parse {
                            // Profiling escape hatch — bypass the codec
                            // parser and pass the raw PES bytes straight
                            // through as a single PesFrame. Lets us
                            // attribute consumer-thread time to
                            // "demux + framing" vs "codec parse".
                            self.pending_frames.push_back(crate::pes::PesFrame {
                                coding: None,
                                source: None,
                                track,
                                pts: pes.pts.map(super::codec::pts_to_ns).unwrap_or(0),
                                keyframe: false,
                                data: pes.data,
                                duration_ns: None,
                            });
                        } else if let Some((_, parser)) =
                            self.parsers.iter_mut().find(|(pid, _)| *pid == pes.pid)
                        {
                            for frame in parser.parse(&pes) {
                                self.pending_frames.push_back(
                                    crate::pes::PesFrame::from_codec_frame(track, frame),
                                );
                            }
                        }
                    }
                }
                let t3 = self.profiling.then(std::time::Instant::now);
                if let (Some(t2), Some(t3)) = (t2, t3) {
                    prof_tick("consume", t3.duration_since(t2).as_nanos(), 0);
                }
            } else if let Some(ref mut demuxer) = self.ps_demuxer {
                let packets = demuxer.feed(&self.read_buf[..bytes]);
                for ps in &packets {
                    // Route by the REAL DVD PID (see consume_ps in
                    // pipelined_stream.rs); the old (sub_id & 0x1F)+1
                    // heuristic mis-routed VobSub into the AC-3 parser.
                    let Some(pid) = ps.dvd_pid() else {
                        if ps.is_nav() {
                            // Expected DVD navigation packet (PCI/DSI) — tally,
                            // no WARN.
                            self.dropped_nav_packets += 1;
                        } else {
                            // Unexpected unmappable stream_id (a possibly-dropped
                            // real stream). Keep the individual WARN.
                            tracing::warn!(
                                target: "mux",
                                "dropping unmappable PS packet (stream_id={:#04x}, sub_stream_id={:?})",
                                ps.stream_id,
                                ps.sub_stream_id,
                            );
                        }
                        continue;
                    };
                    let Some((_, track)) =
                        self.pid_to_track.iter().find(|(p, _)| *p == pid).copied()
                    else {
                        tracing::warn!(
                            target: "mux",
                            "dropping PS packet for unmapped PID {:#06x} (stream_id={:#04x}, sub_stream_id={:?})",
                            pid,
                            ps.stream_id,
                            ps.sub_stream_id,
                        );
                        continue;
                    };

                    let pes = super::ts::PesPacket {
                        source: None,
                        pid,
                        pts: ps.pts.map(|p| p as i64),
                        dts: ps.dts.map(|d| d as i64),
                        data: ps.data.clone(),
                    };

                    if let Some((_, parser)) = self.parsers.iter_mut().find(|(p, _)| *p == pid) {
                        for frame in parser.parse(&pes) {
                            self.pending_frames
                                .push_back(crate::pes::PesFrame::from_codec_frame(track, frame));
                        }
                    }
                }
            }

            self.buf_valid = 0;

            if let Some(frame) = self.pending_frames.pop_front() {
                return Ok(Some(frame));
            }
        }
    }

    fn write(&mut self, _frame: &crate::pes::PesFrame) -> io::Result<()> {
        Err(crate::error::Error::StreamReadOnly.into())
    }

    fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn info(&self) -> &DiscTitle {
        &self.title
    }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        let pid = self
            .pid_to_track
            .iter()
            .find(|(_, idx)| *idx == track)
            .map(|(pid, _)| *pid)?;
        self.parsers
            .iter()
            .find(|(p, _)| *p == pid)
            .and_then(|(_, parser)| parser.codec_private())
    }

    fn headers_ready(&self) -> bool {
        // FREEMKV_SKIP_PARSE bypasses codec parsers entirely for
        // bottleneck profiling, so codec_private is never populated.
        // Pretend headers are ready immediately in that mode so the
        // CLI loop doesn't hang waiting for them.
        if self.skip_parse {
            return true;
        }
        for (idx, s) in self.title.streams.iter().enumerate() {
            if let crate::disc::Stream::Video(v) = s {
                if !v.secondary && self.codec_private(idx).is_none() {
                    return false;
                }
            }
        }
        true
    }

    fn errors(&self) -> u64 {
        self.errors
    }

    fn lost_bytes(&self) -> u64 {
        // Read-error zero-fill loss (counted in fill_extents) PLUS decrypt-time
        // loss — bytes of scrambled AACS units the decorator could not decrypt
        // and passed through still encrypted (the TS assembler silently drops
        // them). Both are real missing content the abort gate must see; without
        // the decrypt term a partial key failure reports lost_bytes=0 and a rip
        // missing segments passes even under abort_on_lost_secs=0.
        self.lost_bytes
            .saturating_add(self.decrypt_loss.load(std::sync::atomic::Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    //! `DiscStream` is the only read-only `Stream` impl in tree (every
    //! other concrete impl in `mux/*` is bidirectional or write-only).
    //! These tests lock down a static `Send` assertion plus a
    //! `Box<dyn Stream>` round trip exercising every method through the
    //! trait object, so future Send-breaking edits to `DiscStream`'s
    //! interior types fail at compile time.
    use super::*;
    use crate::disc::{ContentFormat, DiscTitle};
    use crate::pes::Stream;

    /// Static-assert `DiscStream: Send`. The `Stream` trait has `Send` as a
    /// supertrait — if a future field on `DiscStream` is non-`Send` (e.g.
    /// a `Box<dyn Read>` instead of `Box<dyn SectorSource>`), this fails
    /// at compile time, before the runtime trait-object test below.
    fn _assert_disc_stream_is_send() {
        fn requires_send<T: Send>() {}
        requires_send::<DiscStream>();
    }

    /// Trivial `SectorSource` that yields zeroed sectors. Empty title means
    /// the demuxer produces no PES frames, so `read()` walks the extents to
    /// EOF and returns `Ok(None)`. That's enough to exercise the trait-object
    /// dispatch — the goal here is the bridge, not the demuxer.
    struct ZeroReader {
        capacity: u32,
    }

    impl crate::sector::SectorSource for ZeroReader {
        fn read_sectors(
            &mut self,
            _lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            let bytes = count as usize * 2048;
            buf[..bytes].fill(0);
            Ok(bytes)
        }

        fn capacity_sectors(&self) -> u32 {
            self.capacity
        }
    }

    fn synthetic_title(sector_count: u32) -> DiscTitle {
        DiscTitle {
            extents: vec![crate::disc::Extent {
                start_lba: 0,
                sector_count,
            }],
            ..DiscTitle::empty()
        }
    }

    /// Smallest credible witness that `DiscStream` flows through `dyn Stream`:
    /// build a `Box<dyn Stream>`, drive `read()` to EOF, exercise `info()` /
    /// `headers_ready()` / `codec_private()` through the trait object.
    #[test]
    fn stream_via_dyn_object() {
        let reader = ZeroReader { capacity: 8 };
        let title = synthetic_title(8);
        let stream = DiscStream::new(
            Box::new(reader),
            title,
            crate::decrypt::DecryptKeys::None,
            8,
            ContentFormat::BdTs,
        );

        let mut src: Box<dyn Stream> = Box::new(stream);

        // Empty-title fixture has no streams configured, so headers are
        // trivially ready and codec_private() yields nothing on track 0.
        assert!(src.headers_ready());
        assert!(src.codec_private(0).is_none());
        let _ = src.info();

        // Drive read() to EOF through the trait object — empty-title fixture
        // produces no frames, but the call still routes through the blanket
        // dispatch into Stream::read.
        let mut frames = 0usize;
        while src.read().expect("read").is_some() {
            frames += 1;
            if frames > 1024 {
                panic!("unexpected unbounded frame stream from empty title");
            }
        }
        assert_eq!(frames, 0);
    }

    /// `is_halted()` must observe a cancellation signal installed via
    /// `with_halt(Halt)` — flipping the token must cause the next
    /// `fill_extents` retry boundary to bail.
    #[test]
    fn halt_via_with_halt_observed_by_is_halted() {
        let halt = Halt::new();
        let stream = DiscStream::new(
            Box::new(ZeroReader { capacity: 8 }),
            synthetic_title(8),
            crate::decrypt::DecryptKeys::None,
            8,
            crate::disc::ContentFormat::BdTs,
        )
        .with_halt(halt.clone());
        assert!(!stream.is_halted());
        halt.cancel();
        assert!(
            stream.is_halted(),
            "with_halt token cancellation must be observed by is_halted()"
        );
    }

    /// Recording `SectorSource`: logs every `(lba, count)` request and
    /// returns `Err` whenever the requested range covers `bad_sector`.
    /// Successful reads return zeroed sectors (which are NOT
    /// `ts_sync_destroyed`, so `DecryptingSectorSource` passes them through
    /// even with synthetic AACS keys — no real decrypt is attempted).
    struct RecordingReader {
        capacity: u32,
        bad_sector: u32,
        log: std::sync::Arc<std::sync::Mutex<Vec<(u32, u16)>>>,
    }

    impl crate::sector::SectorSource for RecordingReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            self.log.lock().unwrap().push((lba, count));
            let end = lba + count as u32;
            if self.bad_sector >= lba && self.bad_sector < end {
                return Err(crate::error::Error::DiscRead {
                    sector: self.bad_sector as u64,
                    status: Some(0x02),
                    sense: None,
                });
            }
            let bytes = count as usize * 2048;
            buf[..bytes].fill(0);
            Ok(bytes)
        }

        fn capacity_sectors(&self) -> u32 {
            self.capacity
        }
    }

    /// `SectorSource` that fails every read covering `bad_sector` with a
    /// SCSI **transport failure** (status=0xFF) — the USB-bridge-crash sentinel
    /// that `Drive::read` surfaces as `DiscRead { status: Some(0xFF), .. }`.
    /// Logs each `(lba, count)` so a test can prove the failure was not
    /// retried/skipped.
    struct TransportFailReader {
        capacity: u32,
        bad_sector: u32,
        log: std::sync::Arc<std::sync::Mutex<Vec<(u32, u16)>>>,
    }

    impl crate::sector::SectorSource for TransportFailReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            self.log.lock().unwrap().push((lba, count));
            let end = lba + count as u32;
            if self.bad_sector >= lba && self.bad_sector < end {
                return Err(crate::error::Error::DiscRead {
                    sector: self.bad_sector as u64,
                    status: Some(crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE),
                    sense: None,
                });
            }
            let bytes = count as usize * 2048;
            buf[..bytes].fill(0);
            Ok(bytes)
        }

        fn capacity_sectors(&self) -> u32 {
            self.capacity
        }
    }

    /// `SectorSource` that mirrors a marginal sector recoverable only with the
    /// drive's full ECC budget: every read covering `bad_sector` FAILS while
    /// `recovery=false` (the fast 10s pass) and SUCCEEDS (zeroed bytes) once
    /// `recovery=true` (the 60s ECC pass). Drives the single-pass bottom-out
    /// "last-chance recovery read" success branch in `fill_extents`, which the
    /// other test sources (ignoring the flag) never exercise.
    struct RecoverableReader {
        capacity: u32,
        bad_sector: u32,
        /// `(lba, count, recovery)` for every issued read.
        log: std::sync::Arc<std::sync::Mutex<Vec<(u32, u16, bool)>>>,
    }

    impl crate::sector::SectorSource for RecoverableReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> crate::error::Result<usize> {
            self.log.lock().unwrap().push((lba, count, recovery));
            let end = lba + count as u32;
            let covers_bad = self.bad_sector >= lba && self.bad_sector < end;
            // Fail on the fast (non-recovery) pass; the 60s ECC recovery read
            // succeeds. Distinct non-0x02 sense byte so a transport-failure
            // re-check (status 0xFF) is provably NOT triggered here.
            if covers_bad && !recovery {
                return Err(crate::error::Error::DiscRead {
                    sector: self.bad_sector as u64,
                    status: Some(0x02),
                    sense: None,
                });
            }
            let bytes = count as usize * 2048;
            buf[..bytes].fill(0);
            Ok(bytes)
        }

        fn capacity_sectors(&self) -> u32 {
            self.capacity
        }
    }

    /// Coverage for the single-pass bottom-out RECOVERY-READ SUCCESS branch
    /// (rc.5.2 audit #3): a sector that fails the fast 10s read but reads clean
    /// on the 60s ECC recovery read must have its RECOVERED data muxed — the
    /// cursor advances over the whole unit, byte counters move, and NO skip is
    /// counted. Pre-fix the test sources ignored `recovery`, so this branch was
    /// untested. Uses `unit_align=1` (None) so the bottom-out unit is a single
    /// sector, exercising the `(sectors as u32) <= align` path precisely.
    #[test]
    fn recovery_read_success_muxes_recovered_data_no_skip() {
        const COUNT: u32 = 10;
        let bad = 4u32;
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reader = RecoverableReader {
            capacity: COUNT,
            bad_sector: bad,
            log: log.clone(),
        };
        let mut stream = DiscStream::new(
            Box::new(reader),
            synthetic_title(COUNT),
            crate::decrypt::DecryptKeys::None,
            8,
            ContentFormat::BdTs,
        );
        // skip_errors=false: if the recovery read did NOT succeed, fill_extents
        // would return Err — so reaching EOF cleanly proves recovery worked.
        stream.skip_errors = false;

        let mut guard = 0;
        loop {
            match stream.fill_extents() {
                Ok(true) => {}
                Ok(false) => break,
                Err(e) => panic!("recovery read should have succeeded, got: {e}"),
            }
            guard += 1;
            assert!(guard < 1000, "fill_extents did not reach EOF");
        }

        // No skip counted: the recovered unit was muxed, not zero-filled.
        assert_eq!(
            stream.errors, 0,
            "a successful recovery read must not count as a skipped sector"
        );
        assert_eq!(
            stream.lost_bytes, 0,
            "a successful recovery read loses no bytes"
        );
        // All COUNT sectors' worth of bytes were read through to the cursor end.
        assert_eq!(
            stream.bytes_read_total,
            COUNT as u64 * 2048,
            "every sector (including the recovered one) must be counted as read"
        );

        // The bad sector was retried with recovery=true and that read SUCCEEDED.
        let reads = log.lock().unwrap();
        assert!(
            reads
                .iter()
                .any(|&(lba, count, rec)| rec && lba == bad && count == 1),
            "expected a recovery=true single-sector read at the bad sector; got {reads:?}"
        );
        // And the fast pass at the bad sector did happen with recovery=false.
        assert!(
            reads.iter().any(|&(lba, _c, rec)| !rec && lba == bad),
            "expected a non-recovery read to have first failed at the bad sector"
        );
    }

    /// `SectorSource` that fails the fast (non-recovery) read covering
    /// `bad_sector` with an ordinary bad-sector error (status 0x02), then fails
    /// the 60s ECC recovery read with a TRANSPORT failure (status 0xFF). Models
    /// a bridge that wedges precisely during the last-chance recovery read.
    struct RecoveryTransportFailReader {
        capacity: u32,
        bad_sector: u32,
        log: std::sync::Arc<std::sync::Mutex<Vec<(u32, u16, bool)>>>,
    }

    impl crate::sector::SectorSource for RecoveryTransportFailReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> crate::error::Result<usize> {
            self.log.lock().unwrap().push((lba, count, recovery));
            let end = lba + count as u32;
            if self.bad_sector >= lba && self.bad_sector < end {
                let status = if recovery {
                    crate::scsi::SCSI_STATUS_TRANSPORT_FAILURE
                } else {
                    0x02
                };
                return Err(crate::error::Error::DiscRead {
                    sector: self.bad_sector as u64,
                    status: Some(status),
                    sense: None,
                });
            }
            let bytes = count as usize * 2048;
            buf[..bytes].fill(0);
            Ok(bytes)
        }

        fn capacity_sectors(&self) -> u32 {
            self.capacity
        }
    }

    /// Regression (rc.5.2 audit #2): a transport failure on the 60s ECC
    /// RECOVERY read (not just the initial 10s read) must ABORT, even under
    /// `skip_errors=true`. The line-442 short-circuit only inspected the
    /// original `res`; without a re-check the wedged-bridge recovery failure
    /// fell into the skip branch — zero-fill + advance — marching the disc at
    /// one bridge-recovery per unit ("runs forever, no MKV", hard rule #2). The
    /// fix re-checks the recovery error for `is_scsi_transport_failure()` before
    /// the skip block and returns `Error::DiscRead`.
    #[test]
    fn transport_failure_on_recovery_read_aborts_even_with_skip_errors() {
        const COUNT: u32 = 10;
        let bad = 4u32;
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reader = RecoveryTransportFailReader {
            capacity: COUNT,
            bad_sector: bad,
            log: log.clone(),
        };
        let mut stream = DiscStream::new(
            Box::new(reader),
            synthetic_title(COUNT),
            crate::decrypt::DecryptKeys::None,
            8,
            ContentFormat::BdTs,
        );
        stream.skip_errors = true;

        // Drive fill_extents across batches: the good leading sectors mux fine,
        // and the batch covering the bad sector shrinks to size 1, fails the
        // fast read (0x02), then the bottom-out recovery read returns the
        // transport failure (0xFF) — which must abort.
        let mut res = Ok(true);
        for _ in 0..1000 {
            res = stream.fill_extents();
            if !matches!(res, Ok(true)) {
                break;
            }
        }
        assert!(
            res.is_err(),
            "a transport failure on the recovery read must abort fill_extents, got {res:?}"
        );
        assert_eq!(
            stream.errors, 0,
            "a recovery-read transport-failure abort must NOT count as a skip"
        );
        assert_eq!(
            stream.lost_bytes, 0,
            "a transport-failure abort zero-fills nothing"
        );
        // Prove the bottom-out recovery read was actually reached and aborted on.
        let reads = log.lock().unwrap();
        assert!(
            reads
                .iter()
                .any(|&(lba, count, rec)| rec && lba == bad && count == 1),
            "expected a recovery=true read at the bad sector to have been attempted; got {reads:?}"
        );
    }

    /// Regression: a USB-bridge transport crash (status=0xFF) during a direct
    /// single-pass `disc://→mkv://` rip must ABORT immediately, even under
    /// `skip_errors=true`. The pre-fix behavior treated it as a skippable bad
    /// sector: zero-fill, advance, repeat — marching the whole disc at one
    /// ~15s bridge-recovery per probe, producing no MKV ("runs forever"). The
    /// fix mirrors the multipass sweep: transport failure short-circuits to an
    /// error before any shrink/skip, so exactly ONE read is issued and no skip
    /// is counted.
    #[test]
    fn transport_failure_aborts_single_pass_even_with_skip_errors() {
        const COUNT: u32 = 10;
        let bad = 4u32;
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reader = TransportFailReader {
            capacity: COUNT,
            bad_sector: bad,
            log: log.clone(),
        };
        let mut stream = DiscStream::new(
            Box::new(reader),
            synthetic_title(COUNT),
            crate::decrypt::DecryptKeys::None,
            8,
            ContentFormat::BdTs,
        );
        stream.skip_errors = true;

        let res = stream.fill_extents();
        assert!(
            res.is_err(),
            "transport failure must abort fill_extents, not skip past it"
        );
        assert_eq!(
            stream.errors, 0,
            "a transport-failure abort must NOT count as a skipped sector"
        );
        let reads = log.lock().unwrap();
        assert_eq!(
            reads.len(),
            1,
            "transport failure must abort after the first failed read with no \
             shrink/retry/skip-ahead; got reads {reads:?}"
        );
    }

    /// AACS unit-alignment skip (the #1 coverage gap). With `unit_align=3`
    /// (DecryptKeys::Aacs) and `skip_errors=true`, a single bad mid-extent
    /// sector must NOT desync the rest of the title: every `read_sectors`
    /// request must start on a 3-sector unit boundary relative to the extent
    /// start, and the skip over the failed unit must advance the cursor by a
    /// whole 3-sector unit (never a single sector).
    #[test]
    fn aacs_reads_stay_unit_aligned_and_skip_whole_units() {
        const COUNT: u32 = 30;
        const ALIGN: u32 = 3;
        // Bad sector at offset 13 — inside unit 4 (offsets 12,13,14). The
        // whole unit must be skipped, keeping the cursor unit-aligned.
        let bad = 13u32;
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reader = RecordingReader {
            capacity: COUNT,
            bad_sector: bad,
            log: log.clone(),
        };
        let title = synthetic_title(COUNT);
        let keys = crate::decrypt::DecryptKeys::Aacs {
            unit_keys: vec![(0, [0u8; 16])],
            read_data_key: None,
        };
        let mut stream = DiscStream::new(Box::new(reader), title, keys, 8, ContentFormat::BdTs);
        stream.skip_errors = true;
        assert_eq!(
            stream.unit_align, ALIGN as u16,
            "AACS keys must set unit_align=3"
        );

        // Drive fill_extents to EOF (no PES demux needed — we observe the
        // raw read pattern directly).
        let ext_start = 0u32;
        let mut guard = 0;
        loop {
            match stream.fill_extents() {
                Ok(true) => {}
                Ok(false) => break,
                Err(e) => panic!("fill_extents errored unexpectedly: {e}"),
            }
            guard += 1;
            assert!(guard < 1000, "fill_extents did not reach EOF");
        }

        let reads = log.lock().unwrap();
        assert!(!reads.is_empty(), "expected at least one read");
        for &(lba, count) in reads.iter() {
            assert_eq!(
                (lba - ext_start) % ALIGN,
                0,
                "read at lba {lba} is not unit-aligned (offset {} % {ALIGN} != 0)",
                lba - ext_start
            );
            // Non-tail reads must be a whole number of units; the only
            // permitted short read is the final partial unit (here COUNT is a
            // multiple of ALIGN, so every read should be unit-multiple unless
            // it shrank below one unit — which is itself a single unit).
            let _ = count;
        }

        // At least one error was skipped (the bad unit) and a SectorSkipped
        // event was emitted; errors counter advanced by exactly the bad units.
        assert!(stream.errors >= 1, "expected the bad unit to be skipped");

        // Regression: `lost_bytes` must account for the WHOLE skipped unit
        // (3 sectors = 6144 bytes), not a single sector. A loss estimate
        // built from `errors * 2048` would undercount AACS loss ~3x — the
        // single-pass abort-gate bug this guards against. Exactly one unit
        // is bad in this fixture, so lost_bytes == errors * ALIGN * 2048.
        assert_eq!(
            stream.lost_bytes,
            stream.errors * ALIGN as u64 * 2048,
            "AACS skip must record a whole unit (6144 B) per skip event, not 2048"
        );
        assert!(
            stream.lost_bytes > stream.errors * 2048,
            "lost_bytes must exceed the errors*2048 undercount for AACS units"
        );

        // Crucial anti-desync assertion: the read that bottomed out and was
        // skipped must have been a single 3-sector unit starting at offset 12
        // (the unit boundary at or below the bad sector 13), NOT a 1-sector
        // read at 13. Find a recorded read of (12, 3).
        assert!(
            reads
                .iter()
                .any(|&(lba, count)| lba == 12 && count == ALIGN as u16),
            "expected a unit-aligned (lba=12,count=3) read over the bad unit; got {reads:?}"
        );
        // And NO single-sector read at the bad sector itself (would be a desync).
        assert!(
            !reads.iter().any(|&(lba, count)| lba == bad && count == 1),
            "a 1-sector read at the bad sector {bad} would desync the AACS unit stream"
        );
    }

    /// `unit_align == 1` (DecryptKeys::None) variant: single-sector skips
    /// still work (CSS/raw is self-synchronizing, so a 1-sector skip is
    /// correct there — contrast with the AACS whole-unit skip above).
    #[test]
    fn unencrypted_single_sector_skip_works() {
        const COUNT: u32 = 10;
        let bad = 4u32;
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reader = RecordingReader {
            capacity: COUNT,
            bad_sector: bad,
            log: log.clone(),
        };
        let mut stream = DiscStream::new(
            Box::new(reader),
            synthetic_title(COUNT),
            crate::decrypt::DecryptKeys::None,
            8,
            ContentFormat::BdTs,
        );
        stream.skip_errors = true;
        assert_eq!(stream.unit_align, 1, "None keys must leave unit_align=1");

        let mut guard = 0;
        loop {
            match stream.fill_extents() {
                Ok(true) => {}
                Ok(false) => break,
                Err(e) => panic!("fill_extents errored unexpectedly: {e}"),
            }
            guard += 1;
            assert!(guard < 1000, "fill_extents did not reach EOF");
        }

        let reads = log.lock().unwrap();
        // The bad sector must have been retried down to a single sector and
        // skipped at count==1 — the self-synchronizing per-sector path.
        assert!(
            reads.iter().any(|&(lba, count)| lba == bad && count == 1),
            "align=1 must bottom out at a 1-sector read over the bad sector; got {reads:?}"
        );
        assert!(stream.errors >= 1);
        // align=1: a skip event covers exactly one sector, so lost_bytes
        // and errors*2048 agree (the AACS undercount does not apply here).
        assert_eq!(
            stream.lost_bytes,
            stream.errors * 2048,
            "single-sector (align=1) skip must record exactly 2048 B per event"
        );
    }

    #[test]
    fn halt_via_with_halt_from_arc_observed_by_is_halted() {
        let arc = Arc::new(AtomicBool::new(false));
        let stream = DiscStream::new(
            Box::new(ZeroReader { capacity: 8 }),
            synthetic_title(8),
            crate::decrypt::DecryptKeys::None,
            8,
            crate::disc::ContentFormat::BdTs,
        )
        .with_halt(Halt::from_arc(arc.clone()));
        assert!(!stream.is_halted());
        arc.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(
            stream.is_halted(),
            "with_halt(Halt::from_arc) must observe Arc-side flips"
        );
    }
}
