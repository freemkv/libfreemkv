//! DiscStream — read any disc (physical drive or ISO file) → PES frames.
//!
//! One stream type for all disc sources. The source is a SectorReader —
//! Drive (hardware) or IsoSectorReader (file). DiscStream doesn't care.
//!
//! Read-only. For disc→ISO (raw sector copy), use `Disc::copy()`.

use crate::disc::{Disc, DiscTitle, Extent};
use crate::drive::extract_scsi_context;
use crate::event::{BatchSizeReason, Event, EventKind};
use crate::halt::Halt;
use crate::sector::{DecryptingSectorSource, SectorReader, SectorSource};
use std::io;
use std::sync::Arc;
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
/// Sources: physical drive, ISO file, or any SectorReader.
/// Decrypt, demux, and codec parsing happen internally.
pub struct DiscStream {
    /// Underlying sector source wrapped in the 0.18
    /// [`DecryptingSectorSource`] decorator. Every `read_sectors`
    /// call yields plaintext, so `fill_extents` no longer needs an
    /// inline `decrypt::decrypt_sectors` step. `DecryptKeys::None`
    /// (raw / unencrypted disc) makes the decorator a pass-through.
    reader: DecryptingSectorSource<Box<dyn SectorReader>>,
    title: DiscTitle,
    disc: Option<Disc>,
    /// Mirror of the keys handed in at construction. The decorator
    /// owns the cryptographic state; this field is kept for
    /// metadata-side callers (`info()` and friends) that want to
    /// know whether the disc was encrypted, without reaching through
    /// the wrapper.
    decrypt_keys: crate::decrypt::DecryptKeys,

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
    pub skip_errors: bool,
    /// When set and the token is cancelled, fill_extents returns Err(Halted)
    /// at the next retry boundary. Unlike skip_errors, this propagates the
    /// error up so the rip terminates cleanly. Construct with
    /// [`DiscStream::with_halt`] (preferred) or set post-hoc via the
    /// deprecated [`DiscStream::set_halt`] bridge — both populate this same
    /// field and either entry point yields one source of truth.
    halt: Option<Halt>,
    event_fn: Option<Box<dyn Fn(Event) + Send>>,
    eof: bool,

    // Cumulative bytes successfully read from the source. Drives
    // EventKind::BytesRead emission and autorip's per-device progress.
    bytes_read_total: u64,
    // Pre-computed total of all extents in bytes (or 0 if extents are
    // empty). Carried in EventKind::BytesRead.total so consumers can show
    // a percent without a separate API call.
    bytes_total_extents: u64,

    // PES output
    ts_demuxer: Option<super::ts::TsDemuxer>,
    ps_demuxer: Option<super::ps::PsDemuxer>,
    parsers: Vec<(u16, Box<dyn super::codec::CodecParser>)>,
    pending_frames: std::collections::VecDeque<crate::pes::PesFrame>,
    pid_to_track: Vec<(u16, usize)>,
}

impl DiscStream {
    /// Create a disc stream from any sector reader.
    ///
    /// Works with physical drives and ISO files — both implement SectorReader.
    /// The caller opens the source, scans for titles/keys, and passes them in.
    /// The stream handles demuxing, decryption, and codec parsing internally.
    pub fn new(
        reader: Box<dyn SectorReader>,
        title: DiscTitle,
        decrypt_keys: crate::decrypt::DecryptKeys,
        batch_sectors: u16,
        content_format: crate::disc::ContentFormat,
    ) -> Self {
        let extents = title.extents.clone();
        let bytes_total_extents: u64 = extents.iter().map(|e| e.sector_count as u64 * 2048).sum();

        // Debug log reader type at construction — critical for diagnosing mux reading from drive instead of ISO
        tracing::debug!(
            target: "mux",
            "DiscStream constructed with reader type: {}",
            std::any::type_name::<dyn SectorReader>()
        );

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
            parsers.push((pid, super::codec::parser_for_codec(codec, None)));
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

        Self {
            // Wrap the input reader in DecryptingSectorSource so the
            // internal fill_extents path sees plaintext bytes. For
            // DecryptKeys::None (unencrypted / raw / test fixtures)
            // the decorator is a pass-through.
            reader: DecryptingSectorSource::new(reader, decrypt_keys.clone()),
            title,
            disc: None,
            decrypt_keys,
            extents,
            current_extent: 0,
            current_offset: 0,
            read_buf: Vec::with_capacity(batch_sectors as usize * 2048),
            buf_valid: 0,
            adaptive: AdaptiveBatch::new(batch_sectors),
            errors: 0,
            skip_errors: false,
            halt: None,
            event_fn: None,
            eof: false,
            bytes_read_total: 0,
            bytes_total_extents,
            ts_demuxer,
            ps_demuxer,
            parsers,
            pending_frames: std::collections::VecDeque::new(),
            pid_to_track,
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
    /// Preferred over the post-hoc [`DiscStream::set_halt`] bridge —
    /// pass the same `Halt` clone you hand to sweep / patch / mux so
    /// every phase observes a single Stop signal.
    pub fn with_halt(mut self, halt: Halt) -> Self {
        self.halt = Some(halt);
        self
    }

    /// Bridge for callers that haven't migrated to the
    /// [`DiscStream::with_halt`] constructor-time path yet. Wraps the
    /// supplied `Arc<AtomicBool>` as a [`Halt`] (`Halt::from_arc`) and
    /// stores it in the same internal slot, so a halt installed via
    /// either entry point goes through one halt-check inside
    /// `fill_extents`. Calling `set_halt` after `with_halt` (or vice
    /// versa) replaces the previous token with the new one.
    #[deprecated(
        since = "0.18.0",
        note = "use `DiscStream::with_halt(Halt)` at construction instead"
    )]
    pub fn set_halt(&mut self, flag: Arc<AtomicBool>) {
        self.halt = Some(Halt::from_arc(flag));
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

    /// Get the scanned Disc (for listing all titles).
    pub fn disc(&self) -> Option<&Disc> {
        self.disc.as_ref()
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

        let lba = ext_start + self.current_offset;

        // Adaptive sizer: start at current (preferred until a failure), shrink
        // on failure, advance on success. One 5s read attempt per try — no
        // retry loops, no sleeps. On size-1 failure, skip or error.
        //
      // Halt is checked at the top of every iteration — in a dense bad zone
        // this loop can spend minutes shrinking and skipping sectors; without
        // the check, Stop wouldn't take effect until the outer PES read() loop
        // finally emits a frame, which may never happen.
        
        let start_lba = lba;
        let start_time = std::time::Instant::now();
        
        loop {
            if self.is_halted() {
                return Err(crate::error::Error::Halted.into());
            }

            // Debug: log slow reads during mux — helps diagnose stalls
            if cfg!(debug_assertions) && start_time.elapsed().as_secs() > 5 {
                tracing::debug!(target: "mux", "fill_extents waiting at LBA {} ({}s elapsed, sectors={})", lba, start_time.elapsed().as_secs(), remaining);
            }

            let mut sectors = remaining.min(self.adaptive.current() as u32) as u16;
            // Align to 3-sector AACS units when possible. Partial units at
            // extent boundaries are safely handled by decrypt_sectors().
            if sectors >= 3 {
                sectors -= sectors % 3;
            }
            let bytes = sectors as usize * 2048;
            self.read_buf.resize(bytes, 0);

            let ok = self
                .reader
                .read_sectors(lba, sectors, &mut self.read_buf[..bytes], false)
                .is_ok();

            if ok {
                if let Some(ev) = self.adaptive.on_success(sectors) {
                    self.emit(ev);
                }
                self.buf_valid = bytes;
                self.current_offset += sectors as u32;
                self.bytes_read_total = self.bytes_read_total.saturating_add(bytes as u64);
                self.emit(EventKind::BytesRead {
                    bytes: self.bytes_read_total,
                    total: self.bytes_total_extents,
                });
                break;
            }

            if sectors == 1 {
                // Bottomed out. Skip this sector or bail.
                if self.skip_errors {
                    self.read_buf.resize(2048, 0);
                    self.read_buf[..2048].fill(0);
                    self.buf_valid = 2048;
                    self.errors += 1;
                    self.emit(EventKind::SectorSkipped { sector: lba as u64 });
                    self.current_offset += 1;
                    break;
                } else {
                    let err = self
                        .reader
                        .read_sectors(lba, sectors, &mut self.read_buf[..2048], false)
                        .err();
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

#[allow(deprecated)] // 0.18 trait split: migrate to FrameSource/FrameSink in follow-up commit.
impl crate::pes::Stream for DiscStream {
    fn read(&mut self) -> io::Result<Option<crate::pes::PesFrame>> {
        if let Some(frame) = self.pending_frames.pop_front() {
            return Ok(Some(frame));
        }

        if self.eof {
            return Ok(None);
        }

        loop {
            if !self.fill_extents()? {
                self.eof = true;
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
                        let track = match ps.stream_id {
                            0xE0..=0xEF => 0,
                            0xC0..=0xDF => 1,
                            0xBD => ps
                                .sub_stream_id
                                .map(|s| (s & 0x1F) as usize + 1)
                                .unwrap_or(1),
                            _ => continue,
                        };
                        if track >= self.title.streams.len() {
                            continue;
                        }
                        let pid = self
                            .pid_to_track
                            .iter()
                            .find(|(_, idx)| *idx == track)
                            .map(|(p, _)| *p)
                            .unwrap_or(0);
                        let pes = super::ts::PesPacket {
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
                return Ok(self.pending_frames.pop_front());
            }

            let bytes = self.buf_valid;
            // Plaintext: the wrapped reader (DecryptingSectorSource)
            // applied AACS / CSS in-place during fill_extents'
            // read_sectors call. The pre-0.18 inline decrypt step
            // lived here.

            if let Some(ref mut demuxer) = self.ts_demuxer {
                let packets = demuxer.feed(&self.read_buf[..bytes]);
                for pes in &packets {
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
            } else if let Some(ref mut demuxer) = self.ps_demuxer {
                let packets = demuxer.feed(&self.read_buf[..bytes]);
                for ps in &packets {
                    let track = match ps.stream_id {
                        0xE0..=0xEF => 0,
                        0xC0..=0xDF => 1,
                        0xBD => ps
                            .sub_stream_id
                            .map(|s| (s & 0x1F) as usize + 1)
                            .unwrap_or(1),
                        _ => continue,
                    };
                    if track >= self.title.streams.len() {
                        continue;
                    }

                    // Convert PsPacket to PesPacket for codec parser (same as BD-TS path)
                    let pid = self
                        .pid_to_track
                        .iter()
                        .find(|(_, idx)| *idx == track)
                        .map(|(p, _)| *p)
                        .unwrap_or(0);

                    let pes = super::ts::PesPacket {
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
        for (idx, s) in self.title.streams.iter().enumerate() {
            if let crate::disc::Stream::Video(v) = s {
                if !v.secondary && self.codec_private(idx).is_none() {
                    return false;
                }
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    //! `DiscStream` is the only meaningful `FrameSource` impl in tree (every
    //! other concrete `pes::Stream` impl in `mux/*` is a sink). The 0.18
    //! round-1 blanket `impl<T: pes::Stream + Send> pes::FrameSource for T`
    //! covers `DiscStream` for free as long as it is `Send`. These tests
    //! lock that down: a static `Send` assertion plus a `Box<dyn FrameSource>`
    //! round trip exercising every `FrameSource` method through the trait
    //! object, so future Send-breaking edits to `DiscStream`'s interior
    //! types fail at compile time and the trait-bridge dispatch is verified
    //! at runtime.
    #![allow(deprecated)] // exercising the 0.18 deprecation-window blanket bridge.
    use super::*;
    use crate::disc::{ContentFormat, DiscTitle};
    use crate::pes::FrameSource;

    /// Static-assert `DiscStream: Send`. The blanket
    /// `impl<T: pes::Stream + Send> pes::FrameSource for T` only fires for
    /// `Send` types — if a future field on `DiscStream` is non-`Send` (e.g.
    /// a `Box<dyn Read>` instead of `Box<dyn SectorReader>`), this fails
    /// at compile time, before the runtime trait-object test below.
    fn _assert_disc_stream_is_send() {
        fn requires_send<T: Send>() {}
        requires_send::<DiscStream>();
    }

    /// Trivial `SectorReader` that yields zeroed sectors. Empty title means
    /// the demuxer produces no PES frames, so `read()` walks the extents to
    /// EOF and returns `Ok(None)`. That's enough to exercise the trait-object
    /// dispatch — the goal here is the bridge, not the demuxer.
    struct ZeroReader {
        capacity: u32,
    }

    impl crate::sector::SectorReader for ZeroReader {
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

        fn capacity(&self) -> u32 {
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

    /// Smallest credible witness that `DiscStream` flows through the
    /// `FrameSource` blanket impl: build a `Box<dyn FrameSource>`, drive
    /// `read()` to EOF, exercise `info()` / `headers_ready()` /
    /// `codec_private()` through the trait object. The trait-bridge
    /// correctness is what's being verified — not demuxer behaviour.
    #[test]
    fn frame_source_via_dyn_object() {
        let reader = ZeroReader { capacity: 8 };
        let title = synthetic_title(8);
        let stream = DiscStream::new(
            Box::new(reader),
            title,
            crate::decrypt::DecryptKeys::None,
            8,
            ContentFormat::BdTs,
        );

        let mut src: Box<dyn FrameSource> = Box::new(stream);

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

    /// `is_halted()` must observe a cancellation signal regardless of
    /// which entry point installed the token. The deprecated
    /// `set_halt(Arc<AtomicBool>)` and the new `with_halt(Halt)` are
    /// two views over one slot — flipping either bit must cause the
    /// next `fill_extents` retry boundary to bail.
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

    #[test]
    fn halt_via_set_halt_bridge_observed_by_is_halted() {
        let arc = Arc::new(AtomicBool::new(false));
        let mut stream = DiscStream::new(
            Box::new(ZeroReader { capacity: 8 }),
            synthetic_title(8),
            crate::decrypt::DecryptKeys::None,
            8,
            crate::disc::ContentFormat::BdTs,
        );
        stream.set_halt(arc.clone());
        assert!(!stream.is_halted());
        arc.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(
            stream.is_halted(),
            "set_halt(Arc<AtomicBool>) bridge must observe Arc-side flips"
        );
    }
}
