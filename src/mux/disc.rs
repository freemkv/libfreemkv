//! DiscStream — read any disc (physical drive or ISO file) → PES frames.
//!
//! One stream type for all disc sources. The source is a SectorReader —
//! Drive (hardware) or IsoSectorReader (file). DiscStream doesn't care.
//!
//! Read-only. For disc→ISO (raw sector copy), use `Disc::copy()`.

use crate::disc::{Disc, DiscTitle, Extent};
use crate::event::{BatchSizeReason, Event, EventKind};
use crate::sector::SectorReader;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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
    reader: Box<dyn SectorReader>,
    title: DiscTitle,
    disc: Option<Disc>,
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
    /// When set and the flag is raised, fill_extents returns Err(Halted) at the
    /// next retry boundary. Unlike skip_errors, this propagates the error up so
    /// the rip terminates cleanly. Share the Arc with Drive::halt_flag() to get
    /// unified Stop behavior across drive reads and sector processing.
    halt: Option<Arc<AtomicBool>>,
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
        let bytes_total_extents: u64 =
            extents.iter().map(|e| e.sector_count as u64 * 2048).sum();

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
            reader,
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

    /// Share a halt flag — typically from `Drive::halt_flag()`. When raised,
    /// the next read-retry boundary inside fill_extents returns Err(Halted)
    /// instead of continuing. Required for Stop to work during dense bad-sector
    /// regions (where read() loops internally waiting for enough clean data to
    /// emit a frame and would otherwise never check an external stop signal).
    pub fn set_halt(&mut self, flag: Arc<AtomicBool>) {
        self.halt = Some(flag);
    }

    fn is_halted(&self) -> bool {
        self.halt
            .as_ref()
            .map(|h| h.load(Ordering::Relaxed))
            .unwrap_or(false)
    }

    fn emit(&self, kind: EventKind) {
        if let Some(ref f) = self.event_fn {
            f(Event { kind });
        }
    }

    /// Skip decryption — return raw encrypted bytes.
    pub fn set_raw(&mut self) {
        self.decrypt_keys = crate::decrypt::DecryptKeys::None;
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
        loop {
            if self.is_halted() {
                return Err(crate::error::Error::Halted.into());
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
                    return Err(crate::error::Error::DiscRead { sector: lba as u64 }.into());
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
            if let Err(e) =
                crate::decrypt::decrypt_sectors(&mut self.read_buf[..bytes], &self.decrypt_keys, 0)
            {
                return Err(e.into());
            }

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
