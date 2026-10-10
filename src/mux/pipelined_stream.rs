//! `PipelinedPesStream` — the read-side of the freemkv mux highway.
//!
//! Given a `crate::mux::demux_thread::DemuxThread` and a set of codec
//! parsers, this struct implements [`crate::pes::PesSource`] by running codec
//! parse on the caller's thread and emitting `PesFrame`s one at a time.
//!
//! ```text
//! Thread A: read + decrypt   (PrefetchedSectorSource)
//! Thread B: M2TS demux       (DemuxThread)
//! Thread C: codec parse      (this struct, on the caller's thread)
//! ```

use super::codec::CodecParser;
use super::demux_thread::{DemuxBatch, DemuxThread};
use super::ts::PesPacket;
use crate::disc::DiscTitle;
use crate::pes::{PesFrame, PesSource};
use crossbeam_channel::Receiver;
use std::io;

/// Stream impl that consumes pre-demuxed `PesPacket` batches from a
/// `DemuxThread` and runs codec parse on the caller's thread.
pub struct PipelinedPesStream {
    title: DiscTitle,
    parsers: Vec<(u16, Box<dyn CodecParser>)>,
    pid_to_track: Vec<(u16, usize)>,

    /// Declared BEFORE `demux_thread`: dropped first, so a worker blocked on a
    /// full channel exits before `DemuxThread::drop` joins it.
    demux_rx: Receiver<DemuxBatch>,
    /// Kept alive so dropping this stream joins the demux + producer
    /// workers deterministically. Never poked directly after spawn.
    #[allow(dead_code)]
    demux_thread: DemuxThread,

    pending_frames: std::collections::VecDeque<PesFrame>,
    eof: bool,
    // The terminal read error (kind, rendered code), repeated by every later read.
    failed: Option<(io::ErrorKind, String)>,
    /// `ctx.diag.skip_parse`: bypass the codec parsers (profiling).
    skip_parse: bool,
    /// Count of dropped DVD navigation packets (private_stream_2, 0xBF). These
    /// are expected on every disc; instead of a per-packet WARN they're tallied
    /// and summarised once at EOF.
    dropped_nav_packets: u64,
    /// DVD: keeps each cell's own VOBUs and joins the VOBs' clocks into one timeline.
    vobu_nav: Option<super::ps::VobuNav>,
    /// Packets of MPEG-2 audio extension streams (`0xD0|n`) with no declared extension track,
    /// reported once at EOF (see `ps::warn_undeclared_extensions`).
    mpeg_extension_packets: [u64; 8],
    dropped_ps: super::ps::DroppedPs,
    /// Per-track (by stream index) B1 drop-to-keyframe gate. After a TS gap on a
    /// video track, drop inter-coded frames until the next IRAP so the muxed
    /// stream stays decode-clean across an upstream concealed loss (P3/B1).
    resync: Vec<super::resync::ResyncGate>,
    /// Per-track "is inter-coded video" flag (only video has cross-frame
    /// references the gate must protect). Indexed by stream index.
    is_video: Vec<bool>,
    /// Per-track access-unit assembler. On the PS path a program-stream video AU
    /// is split across many fixed-size PES fragments; this reassembles them to the
    /// codec's AU boundary so the parser sees AU-complete PES — the same shape the
    /// TS demuxer already delivers via PUSI. Self-framing codecs (MPEG-2, audio)
    /// use passthrough, so every track runs through it uniformly. Indexed by
    /// stream index. (TS titles are AU-complete already, so this is a passthrough
    /// there too — `consume_ts` does not use it.)
    au_asm: Vec<super::au_assembly::AuAssembler>,
    /// Per-track PAFF second-field merge after the assembler (H.264 only, MPG2-7).
    field_merge: Vec<Option<super::au_assembly::SecondFieldMerge>>,
    /// Bounds the header pump's wait for in-band codec configs (AAC).
    header_gate: super::header_gate::HeaderGate,
    /// The op's stop token; a never-cancelled stand-in when there is none.
    halt: crate::halt::Halt,
    /// Damaged AACS units the decrypting reader blanked (reported as loss).
    blanked: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// `mpg://` input: the one video `stream_id` routed; other video ids are left out.
    video_stream_id: Option<u8>,
    /// Packets of a video `stream_id` other than `video_stream_id`, dropped.
    other_video_packets: u64,
    /// Read errors the Read stage skipped past (zero-filled), when it skips.
    read_loss: Option<std::sync::Arc<crate::sector::read_stage::ReadLoss>>,
}

/// The `Codec` of a stream, for configuring its [`AuAssembler`](crate::mux::au_assembly::AuAssembler).
pub(super) fn stream_codec(s: &crate::disc::Stream) -> crate::disc::Codec {
    use crate::disc::Stream;
    match s {
        Stream::Video(v) => v.codec,
        Stream::Audio(a) => a.codec,
        Stream::Subtitle(sub) => sub.codec,
    }
}

impl PipelinedPesStream {
    // Wire up the stream; takes ownership of DemuxThread so cleanup is bounded
    // on drop. pub(crate): external callers go through super::resolve::input /
    // build_iso_pipeline instead.
    pub(crate) fn new(
        demux_thread: DemuxThread,
        demux_rx: Receiver<DemuxBatch>,
        title: DiscTitle,
        parsers: Vec<(u16, Box<dyn CodecParser>)>,
        pid_to_track: Vec<(u16, usize)>,
    ) -> Self {
        let is_video: Vec<bool> = title
            .streams
            .iter()
            .map(|s| matches!(s, crate::disc::Stream::Video(_)))
            .collect();
        let resync = (0..title.streams.len())
            .map(|_| super::resync::ResyncGate::new())
            .collect();
        let au_asm = title
            .streams
            .iter()
            .map(|s| super::au_assembly::AuAssembler::for_codec(stream_codec(s)))
            .collect();
        let field_merge = title
            .streams
            .iter()
            .map(|s| {
                (stream_codec(s) == crate::disc::Codec::H264)
                    .then(super::au_assembly::SecondFieldMerge::default)
            })
            .collect();
        // DVD navigation is the DVD scan's decision (`DvdPs`): an HD DVD `.evo` has its own
        // pack layout, and an `mpg://` file is one extent, not a cell per extent.
        let vobu_nav = (title.content_format == crate::disc::ContentFormat::DvdPs
            && !title.extents.is_empty())
        .then(|| super::ps::VobuNav::new(&title.extents));
        Self {
            title,
            parsers,
            pid_to_track,
            demux_rx,
            demux_thread,
            pending_frames: std::collections::VecDeque::new(),
            eof: false,
            failed: None,
            skip_parse: false,
            dropped_nav_packets: 0,
            vobu_nav,
            mpeg_extension_packets: [0; 8],
            dropped_ps: Default::default(),
            resync,
            is_video,
            au_asm,
            field_merge,
            header_gate: super::header_gate::HeaderGate::default(),
            halt: crate::halt::Halt::new(),
            blanked: std::sync::Arc::default(),
            read_loss: None,
            video_stream_id: None,
            other_video_packets: 0,
        }
    }

    // `mpg://`: route only video `stream_id` `id` (design §4: one video track).
    pub(crate) fn with_video_stream_id(mut self, id: Option<u8>) -> Self {
        self.video_stream_id = id;
        self
    }

    // The decrypting reader's blanked-unit counter, reported as `errors` / `lost_bytes`.
    pub(crate) fn with_blanked(
        mut self,
        blanked: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        self.blanked = blanked;
        self
    }

    // The read loss the Read stage counts (a live drive's skipped units).
    pub(crate) fn with_read_loss(
        mut self,
        loss: std::sync::Arc<crate::sector::read_stage::ReadLoss>,
    ) -> Self {
        self.read_loss = Some(loss);
        self
    }

    // The run: its halt ends a blocked `read` with `Halted` (LP11), its stats count
    // resync drops, its diagnostics apply.
    pub(crate) fn with_ctx(mut self, ctx: &crate::ctx::Ctx) -> Self {
        self.halt = ctx.halt.clone();
        self.skip_parse = ctx.diag.skip_parse;
        for gate in &mut self.resync {
            *gate = super::resync::ResyncGate::counted(ctx.stats.clone());
        }
        self
    }

    // Pull one batch, parse it, enqueue frames on pending_frames. Ok(true) =
    // success, Ok(false) = clean EOF, Err = demuxer error.
    fn pump_one_batch(&mut self) -> io::Result<bool> {
        let batch = match self
            .halt
            .recv_timeout(&self.demux_rx, std::time::Duration::MAX)
        {
            Ok(crate::halt::Recv::Item(b)) => Ok(b),
            Ok(_) => Err(()),
            Err(halted) => return Err(halted.into()),
        };
        match batch {
            Ok(DemuxBatch::Ts(packets)) => {
                self.consume_ts(packets);
                Ok(true)
            }
            Ok(DemuxBatch::Ps(packets)) => {
                self.consume_ps(packets);
                Ok(true)
            }
            Ok(DemuxBatch::Err(e)) => Err(e),
            // Explicit clean-completion sentinel from the demux worker; a Stop that
            // raced it is still `Halted`, never a truncated clean end (LP11).
            Ok(DemuxBatch::Eof) if self.halt.is_cancelled() => {
                Err(crate::error::Error::Halted.into())
            }
            Ok(DemuxBatch::Eof) => Ok(false),
            // Channel disconnected WITHOUT an `Eof`/`Err` sentinel — the worker
            // panicked or was dropped mid-stream. Surface as an error so a parser/demux
            // panic is never reported as a clean end-of-stream (silent truncation).
            Err(_) => Err(crate::error::Error::DemuxThreadPanicked.into()),
        }
    }

    fn consume_ts(&mut self, packets: Vec<PesPacket>) {
        let skip_parse = self.skip_parse;
        for pes in packets {
            if let Some((_, track)) = self
                .pid_to_track
                .iter()
                .find(|(pid, _)| *pid == pes.pid)
                .copied()
            {
                if skip_parse {
                    // Profiling escape hatch — bypass codec parser.
                    self.pending_frames.push_back(PesFrame {
                        discard_padding_ns: 0,
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
                    let is_video = self.is_video.get(track).copied().unwrap_or(false);
                    for frame in parser.parse(&pes) {
                        // B1: after a concealed/lost gap, drop forward to the next video
                        // keyframe so no dangling-reference frame is emitted. Read PER-FRAME
                        // (post-gap picture); audio/subtitle admit, no-gate track emits as-is.
                        let emit = match self.resync.get_mut(track) {
                            Some(gate) => {
                                let was_armed = gate.is_armed();
                                let dropped = gate.dropped_in_run();
                                let admit =
                                    gate.admit(is_video, frame.discontinuity, frame.keyframe);
                                if admit && was_armed {
                                    tracing::warn!(
                                        target: "mux",
                                        track,
                                        pid = pes.pid,
                                        dropped,
                                        "B1: resynced at keyframe after concealed gap"
                                    );
                                }
                                admit
                            }
                            None => true,
                        };
                        if emit {
                            self.pending_frames
                                .push_back(PesFrame::from_codec_frame(track, frame));
                        }
                    }
                }
            }
        }
    }

    fn consume_ps(&mut self, packets: Vec<super::ps::PsPacket>) {
        for mut ps in packets {
            if let Some(nav) = self.vobu_nav.as_mut()
                && !nav.admit(&mut ps)
            {
                continue;
            }
            if let Some(id) = self.video_stream_id
                && (0xE0..=0xEF).contains(&ps.stream_id)
                && ps.stream_id != id
            {
                if self.other_video_packets == 0 {
                    tracing::warn!(target: "mux", stream_id = ps.stream_id, "mpg: a second video stream is left out");
                }
                self.other_video_packets += 1;
                continue;
            }
            // Route by the REAL DVD PID (matching `scan_dvd_titles`) not a synthetic
            // track index. The old `(sub_id & 0x1F) + 1` heuristic collided subtitle
            // sub-id 0x20+j with audio track j+1, feeding VobSub PES into the AC-3 parser.
            let Some(pid) = ps.dvd_pid() else {
                if ps.is_nav() {
                    // Expected DVD navigation packet (PCI/DSI) — tally, no WARN.
                    self.dropped_nav_packets += 1;
                } else {
                    // Unexpected unmappable stream_id: warned once, then counted.
                    self.dropped_ps.drop_packet(&ps, None);
                }
                continue;
            };
            let Some((_, track)) = self.pid_to_track.iter().find(|(p, _)| *p == pid).copied()
            else {
                if let Some(base) = super::ps::dvd_mpeg_audio_extension_base(pid) {
                    // Counted and reported once at EOF: no extension track was declared.
                    self.mpeg_extension_packets[(base & 0x07) as usize] += 1;
                    continue;
                }
                self.dropped_ps.drop_packet(&ps, Some(pid));
                continue;
            };
            // Carry the PS demuxer's byte-exact source stamp through to the codec parser
            // as the TS path does — provenance must survive the PsPacket → PesPacket seam
            // so the frame's `source` reaches the mux/index (FVI `src`), never rebuilt.
            let (pts_i64, dts_i64, src) = (
                ps.pts.map(|p| p as i64),
                ps.dts.map(|d| d as i64),
                ps.source,
            );
            // Reassemble PS fragments into AU-complete PES (passthrough for self-framing
            // MPEG-2/audio) so the parser sees the same shape a TS delivers; AU-start PTS/
            // source survive, no assembler → passthrough. (PS: no AACS conceal, no gap.)
            let pkts: Vec<PesPacket> = match self.au_asm.get_mut(track) {
                Some(asm) => asm
                    .push_owned(ps.data, pts_i64, dts_i64, src, false)
                    .into_iter()
                    .map(|au| PesPacket {
                        source: au.source,
                        pid,
                        pts: au.pts,
                        dts: au.dts,
                        data: au.data,
                        discontinuity: au.discontinuity,
                    })
                    .collect(),
                None => vec![PesPacket {
                    source: src,
                    pid,
                    pts: pts_i64,
                    dts: dts_i64,
                    data: ps.data,
                    discontinuity: false,
                }],
            };
            let pkts = match self.field_merge.get_mut(track).and_then(Option::as_mut) {
                Some(m) => pkts.into_iter().flat_map(|p| m.push(p)).collect(),
                None => pkts,
            };
            for pes in &pkts {
                if let Some((_, parser)) = self.parsers.iter_mut().find(|(p, _)| *p == pid) {
                    for frame in parser.parse(pes) {
                        self.pending_frames
                            .push_back(PesFrame::from_codec_frame(track, frame));
                    }
                }
            }
        }
    }
}

impl PipelinedPesStream {
    fn read_frame(&mut self) -> io::Result<Option<PesFrame>> {
        if let Some(frame) = self.pending_frames.pop_front() {
            return Ok(Some(frame));
        }
        if self.eof {
            return Ok(None);
        }
        loop {
            match self.pump_one_batch()? {
                true => {
                    if let Some(frame) = self.pending_frames.pop_front() {
                        return Ok(Some(frame));
                    }
                    // Batch contained no trackable packets — pull again.
                }
                false => {
                    self.eof = true;
                    if self.dropped_nav_packets > 0 {
                        tracing::debug!(
                            target: "mux",
                            "dropped {} DVD navigation packets (private_stream_2/0xBF) — expected, carry no elementary stream",
                            self.dropped_nav_packets
                        );
                    }
                    if let Some(n) = self
                        .vobu_nav
                        .as_ref()
                        .map(|g| g.dropped_vobus)
                        .filter(|&n| n > 0)
                    {
                        tracing::info!(target: "mux", vobus = n, "left out VOBUs interleaved from another program");
                    }
                    super::ps::warn_undeclared_extensions(&self.mpeg_extension_packets);
                    self.dropped_ps.report();
                    // Drain any AU a parser buffered past the last PES (DTS-HD tail, MPEG-2
                    // final GOP), routing through the SAME B1 gate — flush frames carry their
                    // own `discontinuity`, so a trailing dangling-ref frame must not bypass it.
                    let pid_to_track = &self.pid_to_track;
                    let pending = &mut self.pending_frames;
                    let resync = &mut self.resync;
                    let is_video = &self.is_video;
                    let au_asm = &mut self.au_asm;
                    let field_merge = &mut self.field_merge;
                    for (pid, parser) in self.parsers.iter_mut() {
                        let Some(&(_, track)) = pid_to_track.iter().find(|(p, _)| p == pid) else {
                            continue;
                        };
                        // First the trailing AU(s) the PS assembler buffered past the
                        // final fragment (last AU has no following boundary); THEN drain
                        // the parser's own buffer (MPEG-2 final GOP, DTS-HD tail).
                        let mut frames = Vec::new();
                        let tail = au_asm.get_mut(track).map(|a| a.flush()).unwrap_or_default();
                        let mut tail: Vec<PesPacket> = tail
                            .into_iter()
                            .map(|au| PesPacket {
                                source: au.source,
                                pid: *pid,
                                pts: au.pts,
                                dts: au.dts,
                                data: au.data,
                                discontinuity: au.discontinuity,
                            })
                            .collect();
                        if let Some(m) = field_merge.get_mut(track).and_then(Option::as_mut) {
                            tail = tail.into_iter().flat_map(|p| m.push(p)).collect();
                            tail.extend(m.flush());
                        }
                        for pes in &tail {
                            frames.extend(parser.parse(pes));
                        }
                        frames.extend(parser.flush());
                        for frame in frames {
                            let emit = match resync.get_mut(track) {
                                Some(gate) => gate.admit(
                                    is_video.get(track).copied().unwrap_or(false),
                                    frame.discontinuity,
                                    frame.keyframe,
                                ),
                                None => true,
                            };
                            if emit {
                                pending.push_back(PesFrame::from_codec_frame(track, frame));
                            }
                        }
                    }
                    // A gate still armed at EOF dropped post-gap frames that never
                    // reached a keyframe (e.g. a concealed gap in the final GOP).
                    // Surface it once so the loss is visible, not silent.
                    for (track, gate) in self.resync.iter().enumerate() {
                        if gate.is_armed() {
                            tracing::warn!(
                                target: "mux",
                                track,
                                dropped = gate.dropped_in_run(),
                                "B1: stream ended while dropping to a keyframe after a concealed gap (no trailing keyframe)"
                            );
                        }
                    }
                    return Ok(self.pending_frames.pop_front());
                }
            }
        }
    }
}

impl PesSource for PipelinedPesStream {
    fn read(&mut self) -> io::Result<Option<PesFrame>> {
        if let Some((kind, code)) = &self.failed {
            return Err(io::Error::new(*kind, code.clone()));
        }
        let read = self.read_frame();
        match &read {
            Ok(Some(frame)) => self.header_gate.observe(frame),
            Ok(None) => self.header_gate.expire(),
            Err(e) => self.failed = Some((e.kind(), e.to_string())),
        }
        read
    }

    fn info(&self) -> &DiscTitle {
        &self.title
    }

    fn errors(&self) -> u64 {
        // Read errors skipped past, blanked units, and frames the B1 gates dropped.
        let dropped: u64 = self.resync.iter().map(|g| g.dropped_total()).sum();
        let skips = self.read_loss.as_ref().map_or(0, |l| l.skips());
        skips + self.blanked.load(std::sync::atomic::Ordering::Relaxed) + dropped
    }

    fn lost_bytes(&self) -> u64 {
        // Zero-filled read errors, plus each blanked unit: one whole aligned unit of zeros
        // (KS-2: 6144 bytes).
        let blanked = self.blanked.load(std::sync::atomic::Ordering::Relaxed);
        let skipped = self.read_loss.as_ref().map_or(0, |l| l.bytes());
        skipped + blanked * crate::aacs::content::ALIGNED_UNIT_LEN as u64
    }

    fn config_changes(&self) -> Vec<(usize, u64)> {
        self.pid_to_track
            .iter()
            .filter_map(|&(pid, track)| {
                let (_, parser) = self.parsers.iter().find(|(p, _)| *p == pid)?;
                let n = parser.config_changes();
                (n > 0).then_some((track, n))
            })
            .collect()
    }

    fn headers_ready(&self) -> bool {
        // Match DiscStream semantics: video tracks need codec_private before the
        // consumer can write the container header. `Diag::skip_parse` forces ready
        // (no parser populates codec_private in that mode).
        if self.skip_parse {
            return true;
        }
        self.header_gate
            .ready(&self.title, |idx| self.codec_private(idx))
    }

    fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
        let pid = self
            .pid_to_track
            .iter()
            .find(|(_, idx)| *idx == track)
            .map(|(p, _)| *p)?;
        self.parsers
            .iter()
            .find(|(p, _)| *p == pid)
            .and_then(|(_, parser)| parser.codec_private())
            // FLAC/Opus ES carry no init data: keep what the source header supplied.
            .or_else(|| self.title.codec_privates.get(track).cloned().flatten())
    }
}

#[cfg(test)]
#[path = "pipelined_stream_tests.rs"]
mod tests;
