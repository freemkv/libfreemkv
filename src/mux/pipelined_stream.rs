//! `PipelinedPesStream` — the read-side of the freemkv mux
//! highway.
//!
//! Given a [`crate::mux::demux_thread::DemuxThread`] (which has the
//! producer + demux workers already spawned), a set of codec
//! parsers, and the title metadata, this struct implements
//! [`crate::pes::Stream`] by running codec parse on the caller's
//! thread and emitting `PesFrame`s one at a time.
//!
//! The pipeline runs three threads in parallel:
//!
//! ```text
//! Thread A: read + decrypt   (PrefetchedSectorSource / BytePrefetcher)
//! Thread B: M2TS demux       (DemuxThread)
//! Thread C: codec parse      (this struct, on the caller's thread)
//! ```
//!
//! Communication between A→B and B→C is via bounded channels with
//! recycled buffer pools — no allocations or memcpys in the steady-
//! state hot loop.
//!
//! This is the *only* read-side `Stream` impl in tree. Both the ISO
//! file mux and the BD-TS (`m2ts://`) file mux input paths are built by
//! [`crate::mux::resolve`] (`build_iso_pipeline` / the m2ts pipeline
//! builder) and hand back a `PipelinedPesStream`; the differences are
//! in how the producer thread (A) is configured — sector-aligned reads
//! with AACS decrypt for ISO, raw byte reads for M2TS.
//! ([`crate::mux::M2tsStream`] itself is a write-only sink and does not
//! construct this type.)

use super::codec::CodecParser;
use super::demux_thread::{DemuxBatch, DemuxThread};
use super::ts::PesPacket;
use crate::disc::DiscTitle;
use crate::pes::{PesFrame, Stream};
use crossbeam_channel::Receiver;
use std::io;

/// Stream impl that consumes pre-demuxed `PesPacket` batches from a
/// [`DemuxThread`] and runs codec parse on the caller's thread.
pub struct PipelinedPesStream {
    title: DiscTitle,
    parsers: Vec<(u16, Box<dyn CodecParser>)>,
    pid_to_track: Vec<(u16, usize)>,

    demux_rx: Receiver<DemuxBatch>,
    /// Kept alive so dropping this stream joins the demux + producer
    /// workers deterministically. Never poked directly after spawn.
    #[allow(dead_code)]
    demux_thread: DemuxThread,

    pending_frames: std::collections::VecDeque<PesFrame>,
    eof: bool,
    /// Cached `FREEMKV_SKIP_PARSE` profiling flag. Read once in `new()`
    /// — the env var cannot change for the life of the stream, and
    /// `std::env::var_os` takes a process-wide lock, so the per-batch /
    /// per-poll reads it replaces were needless hot-path overhead.
    skip_parse: bool,
    /// Count of dropped DVD navigation packets (private_stream_2, 0xBF). These
    /// are expected on every disc; instead of a per-packet WARN they're tallied
    /// and summarised once at EOF.
    dropped_nav_packets: u64,
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
}

/// The `Codec` of a stream, for configuring its [`AuAssembler`].
fn stream_codec(s: &crate::disc::Stream) -> crate::disc::Codec {
    use crate::disc::Stream;
    match s {
        Stream::Video(v) => v.codec,
        Stream::Audio(a) => a.codec,
        Stream::Subtitle(sub) => sub.codec,
    }
}

impl PipelinedPesStream {
    /// Wire up the stream. Caller has already spawned the
    /// `DemuxThread` (which in turn owns the producer); we take the
    /// receiver end + the join handle bundle so cleanup is bounded
    /// on drop.
    ///
    /// `pub(crate)`: the signature takes the internal `DemuxThread` /
    /// `DemuxBatch` / `CodecParser` types, so external callers reach this
    /// stream via [`super::resolve::input`] / `build_iso_pipeline`
    /// instead.
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
        Self {
            title,
            parsers,
            pid_to_track,
            demux_rx,
            demux_thread,
            pending_frames: std::collections::VecDeque::new(),
            eof: false,
            skip_parse: std::env::var_os("FREEMKV_SKIP_PARSE").is_some(),
            dropped_nav_packets: 0,
            resync,
            is_video,
            au_asm,
        }
    }

    /// Pull one batch of `PesPacket`s from the demux thread, run
    /// codec parse on each, enqueue resulting `PesFrame`s on
    /// `pending_frames`. Returns Ok(true) on success, Ok(false) on
    /// EOF (channel closed cleanly), Err on demuxer error.
    fn pump_one_batch(&mut self) -> io::Result<bool> {
        match self.demux_rx.recv() {
            Ok(DemuxBatch::Ts(packets)) => {
                self.consume_ts(packets);
                Ok(true)
            }
            Ok(DemuxBatch::Ps(packets)) => {
                self.consume_ps(packets);
                Ok(true)
            }
            Ok(DemuxBatch::Err(e)) => Err(e),
            // Explicit clean-completion sentinel from the demux worker.
            Ok(DemuxBatch::Eof) => Ok(false),
            // The channel disconnected WITHOUT the worker first sending
            // an `Eof` (or `Err`) sentinel — the worker panicked or was
            // dropped mid-stream. Surface this as an error so a parser /
            // demux panic is never reported to the caller as a clean
            // end-of-stream (which would silently truncate output).
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
                        // B1: after a concealed/lost gap, drop forward to the next
                        // keyframe on a video track so no frame with a dangling
                        // reference is emitted. The signal is read PER-FRAME
                        // (`frame.discontinuity`), not per-PES: buffering parsers
                        // (MPEG-2 GOPs, H.264/HEVC AU lag) stamp the exact post-gap
                        // picture, so only it arms the gate — not a whole PES of
                        // frames. Audio/subtitle always admit (independent frames);
                        // a track with no gate (out-of-range index) emits as-is.
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
        for ps in packets {
            // Route by the REAL DVD PID (matching the PIDs that
            // `scan_dvd_titles` assigns) rather than a synthetic track
            // index. The old `(sub_id & 0x1F) + 1` heuristic collided
            // subtitle sub-id 0x20+j with audio track j+1, feeding
            // VobSub PES into the AC-3 parser.
            let Some(pid) = ps.dvd_pid() else {
                if ps.is_nav() {
                    // Expected DVD navigation packet (PCI/DSI) — tally, no WARN.
                    self.dropped_nav_packets += 1;
                } else {
                    // Unexpected unmappable stream_id — a possibly-dropped real
                    // stream. Keep the individual WARN: its repetition is signal.
                    tracing::warn!(
                        target: "mux",
                        "dropping unmappable PS packet (stream_id={:#04x}, sub_stream_id={:?})",
                        ps.stream_id,
                        ps.sub_stream_id,
                    );
                }
                continue;
            };
            let Some((_, track)) = self.pid_to_track.iter().find(|(p, _)| *p == pid).copied()
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
            // Carry the PS demuxer's byte-exact source stamp through to the codec
            // parser, exactly as the TS path does — provenance must survive the
            // PsPacket → PesPacket seam so the frame's `source` reaches the
            // mux/index (FVI `src`), never reconstructed.
            let (pts_i64, dts_i64, src) = (
                ps.pts.map(|p| p as i64),
                ps.dts.map(|d| d as i64),
                ps.source,
            );
            // Reassemble the PS fragments into AU-complete PES for this track
            // (passthrough for self-framing codecs — MPEG-2/audio), so the parser
            // sees exactly the AU-complete shape a transport stream delivers. The
            // AU-start PTS/source survive the reassembly. A track with no assembler
            // (only reachable via a hand-built `pid_to_track` outrunning the stream
            // list) passes the fragment straight through. (PS path: no AACS conceal
            // → no continuity-gap flag.)
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

impl Stream for PipelinedPesStream {
    fn read(&mut self) -> io::Result<Option<PesFrame>> {
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
                    // Drain any access unit a parser buffered past the last
                    // PES (e.g. DTS-HD's final core+extension unit, or MPEG-2's
                    // final GOP). These flush frames carry their own per-frame
                    // `discontinuity` (a post-gap picture buffered at EOF was
                    // stamped by the parser), so route them through the SAME B1
                    // gate the in-stream path uses — otherwise a trailing
                    // dangling-reference frame (MPEG-2 final-GOP corner) would
                    // bypass the resync. Disjoint field borrows so the gate +
                    // is_video reads coexist with the mutable parser drain.
                    let pid_to_track = &self.pid_to_track;
                    let pending = &mut self.pending_frames;
                    let resync = &mut self.resync;
                    let is_video = &self.is_video;
                    let au_asm = &mut self.au_asm;
                    for (pid, parser) in self.parsers.iter_mut() {
                        let Some(&(_, track)) = pid_to_track.iter().find(|(p, _)| p == pid) else {
                            continue;
                        };
                        // First: the trailing access unit(s) the PS assembler
                        // buffered past the final fragment (the last AU has no
                        // following boundary). Parse them, THEN drain the parser's
                        // own internal buffer (MPEG-2 final GOP, DTS-HD tail).
                        let mut frames = Vec::new();
                        let tail = au_asm.get_mut(track).map(|a| a.flush()).unwrap_or_default();
                        for au in tail {
                            let pes = PesPacket {
                                source: au.source,
                                pid: *pid,
                                pts: au.pts,
                                dts: au.dts,
                                data: au.data,
                                discontinuity: au.discontinuity,
                            };
                            frames.extend(parser.parse(&pes));
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

    fn write(&mut self, _: &PesFrame) -> io::Result<()> {
        Err(crate::error::Error::StreamReadOnly.into())
    }

    fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn info(&self) -> &DiscTitle {
        &self.title
    }

    fn headers_ready(&self) -> bool {
        // Match the previous DiscStream semantics: video tracks need
        // codec_private before the consumer can write the container
        // header. FREEMKV_SKIP_PARSE forces ready (no parser ever
        // populates codec_private in that mode).
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
    }

    // `lost_bytes` uses the trait default (0): the file-backed highway has no
    // read-error zero-fill term (resolve/mapfile tracks physical read loss
    // separately) and the decrypt path no longer reports a decrypt-loss term.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::{
        AudioChannels, AudioStream, Codec, ColorSpace, DiscTitle, FrameRate, HdrFormat,
        LabelPurpose, Resolution, SampleRate, VideoStream,
    };
    use crate::mux::demux_thread::{DemuxBatch, DemuxThread};
    use crate::mux::ps::PsPacket;
    use crate::mux::ts::PesPacket;
    use crossbeam_channel::{Sender, bounded};

    /// Build a real, cleanly-exiting `DemuxThread` whose own receiver we
    /// discard. The worker exits immediately (its prefetch sender is dropped)
    /// and joins on drop — it exists only to satisfy `new()`'s ownership of a
    /// `DemuxThread`. The caller controls the SEPARATE `demux_rx` we hand to
    /// `PipelinedPesStream::new`, so we can inject any `DemuxBatch` sequence
    /// (or a bare disconnect) independent of the dummy worker.
    fn dummy_demux_thread() -> DemuxThread {
        let (_pf_tx, pf_rx) = bounded::<std::io::Result<Vec<u8>>>(1);
        let (rec_tx, _rec_rx) = bounded::<Vec<u8>>(2);
        // No TS/PS demuxer; the worker just drains (nothing) and exits Eof.
        let (dt, _own_rx) =
            DemuxThread::spawn_zero_copy(pf_rx, rec_tx, (), None, None, None).expect("spawn");
        dt
    }

    /// Assemble a `PipelinedPesStream` over a caller-controlled demux channel.
    /// Returns the stream plus the `Sender` so the test drives batches/EOF.
    fn make_stream(
        title: DiscTitle,
        parsers: Vec<(u16, Box<dyn CodecParser>)>,
        pid_to_track: Vec<(u16, usize)>,
    ) -> (PipelinedPesStream, Sender<DemuxBatch>) {
        let (tx, rx) = bounded::<DemuxBatch>(8);
        let stream =
            PipelinedPesStream::new(dummy_demux_thread(), rx, title, parsers, pid_to_track);
        (stream, tx)
    }

    /// A parser that emits exactly `n` frames per PES, with a fixed
    /// codec_private. Lets tests assert routing/flush without depending on a
    /// real codec's byte parsing.
    struct CountingParser {
        per_pes: usize,
        flush_n: usize,
        cp: Option<Vec<u8>>,
    }
    impl CodecParser for CountingParser {
        fn parse(&mut self, pes: &PesPacket) -> Vec<super::super::codec::Frame> {
            (0..self.per_pes)
                .map(|i| super::super::codec::Frame {
                    coding: None,
                    source: None,
                    pts_ns: pes.pts.unwrap_or(0) + i as i64,
                    keyframe: i == 0,
                    discontinuity: false,
                    data: pes.data.clone(),
                    duration_ns: None,
                })
                .collect()
        }
        fn flush(&mut self) -> Vec<super::super::codec::Frame> {
            (0..self.flush_n)
                .map(|_| super::super::codec::Frame {
                    coding: None,
                    source: None,
                    pts_ns: 0,
                    keyframe: false,
                    discontinuity: false,
                    data: vec![0xEE],
                    duration_ns: None,
                })
                .collect()
        }
        fn codec_private(&self) -> Option<Vec<u8>> {
            self.cp.clone()
        }
    }

    fn ts_pes(pid: u16, data: Vec<u8>) -> PesPacket {
        PesPacket {
            source: None,
            pid,
            pts: Some(90_000),
            dts: None,
            data,
            discontinuity: false,
        }
    }

    /// CLEAN EOF: the demux worker sends the explicit `Eof` sentinel. The
    /// consumer must return Ok(None) — a normal end-of-stream — and stay
    /// Ok(None) on subsequent reads. (DemuxBatch::Eof doc: "explicit
    /// clean-completion sentinel".)
    #[test]
    fn eof_sentinel_yields_clean_none() {
        let (mut stream, tx) = make_stream(DiscTitle::empty(), vec![], vec![]);
        tx.send(DemuxBatch::Eof).unwrap();
        assert!(stream.read().unwrap().is_none(), "Eof → Ok(None)");
        // The eof flag latches: a further read is still Ok(None), not an error.
        assert!(stream.read().unwrap().is_none());
    }

    /// PANIC / BARE DISCONNECT: the channel closes WITHOUT an Eof (or Err)
    /// sentinel — exactly what happens when the demux worker panics and drops
    /// its sender. The consumer MUST surface DemuxThreadPanicked, never a
    /// clean Ok(None) (which would silently truncate the output). This is the
    /// truncation guard the module docstring promises.
    #[test]
    fn bare_disconnect_is_error_not_silent_eof() {
        let (mut stream, tx) = make_stream(DiscTitle::empty(), vec![], vec![]);
        drop(tx); // sender gone, no Eof sent → RecvError on the consumer side
        let err = stream.read().expect_err("bare disconnect must be an error");
        // E_DEMUX_THREAD_PANICKED (9013) maps to ErrorKind::Other.
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        let e = crate::error::Error::DemuxThreadPanicked;
        assert!(
            err.to_string().contains(&e.code().to_string()),
            "error must carry the DemuxThreadPanicked code, got: {err}"
        );
    }

    /// A `DemuxBatch::Err` from the worker (underlying reader error) is
    /// terminal and must propagate to the caller verbatim, not be masked as
    /// EOF.
    #[test]
    fn demux_err_propagates() {
        let (mut stream, tx) = make_stream(DiscTitle::empty(), vec![], vec![]);
        tx.send(DemuxBatch::Err(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied,
        )))
        .unwrap();
        let err = stream.read().expect_err("Err batch must propagate");
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    /// consume_ts must route a PES to the track mapped to its PID and emit
    /// the parser's frames in order. A PES whose PID is NOT in pid_to_track
    /// must be dropped (no frame), never mis-attributed to another track.
    #[test]
    fn ts_routing_maps_pid_to_track_and_drops_untracked() {
        let title = DiscTitle::empty();
        let parsers: Vec<(u16, Box<dyn CodecParser>)> = vec![(
            0x1100,
            Box::new(CountingParser {
                per_pes: 2,
                flush_n: 0,
                cp: None,
            }),
        )];
        let pid_to_track = vec![(0x1100u16, 3usize)];
        let (mut stream, tx) = make_stream(title, parsers, pid_to_track);

        // One tracked PES (PID 0x1100) and one untracked (PID 0x2222).
        tx.send(DemuxBatch::Ts(vec![
            ts_pes(0x1100, vec![0xAA, 0xBB]),
            ts_pes(0x2222, vec![0xCC]),
        ]))
        .unwrap();
        tx.send(DemuxBatch::Eof).unwrap();

        // Tracked PES → 2 frames on track 3, in order; untracked → nothing.
        let f0 = stream.read().unwrap().expect("frame 0");
        assert_eq!(f0.track, 3, "routed to the PID's mapped track");
        assert_eq!(f0.data, vec![0xAA, 0xBB]);
        let f1 = stream.read().unwrap().expect("frame 1");
        assert_eq!(f1.track, 3);
        // Only the two frames from the tracked PES exist, then clean EOF.
        assert!(
            stream.read().unwrap().is_none(),
            "untracked PES dropped, EOF"
        );
    }

    /// A parser that emits exactly one frame per PES, marking it a keyframe iff
    /// the PES payload's first byte is `b'K'`. Lets a test script a precise
    /// keyframe/inter-frame sequence to exercise the B1 resync gate.
    struct KeyframeParser;
    impl CodecParser for KeyframeParser {
        fn parse(&mut self, pes: &PesPacket) -> Vec<super::super::codec::Frame> {
            vec![super::super::codec::Frame {
                coding: None,
                source: None,
                pts_ns: pes.pts.unwrap_or(0),
                keyframe: pes.data.first() == Some(&b'K'),
                // Propagate so the B1 gate can be driven end-to-end in tests.
                discontinuity: pes.discontinuity,
                data: pes.data.clone(),
                duration_ns: None,
            }]
        }
        fn flush(&mut self) -> Vec<super::super::codec::Frame> {
            vec![]
        }
        fn codec_private(&self) -> Option<Vec<u8>> {
            None
        }
    }

    fn ts_pes_disc(pid: u16, data: Vec<u8>, discontinuity: bool) -> PesPacket {
        PesPacket {
            source: None,
            pid,
            pts: Some(90_000),
            dts: None,
            data,
            discontinuity,
        }
    }

    /// B1 end-to-end: after a TS discontinuity on a VIDEO track the consumer
    /// must DROP every inter-coded frame until the next keyframe, so no frame
    /// with a dangling reference reaches the muxer (an ffmpeg deep-scan would
    /// otherwise report a missing reference). The frame carrying the
    /// discontinuity and the inter frames behind it are dropped; the stream
    /// resumes cleanly at the next keyframe.
    #[test]
    fn b1_video_drops_to_keyframe_after_discontinuity() {
        let title = video_title(false); // one HEVC video stream, PID 0x1011
        let parsers: Vec<(u16, Box<dyn CodecParser>)> = vec![(0x1011, Box::new(KeyframeParser))];
        let pid_to_track = vec![(0x1011u16, 0usize)];
        let (mut stream, tx) = make_stream(title, parsers, pid_to_track);

        // K0,P1 clean → emit. P2 carries the gap (inter frame referencing the
        // lost data) → arms the gate; P2,P3 drop. K4 is the next keyframe →
        // resync + emit. P5 then admits cleanly.
        tx.send(DemuxBatch::Ts(vec![
            ts_pes_disc(0x1011, b"K0".to_vec(), false),
            ts_pes_disc(0x1011, b"P1".to_vec(), false),
            ts_pes_disc(0x1011, b"P2".to_vec(), true),
            ts_pes_disc(0x1011, b"P3".to_vec(), false),
            ts_pes_disc(0x1011, b"K4".to_vec(), false),
            ts_pes_disc(0x1011, b"P5".to_vec(), false),
        ]))
        .unwrap();
        tx.send(DemuxBatch::Eof).unwrap();

        let mut emitted = Vec::new();
        while let Some(f) = stream.read().unwrap() {
            emitted.push(f.data);
        }
        // P2 (gap) and P3 (still no keyframe) are dropped; the rest survive in
        // order. Crucially the FIRST frame after the gap that we emit is the
        // keyframe K4 — never a dangling-reference inter frame.
        assert_eq!(
            emitted,
            vec![
                b"K0".to_vec(),
                b"P1".to_vec(),
                b"K4".to_vec(),
                b"P5".to_vec()
            ],
            "post-gap inter frames dropped, stream resumes at the keyframe"
        );
    }

    /// Counterpart to B1: a discontinuity on a NON-video track must NOT drop
    /// frames — audio/subtitle access units are independent, so the gate admits
    /// every frame the parser still produces.
    #[test]
    fn b1_audio_does_not_drop_on_discontinuity() {
        let mut title = DiscTitle::empty();
        title.streams.push(crate::disc::Stream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::Ac3,
            channels: AudioChannels::Surround51,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        }));
        let parsers: Vec<(u16, Box<dyn CodecParser>)> = vec![(0x1100, Box::new(KeyframeParser))];
        let pid_to_track = vec![(0x1100u16, 0usize)];
        let (mut stream, tx) = make_stream(title, parsers, pid_to_track);

        tx.send(DemuxBatch::Ts(vec![
            ts_pes_disc(0x1100, b"a0".to_vec(), false),
            ts_pes_disc(0x1100, b"a1".to_vec(), true), // gap — but audio is independent
            ts_pes_disc(0x1100, b"a2".to_vec(), false),
        ]))
        .unwrap();
        tx.send(DemuxBatch::Eof).unwrap();

        let mut emitted = Vec::new();
        while let Some(f) = stream.read().unwrap() {
            emitted.push(f.data);
        }
        assert_eq!(
            emitted,
            vec![b"a0".to_vec(), b"a1".to_vec(), b"a2".to_vec()],
            "audio frames are never dropped on a discontinuity"
        );
    }

    /// At EOF the consumer must call `flush()` on every parser and emit the
    /// buffered tail frames — a parser that holds the final access unit (e.g.
    /// DTS-HD) must NOT have it dropped. Without the flush the last frame is
    /// silently truncated.
    #[test]
    fn flush_tail_emitted_at_eof() {
        let title = DiscTitle::empty();
        let parsers: Vec<(u16, Box<dyn CodecParser>)> = vec![(
            0x1100,
            Box::new(CountingParser {
                per_pes: 0, // parse emits nothing; everything comes from flush
                flush_n: 1,
                cp: None,
            }),
        )];
        let pid_to_track = vec![(0x1100u16, 0usize)];
        let (mut stream, tx) = make_stream(title, parsers, pid_to_track);

        tx.send(DemuxBatch::Ts(vec![ts_pes(0x1100, vec![0x01])]))
            .unwrap();
        tx.send(DemuxBatch::Eof).unwrap();

        // No frames from parse; the single flush() frame must surface at EOF.
        let tail = stream.read().unwrap().expect("flush tail frame at EOF");
        assert_eq!(tail.track, 0);
        assert_eq!(tail.data, vec![0xEE], "flush() tail, not dropped");
        assert!(stream.read().unwrap().is_none());
    }

    /// A flush parser whose PID is not in pid_to_track must be skipped at EOF
    /// (the `continue` guard) — no panic, no frame attributed to a phantom
    /// track.
    #[test]
    fn flush_skips_parser_with_unmapped_pid() {
        let title = DiscTitle::empty();
        let parsers: Vec<(u16, Box<dyn CodecParser>)> = vec![(
            0x9999, // PID present as a parser but absent from pid_to_track
            Box::new(CountingParser {
                per_pes: 0,
                flush_n: 5,
                cp: None,
            }),
        )];
        let pid_to_track = vec![]; // nothing mapped
        let (mut stream, tx) = make_stream(title, parsers, pid_to_track);
        tx.send(DemuxBatch::Eof).unwrap();
        // The unmapped parser's 5 flush frames must be discarded, not emitted.
        assert!(
            stream.read().unwrap().is_none(),
            "flush frames for an unmapped PID are skipped"
        );
    }

    /// consume_ps must route by the REAL DVD PID (via PsPacket::dvd_pid).
    /// An audio private-stream-1 packet (stream_id 0xBD, sub-id 0x80 → PID
    /// 0xBD80) routes to the track mapped to 0xBD80. A packet with an
    /// unmappable (stream_id, sub_id) is dropped, never mis-routed.
    #[test]
    fn ps_routing_uses_dvd_pid_and_drops_unmappable() {
        let title = DiscTitle::empty();
        // PID for AC-3 sub-id 0x80 is 0xBD00 | 0x80 = 0xBD80.
        let parsers: Vec<(u16, Box<dyn CodecParser>)> = vec![(
            0xBD80,
            Box::new(CountingParser {
                per_pes: 1,
                flush_n: 0,
                cp: None,
            }),
        )];
        let pid_to_track = vec![(0xBD80u16, 1usize)];
        let (mut stream, tx) = make_stream(title, parsers, pid_to_track);

        let mappable = PsPacket {
            source: None,
            stream_id: 0xBD,
            sub_stream_id: Some(0x80),
            pts: Some(90_000),
            dts: None,
            data: vec![0x12, 0x34],
        };
        // stream_id 0xC0 (MPEG audio) has no DVD PID mapping → dropped.
        let unmappable = PsPacket {
            source: None,
            stream_id: 0xC0,
            sub_stream_id: None,
            pts: None,
            dts: None,
            data: vec![0xFF],
        };
        tx.send(DemuxBatch::Ps(vec![mappable, unmappable])).unwrap();
        tx.send(DemuxBatch::Eof).unwrap();

        let f = stream.read().unwrap().expect("one routed PS frame");
        assert_eq!(f.track, 1, "routed by dvd_pid to track 1");
        assert_eq!(f.data, vec![0x12, 0x34]);
        assert!(stream.read().unwrap().is_none(), "unmappable PS dropped");
    }

    /// Build a single-video-stream title on `codec`, a [`CountingParser`] (1 frame
    /// per PES it is handed), and feed three 0xE0 program-stream fragments that
    /// together form TWO H.264 access units (AUD-delimited); only AU-start
    /// fragments carry a PTS. Returns every emitted frame.
    fn run_ps_fragments(codec: Codec) -> Vec<crate::pes::PesFrame> {
        let mut title = DiscTitle::empty();
        title.streams.push(crate::disc::Stream::Video(VideoStream {
            pid: crate::mux::ps::DVD_VIDEO_PID,
            codec,
            resolution: Resolution::R1080p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            display_aspect: None,
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        }));
        let parsers: Vec<(u16, Box<dyn CodecParser>)> = vec![(
            crate::mux::ps::DVD_VIDEO_PID,
            Box::new(CountingParser {
                per_pes: 1,
                flush_n: 0,
                cp: None,
            }),
        )];
        let pid_to_track = vec![(crate::mux::ps::DVD_VIDEO_PID, 0usize)];
        let (mut stream, tx) = make_stream(title, parsers, pid_to_track);

        let frag = |pts, data: &[u8]| PsPacket {
            source: None,
            stream_id: 0xE0,
            sub_stream_id: None,
            pts,
            dts: None,
            data: data.to_vec(),
        };
        tx.send(DemuxBatch::Ps(vec![
            frag(Some(9_000), &[0, 0, 1, 0x09, 0xF0, 0, 0, 1, 0x65, 0xAA]), // AU1: AUD + slice head
            frag(None, &[0xBB, 0xCC]), // AU1: slice tail (no PTS)
            frag(Some(18_000), &[0, 0, 1, 0x09, 0xF0, 0, 0, 1, 0x65, 0xDD]), // AU2 opener (AUD closes AU1)
        ]))
        .unwrap();
        tx.send(DemuxBatch::Eof).unwrap();

        let mut out = Vec::new();
        while let Some(f) = stream.read().unwrap() {
            out.push(f);
        }
        out
    }

    /// PS-path integration: an H.264 access unit split across several fixed-size
    /// PES fragments (only the first with a PTS) must be REJOINED so the parser
    /// sees one AU-complete PES with the AU-START pts — not one bogus per-fragment
    /// frame each with pts 0 (the HD-DVD truncation/corruption bug). The
    /// `CountingParser` makes it observable: 3 fragments forming 2 AUs → 2 frames.
    #[test]
    fn ps_h264_au_split_across_fragments_reassembles_to_one_frame() {
        let frames = run_ps_fragments(Codec::H264);
        assert_eq!(
            frames.len(),
            2,
            "3 fragments → 2 access units, not 3 frames"
        );
        assert_eq!(frames[0].track, 0);
        assert_eq!(
            frames[0].data,
            vec![0, 0, 1, 0x09, 0xF0, 0, 0, 1, 0x65, 0xAA, 0xBB, 0xCC],
            "AU1 = fragment1 + fragment2 rejoined"
        );
        assert_eq!(
            frames[0].pts, 9_000,
            "AU carries its START pts, not the mid-fragment None→0"
        );
        assert_eq!(
            frames[1].data,
            vec![0, 0, 1, 0x09, 0xF0, 0, 0, 1, 0x65, 0xDD],
            "AU2 flushed at EOF (no following boundary)"
        );
        assert_eq!(frames[1].pts, 18_000);
    }

    /// Contrast: a self-framing codec (MPEG-2 reassembles in its own parser) uses
    /// a Passthrough assembler — the SAME three fragments pass straight through as
    /// three frames, byte-identical to the pre-assembler behaviour. This proves the
    /// reassembly is gated by codec and does not disturb the DVD/MPEG-2 path.
    #[test]
    fn ps_self_framing_codec_is_not_reassembled() {
        let frames = run_ps_fragments(Codec::Mpeg2);
        assert_eq!(
            frames.len(),
            3,
            "MPEG-2 passthrough: one frame per fragment"
        );
        assert_eq!(frames[0].pts, 9_000);
        assert_eq!(
            frames[1].pts, 0,
            "mid-fragment has no PTS under passthrough"
        );
        assert_eq!(frames[2].pts, 18_000);
    }

    /// A batch with no trackable packets must NOT terminate the stream early:
    /// pump_one_batch loops to the next batch. Here an empty-but-untracked
    /// batch is followed by a real frame batch — the consumer must skip the
    /// first and deliver the second (not return Ok(None) prematurely).
    #[test]
    fn empty_batch_does_not_end_stream_early() {
        let title = DiscTitle::empty();
        let parsers: Vec<(u16, Box<dyn CodecParser>)> = vec![(
            0x1100,
            Box::new(CountingParser {
                per_pes: 1,
                flush_n: 0,
                cp: None,
            }),
        )];
        let pid_to_track = vec![(0x1100u16, 0usize)];
        let (mut stream, tx) = make_stream(title, parsers, pid_to_track);

        // First batch: only an untracked PID → yields zero frames.
        tx.send(DemuxBatch::Ts(vec![ts_pes(0x4444, vec![0x00])]))
            .unwrap();
        // Second batch: tracked PID → one frame.
        tx.send(DemuxBatch::Ts(vec![ts_pes(0x1100, vec![0x55])]))
            .unwrap();
        tx.send(DemuxBatch::Eof).unwrap();

        let f = stream.read().unwrap().expect("frame from the second batch");
        assert_eq!(f.data, vec![0x55], "did not stop on the empty first batch");
    }

    /// write() on the read-only pipeline must return StreamReadOnly
    /// (E9000 → Unsupported) — the highway is input-only.
    #[test]
    fn write_is_read_only_error() {
        let (mut stream, _tx) = make_stream(DiscTitle::empty(), vec![], vec![]);
        let frame = PesFrame {
            coding: None,
            source: None,
            track: 0,
            pts: 0,
            keyframe: false,
            data: vec![1],
            duration_ns: None,
        };
        let err = stream.write(&frame).expect_err("write must error");
        assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
    }

    fn video_title(secondary: bool) -> DiscTitle {
        let mut t = DiscTitle::empty();
        t.streams.push(crate::disc::Stream::Video(VideoStream {
            pid: 0x1011,
            codec: Codec::Hevc,
            resolution: Resolution::R2160p,
            frame_rate: FrameRate::F23_976,
            hdr: HdrFormat::Hdr10,
            color_space: ColorSpace::Bt2020,
            display_aspect: None,
            secondary,
            label: String::new(),
            measured_cicp: None,
        }));
        t
    }

    /// headers_ready() is false for a PRIMARY video track until its parser
    /// produces codec_private — MKV can't write the container header without
    /// init data, so the consumer must keep buffering.
    #[test]
    fn headers_not_ready_when_primary_video_lacks_codec_private() {
        let title = video_title(false);
        let parsers: Vec<(u16, Box<dyn CodecParser>)> = vec![(
            0x1011,
            Box::new(CountingParser {
                per_pes: 0,
                flush_n: 0,
                cp: None, // no codec_private yet
            }),
        )];
        let pid_to_track = vec![(0x1011u16, 0usize)];
        let (stream, _tx) = make_stream(title, parsers, pid_to_track);
        assert!(
            !stream.headers_ready(),
            "primary video w/o codec_private not ready"
        );
    }

    /// headers_ready() flips true once the primary video parser exposes
    /// codec_private.
    #[test]
    fn headers_ready_when_primary_video_has_codec_private() {
        let title = video_title(false);
        let parsers: Vec<(u16, Box<dyn CodecParser>)> = vec![(
            0x1011,
            Box::new(CountingParser {
                per_pes: 0,
                flush_n: 0,
                cp: Some(vec![0x01, 0x02, 0x03]),
            }),
        )];
        let pid_to_track = vec![(0x1011u16, 0usize)];
        let (stream, _tx) = make_stream(title, parsers, pid_to_track);
        assert!(stream.headers_ready(), "codec_private present → ready");
        // codec_private(track) resolves track→PID→parser and returns the data.
        assert_eq!(
            stream.codec_private(0).as_deref(),
            Some(&[0x01, 0x02, 0x03][..])
        );
    }

    /// A SECONDARY video track without codec_private must NOT block
    /// headers_ready() — the `!v.secondary` guard means PiP/secondary video
    /// is exempt from the init-data gate.
    #[test]
    fn headers_ready_ignores_secondary_video_without_codec_private() {
        let title = video_title(true); // secondary = true
        let parsers: Vec<(u16, Box<dyn CodecParser>)> = vec![(
            0x1011,
            Box::new(CountingParser {
                per_pes: 0,
                flush_n: 0,
                cp: None,
            }),
        )];
        let pid_to_track = vec![(0x1011u16, 0usize)];
        let (stream, _tx) = make_stream(title, parsers, pid_to_track);
        assert!(
            stream.headers_ready(),
            "secondary video is exempt from the codec_private gate"
        );
    }

    /// codec_private(track) returns None for a track index not present in
    /// pid_to_track — no panic, no wrong-track data.
    #[test]
    fn codec_private_none_for_unmapped_track() {
        let (stream, _tx) = make_stream(DiscTitle::empty(), vec![], vec![]);
        assert_eq!(stream.codec_private(7), None);
    }

    /// An audio-only title (no video streams) is always headers_ready — the
    /// codec_private gate only applies to primary video.
    #[test]
    fn headers_ready_true_for_audio_only_title() {
        let mut title = DiscTitle::empty();
        title.streams.push(crate::disc::Stream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::Ac3,
            channels: AudioChannels::Surround51,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        }));
        let (stream, _tx) = make_stream(title, vec![], vec![]);
        assert!(stream.headers_ready(), "no video → always ready");
    }

    /// finish() on the read-only pipeline is a no-op that returns Ok — the
    /// consumer drives termination via read() returning None.
    #[test]
    fn finish_is_ok_noop() {
        let (mut stream, _tx) = make_stream(DiscTitle::empty(), vec![], vec![]);
        assert!(stream.finish().is_ok());
    }

    // --- DVD highway: keyframe flag must survive the PS → parser → frame path ---

    /// Build a minimal MPEG-2 720x480/29.97 sequence header.
    fn m2_seq_header() -> Vec<u8> {
        let (w, h, aspect, fr): (u16, u16, u8, u8) = (720, 480, 2, 4);
        let mut hdr = vec![0x00, 0x00, 0x01, 0xB3u8];
        hdr.push((w >> 4) as u8);
        hdr.push((((w & 0x0F) as u8) << 4) | (((h >> 8) & 0x0F) as u8));
        hdr.push((h & 0xFF) as u8);
        hdr.push((aspect << 4) | (fr & 0x0F));
        hdr.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0x00]);
        hdr
    }
    fn m2_gop() -> Vec<u8> {
        vec![0x00, 0x00, 0x01, 0xB8u8, 0x00, 0x00, 0x00, 0x00]
    }
    /// Frame-picture AU: picture header (coding_type, temporal_reference) +
    /// coding extension (frame picture, 2 fields) + slice.
    fn m2_pic(coding_type: u8, tr: u16) -> Vec<u8> {
        let b4 = ((tr >> 2) & 0xFF) as u8;
        let b5 = (((tr & 0x03) as u8) << 6) | ((coding_type & 0x07) << 3);
        let mut au = vec![0x00, 0x00, 0x01, 0x00u8, b4, b5, 0x00, 0x00];
        au.extend_from_slice(&[0x00, 0x00, 0x01, 0xB5u8, 0x80, 0x00, 0x03, 0x00, 0x80]);
        au.extend_from_slice(&[0xAA; 32]);
        au
    }

    /// DVD seek-index regression at the HIGHWAY level. The real CLI mux runs
    /// `PsDemuxer → PipelinedPesStream (codec parse) → frame out`, NOT the
    /// codec parser straight into the muxer. The keyframe flag and per-frame
    /// duration the `Mpeg2Parser` sets on each `Frame` must survive that path
    /// (`from_codec_frame`) so the muxer's cluster/cue logic — which opens a
    /// cluster + pushes a cue on `keyframe && track 0` — actually fires. If the
    /// highway dropped the keyframe flag, every video I-frame would arrive as a
    /// non-keyframe and the DVD MKV would get thousands of clusters with ZERO
    /// cues (chapter-seek only, no scrub).
    #[test]
    fn dvd_highway_preserves_video_keyframe_and_duration() {
        use crate::mux::codec::mpeg2::Mpeg2Parser;

        let mut title = DiscTitle::empty();
        title.streams.push(crate::disc::Stream::Video(VideoStream {
            pid: crate::mux::ps::DVD_VIDEO_PID,
            codec: Codec::Mpeg2,
            resolution: Resolution::R480i,
            frame_rate: FrameRate::F29_97,
            hdr: HdrFormat::Sdr,
            color_space: ColorSpace::Bt709,
            display_aspect: Some((4, 3)),
            secondary: false,
            label: String::new(),
            measured_cicp: None,
        }));
        let parsers: Vec<(u16, Box<dyn CodecParser>)> =
            vec![(crate::mux::ps::DVD_VIDEO_PID, Box::new(Mpeg2Parser::new()))];
        let pid_to_track = vec![(crate::mux::ps::DVD_VIDEO_PID, 0usize)];
        let (mut stream, tx) = make_stream(title, parsers, pid_to_track);

        // 6 GOPs × 12 frames, each GOP one PS batch with one PTS-stamped video
        // PES (stream_id 0xE0 → DVD_VIDEO_PID). Decode order I + P/B.
        let field_ns = 1_000_000_000i64 * 1001 / 30000 / 2;
        let frame_ns = 2 * field_ns;
        let gop_len = 12u16;
        for g in 0..6i64 {
            let mut es = m2_seq_header();
            es.extend_from_slice(&m2_gop());
            es.extend_from_slice(&m2_pic(1, 0)); // I-frame, keyframe
            for tr in 1..gop_len {
                let ct = if tr % 3 == 0 { 2 } else { 3 };
                es.extend_from_slice(&m2_pic(ct, tr));
            }
            let gop_pts = (g * gop_len as i64 * frame_ns * 90_000 / 1_000_000_000) as u64;
            tx.send(DemuxBatch::Ps(vec![PsPacket {
                source: None,
                stream_id: 0xE0,
                sub_stream_id: None,
                pts: Some(gop_pts),
                dts: None,
                data: es,
            }]))
            .unwrap();
        }
        tx.send(DemuxBatch::Eof).unwrap();

        // Drain every frame THROUGH the highway's read().
        let mut frames = Vec::new();
        while let Some(f) = stream.read().unwrap() {
            frames.push(f);
        }

        assert!(!frames.is_empty(), "highway produced no frames");
        let keyframes = frames.iter().filter(|f| f.keyframe).count();
        let dur_some = frames.iter().filter(|f| f.duration_ns.is_some()).count();
        assert_eq!(
            keyframes, 6,
            "the 6 GOP-opening I-frames must arrive as keyframes THROUGH the \
             highway (one per GOP); got {keyframes} — if 0, the keyframe flag \
             is being lost in the pipelined path and the DVD seek index dies"
        );
        assert_eq!(
            dur_some,
            frames.len(),
            "every DVD VFR frame must carry its duration through the highway \
             (BlockGroup path)"
        );
        assert!(
            frames.iter().all(|f| f.track == 0),
            "video routed to track 0 (cluster/cue open requires track 0)"
        );
    }
}
