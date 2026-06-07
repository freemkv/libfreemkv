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
//! This is the *only* read-side `Stream` impl in tree. Both ISO file
//! mux ([`crate::mux::resolve`]) and BD-TS file mux ([`crate::mux::M2tsStream`])
//! return a `PipelinedPesStream`; the differences are in how the
//! producer thread (A) is configured — sector-aligned reads with
//! AACS decrypt for ISO, raw byte reads for M2TS.

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
}

impl PipelinedPesStream {
    /// Wire up the stream. Caller has already spawned the
    /// `DemuxThread` (which in turn owns the producer); we take the
    /// receiver end + the join handle bundle so cleanup is bounded
    /// on drop.
    pub fn new(
        demux_thread: DemuxThread,
        demux_rx: Receiver<DemuxBatch>,
        title: DiscTitle,
        parsers: Vec<(u16, Box<dyn CodecParser>)>,
        pid_to_track: Vec<(u16, usize)>,
    ) -> Self {
        Self {
            title,
            parsers,
            pid_to_track,
            demux_rx,
            demux_thread,
            pending_frames: std::collections::VecDeque::new(),
            eof: false,
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
            Err(_) => Ok(false),
        }
    }

    fn consume_ts(&mut self, packets: Vec<PesPacket>) {
        let skip_parse = std::env::var_os("FREEMKV_SKIP_PARSE").is_some();
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
                        self.pending_frames
                            .push_back(PesFrame::from_codec_frame(track, frame));
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
                tracing::warn!(
                    target: "mux",
                    "dropping unmappable PS packet (stream_id={:#04x}, sub_stream_id={:?})",
                    ps.stream_id,
                    ps.sub_stream_id,
                );
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
            let pes = PesPacket {
                pid,
                pts: ps.pts.map(|p| p as i64),
                dts: ps.dts.map(|d| d as i64),
                data: ps.data,
            };
            if let Some((_, parser)) = self.parsers.iter_mut().find(|(p, _)| *p == pid) {
                for frame in parser.parse(&pes) {
                    self.pending_frames
                        .push_back(PesFrame::from_codec_frame(track, frame));
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
                    // Drain any access unit a parser buffered past the last
                    // PES (e.g. DTS-HD's final core+extension unit).
                    let pid_to_track = &self.pid_to_track;
                    let pending = &mut self.pending_frames;
                    for (pid, parser) in self.parsers.iter_mut() {
                        let Some(&(_, track)) = pid_to_track.iter().find(|(p, _)| p == pid) else {
                            continue;
                        };
                        for frame in parser.flush() {
                            pending.push_back(PesFrame::from_codec_frame(track, frame));
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
        if std::env::var_os("FREEMKV_SKIP_PARSE").is_some() {
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
}
