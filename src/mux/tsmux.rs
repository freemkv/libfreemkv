//! BD Transport Stream muxer — PES frames → 192-byte BD-TS packets.
//!
//! Takes PES frames and writes them as BD-TS (Blu-ray transport stream)
//! packets. Each frame is wrapped in a PES header, split into TS packets,
//! and prepended with the 4-byte TP_extra_header.

use super::codec::ns_to_ticks;
use super::decode_ts::{DtsCounters, DtsDeriver};
use super::hevc::{
    append_length_prefixed_as_annex_b_sized, avcc_to_annex_b, hvcc_to_annex_b, nal_length_size,
};
use crate::disc::Codec;
use std::collections::VecDeque;
use std::io::{self, Write};

const SYNC_BYTE: u8 = 0x47;
use crate::consts::TS_PAYLOAD_BYTES;

/// PID range treated as video (HEVC, triggers Annex-B conversion + RAI
/// on keyframes). Both `write_frame` and `build_pes_header` consult this
/// so a PID's stream_id and its NAL handling can never disagree.
pub(crate) const VIDEO_PID_RANGE: std::ops::RangeInclusive<u16> = 0x1011..=0x101F;

// Largest PES payload that fits a bounded `PES_packet_length` (u16) on a
// `0xBD` stream after the 8 PES-header bytes. Frames larger than this split
// into multiple PES (unbounded `0` length is video-only).
const MAX_BD_PES_PAYLOAD: usize = u16::MAX as usize - 8;

pub(crate) fn is_video_pid(pid: u16) -> bool {
    VIDEO_PID_RANGE.contains(&pid)
}

// Headroom below the first frame's PTS for frames presented before it but emitted
// after it. ISO/IEC 13818-1 §2.4.2.6 caps T-STD buffer delay at 1 s.
#[cfg(test)]
pub(crate) const ORIGIN_HEADROOM_NS: i64 = 1_000_000_000;
// The same headroom in 90 kHz ticks: 1 s is exactly 90 000 ticks.
const ORIGIN_HEADROOM_TICKS: i64 = 90_000;

// Start-up hold cap (design §2.3): "1 s of IR timeline or 64 MiB", then release in
// arrival order and count `dts_hold_overflow`.
const HOLD_CAP_TICKS: u64 = 90_000;
const HOLD_CAP_BYTES: usize = 64 * 1024 * 1024;

/// PID of the program_map_section (BD-ROM convention); no stream may use it.
pub(crate) const PMT_PID: u16 = 0x0100;
/// PID of the adaptation-field-only PCR packets (BD-ROM convention); no stream may use it.
pub(crate) const PCR_PID: u16 = 0x1001;
// System clock (27 MHz) of a declared program: a frame's packets arrive this far before its
// DTS, the T-STD delay cap (13818-1 §2.4.2.6), at most at 128 Mb/s (the UHD BD TS rate).
const STC_LEAD: u64 = 27_000_000;
const PACKET_STC: u64 = 188 * 8 * 27_000_000 / 128_000_000;
// PCR at most 100 ms apart (BD-ROM, 13818-1 §2.7.2); a timeline jump past 10 s restarts it.
const PCR_INTERVAL: u64 = 2_700_000;
const STC_RESET: u64 = 10 * 27_000_000;
// PAT/PMT repeat every 100 ms of PTS progress (the ETSI TR 101 290 PSI interval); a PTS
// this far below the highest seen is a discontinuity and restarts the clock.
const PSI_INTERVAL_TICKS: u64 = 9_000;
const PSI_RESET_TICKS: u64 = 10 * 90_000;
// HDMV registration_descriptor (13818-1 §2.6.8): the program's 0x80-0xFF stream_types are
// BD-ROM's, e.g. 0x80 = BD LPCM.
const HDMV_REGISTRATION: [u8; 6] = [0x05, 0x04, b'H', b'D', b'M', b'V'];
// ES_info loop entries fit one 1024-byte section: 1021 - 13 fixed - 6 descriptor bytes.
const MAX_PMT_ENTRIES: usize = (1021 - 13 - HDMV_REGISTRATION.len()) / 5;

/// A frame held, in arrival order, while a video track's start-up window is open.
struct Held {
    track: usize,
    pts_90k: u64,
    keyframe: bool,
    data: Vec<u8>,
}

/// BD-TS muxer: PES frames in, 192-byte BD-TS packets out.
///
/// Constructed over an output writer and a slice of per-track PIDs. The `track` index passed to
/// [`TsMuxer::write_frame`] and [`TsMuxer::set_codec_private`] is the position in that PID
/// slice; all per-track state vectors are sized to `pids.len()`.
pub struct TsMuxer<W: Write> {
    writer: W,
    pids: Vec<u16>,
    continuity: Vec<u8>,                  // per-PID continuity counter (0-15)
    codec_privates: Vec<Option<Vec<u8>>>, // per-track codec_private (for video parameter sets)
    params_written: Vec<bool>,            // per-track: have we written parameter sets?
    /// Per-track video codec: decides how the ES is framed AND how its `codec_private`
    /// parameter-set record is parsed. HEVC/H.264 need Annex-B conversion (hvcC/avcC parameter
    /// sets); MPEG-2/VC-1 are already start-code ES and must NOT be converted. Defaults to
    /// [`Codec::Hevc`]; ignored for non-video tracks.
    video_codec: Vec<Codec>,
    /// Global PTS origin in integer 90 kHz ticks, seeded by the FIRST frame of any
    /// kind (video or audio) so a single fixed origin rebases every frame and the
    /// audio/video offset is preserved. It lies `ORIGIN_HEADROOM_TICKS` (1 s, 90 000 ticks)
    /// before the seeding frame's tick; only a frame earlier than that saturates to 0.
    origin_ticks: Option<i64>,
    /// Count of PES frames actually emitted (a frame dropped as non-key
    /// before the first keyframe does NOT count). `finish()` returns
    /// [`Error::MuxEmpty`](crate::error::Error::MuxEmpty) when this is zero,
    /// so a header-only `m2ts://` output can't be reported as success —
    /// mirroring `MkvMuxer.frame_count`.
    frame_count: u64,
    /// Reusable Annex-B conversion buffer for NAL video, kept across frames so
    /// the conversion does not allocate (and free) a whole-frame buffer per
    /// coded picture — the same reason `MkvMuxer` keeps its `block_group_buf`.
    /// A UHD frame is ~310 KB, i.e. an mmap + first-touch page faults + munmap
    /// per frame, ~200k times per feature. Cleared (never shrunk) per use, so it
    /// settles at the largest frame's size. Taken out of `self` while in use, so
    /// the borrow of the converted bytes does not conflict with the `&mut self`
    /// the writer needs.
    annex_b: Vec<u8>,
    /// Per-track: a video frame has arrived past the pre-keyframe drop guard, which runs
    /// before the start-up hold (design §2.3 tick-domain rule 1).
    arrival_armed: Vec<bool>,
    /// Per video track: the online DTS deriver, built at its first frame.
    dts: Vec<Option<DtsDeriver>>,
    /// Per track: `Some(base)` for an MVC dependent view, whose deriver follows the base
    /// view's R and frame period.
    mvc_base: Vec<Option<usize>>,
    /// Frames of every track held, in arrival order, while a start-up window is open.
    hold: VecDeque<Held>,
    hold_bytes: usize,
    /// Lowest and highest PTS in the hold (its span against `HOLD_CAP_TICKS`).
    hold_span: Option<(u64, u64)>,
    /// Warn once at `finish` when a DTS anomaly was counted.
    warned: bool,
    /// Per-track PMT stream_type; `None` writes no PAT/PMT.
    stream_types: Option<Vec<u8>>,
    /// Continuity counters of the PAT and PMT PIDs.
    psi_cc: [u8; 2],
    /// (PTS of the last PAT/PMT, highest PTS since): decode-order PTS jitter and
    /// interleaved tracks do not re-send them.
    last_psi: Option<(u64, u64)>,
    /// Arrival time of the next packet on the 27 MHz system clock; `None` before a
    /// declared program's first frame.
    stc: Option<u64>,
    /// The last PCR written, and whether the next one flags a discontinuity.
    last_pcr: Option<u64>,
    pcr_discontinuity: bool,
    /// Frames whose packets arrived after their DTS (input interleave beyond `STC_LEAD`).
    late_frames: u64,
}

impl<W: Write> TsMuxer<W> {
    pub fn new(writer: W, pids: &[u16]) -> Self {
        let n = pids.len();
        Self {
            writer,
            pids: pids.to_vec(),
            continuity: vec![0u8; n],
            codec_privates: vec![None; n],
            params_written: vec![false; n],
            video_codec: vec![Codec::Hevc; n],
            origin_ticks: None,
            frame_count: 0,
            annex_b: Vec::new(),
            arrival_armed: vec![false; n],
            dts: (0..n).map(|_| None).collect(),
            mvc_base: vec![None; n],
            hold: VecDeque::new(),
            hold_bytes: 0,
            hold_span: None,
            warned: false,
            stream_types: None,
            psi_cc: [0; 2],
            last_psi: None,
            stc: None,
            last_pcr: None,
            pcr_discontinuity: false,
            late_frames: 0,
        }
    }

    /// Declare each track's PMT stream_type: the output then carries a PAT and an HDMV
    /// PMT (PID [`PMT_PID`]) before the first PES and every 100 ms of PTS after it.
    pub(crate) fn set_program(&mut self, stream_types: Vec<u8>) -> io::Result<()> {
        if stream_types.len() != self.pids.len() {
            return Err(crate::error::Error::MuxTrackRange {
                track: stream_types.len(),
                tracks: self.pids.len(),
            }
            .into());
        }
        self.stream_types = Some(stream_types);
        Ok(())
    }

    // PES stream_id: video, MPEG audio for 11172-3/13818-3/13818-7 stream_types
    // (13818-1 Table 2-22), else private_stream_1 as BD-ROM uses.
    fn stream_id(&self, track: usize) -> u8 {
        use crate::consts::pes_stream_id;
        if is_video_pid(self.pids[track]) {
            return pes_stream_id::VIDEO;
        }
        match self.stream_types.as_ref().map(|t| t[track]) {
            Some(0x03 | 0x04 | 0x0F) => pes_stream_id::MPEG_AUDIO,
            _ => pes_stream_id::PRIVATE_STREAM_1,
        }
    }

    // PAT and PMT, when declared and due at `pts`.
    fn write_psi_if_due(&mut self, pts: u64) -> io::Result<()> {
        let Some(types) = &self.stream_types else {
            return Ok(());
        };
        if let Some((sent, hi)) = &mut self.last_psi {
            *hi = (*hi).max(pts);
            if pts < *sent + PSI_INTERVAL_TICKS && pts + PSI_RESET_TICKS >= *hi {
                return Ok(());
            }
        }
        if types.len() > MAX_PMT_ENTRIES && self.last_psi.is_none() {
            tracing::warn!(target: "mux", tracks = types.len(), "bd-ts: PMT lists the first {MAX_PMT_ENTRIES} tracks only");
        }
        self.last_psi = Some((pts, pts));
        let mut pat = vec![0x00, 0x01, 0xC1, 0x00, 0x00, 0x00, 0x01];
        pat.extend_from_slice(&(0xE000 | PMT_PID).to_be_bytes());
        let pat = psi_section(0x00, &pat);
        let mut pmt = vec![0x00, 0x01, 0xC1, 0x00, 0x00];
        pmt.extend_from_slice(&(0xE000 | PCR_PID).to_be_bytes());
        pmt.extend_from_slice(&(0xF000 | HDMV_REGISTRATION.len() as u16).to_be_bytes());
        pmt.extend_from_slice(&HDMV_REGISTRATION);
        for (&t, &pid) in types.iter().zip(&self.pids).take(MAX_PMT_ENTRIES) {
            pmt.push(t);
            pmt.extend_from_slice(&(0xE000 | pid).to_be_bytes());
            pmt.extend_from_slice(&[0xF0, 0x00]);
        }
        let pmt = psi_section(0x02, &pmt);
        self.write_psi(0, 0, &pat)?;
        self.write_psi(1, PMT_PID, &pmt)
    }

    // One PSI section as TS packets: pointer_field 0, then the section, 0xFF-stuffed.
    fn write_psi(&mut self, slot: usize, pid: u16, section: &[u8]) -> io::Result<()> {
        let payload = [&[0u8][..], section].concat();
        for (i, chunk) in payload.chunks(TS_PAYLOAD_BYTES).enumerate() {
            let cc = self.psi_cc[slot];
            self.psi_cc[slot] = (cc + 1) & 0x0F;
            let pusi = if i == 0 { 0x40 } else { 0 };
            let mut pkt = [0xFFu8; 192];
            pkt[..4].copy_from_slice(&self.arrival()?);
            pkt[4..8].copy_from_slice(&[SYNC_BYTE, pusi | (pid >> 8) as u8, pid as u8, 0x10 | cc]);
            pkt[8..8 + chunk.len()].copy_from_slice(chunk);
            self.writer.write_all(&pkt)?;
        }
        Ok(())
    }

    // Set the system clock for a frame decoded at `dts` (90 kHz): STC_LEAD before it, never
    // backwards; a jump past STC_RESET either way restarts it.
    fn clock_frame(&mut self, dts: u64) {
        if self.stream_types.is_none() {
            return;
        }
        let (dts, target) = (dts * 300, (dts * 300).saturating_sub(STC_LEAD));
        let stc = match self.stc {
            Some(stc) if target <= stc + STC_RESET && stc <= target + STC_RESET => {
                self.late_frames += u64::from(dts < stc);
                stc.max(target)
            }
            Some(_) => {
                self.pcr_discontinuity = true;
                self.last_pcr = None;
                target
            }
            None => target,
        };
        self.stc = Some(stc);
    }

    // TP_extra_header of the next packet: its arrival_time_stamp (low 30 bits of the system
    // clock), after any PCR packet due by then. All zero without a declared program.
    fn arrival(&mut self) -> io::Result<[u8; 4]> {
        let Some(mut t) = self.stc else {
            return Ok([0; 4]);
        };
        loop {
            let at = match self.last_pcr {
                Some(p) if t < p + PCR_INTERVAL => break,
                Some(p) => t.min(p + PCR_INTERVAL),
                None => t,
            };
            self.write_pcr(at)?;
            if at == t {
                t += PACKET_STC;
            }
        }
        self.stc = Some(t + PACKET_STC);
        Ok(((t & 0x3FFF_FFFF) as u32).to_be_bytes())
    }

    // An adaptation-field-only packet on PCR_PID carrying PCR `at` (13818-1 §2.4.3.4); its
    // continuity_counter does not advance (no payload).
    fn write_pcr(&mut self, at: u64) -> io::Result<()> {
        let base = (at / 300) & 0x1_FFFF_FFFF;
        let ext = at % 300;
        let flags = 0x10 | if self.pcr_discontinuity { 0x80 } else { 0 };
        let mut pkt = [0xFFu8; 192];
        pkt[..4].copy_from_slice(&((at & 0x3FFF_FFFF) as u32).to_be_bytes());
        pkt[4..14].copy_from_slice(&[
            SYNC_BYTE,
            (PCR_PID >> 8) as u8,
            PCR_PID as u8,
            0x20,
            183,
            flags,
            (base >> 25) as u8,
            (base >> 17) as u8,
            (base >> 9) as u8,
            (base >> 1) as u8,
        ]);
        pkt[14] = ((base & 1) as u8) << 7 | 0x7E | (ext >> 8) as u8;
        pkt[15] = ext as u8;
        self.writer.write_all(&pkt)?;
        self.last_pcr = Some(at);
        self.pcr_discontinuity = false;
        Ok(())
    }

    /// Mark `track` as the MVC dependent view of `base` (design §2.3). Its own deriver
    /// takes the base's R and frame period; the PTS sequences match, so the DTS match
    /// whichever view arrives first (SSIF reads deliver the dependent first). Returns
    /// [`Error::MuxTrackRange`](crate::error::Error::MuxTrackRange) for a bad index.
    pub(crate) fn set_mvc_base(&mut self, track: usize, base: usize) -> io::Result<()> {
        let tracks = self.pids.len();
        if track >= tracks || base >= tracks {
            return Err(crate::error::Error::MuxTrackRange {
                track: track.max(base),
                tracks,
            }
            .into());
        }
        self.mvc_base[track] = Some(base);
        Ok(())
    }

    /// Set codec_private data for a track. Used to prepend VPS/SPS/PPS
    /// as Annex B NALs before the first keyframe in the transport stream.
    ///
    /// `track` is the index into the PID slice passed to [`TsMuxer::new`].
    /// Returns [`Error::MuxTrackRange`](crate::error::Error::MuxTrackRange)
    /// for an out-of-range index.
    pub fn set_codec_private(&mut self, track: usize, data: Vec<u8>) -> io::Result<()> {
        if track >= self.codec_privates.len() {
            return Err(crate::error::Error::MuxTrackRange {
                track,
                tracks: self.codec_privates.len(),
            }
            .into());
        }
        self.codec_privates[track] = Some(data);
        Ok(())
    }

    /// Declare a video track's codec. This selects both the ES framing (NAL
    /// length-prefixed vs. plain start-code) and the `codec_private`
    /// parameter-set parser (avcC for H.264, hvcC for HEVC) — see
    /// [`Self::video_codec`]. Ignored (harmlessly) for a non-video track.
    /// Returns [`Error::MuxTrackRange`](crate::error::Error::MuxTrackRange) for
    /// an out-of-range index.
    pub fn set_video_codec(&mut self, track: usize, codec: Codec) -> io::Result<()> {
        if track >= self.video_codec.len() {
            return Err(crate::error::Error::MuxTrackRange {
                track,
                tracks: self.video_codec.len(),
            }
            .into());
        }
        self.video_codec[track] = codec;
        Ok(())
    }

    /// True when this track's video ES arrives length-prefixed and must be
    /// converted to Annex B. HEVC and H.264 are the only NAL codecs carried.
    fn is_nal_video(&self, track: usize) -> bool {
        matches!(self.video_codec[track], Codec::Hevc | Codec::H264)
    }

    /// Write a PES frame as BD-TS packets.
    /// Video frame data is expected as length-prefixed NALUs (MKV/PES format)
    /// and is converted to Annex B for transport stream.
    ///
    /// `track` is the index into the PID slice passed to [`TsMuxer::new`].
    /// Returns [`Error::MuxTrackRange`](crate::error::Error::MuxTrackRange)
    /// for an out-of-range index.
    pub fn write_frame(
        &mut self,
        track: usize,
        pts_ns: i64,
        keyframe: bool,
        data: &[u8],
    ) -> io::Result<()> {
        if track >= self.pids.len() {
            return Err(crate::error::Error::MuxTrackRange {
                track,
                tracks: self.pids.len(),
            }
            .into());
        }
        let is_video = is_video_pid(self.pids[track]);

        // Arrival rules (design §2.3 rule 1): the drop guard and the origin seed run here,
        // before the hold, so a replay in arrival order reaches the same decisions.
        // Drop non-key video before any keyframe — decoder has no IDR or parameter sets.
        if is_video && !keyframe && !self.arrival_armed[track] {
            return Ok(());
        }
        if is_video {
            self.arrival_armed[track] = true;
        }

        // One integer-tick origin for every track (J16), seeded by the FIRST frame of any
        // kind and set 90 000 ticks below its tick, so a frame emitted later but presented
        // earlier (video behind its GOP) keeps its offset.
        let ticks = ns_to_ticks(pts_ns);
        let origin = *self
            .origin_ticks
            .get_or_insert(ticks.saturating_sub(ORIGIN_HEADROOM_TICKS));
        let pts_90k = ticks.saturating_sub(origin).max(0) as u64;

        // The deriver sees exactly the PTS the header carries (design §2.3 rule 3);
        // dropped frames never reach it (rule 2).
        if is_video {
            self.push_dts(track, pts_90k as i64, data);
        }

        if self.hold.is_empty() && !self.dts_pending() {
            return self.emit(track, pts_90k, keyframe, data);
        }
        self.hold_push(track, pts_90k, keyframe, data);
        if self.dts_pending() && self.hold_over_cap() {
            for d in self.dts.iter_mut().flatten() {
                d.release_cap();
            }
        }
        if self.dts_pending() {
            return Ok(());
        }
        self.drain_hold()
    }

    fn deriver(&mut self, track: usize) -> &mut DtsDeriver {
        let codec = self.video_codec[track];
        let cp = self.codec_privates[track].as_deref();
        self.dts[track].get_or_insert_with(|| DtsDeriver::for_codec(codec, cp))
    }

    // Push to `track`'s deriver. An MVC dependent's NAL units carry no SPS, so its deriver
    // adopts the base view's parameters (from the base's codec_private if it has no frame yet).
    fn push_dts(&mut self, track: usize, pts: i64, data: &[u8]) {
        let Some(base) = self.mvc_base[track] else {
            self.deriver(track).push(pts, data);
            return;
        };
        let params = self.deriver(base).params();
        let dep = self.dts[track].get_or_insert_with(DtsDeriver::follower);
        dep.adopt(params);
        dep.push(pts, data);
    }

    fn dts_pending(&self) -> bool {
        self.dts.iter().flatten().any(DtsDeriver::pending)
    }

    fn hold_push(&mut self, track: usize, pts_90k: u64, keyframe: bool, data: &[u8]) {
        self.hold_bytes += data.len();
        let (lo, hi) = self.hold_span.unwrap_or((pts_90k, pts_90k));
        self.hold_span = Some((lo.min(pts_90k), hi.max(pts_90k)));
        self.hold.push_back(Held {
            track,
            pts_90k,
            keyframe,
            data: data.to_vec(),
        });
    }

    fn hold_over_cap(&self) -> bool {
        let span = self.hold_span.map_or(0, |(lo, hi)| hi - lo);
        span > HOLD_CAP_TICKS || self.hold_bytes > HOLD_CAP_BYTES
    }

    // Replay the hold through the unchanged write path, in arrival order.
    fn drain_hold(&mut self) -> io::Result<()> {
        self.hold_bytes = 0;
        self.hold_span = None;
        while let Some(h) = self.hold.pop_front() {
            self.emit(h.track, h.pts_90k, h.keyframe, &h.data)?;
        }
        Ok(())
    }

    // The DTS to write for `track`'s next frame: its deriver's decision.
    fn next_dts(&mut self, track: usize) -> Option<u64> {
        let d = self.dts[track].as_mut()?.pop();
        debug_assert!(
            d.is_some(),
            "a frame is emitted only once its DTS is resolved"
        );
        d.flatten().map(|d| d as u64)
    }

    // Write one arrived, accepted frame (after the drop guard and origin seed).
    fn emit(&mut self, track: usize, pts_90k: u64, keyframe: bool, data: &[u8]) -> io::Result<()> {
        let pid = self.pids[track];
        let is_video = is_video_pid(pid);
        let dts_90k = if is_video { self.next_dts(track) } else { None };
        self.clock_frame(dts_90k.unwrap_or(pts_90k));
        self.write_psi_if_due(pts_90k)?;

        // NAL video (HEVC/H.264): convert length-prefixed NALUs to Annex B and prepend
        // codec_private params on the first keyframe only (`params_written` latches even if
        // data is empty, so no later keyframe re-prepends them). Others pass through.
        let mut annex_b = std::mem::take(&mut self.annex_b);
        let convert = is_video && self.is_nal_video(track);
        if convert {
            annex_b.clear();
            // Size once for the whole frame; the slack covers any prepended
            // parameter sets. `reserve` is a no-op once the buffer has settled at
            // the largest frame's size.
            annex_b.reserve(data.len() + 1024);
            if keyframe && !self.params_written[track] {
                if let Some(ref cp) = self.codec_privates[track] {
                    // avcC/hvcC are different box layouts; parsing one with the other's
                    // parser yields no params. Dispatch matches `demux_sink::annexb_param_sets`.
                    let params = match self.video_codec[track] {
                        Codec::H264 => avcc_to_annex_b(cp),
                        _ => hvcc_to_annex_b(cp),
                    };
                    match params {
                        Some(params) => annex_b.extend_from_slice(&params),
                        // A codec_private that exists but won't parse leaves the stream
                        // undecodable; still arm the flag (retrying can't succeed) but warn
                        // instead of silently reporting success on parameter-set-free video.
                        None => tracing::warn!(
                            track,
                            codec = ?self.video_codec[track],
                            codec_private_len = cp.len(),
                            "bd-ts: codec_private did not parse as an avcC/hvcC \
                             record; no parameter sets emitted and the video will \
                             not decode"
                        ),
                    }
                }
                self.params_written[track] = true;
            }
            // Write straight into the destination (avoids ~124 GB of memcpy at UHD frame
            // rates). Prefix width comes from the source's avcC/hvcC, not an assumed 4.
            let length_size = nal_length_size(
                self.video_codec[track],
                self.codec_privates[track].as_deref(),
            );
            append_length_prefixed_as_annex_b_sized(&mut annex_b, data, length_size);
        } else if is_video {
            self.params_written[track] = true;
        }
        // Borrowed from a LOCAL (the taken-out buffer), never from `self`, so the
        // `&mut self` writes below are free of it.
        let es_data: &[u8] = if convert { &annex_b } else { data };

        // 0xBD private_stream_1 needs a bounded length, so oversized audio/sub units
        // split into multiple PES packets; only the first carries the PTS (§2.4.3.7) —
        // repeating it made a demuxer read one display set as two same-timestamp blocks.
        let res = if is_video || es_data.len() <= MAX_BD_PES_PAYLOAD {
            let times = PesTimes::Pts {
                pts: pts_90k,
                dts: dts_90k,
            };
            self.write_pes_chain(track, times, is_video, keyframe, es_data)
        } else {
            let mut first_pes = true;
            let mut res = Ok(());
            for chunk in es_data.chunks(MAX_BD_PES_PAYLOAD) {
                let times = if first_pes {
                    PesTimes::Pts {
                        pts: pts_90k,
                        dts: None,
                    }
                } else {
                    PesTimes::None
                };
                res = self.write_pes_chain(track, times, is_video, keyframe && first_pes, chunk);
                if res.is_err() {
                    break;
                }
                first_pes = false;
            }
            res
        };
        self.annex_b = annex_b;
        res?;
        // A frame that survived the pre-keyframe drop guard above and reached
        // the writer counts as emitted. `finish()` checks this so a zero-frame
        // mux fails loudly instead of producing a header-only "success".
        self.frame_count += 1;
        Ok(())
    }

    // Wrap `es_data` in a PES header and split it into 192-byte BD-TS
    // packets. `keyframe` drives the RAI bit on the first video packet;
    // header and ES bytes are sliced in place (no second full-frame copy).
    fn write_pes_chain(
        &mut self,
        track: usize,
        times: PesTimes,
        is_video: bool,
        keyframe: bool,
        es_data: &[u8],
    ) -> io::Result<()> {
        let pid = self.pids[track];
        let pes_header = build_pes_header(self.stream_id(track), times, es_data.len());

        // Logical PES packet = header bytes followed by es_data. It is
        // indexed (and written) in place, without materializing the
        // concatenation, to avoid a second full-frame copy on the hot path.
        let pes_len = pes_header.len() + es_data.len();

        let mut offset = 0;
        let mut first = true;
        while offset < pes_len {
            let remaining = pes_len - offset;

            // Invariant: TP_extra(4) + TS_header(4) + AF(af_bytes) + payload(payload_len) = 192,
            // i.e. af_bytes + payload_len = TS_PAYLOAD_BYTES (184).
            // RAI on first packet of a keyframe video PES requires AF with flags=0x40.
            let want_rai = first && keyframe && is_video;

            // Pick payload_len and af_bytes per case.
            let (af_bytes, payload_len): (usize, usize) = if want_rai {
                // Minimum AF = 2 bytes (length=1, flags=0x40). Payload caps at 182.
                let max_payload = TS_PAYLOAD_BYTES - 2;
                let p = remaining.min(max_payload);
                (TS_PAYLOAD_BYTES - p, p)
            } else if remaining >= TS_PAYLOAD_BYTES {
                (0, TS_PAYLOAD_BYTES) // no AF, full payload
            } else {
                // Stuffing-only AF, payload = remaining.
                (TS_PAYLOAD_BYTES - remaining, remaining)
            };

            let tp_extra = self.arrival()?;

            // TS header (4 bytes)
            let cc = self.continuity[track];
            self.continuity[track] = (cc + 1) & 0x0F;

            let mut ts_header = [0u8; 4];
            ts_header[0] = SYNC_BYTE;
            ts_header[1] = ((pid >> 8) as u8) & 0x1F;
            if first {
                ts_header[1] |= 0x40; // PUSI
            }
            ts_header[2] = pid as u8;
            ts_header[3] = if af_bytes > 0 {
                0x30 | cc // AF + payload
            } else {
                0x10 | cc // payload only
            };

            self.writer.write_all(&tp_extra)?;
            self.writer.write_all(&ts_header)?;

            if af_bytes > 0 {
                static STUFF_FF: [u8; 184] = [0xFF; 184];
                if want_rai {
                    // RAI AF: length byte + flags(0x40) + (af_bytes - 2) stuffing.
                    let af_len_field = (af_bytes - 1) as u8;
                    self.writer.write_all(&[af_len_field])?;
                    self.writer.write_all(&[0x40u8])?;
                    let stuff = af_bytes - 2;
                    if stuff > 0 {
                        self.writer.write_all(&STUFF_FF[..stuff])?;
                    }
                } else {
                    // Stuffing-only AF.
                    // af_bytes == 1: length=0, no flags.
                    // af_bytes >= 2: length = af_bytes-1, flags=0, rest 0xFF.
                    if af_bytes == 1 {
                        self.writer.write_all(&[0u8])?;
                    } else {
                        self.writer.write_all(&[(af_bytes - 1) as u8])?;
                        self.writer.write_all(&[0u8])?;
                        if af_bytes > 2 {
                            self.writer.write_all(&STUFF_FF[..af_bytes - 2])?;
                        }
                    }
                }
            }

            // Write the payload span [offset, offset+payload_len), which may
            // straddle the header/es_data boundary — emit each side in one
            // write_all rather than copying the whole frame again.
            let end = offset + payload_len;
            let hdr_len = pes_header.len();
            if offset < hdr_len {
                let hdr_end = end.min(hdr_len);
                self.writer.write_all(&pes_header[offset..hdr_end])?;
            }
            if end > hdr_len {
                let es_start = offset.max(hdr_len) - hdr_len;
                let es_end = end - hdr_len;
                self.writer.write_all(&es_data[es_start..es_end])?;
            }

            offset += payload_len;
            first = false;
        }

        Ok(())
    }

    /// DTS anomalies counted so far, summed over the video tracks (design §2.3).
    pub(crate) fn dts_counters(&self) -> DtsCounters {
        let mut c = DtsCounters::default();
        for d in self.dts.iter().flatten() {
            c += d.counters();
        }
        c
    }

    /// End the stream: resolve the DTS start-up windows, drain the hold and flush the
    /// writer. BD-TS needs no stream trailer. Call once, after the last frame.
    ///
    /// Returns [`Error::MuxEmpty`](crate::error::Error::MuxEmpty) when not a
    /// single frame was emitted: an `m2ts://` sink that wrote only the FMKV
    /// header (e.g. undecryptable ciphertext yielded no demuxable frames, or
    /// every frame was dropped before the first keyframe) would otherwise be a
    /// header-only file reported as a successful rip. Mirrors the zero-frame
    /// guard in `MkvMuxer::finish`.
    pub fn finish(&mut self) -> io::Result<()> {
        // EOF (design §2.3): resolve open start-up windows over the units that arrived and
        // drain the hold BEFORE the MuxEmpty check, so a short input is written, not failed.
        for d in self.dts.iter_mut().flatten() {
            d.finish();
        }
        self.drain_hold()?;
        let c = self.dts_counters();
        if c != DtsCounters::default() && !self.warned {
            self.warned = true;
            tracing::warn!(
                dts_order_violations = c.order_violations,
                dts_hold_overflow = c.hold_overflow,
                "bd-ts: video DTS needed correction (PTS-only or guarded AUs)"
            );
        }
        if self.late_frames > 0 {
            tracing::warn!(
                late_frames = self.late_frames,
                "bd-ts: frames arrive after their DTS (source interleave over 1 s)"
            );
            self.late_frames = 0;
        }
        if self.frame_count == 0 {
            return Err(crate::error::Error::MuxEmpty.into());
        }
        for (t, armed) in self.arrival_armed.iter().enumerate() {
            if is_video_pid(self.pids[t]) && !armed {
                tracing::warn!(
                    track = t,
                    "bd-ts: video track never saw a keyframe; no video written"
                );
            }
        }
        self.writer.flush()
    }
}

/// Timestamps of one PES header.
#[derive(Clone, Copy)]
enum PesTimes {
    /// A continuation PES (rest of an oversized private_stream_1 access unit): only the
    /// first PES of an access unit may carry a PTS.
    None,
    /// PTS, plus a DTS only where it differs from the PTS (h222:6425-6429).
    Pts { pts: u64, dts: Option<u64> },
}

// A 33-bit timestamp with its 4-bit prefix and marker bits (h222 Table 2-17).
fn push_timestamp(header: &mut Vec<u8>, prefix: u8, ts: u64) {
    let ts = ts & 0x1_FFFF_FFFF;
    header.push((prefix << 4) | 0x01 | (((ts >> 29) & 0x0E) as u8));
    header.push(((ts >> 22) & 0xFF) as u8);
    header.push(0x01 | (((ts >> 14) & 0xFE) as u8));
    header.push(((ts >> 7) & 0xFF) as u8);
    header.push(0x01 | (((ts << 1) & 0xFE) as u8));
}

// A long-form PSI section: table_id, section_length, `body` (from the 16-bit id on), CRC_32.
fn psi_section(table_id: u8, body: &[u8]) -> Vec<u8> {
    let len = (body.len() + 4) as u16;
    let mut s = vec![table_id, 0xB0 | (len >> 8) as u8, len as u8];
    s.extend_from_slice(body);
    let crc = super::mpg::pack::crc32(&s);
    s.extend_from_slice(&crc.to_be_bytes());
    s
}

// Build a PES packet header for a BD stream.
fn build_pes_header(stream_id: u8, times: PesTimes, data_len: usize) -> Vec<u8> {
    use crate::consts::pes_stream_id;
    // PES_header_data_length: 5 PTS bytes, plus 5 DTS bytes when present.
    let header_data_len: usize = match times {
        PesTimes::None => 0,
        PesTimes::Pts { dts: None, .. } => 5,
        PesTimes::Pts { dts: Some(_), .. } => 10,
    };

    // 3 optional-header bytes + the timestamps + data.
    let pes_data_len = data_len + 3 + header_data_len;
    let mut header = Vec::with_capacity(19);

    // Start code: 00 00 01 stream_id
    header.push(0x00);
    header.push(0x00);
    header.push(0x01);
    header.push(stream_id);

    // Unbounded length (0) is spec-legal only for video; `write_frame` splits
    // oversized 0xBD units so a private stream always fits a bounded u16 here.
    if stream_id == pes_stream_id::VIDEO || pes_data_len > u16::MAX as usize {
        header.push(0x00);
        header.push(0x00);
    } else {
        let len = pes_data_len as u16;
        header.push((len >> 8) as u8);
        header.push(len as u8);
    }

    // Flags: 10xx xxxx — MPEG-2
    header.push(0x80); // marker bits
    let PesTimes::Pts { pts, dts } = times else {
        // Continuation packet: PTS_DTS_flags = 00, no optional fields.
        header.push(0x00);
        header.push(0);
        return header;
    };
    // "When the PTS_DTS_flags field is set to '11', both the PTS fields and DTS fields
    // shall be present" (h222:3237-3238); '10' = PTS only.
    header.push(if dts.is_some() { 0xC0 } else { 0x80 });
    header.push(header_data_len as u8);
    match dts {
        // '0011' + PTS, then '0001' + DTS (h222 Table 2-17).
        Some(dts) => {
            push_timestamp(&mut header, 0b0011, pts);
            push_timestamp(&mut header, 0b0001, dts);
        }
        None => push_timestamp(&mut header, 0b0010, pts),
    }

    header
}

#[cfg(test)]
#[path = "tsmux_tests.rs"]
mod tests;
