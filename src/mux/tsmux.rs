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
    /// Per-track arrival-time copy of `params_written` for the pre-keyframe drop guard,
    /// which runs before the start-up hold (design §2.3 tick-domain rule 1).
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
        // codec_private params on the first keyframe only (arm `params_written` even if
        // data is empty, else later non-key frames fail the drop guard). Others pass through.
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

    /// Flush the underlying writer. BD-TS needs no stream trailer, so this
    /// only drains buffering; the muxer remains usable afterwards.
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
mod tests {
    use super::*;

    // The origin headroom in 90 kHz ticks: the seeding frame's encoded PTS.
    const H: u64 = ORIGIN_HEADROOM_NS as u64 * 9 / 100_000;

    use crate::consts::BD_SOURCE_PACKET_BYTES;
    const VIDEO_PID: u16 = 0x1011;

    /// Parsed BD-TS packet (192 bytes total: 4 TP_extra + 4 TS header + 184 body).
    struct TsPacket {
        /// TP_extra_header arrival_time_stamp (low 30 bits).
        ats: u32,
        pid: u16,
        pusi: bool,
        #[allow(dead_code)]
        cc: u8,
        /// Adaptation field body (length byte stripped) when present.
        af: Option<Vec<u8>>,
        /// Payload bytes (after AF, if any).
        payload: Vec<u8>,
    }

    /// Walk 192-byte BD-TS packets.
    fn parse_bd_ts(buf: &[u8]) -> Vec<TsPacket> {
        let mut out = Vec::new();
        for chunk in buf.chunks(BD_SOURCE_PACKET_BYTES) {
            if chunk.len() != BD_SOURCE_PACKET_BYTES {
                break;
            }
            // Skip TP_extra_header (4 bytes), parse TS header.
            let h = &chunk[4..];
            assert_eq!(h[0], 0x47, "bad sync byte");
            let pusi = (h[1] & 0x40) != 0;
            let pid = (((h[1] & 0x1F) as u16) << 8) | h[2] as u16;
            let afc = (h[3] >> 4) & 0x03;
            let cc = h[3] & 0x0F;
            let body = &h[4..]; // 184 bytes

            let (af, payload) = match afc {
                0b01 => (None, body.to_vec()),
                0b11 => {
                    let af_len = body[0] as usize;
                    let af_body = body[1..1 + af_len].to_vec();
                    let payload = body[1 + af_len..].to_vec();
                    (Some(af_body), payload)
                }
                0b10 => {
                    let af_len = body[0] as usize;
                    (Some(body[1..1 + af_len].to_vec()), Vec::new())
                }
                _ => (None, Vec::new()),
            };

            out.push(TsPacket {
                ats: u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) & 0x3FFF_FFFF,
                pid,
                pusi,
                cc,
                af,
                payload,
            });
        }
        out
    }

    /// Build a fake HEVC NAL with a 4-byte length prefix.
    /// nal_type=19/20 are IDR; 1 is non-key (TRAIL_N/R).
    fn fake_hevc_nal(nal_type: u8, body_len: usize) -> Vec<u8> {
        let mut nal = Vec::with_capacity(2 + body_len);
        // 2-byte NAL header: forbidden_zero(1)=0 | nal_unit_type(6) | layer_id(6)=0 | tid_plus1(3)=1
        nal.push((nal_type & 0x3F) << 1);
        nal.push(0x01);
        for i in 0..body_len {
            nal.push((i & 0xFF) as u8);
        }
        let mut framed = Vec::with_capacity(4 + nal.len());
        framed.extend_from_slice(&(nal.len() as u32).to_be_bytes());
        framed.extend_from_slice(&nal);
        framed
    }

    #[test]
    fn keyframe_param_threads_through() {
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            let idr = fake_hevc_nal(19, 100);
            mux.write_frame(0, 0, true, &idr).unwrap();
            let p = fake_hevc_nal(1, 80);
            mux.write_frame(0, 41_000_000, false, &p).unwrap();
            mux.finish().unwrap();
        }
        assert!(!sink.is_empty());
        let packets = parse_bd_ts(&sink);
        assert!(packets.iter().any(|p| p.pid == VIDEO_PID && p.pusi));
    }

    #[test]
    fn rai_set_on_first_packet_of_keyframe_pes() {
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            let idr = fake_hevc_nal(19, 200);
            mux.write_frame(0, 0, true, &idr).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let first_pusi = packets
            .iter()
            .find(|p| p.pid == VIDEO_PID && p.pusi)
            .expect("video PUSI packet exists");
        let af = first_pusi.af.as_ref().expect("AF present on keyframe PES");
        assert!(!af.is_empty(), "AF body has flags byte");
        assert_eq!(af[0] & 0x40, 0x40, "RAI bit set");
    }

    #[test]
    fn rai_clear_on_non_keyframe_pes() {
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            let idr = fake_hevc_nal(19, 100);
            mux.write_frame(0, 0, true, &idr).unwrap();
            let p = fake_hevc_nal(1, 100);
            mux.write_frame(0, 41_000_000, false, &p).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        // Second PUSI packet on the video PID belongs to the non-key frame.
        let pusi_video: Vec<&TsPacket> = packets
            .iter()
            .filter(|p| p.pid == VIDEO_PID && p.pusi)
            .collect();
        assert!(pusi_video.len() >= 2, "two PUSI packets expected");
        let second = pusi_video[1];
        match &second.af {
            None => {}
            Some(af) if af.is_empty() => {} // length=0 case
            Some(af) => assert_eq!(af[0] & 0x40, 0, "RAI must be clear on non-key PES"),
        }
    }

    #[test]
    fn codec_private_prepended_only_on_first_keyframe() {
        // Build a minimal hvcC with one recognizable NAL.
        let marker: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE];
        let mut hvcc = vec![0u8; 22];
        hvcc.push(1); // numArrays
        hvcc.push(32); // VPS NAL type byte (high bits arbitrary)
        hvcc.extend_from_slice(&1u16.to_be_bytes()); // numNalus
        hvcc.extend_from_slice(&(marker.len() as u16).to_be_bytes());
        hvcc.extend_from_slice(marker);

        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            mux.set_codec_private(0, hvcc).unwrap();
            // Non-IDR before any IDR: should be dropped.
            let p = fake_hevc_nal(1, 50);
            mux.write_frame(0, 0, false, &p).unwrap();
            // IDR: should carry codec_private NALs prepended.
            let idr = fake_hevc_nal(19, 50);
            mux.write_frame(0, 41_000_000, true, &idr).unwrap();
            // A later IDR: the parameter sets are not prepended again.
            mux.write_frame(0, 82_000_000, true, &idr).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        // Concatenate all video PID payloads in emission order.
        let video_bytes: Vec<u8> = packets
            .iter()
            .filter(|p| p.pid == VIDEO_PID)
            .flat_map(|p| p.payload.clone())
            .collect();
        // marker bytes must appear in the stream (codec_private was prepended).
        let pos_marker = video_bytes
            .windows(marker.len())
            .position(|w| w == marker)
            .expect("codec_private marker bytes present in TS payload");
        // Find IDR body byte (0x26 = (19<<1)). pos_idr must be AFTER marker.
        let idr_header = (19u8 << 1) & 0x7E;
        let pos_idr = video_bytes
            .iter()
            .position(|&b| b == idr_header)
            .expect("IDR NAL header present in TS payload");
        assert!(
            pos_marker < pos_idr,
            "codec_private must precede IDR in TS payload"
        );
        let markers = video_bytes.windows(marker.len()).filter(|w| *w == marker);
        assert_eq!(
            markers.count(),
            1,
            "codec_private only before the first IDR"
        );
    }

    #[test]
    fn empty_data_keyframe_arms_params_so_later_frames_survive() {
        // An empty-data keyframe must still arm params_written; otherwise
        // every subsequent non-key frame would be dropped by the
        // pre-keyframe guard and the track would emit no real frames.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            // Keyframe with empty payload (e.g. a frame whose NALs were
            // all stripped upstream) — anchors the stream.
            mux.write_frame(0, 0, true, &[]).unwrap();
            // Now a real non-key frame; it must NOT be dropped.
            let p = fake_hevc_nal(1, 80);
            mux.write_frame(0, 41_000_000, false, &p).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        // The non-key frame's NAL body byte (0x02 = (1<<1)) must appear in
        // a video payload — proof it wasn't dropped.
        let video_bytes: Vec<u8> = packets
            .iter()
            .filter(|p| p.pid == VIDEO_PID)
            .flat_map(|p| p.payload.clone())
            .collect();
        assert!(
            video_bytes
                .windows(4)
                .any(|w| w == [0x00, 0x00, 0x00, 0x01]),
            "later non-key frame must survive after an empty-data keyframe"
        );
    }

    #[test]
    fn non_key_before_first_keyframe_dropped() {
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            let p = fake_hevc_nal(1, 80);
            mux.write_frame(0, 0, false, &p).unwrap();
            // The single non-key frame was dropped (no keyframe to anchor), so
            // finish() reports MuxEmpty instead of a header-only "success".
            let err = mux.finish().unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        }
        // Nothing should be emitted for that PID.
        let packets = parse_bd_ts(&sink);
        assert!(
            !packets.iter().any(|p| p.pid == VIDEO_PID),
            "non-key before first keyframe must be dropped"
        );
    }

    #[test]
    fn finish_with_zero_frames_errors_mux_empty() {
        // Fix 4: a TsMuxer that never emitted a frame must not report a clean finish,
        // or an undecryptable m2ts:// source publishes a header-only file as success.
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
        let err = mux.finish().unwrap_err();
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::InvalidData,
            "zero-frame finish must surface MuxEmpty (E9023 → InvalidData)"
        );
        // Pins the variant/code/kind wiring directly, since the io::Error round-trip
        // (From<Error> for io::Error → from-io) loses fidelity back to IoError/E5000.
        assert_eq!(
            crate::error::Error::MuxEmpty.code(),
            crate::error::E_MUX_EMPTY
        );
        let mapped: std::io::Error = crate::error::Error::MuxEmpty.into();
        assert_eq!(mapped.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn finish_after_real_frame_succeeds() {
        // The counterpart: once a genuine keyframe is emitted, finish() is Ok.
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
        let idr = fake_hevc_nal(19, 50);
        mux.write_frame(0, 0, true, &idr).unwrap();
        mux.finish()
            .expect("a written keyframe makes finish succeed");
    }

    const AUDIO_PID: u16 = 0x1100;

    /// Decode the 33-bit PTS from the first PUSI packet on `pid`. Assumes
    /// the PES header carries PTS (flags 0x80 at PES byte 7).
    fn first_pts_90k(packets: &[TsPacket], pid: u16) -> u64 {
        let pkt = packets
            .iter()
            .find(|p| p.pid == pid && p.pusi)
            .expect("PUSI packet present");
        // PES payload starts the packet payload: 00 00 01 stream_id len len
        // flags1 flags2 hdr_len then 5 PTS bytes.
        let p = &pkt.payload;
        let pts = &p[9..14];
        ((((pts[0] >> 1) & 0x07) as u64) << 30)
            | ((pts[1] as u64) << 22)
            | (((pts[2] >> 1) as u64) << 15)
            | ((pts[3] as u64) << 7)
            | ((pts[4] >> 1) as u64)
    }

    /// Decode the 33-bit PTS from every PTS-bearing PUSI packet on `pid`, in
    /// order — one per single-PES access unit. Continuation PES (no PTS flag)
    /// are skipped so only real per-frame timestamps are returned.
    fn all_pts_90k(packets: &[TsPacket], pid: u16) -> Vec<u64> {
        packets
            .iter()
            .filter(|p| p.pid == pid && p.pusi)
            .filter(|p| p.payload.len() >= 14 && p.payload[7] & 0x80 != 0)
            .map(|p| {
                let pts = &p.payload[9..14];
                ((((pts[0] >> 1) & 0x07) as u64) << 30)
                    | ((pts[1] as u64) << 22)
                    | (((pts[2] >> 1) as u64) << 15)
                    | ((pts[3] as u64) << 7)
                    | ((pts[4] >> 1) as u64)
            })
            .collect()
    }

    #[test]
    fn av_offset_preserved_with_audio_before_first_video() {
        // Audio at t=0 before the first video keyframe at t=1s. The FIRST frame of any
        // kind fixes the origin, so audio lands at the headroom, video 1s=90_000
        // ticks later. OLD video-only seeding made both fall back to own pts → collapse to 0, offset lost.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID, AUDIO_PID]);
            // Audio frame first, at t=0 — this seeds the origin.
            mux.write_frame(1, 0, false, &[0x0B, 0x77, 0x00, 0x00])
                .unwrap();
            // Video keyframe at PTS 1s — 1s after the origin.
            let idr = fake_hevc_nal(19, 100);
            mux.write_frame(0, 1_000_000_000, true, &idr).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let video_pts = first_pts_90k(&packets, VIDEO_PID);
        let audio_pts = first_pts_90k(&packets, AUDIO_PID);
        // The leading audio frame seeds the origin one headroom below itself.
        assert_eq!(audio_pts, H, "the first (audio) frame seeds the origin");
        // Video is 1s after the audio ⇒ the audio→video offset is preserved.
        assert_eq!(
            video_pts,
            H + 90_000,
            "video 1s after the origin must stay 90_000 ticks ahead, not collapse to 0"
        );
    }

    #[test]
    fn out_of_range_track_errors() {
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
        let err = mux.write_frame(5, 0, true, &[0xAA]).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        let err2 = mux.set_codec_private(5, vec![0u8; 4]).unwrap_err();
        assert_eq!(err2.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn oversized_bd_audio_pes_is_split_and_bounded() {
        // An oversized 0xBD audio frame must split into multiple PES, each with a
        // non-zero length (the unbounded 0 form is illegal for 0xBD).
        let mut sink: Vec<u8> = Vec::new();
        let big: Vec<u8> = (0..(MAX_BD_PES_PAYLOAD + 5000))
            .map(|i| (i & 0xFF) as u8)
            .collect();
        {
            let mut mux = TsMuxer::new(&mut sink, &[AUDIO_PID]);
            mux.write_frame(0, 0, false, &big).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let pusi: Vec<&TsPacket> = packets
            .iter()
            .filter(|p| p.pid == AUDIO_PID && p.pusi)
            .collect();
        assert!(
            pusi.len() >= 2,
            "oversized audio must span ≥2 PES, got {}",
            pusi.len()
        );
        for p in pusi {
            // PES length field at payload bytes [4..6] must be non-zero.
            let len = u16::from_be_bytes([p.payload[4], p.payload[5]]);
            assert_ne!(len, 0, "0xBD PES must carry a bounded length");
        }
    }

    // ════════════════════════════════════════════════════════════════════
    // Added hardening tests
    // ════════════════════════════════════════════════════════════════════

    /// Concatenate the ES payloads of all packets on `pid`, stripping the PES
    /// header off each PUSI packet (`00 00 01 stream_id len len 80 80 05` +
    /// 5 PTS bytes = 14 bytes; always PTS-present, header_data_length 5).
    fn reassemble_es(packets: &[TsPacket], pid: u16) -> Vec<u8> {
        let mut out = Vec::new();
        for p in packets.iter().filter(|p| p.pid == pid) {
            if p.pusi {
                // Read the PES header's own length rather than assuming 14: a
                // continuation PES carries no PTS (§2.4.3.7), so its header is 9 bytes.
                assert!(p.payload.len() >= 9, "PUSI payload holds a PES header");
                let hdr = 9 + p.payload[8] as usize;
                out.extend_from_slice(&p.payload[hdr..]);
            } else {
                out.extend_from_slice(&p.payload);
            }
        }
        out
    }

    /// PTS_DTS_flags of every PUSI PES header on `pid`, in order.
    fn pes_pts_flags(packets: &[TsPacket], pid: u16) -> Vec<u8> {
        packets
            .iter()
            .filter(|p| p.pid == pid && p.pusi)
            .map(|p| (p.payload[7] >> 6) & 0x03)
            .collect()
    }

    // Non-NAL codec (MPEG-2/VC-1) must pass ES through byte-for-byte; the payload is
    // length-prefix SHAPED so a wrongly-applied Annex-B conversion is visibly detectable.
    #[test]
    fn non_nal_video_es_passes_through_unconverted() {
        // 4-byte BE length (6) + 6 payload bytes: exactly what the Annex-B
        // converter looks for, so a wrongly-applied conversion is unmissable.
        let es: Vec<u8> = vec![0x00, 0x00, 0x00, 0x06, 0xB3, 0x12, 0x34, 0x56, 0x78, 0x9A];

        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            mux.set_video_codec(0, Codec::Mpeg2).unwrap();
            mux.write_frame(0, 0, true, &es).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let out = reassemble_es(&packets, VIDEO_PID);
        assert_eq!(
            &out[..es.len()],
            &es[..],
            "non-NAL video ES must be emitted verbatim, start-code-free"
        );
    }

    /// The default (`video_codec` = HEVC) still converts, so the test above is
    /// pinning the flag rather than a no-op. Same input, opposite expectation.
    // An avcC declaring 2-octet NAL lengths (lengthSizeMinusOne 1) must be converted with
    // 2-octet prefixes, not the 4-octet default.
    #[test]
    fn avcc_nal_length_size_drives_the_annex_b_conversion() {
        let (a, b): (&[u8], &[u8]) = (&[0x65, 0x88, 0x84, 0x21, 0x43], &[0x06, 0x05, 0x01]);
        let mut es = Vec::new();
        for nal in [a, b] {
            es.extend_from_slice(&(nal.len() as u16).to_be_bytes());
            es.extend_from_slice(nal);
        }
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            mux.set_video_codec(0, crate::disc::Codec::H264).unwrap();
            // avcC header only: lengthSizeMinusOne = 1, no SPS/PPS.
            mux.set_codec_private(0, vec![1, 0x42, 0xC0, 0x1E, 0xFD, 0xE0, 0])
                .unwrap();
            mux.write_frame(0, 0, true, &es).unwrap();
            mux.finish().unwrap();
        }
        let out = reassemble_es(&parse_bd_ts(&sink), VIDEO_PID);
        let mut want = vec![0, 0, 0, 1];
        want.extend_from_slice(a);
        want.extend_from_slice(&[0, 0, 0, 1]);
        want.extend_from_slice(b);
        assert_eq!(out, want);
    }

    #[test]
    fn nal_video_es_is_converted_to_annex_b_by_default() {
        let es: Vec<u8> = vec![0x00, 0x00, 0x00, 0x06, 0xB3, 0x12, 0x34, 0x56, 0x78, 0x9A];

        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            // No set_video_codec call — the default must be the converting path.
            mux.write_frame(0, 0, true, &es).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let out = reassemble_es(&packets, VIDEO_PID);
        assert_eq!(
            &out[..4],
            &[0x00, 0x00, 0x00, 0x01],
            "the default path replaces the length prefix with an Annex-B start code"
        );
        assert_eq!(
            &out[4..10],
            &es[4..10],
            "the NAL body itself is carried unchanged"
        );
    }

    // A non-NAL video track must still arm `params_written`, or every later
    // non-keyframe fails the drop guard and silently vanishes — same class of
    // bug `empty_data_keyframe_arms_params_so_later_frames_survive` guards.
    #[test]
    fn non_nal_video_keyframe_arms_params_so_later_frames_survive() {
        let key: Vec<u8> = vec![0x00, 0x00, 0x01, 0xB3, 0xAA, 0xBB];
        let non_key: Vec<u8> = vec![0x00, 0x00, 0x01, 0xB6, 0xCC, 0xDD];

        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            mux.set_video_codec(0, Codec::Mpeg2).unwrap();
            mux.write_frame(0, 0, true, &key).unwrap();
            mux.write_frame(0, 41_000_000, false, &non_key).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let out = reassemble_es(&packets, VIDEO_PID);
        assert!(
            out.windows(non_key.len()).any(|w| w == &non_key[..]),
            "the non-keyframe following a non-NAL keyframe must not be dropped"
        );
    }

    /// `set_video_codec` rejects an out-of-range track rather than panicking on the
    /// index — this is library API and the crate must not panic from it.
    #[test]
    fn set_video_codec_out_of_range_track_errors() {
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
        assert!(
            mux.set_video_codec(1, Codec::Mpeg2).is_err(),
            "track 1 does not exist on a one-track muxer"
        );
        assert!(
            mux.set_video_codec(0, Codec::Mpeg2).is_ok(),
            "track 0 does exist"
        );
    }

    #[test]
    fn every_packet_is_exactly_192_bytes() {
        // BD-TS packets are 192 bytes (4 TP_extra + 188 TS). The muxer must
        // never emit a short or long packet — that would desync any reader.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            let idr = fake_hevc_nal(19, 500); // spans several packets
            mux.write_frame(0, 0, true, &idr).unwrap();
            mux.finish().unwrap();
        }
        assert!(!sink.is_empty());
        assert_eq!(
            sink.len() % BD_SOURCE_PACKET_BYTES,
            0,
            "output must be 192-aligned"
        );
        for chunk in sink.chunks(BD_SOURCE_PACKET_BYTES) {
            assert_eq!(chunk.len(), BD_SOURCE_PACKET_BYTES);
            assert_eq!(chunk[4], SYNC_BYTE, "TS sync byte at offset 4");
        }
    }

    // PSI repeats per 100 ms of PTS progress, not per frame whose PTS strays from the last
    // PSI (interleaved tracks, decode-order jitter); a large backward jump re-sends at once.
    #[test]
    fn pat_repeats_per_100ms_of_pts_progress() {
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[AUDIO_PID, AUDIO_PID + 1]);
            mux.set_program(vec![0x81, 0x81]).unwrap();
            // Two tracks 500 ms apart, 10 ms frames: PSI at 0 and 0.5..=1.4 s, not ~200.
            for n in 0..100i64 {
                mux.write_frame(0, n * 10_000_000, true, &[0x0B, 0x77])
                    .unwrap();
                mux.write_frame(1, 500_000_000 + n * 10_000_000, true, &[0x0B, 0x77])
                    .unwrap();
            }
            // 20 s is progress; back to 1.5 s is a discontinuity (> 10 s below the high).
            for pts in [20_000_000_000, 1_500_000_000] {
                mux.write_frame(0, pts, true, &[0x0B, 0x77]).unwrap();
            }
            mux.finish().unwrap();
        }
        let pats = parse_bd_ts(&sink).iter().filter(|p| p.pid == 0).count();
        assert_eq!(pats, 13);
    }

    // A PES header's 33-bit PTS and DTS (DTS = PTS when absent), 90 kHz.
    fn pes_pts_dts(payload: &[u8]) -> (u64, u64) {
        let ts = |b: &[u8]| {
            (u64::from(b[0] >> 1 & 7) << 30)
                | (u64::from(b[1]) << 22)
                | (u64::from(b[2] >> 1) << 15)
                | (u64::from(b[3]) << 7)
                | u64::from(b[4] >> 1)
        };
        let pts = ts(&payload[9..14]);
        let dts = if payload[7] & 0x40 != 0 {
            ts(&payload[14..19])
        } else {
            pts
        };
        (pts, dts)
    }

    // The PCR of an adaptation field (13818-1 §2.4.3.5): base * 300 + extension, 27 MHz.
    fn af_pcr(af: &[u8]) -> Option<u64> {
        (af.len() >= 7 && af[0] & 0x10 != 0).then(|| {
            let base = (u64::from(u32::from_be_bytes([af[1], af[2], af[3], af[4]])) << 1)
                | u64::from(af[5] >> 7);
            base * 300 + (u64::from(af[5] & 1) << 8 | u64::from(af[6]))
        })
    }

    // 13818-1 §2.4.3.5 / BD-ROM: a PCR on the PMT's PCR_PID at most 100 ms apart, before
    // the first PES; arrival stamps on the same 27 MHz clock, never past a PES's DTS nor
    // more than the 1 s T-STD delay (§2.4.2.6) ahead of it.
    #[test]
    fn pcr_paces_arrival_at_most_100ms_apart_and_never_past_a_dts() {
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID, AUDIO_PID]);
            mux.set_program(vec![0x24, 0x81]).unwrap();
            // 25 fps video + audio; frame 10 is a 2 MB keyframe (> 100 ms of packets),
            // then a 400 ms gap from frame 30.
            for i in 0..40i64 {
                let pts = i * 40_000_000 + if i >= 30 { 400_000_000 } else { 0 };
                let size = if i == 10 { 2_000_000 } else { 3_000 };
                let nal = fake_hevc_nal(if i % 10 == 0 { 19 } else { 1 }, size);
                mux.write_frame(0, pts, i % 10 == 0, &nal).unwrap();
                mux.write_frame(1, pts + 5_000_000, true, &[0x0B, 0x77, 0, 0])
                    .unwrap();
            }
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let pmt = packets.iter().find(|p| p.pid == PMT_PID).unwrap();
        assert_eq!(
            u16::from_be_bytes([pmt.payload[9], pmt.payload[10]]) & 0x1FFF,
            PCR_PID
        );
        let (mut last_ats, mut last_pcr, mut pes) = (0, None::<u64>, 0);
        for p in &packets {
            assert!(p.ats >= last_ats, "arrival stamps are monotonic");
            last_ats = p.ats;
            if p.pid == PCR_PID {
                let pcr = af_pcr(p.af.as_deref().unwrap()).unwrap();
                assert_eq!(pcr as u32 & 0x3FFF_FFFF, p.ats, "ATS on the PCR clock");
                if let Some(last) = last_pcr {
                    assert!(pcr > last && pcr - last <= 2_700_000, "PCR {last} -> {pcr}");
                }
                last_pcr = Some(pcr);
            } else if p.pusi && p.pid != PMT_PID && p.pid != 0 {
                assert!(last_pcr.is_some(), "a PCR precedes the first PES");
                let dts = pes_pts_dts(&p.payload).1 * 300;
                let ats = u64::from(p.ats);
                assert!(
                    ats <= dts && dts - ats <= 27_000_000,
                    "ATS {ats} vs DTS {dts}"
                );
                pes += 1;
            }
        }
        assert_eq!(pes, 80);
    }

    // A timeline jump back past the reset window restarts the clock, flagged by the
    // discontinuity_indicator (13818-1 §2.4.3.5), instead of stamping every later PES late.
    #[test]
    fn a_backward_timeline_jump_restarts_the_pcr_with_a_discontinuity() {
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[AUDIO_PID]);
            mux.set_program(vec![0x81]).unwrap();
            for pts in [
                0,
                5_000_000_000,
                10_000_000_000,
                15_000_000_000,
                1_000_000_000,
            ] {
                mux.write_frame(0, pts, true, &[0x0B, 0x77]).unwrap();
            }
            mux.finish().unwrap();
        }
        let pcrs: Vec<(u64, bool)> = parse_bd_ts(&sink)
            .iter()
            .filter(|p| p.pid == PCR_PID)
            .map(|p| {
                let af = p.af.as_deref().unwrap();
                (af_pcr(af).unwrap(), af[0] & 0x80 != 0)
            })
            .collect();
        let (last, flagged) = pcrs[pcrs.len() - 1];
        assert!(flagged && last < pcrs[pcrs.len() - 2].0, "{pcrs:?}");
        assert!(pcrs[..pcrs.len() - 1].iter().all(|&(_, d)| !d));
    }

    #[test]
    fn audio_es_round_trips_byte_for_byte_through_demuxer() {
        // The mux→demux round trip must preserve every audio ES byte. A
        // muxer that dropped/duplicated payload on a packet boundary would
        // silently corrupt the audio. Use a payload spanning many packets.
        let es: Vec<u8> = (0..1000u32).map(|i| (i & 0xFF) as u8).collect();
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[AUDIO_PID]);
            mux.write_frame(0, 0, false, &es).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let got = reassemble_es(&packets, AUDIO_PID);
        assert_eq!(got, es, "audio ES must survive mux→demux unchanged");
    }

    #[test]
    fn continuity_counter_wraps_modulo_16() {
        // ISO 13818-1: continuity_counter is 4 bits, incrementing per packet
        // on a PID and wrapping 15→0. A frame spanning >16 packets exercises
        // the wrap.
        let es: Vec<u8> = vec![0xAB; 20 * 184]; // 20 packets of audio payload
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[AUDIO_PID]);
            mux.write_frame(0, 0, false, &es).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let ccs: Vec<u8> = packets
            .iter()
            .filter(|p| p.pid == AUDIO_PID)
            .map(|p| p.cc)
            .collect();
        assert!(ccs.len() > 16, "need >16 packets to test the wrap");
        for w in ccs.windows(2) {
            assert_eq!(w[1], (w[0] + 1) & 0x0F, "CC increments mod 16");
        }
        // Prove a wrap actually occurred (a 15→0 transition exists).
        assert!(
            ccs.windows(2).any(|w| w[0] == 0x0F && w[1] == 0x00),
            "CC must wrap 15→0 across >16 packets"
        );
    }

    #[test]
    fn pts_encoded_at_90khz_decodes_correctly() {
        // pts_ns → 90 kHz ticks = pts_ns * 9 / 100_000. 1 second (1e9 ns)
        // = 90_000 ticks. The first (base) video frame lands at the headroom, so use
        // a second frame at a known offset and check its encoded PTS.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            let idr = fake_hevc_nal(19, 50);
            mux.write_frame(0, 0, true, &idr).unwrap(); // seeds the origin
            let p = fake_hevc_nal(1, 50);
            // +1 second relative to base.
            mux.write_frame(0, 1_000_000_000, false, &p).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let video_pusi: Vec<&TsPacket> = packets
            .iter()
            .filter(|p| p.pid == VIDEO_PID && p.pusi)
            .collect();
        assert!(video_pusi.len() >= 2);
        // Decode PTS of the SECOND video PES (the +1s frame).
        let p = &video_pusi[1].payload;
        let pts = ((((p[9] >> 1) & 0x07) as u64) << 30)
            | ((p[10] as u64) << 22)
            | (((p[11] >> 1) as u64) << 15)
            | ((p[12] as u64) << 7)
            | ((p[13] >> 1) as u64);
        assert_eq!(pts, H + 90_000, "1s offset encodes to 90000 ticks @ 90 kHz");
    }

    #[test]
    fn video_pes_uses_unbounded_length_field() {
        // build_pes_header: video (stream_id 0xE0) always uses the unbounded
        // (0x0000) PES_packet_length form — video PES can exceed u16.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            let idr = fake_hevc_nal(19, 50);
            mux.write_frame(0, 0, true, &idr).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let pusi = packets
            .iter()
            .find(|p| p.pid == VIDEO_PID && p.pusi)
            .unwrap();
        // PES length field at payload[4..6].
        let len = u16::from_be_bytes([pusi.payload[4], pusi.payload[5]]);
        assert_eq!(len, 0, "video PES length field is the unbounded 0 form");
        // stream_id (payload[3]) is 0xE0 for video.
        assert_eq!(pusi.payload[3], 0xE0, "video stream_id 0xE0");
    }

    #[test]
    fn audio_pes_stream_id_is_private_stream_1() {
        // Non-video PIDs are carried as private_stream_1 (0xBD).
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[AUDIO_PID]);
            mux.write_frame(0, 0, false, &[0x0B, 0x77, 0x01, 0x02])
                .unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let pusi = packets
            .iter()
            .find(|p| p.pid == AUDIO_PID && p.pusi)
            .unwrap();
        assert_eq!(pusi.payload[3], 0xBD, "audio carried as private_stream_1");
    }

    #[test]
    fn negative_relative_pts_saturates_to_zero() {
        // A frame earlier than the origin (negative relative PTS) must encode PTS 0, not
        // an underflowed huge value. Video keyframe at t=2s seeds the origin at t=1s; a
        // later audio frame at t=0 is 1s BEFORE it, which must floor to 0.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID, AUDIO_PID]);
            let idr = fake_hevc_nal(19, 50);
            mux.write_frame(0, 2_000_000_000, true, &idr).unwrap();
            // Audio a full 2s before the origin — must saturate, not wrap.
            mux.write_frame(1, 0, false, &[0x0B, 0x77, 0x00, 0x00])
                .unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        assert_eq!(
            first_pts_90k(&packets, AUDIO_PID),
            0,
            "audio before the origin saturates to 0"
        );
    }

    #[test]
    fn audio_only_stream_preserves_frame_spacing() {
        // Regression: an audio-only stream (no video to seed the origin) must keep its
        // frame spacing. The FIRST audio frame seeds the origin; later frames rebase on
        // it. OLD video-only seeding made EVERY frame fall back to own pts → all on PTS 0.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[AUDIO_PID]);
            mux.write_frame(0, 5_000_000_000, false, &[0x01, 0x02])
                .unwrap();
            // Second frame 1s later — must land 90_000 ticks after the first.
            mux.write_frame(0, 6_000_000_000, false, &[0x03, 0x04])
                .unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let pts = all_pts_90k(&packets, AUDIO_PID);
        assert_eq!(pts.len(), 2, "two audio frames → two PTS-bearing PES");
        assert_eq!(
            pts[0], H,
            "the first audio frame sits one headroom past the origin"
        );
        assert_eq!(
            pts[1],
            H + 90_000,
            "the second frame (1s later) must keep its 1s spacing, not collapse to 0"
        );
    }

    #[test]
    fn oversized_audio_split_preserves_all_bytes() {
        // The oversized-0xBD split must not lose or reorder ES bytes across
        // the multiple PES it produces. Reassembling all audio packets must
        // reproduce the original frame exactly.
        let big: Vec<u8> = (0..(MAX_BD_PES_PAYLOAD + 3000))
            .map(|i| (i & 0xFF) as u8)
            .collect();
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[AUDIO_PID]);
            mux.write_frame(0, 0, false, &big).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let got = reassemble_es(&packets, AUDIO_PID);
        assert_eq!(got.len(), big.len(), "no bytes lost in the PES split");
        assert_eq!(got, big, "split audio reassembles byte-for-byte");
    }

    // ISO/IEC 13818-1 §2.4.3.7: only the first PES of a split access unit may carry a PTS;
    // repeating it on continuations makes each look like an independent AU at the same
    // timestamp.
    #[test]
    fn split_access_unit_carries_pts_only_on_the_first_pes() {
        // Three PES worth of ES so there are two continuations to check.
        let big: Vec<u8> = (0..(2 * MAX_BD_PES_PAYLOAD + 3000))
            .map(|i| (i & 0xFF) as u8)
            .collect();
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[AUDIO_PID]);
            mux.write_frame(0, 1_000_000_000, false, &big).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let flags = pes_pts_flags(&packets, AUDIO_PID);
        assert_eq!(flags.len(), 3, "the AU must split into three PES packets");
        assert_eq!(
            flags,
            vec![0b10, 0b00, 0b00],
            "only the PES containing the first byte of the access unit may carry a PTS"
        );
        // And the split is still lossless with the shorter continuation headers.
        assert_eq!(
            reassemble_es(&packets, AUDIO_PID),
            big,
            "split audio still reassembles byte-for-byte"
        );
    }

    // MEASURED: the Annex-B conversion buffer must be REUSED across video frames, not allocated
    // per frame (~310 KB/frame otherwise).
    #[test]
    fn annex_b_conversion_buffer_is_reused_across_frames() {
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
        let idr = fake_hevc_nal(19, 300_000);
        mux.write_frame(0, 0, true, &idr).unwrap();
        let cap = mux.annex_b.capacity();
        let ptr = mux.annex_b.as_ptr();
        assert!(
            cap >= idr.len(),
            "buffer survives the frame with the frame's capacity, got {cap}"
        );

        for i in 1..4 {
            let p = fake_hevc_nal(1, 300_000);
            mux.write_frame(0, i * 41_000_000, false, &p).unwrap();
            assert_eq!(
                mux.annex_b.as_ptr(),
                ptr,
                "frame {i}: conversion buffer must be the same allocation"
            );
            assert_eq!(
                mux.annex_b.capacity(),
                cap,
                "frame {i}: no re-grow once the buffer has settled"
            );
        }
        mux.finish().unwrap();
    }

    // ════════════════════════════════════════════════════════════════════
    // Mutation-gap hardening (mux-ts pass)
    // ════════════════════════════════════════════════════════════════════

    /// Pin MAX_BD_PES_PAYLOAD against an independently computed literal: the
    /// oversized-split tests read it only through the same symbol, so a
    /// mutated definition would still pass those assertions.
    #[test]
    fn max_bd_pes_payload_has_the_documented_value() {
        assert_eq!(MAX_BD_PES_PAYLOAD, 65_527);
    }

    // A private_stream_1 frame of exactly the limit fills one PES to PES_packet_length
    // 0xFFFF; one byte more splits into two.
    #[test]
    fn bd_audio_split_boundary_is_exact() {
        for (len, pes) in [(65_527usize, 1usize), (65_528, 2)] {
            let mut sink: Vec<u8> = Vec::new();
            {
                let mut mux = TsMuxer::new(&mut sink, &[AUDIO_PID]);
                mux.write_frame(0, 0, false, &vec![0x5A; len]).unwrap();
                mux.finish().unwrap();
            }
            let packets = parse_bd_ts(&sink);
            let starts: Vec<&TsPacket> = packets.iter().filter(|p| p.pusi).collect();
            assert_eq!(starts.len(), pes, "{len} bytes");
            let first = u16::from_be_bytes([starts[0].payload[4], starts[0].payload[5]]);
            assert_eq!(first, 0xFFFF, "{len} bytes: the first PES is full");
            assert_eq!(reassemble_es(&packets, AUDIO_PID), vec![0x5A; len]);
        }
    }

    // An oversized video access unit must stay ONE PES (unbounded-length
    // form), never split like bounded private_stream_1 audio/subtitle data —
    // a split here would emit multiple independent-looking video AUs.
    #[test]
    fn oversized_video_frame_is_one_pes_not_split() {
        let big = fake_hevc_nal(19, MAX_BD_PES_PAYLOAD + 5000);
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            mux.write_frame(0, 0, true, &big).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let pusi_count = packets
            .iter()
            .filter(|p| p.pid == VIDEO_PID && p.pusi)
            .count();
        assert_eq!(
            pusi_count, 1,
            "an oversized video access unit must still be exactly one PES \
             (one PUSI packet), using the unbounded length form, not split \
             into several PES the way bounded private_stream_1 data is"
        );
    }

    // The RAI first packet of a keyframe video PES needs only the MINIMUM
    // adaptation field (2 bytes). A `-`->`/` mutation in `max_payload`'s
    // arithmetic would waste 90 bytes as pointless AF stuffing; pin it.
    #[test]
    fn rai_adaptation_field_uses_the_minimum_two_bytes() {
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            // Comfortably larger than one TS payload so the first packet is
            // entirely full: AF(2) + payload(182) = 184.
            let idr = fake_hevc_nal(19, 1000);
            mux.write_frame(0, 0, true, &idr).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let first_pusi = packets
            .iter()
            .find(|p| p.pid == VIDEO_PID && p.pusi)
            .expect("video PUSI packet exists");
        let af = first_pusi.af.as_ref().expect("AF present on keyframe PES");
        assert_eq!(
            af.len(),
            1,
            "AF body (length byte stripped) must be exactly [flags] = 1 byte \
             (2 total with the length byte) when there is enough data to fill \
             the rest of the packet as payload"
        );
        assert_eq!(first_pusi.payload.len(), 182);
    }

    // build_pes_header's length field is big-endian 16-bit; a `>>`->`<<`
    // mutation would zero the high byte for any PES >255 bytes, silently
    // truncating the declared length. Use a >255-byte audio frame to catch it.
    #[test]
    fn bounded_pes_length_field_encodes_the_high_byte() {
        let es: Vec<u8> = vec![0xAB; 2000];
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[AUDIO_PID]);
            mux.write_frame(0, 0, false, &es).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let pusi = packets
            .iter()
            .find(|p| p.pid == AUDIO_PID && p.pusi)
            .unwrap();
        let len = u16::from_be_bytes([pusi.payload[4], pusi.payload[5]]);
        // pes_data_len = data_len + 8 (3 optional-header bytes + 5 PTS bytes).
        assert_eq!(
            len as usize,
            es.len() + 8,
            "PES_packet_length high byte must survive the encode"
        );
        assert!(
            pusi.payload[4] != 0,
            "a length > 255 must set a nonzero high byte"
        );
    }

    // PTS top byte carries bits 29..32 (`(pts >> 29) & 0x0E`); a `>>`->`<<`
    // mutation always yields 0, undetectable with a small PTS. Use a PTS
    // large enough that bits 29..32 are nonzero.
    #[test]
    fn pts_high_bits_survive_encoding() {
        // pts_ns is an exact multiple of 100_000 so the ns->90kHz conversion is exact;
        // N * 9 lands just above 2^31, setting bit 31 of the 33-bit PTS field.
        const N: u64 = 238_609_295;
        let big_pts_ticks: u64 = N * 9;
        let big_pts_ns = (N * 100_000) as i64;
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            let idr = fake_hevc_nal(19, 50);
            mux.write_frame(0, 0, true, &idr).unwrap(); // base = 0
            let p = fake_hevc_nal(1, 50);
            mux.write_frame(0, big_pts_ns, false, &p).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let video_pusi: Vec<&TsPacket> = packets
            .iter()
            .filter(|p| p.pid == VIDEO_PID && p.pusi)
            .collect();
        assert!(video_pusi.len() >= 2);
        let decoded = first_pts_90k(&packets, VIDEO_PID);
        // first_pts_90k always reads the FIRST pusi packet, which is the
        // base (0); decode the SECOND PES's PTS by hand instead.
        let p = &video_pusi[1].payload;
        let pts = ((((p[9] >> 1) & 0x07) as u64) << 30)
            | ((p[10] as u64) << 22)
            | (((p[11] >> 1) as u64) << 15)
            | ((p[12] as u64) << 7)
            | ((p[13] >> 1) as u64);
        assert_eq!(decoded, H, "base video frame stays at the headroom");
        assert_eq!(
            pts,
            H + big_pts_ticks,
            "the high bits (29..32) of a large PTS must round-trip through encoding"
        );
    }

    // Audio emitted first but presented AFTER the first video frame (the video
    // parser holds its opening GOP) must not clamp that video onto the audio's PTS.
    #[test]
    fn video_presented_before_the_seeding_audio_keeps_its_offset() {
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID, AUDIO_PID]);
            mux.write_frame(1, 100_000_000, false, &[0x0B, 0x77, 0x00, 0x00])
                .unwrap();
            let idr = fake_hevc_nal(19, 100);
            mux.write_frame(0, 50_000_000, true, &idr).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let video = first_pts_90k(&packets, VIDEO_PID);
        let audio = first_pts_90k(&packets, AUDIO_PID);
        assert_eq!(audio - video, 4_500, "50 ms apart, video first");
    }

    // ════════════════════════════════════════════════════════════════════
    // S0a goldens (design §7 "m2ts goldens", MPG3-1, MPG4-3)
    // ════════════════════════════════════════════════════════════════════

    use crate::mux::codec::pts_to_ns;
    use crate::mux::decode_ts::test_es::{self as es, H264Sps};

    /// FNV-1a 64: a stable digest for recorded golden output.
    fn fnv(bytes: &[u8]) -> u64 {
        bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| {
            (h ^ b as u64).wrapping_mul(0x0100_0000_01b3)
        })
    }

    /// Every PES on `pid` in order, with the TS packets it took.
    fn pes_by_pid(packets: &[TsPacket], pid: u16) -> Vec<(Vec<u8>, usize)> {
        let mut out: Vec<(Vec<u8>, usize)> = Vec::new();
        for p in packets.iter().filter(|p| p.pid == pid) {
            if p.pusi || out.is_empty() {
                out.push((Vec::new(), 0));
            }
            let last = out.last_mut().expect("pushed above");
            last.0.extend_from_slice(&p.payload);
            last.1 += 1;
        }
        out
    }

    /// Raw 192-byte packets of `pid`, concatenated.
    fn pid_bytes(buf: &[u8], pid: u16) -> Vec<u8> {
        buf.chunks(BD_SOURCE_PACKET_BYTES)
            .filter(|c| ((((c[5] & 0x1F) as u16) << 8) | c[6] as u16) == pid)
            .flatten()
            .copied()
            .collect()
    }

    // Tick sources (MPG4-3): the seeding audio has p ≡ 0 (mod 9); video steps by 3754 ≡ 1,
    // so its residues cycle and B pictures land on p mod 9 ∈ {1..4} as well as 5..8.
    const AUDIO_TICK0: i64 = 897_120;
    const AUDIO_STEP: i64 = 2_880;
    const VIDEO_TICK0: i64 = 900_001;
    const VIDEO_STEP: i64 = 3_754;

    /// (track, pts_ns, keyframe, data) in arrival order: an audio frame first, optionally a
    /// non-key video frame (dropped at arrival), then video (decode order) + audio.
    fn golden_frames(
        sps: &H264Sps,
        decode: &[(i64, bool)],
        lead_non_key: bool,
    ) -> Vec<(usize, i64, bool, Vec<u8>)> {
        let audio = |j: i64| {
            (
                1,
                pts_to_ns(AUDIO_TICK0 + j * AUDIO_STEP),
                false,
                vec![0x0B, 0x77, j as u8, 0x5A],
            )
        };
        let mut out = vec![audio(0)];
        if lead_non_key {
            let p = es::length_prefixed(&[es::h264_slice(sps, false, 0, 7, None)]);
            out.push((0, pts_to_ns(VIDEO_TICK0 + 40 * VIDEO_STEP), false, p));
        }
        for (i, &(d, idr)) in decode.iter().enumerate() {
            let mut nals = Vec::new();
            if idr {
                nals.push(sps.nal()); // in-band SPS re-assert, as the H.264 parser emits
            }
            let mut slice = es::h264_slice(sps, idr, 0, i as u32 % 16, None);
            slice.extend(std::iter::repeat_n(
                0x5C,
                300 + 37 * i + if i == 6 { 2 } else { 0 },
            ));
            nals.push(slice);
            let pts = pts_to_ns(VIDEO_TICK0 + d * VIDEO_STEP);
            out.push((0, pts, idr, es::length_prefixed(&nals)));
            out.push(audio(i as i64 + 1));
        }
        out
    }

    fn mux_golden(sps: &H264Sps, frames: &[(usize, i64, bool, Vec<u8>)]) -> Vec<u8> {
        let mut sink = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID, AUDIO_PID]);
            mux.set_video_codec(0, Codec::H264).unwrap();
            mux.set_codec_private(0, es::avcc(&sps.nal())).unwrap();
            for (t, pts, key, data) in frames {
                mux.write_frame(*t, *pts, *key, data).unwrap();
            }
            mux.finish().unwrap();
        }
        sink
    }

    /// Golden A: audio + non-reordered H.264 (`max_num_reorder_frames = 0`).
    fn golden_a() -> Vec<u8> {
        let sps = es::sps_with_reorder(0);
        let decode: Vec<_> = (0..12).map(|d| (d, d % 6 == 0)).collect();
        mux_golden(&sps, &golden_frames(&sps, &decode, false))
    }

    /// Golden B: reordered H.264 B-pyramid (R = 2), two GOPs, audio first, the first
    /// arriving video frame non-key.
    fn golden_b() -> Vec<u8> {
        let sps = es::sps_with_reorder(2);
        let gop = [0, 4, 2, 1, 3, 8, 6, 5, 7];
        let decode: Vec<_> = gop
            .iter()
            .map(|&d| (d, d == 0))
            .chain(gop.iter().map(|&d| (9 + d, d == 0)))
            .collect();
        mux_golden(&sps, &golden_frames(&sps, &decode, true))
    }

    /// Golden C: SPS-less fake HEVC (no parameter set, so R = 0) with reordered PTS + audio.
    fn golden_c() -> Vec<u8> {
        let mut sink = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID, AUDIO_PID]);
            for (i, d) in [0i64, 3, 1, 2, 6, 4, 5].into_iter().enumerate() {
                let nal = fake_hevc_nal(if i == 0 { 19 } else { 1 }, 200 + i);
                mux.write_frame(0, pts_to_ns(VIDEO_TICK0 + d * VIDEO_STEP), i == 0, &nal)
                    .unwrap();
                let a = pts_to_ns(AUDIO_TICK0 + i as i64 * AUDIO_STEP);
                mux.write_frame(1, a, false, &[0x0B, 0x77, i as u8])
                    .unwrap();
            }
            mux.finish().unwrap();
        }
        sink
    }

    // Digests of the pre-S0a writer's output (origin/dev bbcce1d); S0b's PTS delta is
    // undone before comparing (`as_s0a`, `s0a_tick`).
    const GOLDEN_A: u64 = 0xe76c_c4fa_bd69_9166;
    const GOLDEN_B_AUDIO: u64 = 0x2f58_2a68_48d5_835c;
    const GOLDEN_B_PES_ORDER: u64 = 0x01d8_5336_c980_c698;
    const GOLDEN_C: u64 = 0xbce9_708f_32b8_f90b;
    const GOLDEN_B_VIDEO: [(u64, usize); 18] = [
        (0xce39_af4d_07da_4e8c, 3),
        (0xdd2c_1dfe_972e_49c4, 2),
        (0x0743_afab_5034_0970, 3),
        (0x378d_5374_ca8e_1948, 3),
        (0x3f4b_f881_2144_fa3d, 3),
        (0x0875_ae46_1852_8cea, 3),
        (0x96f2_8d3d_d738_397f, 3),
        (0x21f1_01a2_c5bb_92d6, 4),
        (0x34a8_aa4b_6083_01d4, 4),
        (0x7214_a185_1443_8afc, 4),
        (0x8313_dbb1_4f96_3e95, 4),
        (0xa95a_516e_c7a1_c243, 4),
        (0x72b0_649e_66e3_901d, 5),
        (0xa15c_2a41_135c_2d58, 5),
        (0x837a_e12e_74b7_ed33, 5),
        (0xbdba_c585_9e07_aca2, 5),
        (0x1df9_bc86_22fd_64fd, 5),
        (0x806d_6835_3776_7346, 6),
    ];

    /// h222:6425-6429 (§2.7.5), verbatim.
    const SPEC_DTS_IFF: &str = "A decoding_timestamp (DTS) shall appear in a PES packet header \
if and only if the following two conditions are met: • a PTS is present in the PES packet \
header; • the decoding time differs from the presentation time.";

    /// h222:3236-3238 (§2.4.3.7), verbatim.
    const SPEC_PTS_DTS_FLAGS: &str = "When the PTS_DTS_flags field is set to '10', the PTS \
fields shall be present in the PES packet header. When the PTS_DTS_flags field is set to '11', \
both the PTS fields and DTS fields shall be present in the PES packet header.";

    fn ts33(b: &[u8]) -> u64 {
        ((((b[0] >> 1) & 0x07) as u64) << 30)
            | ((b[1] as u64) << 22)
            | (((b[2] >> 1) as u64) << 15)
            | ((b[3] as u64) << 7)
            | ((b[4] >> 1) as u64)
    }

    /// (PTS, DTS) of a PES header; DTS `None` unless PTS_DTS_flags = '11'.
    fn pes_times(pes: &[u8]) -> (u64, Option<u64>) {
        let flags = pes[7] >> 6;
        let pts = ts33(&pes[9..14]);
        (pts, (flags == 0b11).then(|| ts33(&pes[14..19])))
    }

    /// The PES as the pre-S0a writer emitted it: DTS removed, flags '10', length 5.
    fn strip_dts(pes: &[u8]) -> Vec<u8> {
        let mut out = pes[..6].to_vec();
        out.extend_from_slice(&[0x80, 0x80, 5, (pes[9] & 0x0F) | 0x20]);
        out.extend_from_slice(&pes[10..14]);
        out.extend_from_slice(&pes[19..]);
        out
    }

    // Per spec (§2.7.5); do not change without a spec citation proving otherwise.
    #[test]
    fn golden_non_reordered_h264_output_is_pre_s0a_plus_the_s0b_tick_delta() {
        let decode: Vec<i64> = (0..12).collect();
        let (s0a, moved) = as_s0a(&golden_a(), &decode, 13, AUDIO_TICK0);
        assert_eq!(
            fnv(&s0a),
            GOLDEN_A,
            "R = 0: PTS only, byte-identical up to the S0b delta"
        );
        // Seed residue 0: exactly the video ticks with p mod 9 ∈ {1..4} moved (J16).
        assert_eq!(
            moved, 7,
            "7 of 12 video PTS move +1 tick; audio (residue 0) none"
        );
    }

    #[test]
    fn golden_sps_less_fake_hevc_output_is_pre_s0a_plus_the_s0b_tick_delta() {
        let (s0a, moved) = as_s0a(&golden_c(), &[0, 3, 1, 2, 6, 4, 5], 7, VIDEO_TICK0);
        assert_eq!(
            fnv(&s0a),
            GOLDEN_C,
            "no parameter set: R = 0, no hold (MPG4-4)"
        );
        // Seed residue 1: a tick moves iff its residue is 2..4 (displays 1..3).
        assert_eq!(
            moved, 3,
            "3 of 7 video PTS move +1 tick; audio (residue 0) none"
        );
    }

    /// The tick S0a wrote for source tick `p` (its ns-domain origin, then floor), with
    /// the origin seeded by source tick `seed`.
    fn s0a_tick(p: i64, seed: i64) -> u64 {
        let base = pts_to_ns(seed) - ORIGIN_HEADROOM_NS;
        (pts_to_ns(p) - base) as u64 * 9 / 100_000
    }

    /// Overwrite a PES header's PTS, keeping its prefix nibble.
    fn set_pts(pes: &mut [u8], pts: u64) {
        let mut v = Vec::new();
        push_timestamp(&mut v, pes[9] >> 4, pts);
        pes[9..14].copy_from_slice(&v);
    }

    /// Undo the S0b delta on a PTS-only output: every PES PTS rewritten to the value S0a
    /// wrote. `video_display` gives each video PES's display index; audio frame j has
    /// source tick `AUDIO_TICK0 + j·AUDIO_STEP`; `seed` is the origin-seeding tick.
    /// Returns the bytes and the count of moved PTS.
    fn as_s0a(buf: &[u8], video_display: &[i64], audio: i64, seed: i64) -> (Vec<u8>, usize) {
        let mut out = buf.to_vec();
        let (mut v, mut a, mut moved) = (0, 0, 0);
        for pkt in out.chunks_mut(BD_SOURCE_PACKET_BYTES) {
            if pkt[5] & 0x40 == 0 {
                continue;
            }
            let pid = (((pkt[5] & 0x1F) as u16) << 8) | pkt[6] as u16;
            let src = if pid == VIDEO_PID {
                v += 1;
                VIDEO_TICK0 + video_display[v - 1] * VIDEO_STEP
            } else {
                a += 1;
                AUDIO_TICK0 + (a - 1) * AUDIO_STEP
            };
            let at = 8 + if pkt[7] & 0x20 != 0 {
                1 + pkt[8] as usize
            } else {
                0
            };
            let pes = &mut pkt[at..];
            assert_eq!(pes[7] >> 6, 0b10, "PTS only");
            assert_eq!(
                ts33(&pes[9..14]),
                (src - seed + 90_000) as u64,
                "source tick"
            );
            moved += (ts33(&pes[9..14]) != s0a_tick(src, seed)) as usize;
            set_pts(pes, s0a_tick(src, seed));
        }
        assert_eq!((v, a), (video_display.len(), audio));
        (out, moved)
    }

    // Per spec (§2.7.5); do not change without a spec citation proving otherwise.
    #[test]
    fn golden_reordered_h264_changes_only_the_dts_bearing_video_pes() {
        let b = golden_b();
        let packets = parse_bd_ts(&b);
        assert_eq!(
            fnv(&pid_bytes(&b, AUDIO_PID)),
            GOLDEN_B_AUDIO,
            "audio PID byte-identical: origin, drop decisions and CC unchanged"
        );
        assert_eq!(
            fnv(&pes_order(&packets)),
            GOLDEN_B_PES_ORDER,
            "PES interleave unchanged"
        );
        let video = pes_by_pid(&packets, VIDEO_PID);
        assert_eq!(
            video.len(),
            GOLDEN_B_VIDEO.len(),
            "the non-key lead frame is still dropped"
        );
        let (mut with_dts, mut extra) = (Vec::new(), 0);
        for (i, ((pes, n), (want, n0))) in video.iter().zip(GOLDEN_B_VIDEO).enumerate() {
            let (pts, dts) = pes_times(pes);
            let pre = match dts {
                Some(dts) => {
                    assert!(dts < pts, "{SPEC_DTS_IFF}: PES {i} DTS {dts} vs PTS {pts}");
                    with_dts.push(i);
                    strip_dts(pes)
                }
                None => pes.clone(),
            };
            // S0b (J16): the PTS is the source tick; S0a wrote its floor.
            let mut pre = pre;
            let src = VIDEO_TICK0 + GOLDEN_B_DISPLAY[i] * VIDEO_STEP;
            assert_eq!(
                pts,
                (src - AUDIO_TICK0 + 90_000) as u64,
                "PES {i}: source tick"
            );
            set_pts(&mut pre, s0a_tick(src, AUDIO_TICK0));
            assert_eq!(
                fnv(&pre),
                want,
                "PES {i} identical to pre-S0a apart from DTS and delta"
            );
            assert!(
                *n == n0 || *n == n0 + 1,
                "PES {i}: +5 header bytes add at most one packet"
            );
            extra += n - n0;
        }
        // b1, b5, b10, b14 are presented as decoded (DTS = PTS, h222:2010-2012): PTS only.
        let pts_only = [3, 7, 12, 16];
        assert_eq!(
            with_dts,
            (0..18)
                .filter(|i| !pts_only.contains(i))
                .collect::<Vec<_>>()
        );
        // The video PID's continuity_counter shifts by this many packets after the first
        // DTS-bearing PES that grew (design §2.3 "What S0 changes" 2); CC stays contiguous.
        assert_eq!(extra, EXPECTED_VIDEO_CC_SHIFT);
        let ccs: Vec<u8> = packets
            .iter()
            .filter(|p| p.pid == VIDEO_PID)
            .map(|p| p.cc)
            .collect();
        assert!(ccs.windows(2).all(|w| w[1] == (w[0] + 1) & 0x0F));
    }
    const EXPECTED_VIDEO_CC_SHIFT: usize = 1;
    // Golden B's video PES in output (decode) order, as display indices.
    const GOLDEN_B_DISPLAY: [i64; 18] =
        [0, 4, 2, 1, 3, 8, 6, 5, 7, 9, 13, 11, 10, 12, 17, 15, 14, 16];

    fn mpeg2_es(first: bool, coding: u8, structure: u8, low_delay: bool) -> Vec<u8> {
        let mut v = if first {
            es::mpeg2_seq(3, low_delay)
        } else {
            Vec::new()
        };
        v.extend(es::mpeg2_pic(coding, structure));
        v
    }

    /// MPEG-2 frame pictures on `pid` (decode order, display index × 1/24 s from 1 s).
    fn mux_mpeg2(pid: u16, order: &[(i64, u8)], low_delay: bool) -> Vec<(u64, Option<u64>)> {
        let mut sink = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[pid]);
            mux.set_video_codec(0, Codec::Mpeg2).unwrap();
            for (i, &(d, t)) in order.iter().enumerate() {
                let data = mpeg2_es(i == 0, t, 3, low_delay);
                mux.write_frame(0, 1_000_000_000 + d * 41_666_667, t == 1, &data)
                    .unwrap();
            }
            mux.finish().unwrap();
        }
        pes_by_pid(&parse_bd_ts(&sink), pid)
            .iter()
            .map(|(p, _)| pes_times(p))
            .collect()
    }

    const IBBP: [(i64, u8); 7] = [(0, 1), (3, 2), (1, 3), (2, 3), (6, 2), (4, 3), (5, 3)];

    // Per spec (§2.7.5, h222:3236-3238); do not change without a spec citation proving otherwise.
    #[test]
    fn mpeg2_ibbp_i_and_p_carry_dts_b_carry_pts_only() {
        let t = mux_mpeg2(VIDEO_PID, &IBBP, false);
        let flags: Vec<bool> = t.iter().map(|(_, d)| d.is_some()).collect();
        assert_eq!(
            flags,
            [true, true, false, false, true, false, false],
            "{SPEC_PTS_DTS_FLAGS}"
        );
        let written: Vec<u64> = t.iter().filter_map(|(_, d)| *d).collect();
        assert!(
            written.windows(2).all(|w| w[0] < w[1]),
            "DTS strictly increase"
        );
        assert!(
            t.iter().all(|(p, d)| d.is_none_or(|d| d < *p)),
            "{SPEC_DTS_IFF}"
        );
        // P3 decodes when I0 is presented (h222:3290-3291).
        assert_eq!(t[1].1, Some(t[0].0));
    }

    // Per spec (design §0 S0 scope, MPG4-8); do not change without a spec citation.
    #[test]
    fn dts_only_on_video_pids_0x1011_to_0x101f() {
        for pid in [0x00E0, 0xFD55, 0x1100] {
            let t = mux_mpeg2(pid, &IBBP, false);
            assert!(
                t.iter().all(|(_, d)| d.is_none()),
                "PID {pid:#x} stays PTS-only (F6/F7)"
            );
        }
        for pid in [0x1011, 0x101F] {
            assert!(
                mux_mpeg2(pid, &IBBP, false)
                    .iter()
                    .any(|(_, d)| d.is_some())
            );
        }
    }

    #[test]
    fn low_delay_mpeg2_on_a_video_pid_stays_pts_only() {
        let t = mux_mpeg2(VIDEO_PID, &[(0, 1), (1, 2), (2, 2)], true);
        assert!(t.iter().all(|(_, d)| d.is_none()));
    }

    #[test]
    fn short_input_with_at_most_r_units_is_written_not_mux_empty() {
        let sps = es::sps_with_reorder(2);
        let mut sink = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            mux.set_video_codec(0, Codec::H264).unwrap();
            mux.set_codec_private(0, es::avcc(&sps.nal())).unwrap();
            let f = |idr, n| es::length_prefixed(&[es::h264_slice(&sps, idr, 0, n, None)]);
            mux.write_frame(0, 0, true, &f(true, 0)).unwrap();
            mux.write_frame(0, 166_666_667, false, &f(false, 1))
                .unwrap();
            mux.finish()
                .expect("held units are drained before the MuxEmpty check");
        }
        let t: Vec<_> = pes_by_pid(&parse_bd_ts(&sink), VIDEO_PID)
            .iter()
            .map(|(p, _)| pes_times(p))
            .collect();
        assert_eq!(t.len(), 2);
        assert_eq!(
            t[1].1,
            Some(t[0].0),
            "EOF start-up over 2 units: DTS_1 = S[0]"
        );
        assert!(t[0].1.is_some_and(|d| d < t[0].0));
    }

    #[test]
    fn slideshow_hold_cap_release_counts_and_never_duplicates_a_dts() {
        // MPEG-2 R = 1, I0 at 0 s, audio every 100 ms, P1 at 5 s (MPG4-1b).
        let mut sink = Vec::new();
        let counters;
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID, AUDIO_PID]);
            mux.set_video_codec(0, Codec::Mpeg2).unwrap();
            mux.write_frame(0, 0, true, &mpeg2_es(true, 1, 3, false))
                .unwrap();
            for j in 0..50 {
                mux.write_frame(1, j * 100_000_000, false, &[0x0B, 0x77, j as u8])
                    .unwrap();
            }
            mux.write_frame(0, 5_000_000_000, false, &mpeg2_es(false, 2, 3, false))
                .unwrap();
            mux.finish().unwrap();
            counters = mux.dts_counters();
        }
        let packets = parse_bd_ts(&sink);
        let t: Vec<_> = pes_by_pid(&packets, VIDEO_PID)
            .iter()
            .map(|(p, _)| pes_times(p))
            .collect();
        assert_eq!(t[0].1, None, "one held unit: DTS = S[0] = PTS");
        assert_eq!(t[1].1, Some(t[0].0 + 1), "DTS_last + 1, not a duplicate");
        assert_eq!(counters.hold_overflow, 1, "dts_hold_overflow");
        assert_eq!(counters.order_violations, 1, "dts_order_violations");
        assert_eq!(
            all_pts_90k(&packets, AUDIO_PID).len(),
            50,
            "every held audio frame written"
        );
    }

    // A video-only slideshow (R = 1, I0 at 0 s, P1 at 5 s): the start-up T is the stated
    // frame period, never the 5 s gap, so no DTS clamps below 0 and nothing is counted.
    #[test]
    fn video_only_slideshow_start_up_uses_the_frame_period_not_the_gap() {
        let mut sink = Vec::new();
        let counters;
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            mux.set_video_codec(0, Codec::Mpeg2).unwrap();
            mux.write_frame(0, 0, true, &mpeg2_es(true, 1, 3, false))
                .unwrap();
            mux.write_frame(0, 5_000_000_000, false, &mpeg2_es(false, 2, 3, false))
                .unwrap();
            mux.finish().unwrap();
            counters = mux.dts_counters();
        }
        let t: Vec<_> = pes_by_pid(&parse_bd_ts(&sink), VIDEO_PID)
            .iter()
            .map(|(p, _)| pes_times(p))
            .collect();
        assert_eq!(
            counters,
            DtsCounters::default(),
            "no false dts_order_violations"
        );
        assert_eq!(
            t[0].1,
            Some(t[0].0 - 3_600),
            "DTS_0 = PTS_0 − one 25 Hz frame"
        );
        assert_eq!(t[1].1, Some(t[0].0), "P1 decodes when I0 is presented");
    }

    #[test]
    fn hold_cap_by_bytes_releases_in_arrival_order() {
        let mut sink = Vec::new();
        let counters;
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID, AUDIO_PID]);
            mux.set_video_codec(0, Codec::Mpeg2).unwrap();
            mux.write_frame(0, 0, true, &mpeg2_es(true, 1, 3, false))
                .unwrap();
            let big = vec![0xA5u8; 64 * 1024 * 1024 + 1];
            mux.write_frame(1, 10_000_000, false, &big).unwrap();
            mux.write_frame(0, 41_666_667, false, &mpeg2_es(false, 2, 3, false))
                .unwrap();
            mux.finish().unwrap();
            counters = mux.dts_counters();
        }
        assert_eq!(counters.hold_overflow, 1, "64 MiB held: released");
        let order = pes_order(&parse_bd_ts(&sink));
        assert_eq!(
            &order[..4],
            &[0x10, 0x11, 0x11, 0x00],
            "video, then the big audio: arrival order"
        );
    }

    // Per design J16 (S0b); do not change without a spec citation proving otherwise.
    // Every written PTS/DTS is the source's own 90 kHz tick minus the integer-tick
    // origin (the seed's tick − 90 000), for every residue mod 9.
    #[test]
    fn m2ts_pts_equals_the_source_tick_after_s0b() {
        let decode: Vec<_> = (0..18).map(|d| (d, d % 9 == 0)).collect();
        let sps = es::sps_with_reorder(0);
        let frames = golden_frames(&sps, &decode, false);
        let buf = mux_golden(&sps, &frames);
        let packets = parse_bd_ts(&buf);
        let video: Vec<u64> = pes_by_pid(&packets, VIDEO_PID)
            .iter()
            .map(|(p, _)| pes_times(p).0)
            .collect();
        let want: Vec<u64> = (0..18)
            .map(|d| (VIDEO_TICK0 + d * VIDEO_STEP - AUDIO_TICK0 + 90_000) as u64)
            .collect();
        assert_eq!(video, want, "PTS now equals the source tick");
        let audio = all_pts_90k(&packets, AUDIO_PID);
        assert!(
            audio
                .iter()
                .enumerate()
                .all(|(j, &p)| p == 90_000 + j as u64 * AUDIO_STEP as u64)
        );
    }

    // Design §2.3 (MPG4-2): a negative absolute IR PTS pair keeps its offset.
    #[test]
    fn negative_pts_audio_video_pair_keeps_its_offset() {
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID, AUDIO_PID]);
            mux.write_frame(0, -40_000_000, true, &fake_hevc_nal(19, 50))
                .unwrap();
            mux.write_frame(1, -80_000_000, false, &[0x0B, 0x77, 0, 0])
                .unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let (v, a) = (
            first_pts_90k(&packets, VIDEO_PID),
            first_pts_90k(&packets, AUDIO_PID),
        );
        assert_eq!(
            (v, a),
            (H, H - 3_600),
            "−40 ms video, −80 ms audio: 40 ms apart"
        );
    }

    /// The PID of every PES start, in output order: the interleave of access units.
    fn pes_order(packets: &[TsPacket]) -> Vec<u8> {
        packets
            .iter()
            .filter(|p| p.pusi)
            .flat_map(|p| p.pid.to_be_bytes())
            .collect()
    }
}
