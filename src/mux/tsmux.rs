//! BD Transport Stream muxer — PES frames → 192-byte BD-TS packets.
//!
//! Takes PES frames and writes them as BD-TS (Blu-ray transport stream)
//! packets. Each frame is wrapped in a PES header, split into TS packets,
//! and prepended with the 4-byte TP_extra_header.

use super::hevc::{append_length_prefixed_as_annex_b, avcc_to_annex_b, hvcc_to_annex_b};
use crate::disc::Codec;
use std::io::{self, Write};

const SYNC_BYTE: u8 = 0x47;
use crate::consts::TS_PAYLOAD_BYTES;

/// PID range treated as video (HEVC, triggers Annex-B conversion + RAI
/// on keyframes). Both `write_frame` and `build_pes_header` consult this
/// so a PID's stream_id and its NAL handling can never disagree.
const VIDEO_PID_RANGE: std::ops::RangeInclusive<u16> = 0x1011..=0x101F;

/// Largest PES payload that fits a bounded `PES_packet_length` (u16) on a
/// `0xBD` (private_stream_1) stream after the 8 PES-header bytes. Frames
/// larger than this are split into multiple PES so the length field stays
/// spec-conformant (the unbounded `0` length is only legal for video).
const MAX_BD_PES_PAYLOAD: usize = u16::MAX as usize - 8;

fn is_video_pid(pid: u16) -> bool {
    VIDEO_PID_RANGE.contains(&pid)
}

/// BD-TS muxer: PES frames in, 192-byte BD-TS packets out.
///
/// Constructed over an output writer and a slice of per-track PIDs. The
/// `track` index passed to [`TsMuxer::write_frame`] and
/// [`TsMuxer::set_codec_private`] is the position in that PID slice; all
/// per-track state vectors are sized to `pids.len()`. PIDs in
/// `0x1011..=0x101F` are treated as video (length-prefixed NALUs in,
/// Annex B out, with parameter-set prepend and RAI on keyframes); every
/// other PID is carried as `private_stream_1` (`0xBD`) audio/subtitle.
/// All tracks share one PTS origin seeded from the first video frame, so
/// audio/video PTS offsets are preserved.
pub struct TsMuxer<W: Write> {
    writer: W,
    pids: Vec<u16>,
    continuity: Vec<u8>,                  // per-PID continuity counter (0-15)
    codec_privates: Vec<Option<Vec<u8>>>, // per-track codec_private (for video parameter sets)
    params_written: Vec<bool>,            // per-track: have we written parameter sets?
    /// Per-track video codec, which decides BOTH how the ES is framed and how
    /// its `codec_private` parameter-set record is parsed. One fact, one field:
    /// carrying NAL-ness separately from the codec is what let a track be
    /// treated as NAL video while its avcC record was handed to the hvcC parser.
    ///
    /// * HEVC / H.264 arrive length-prefixed (MKV/PES NALU convention) and need
    ///   Annex-B conversion; their parameter sets live in an hvcC / avcC record
    ///   respectively, and the two layouts are NOT interchangeable.
    /// * MPEG-2 and VC-1 are already plain start-code ES; running
    ///   `length_prefixed_to_annex_b` over them mangles the frame into
    ///   empty/garbage output while `frame_count` still increments, so the mux
    ///   "succeeds" and silently produces a video-less file.
    ///
    /// Defaults to [`Codec::Hevc`] — the prior, only behaviour — so a caller that
    /// never calls [`TsMuxer::set_video_codec`] is unaffected. Ignored for
    /// non-video tracks.
    video_codec: Vec<Codec>,
    /// Global PTS origin (nanoseconds), seeded by the FIRST video frame so
    /// the audio/video offset is preserved. Frames that arrive before it
    /// is set saturate to 0.
    base_pts_ns: Option<i64>,
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
            base_pts_ns: None,
            frame_count: 0,
            annex_b: Vec::new(),
        }
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
        let pid = self.pids[track];
        let is_video = is_video_pid(pid);

        // Drop non-key video before any keyframe — decoder has no IDR or
        // parameter sets to anchor on.
        if is_video && !keyframe && !self.params_written[track] {
            return Ok(());
        }

        // Seed the global PTS origin from the FIRST video frame only, so the
        // audio/video offset is preserved. A leading audio frame must not
        // pull the base up and collapse the first video IDR to t=0.
        if is_video {
            self.base_pts_ns.get_or_insert(pts_ns);
        }
        let base = self.base_pts_ns.unwrap_or(pts_ns);
        let pts_ns = pts_ns.saturating_sub(base);

        // For NAL-based video (HEVC, H.264): convert length-prefixed NALUs to
        // Annex B (start codes) and prepend codec_private parameter sets on
        // the FIRST keyframe only.
        //
        // Arm `params_written` on the first video keyframe regardless of
        // whether it carries data: an empty-data keyframe still anchors
        // the stream, and leaving the flag unset would make every later
        // non-key frame fail the drop guard above and silently vanish.
        // For non-video AND for non-NAL video (MPEG-2, VC-1 — already
        // start-code ES, never length-prefixed) the bytes pass through
        // unchanged, so borrow `data` directly rather than copying it; only
        // NAL video needs an owned Annex-B conversion buffer.
        // Take the reusable conversion buffer out of `self` for the duration of
        // this frame: that frees the `&mut self` the writer needs below while the
        // converted bytes are still borrowed, and keeps the allocation across
        // frames instead of making (and dropping) a whole-frame one per picture.
        // It is put back at the end of the function, so a mid-frame error path
        // costs only the buffer's capacity, never correctness.
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
                    // avcC and hvcC are DIFFERENT box layouts; parsing one with
                    // the other's parser yields no parameter sets at all, and the
                    // stream is then undecodable. Dispatch on the declared codec,
                    // matching `demux_sink::annexb_param_sets`.
                    let params = match self.video_codec[track] {
                        Codec::H264 => avcc_to_annex_b(cp),
                        _ => hvcc_to_annex_b(cp),
                    };
                    match params {
                        Some(params) => annex_b.extend_from_slice(&params),
                        // A codec_private that EXISTS but will not parse means no
                        // VPS/SPS/PPS reaches the stream and it is undecodable.
                        // Arming the flag below is still right — retrying the same
                        // bytes on the next keyframe cannot succeed — but it must
                        // not be silent, which is what it was: the mux reported
                        // success while emitting parameter-set-free video.
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
            // Write the conversion STRAIGHT into the destination.
            // `length_prefixed_to_annex_b` allocates a whole-frame Vec of its own
            // and we then copied it in, so every video frame cost two full-frame
            // allocations and two full-frame copies. At ~200k frames averaging
            // ~310 KB of ES on a UHD, that is ~124 GB of pointless memcpy.
            append_length_prefixed_as_annex_b(&mut annex_b, data);
        } else if is_video {
            self.params_written[track] = true;
        }
        // Borrowed from a LOCAL (the taken-out buffer), never from `self`, so the
        // `&mut self` writes below are free of it.
        let es_data: &[u8] = if convert { &annex_b } else { data };

        let pts_90k = if pts_ns >= 0 {
            (pts_ns as u64).saturating_mul(9) / 100_000
        } else {
            0
        };

        // Video PES may be unbounded (length 0); a 0xBD private_stream_1
        // PES must carry a bounded length, so split oversized audio/sub
        // access units into multiple PES packets. Each emitted PES carries
        // the same PTS and starts on its own PUSI packet (only the keyframe
        // RAI rides the first packet of the first PES).
        //
        // The write result is held rather than `?`-propagated so the conversion
        // buffer goes back into `self` on every path.
        let res = if is_video || es_data.len() <= MAX_BD_PES_PAYLOAD {
            self.write_pes_chain(track, pid, pts_90k, is_video, keyframe, es_data)
        } else {
            let mut first_pes = true;
            let mut res = Ok(());
            for chunk in es_data.chunks(MAX_BD_PES_PAYLOAD) {
                res = self.write_pes_chain(
                    track,
                    pid,
                    pts_90k,
                    is_video,
                    keyframe && first_pes,
                    chunk,
                );
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

    /// Wrap `es_data` in a PES header and split it into 192-byte BD-TS
    /// packets. `keyframe` drives the RAI bit on the first packet (video
    /// only). The PES header and ES bytes are sliced in place — no second
    /// full-frame copy.
    fn write_pes_chain(
        &mut self,
        track: usize,
        pid: u16,
        pts_90k: u64,
        is_video: bool,
        keyframe: bool,
        es_data: &[u8],
    ) -> io::Result<()> {
        let pes_header = build_pes_header(pid, pts_90k, es_data.len());

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

            // TP_extra_header (4 bytes — arrival time, set to 0)
            let tp_extra = [0u8; 4];

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
        if self.frame_count == 0 {
            return Err(crate::error::Error::MuxEmpty.into());
        }
        self.writer.flush()
    }
}

/// Build a PES packet header for a BD stream.
fn build_pes_header(pid: u16, pts_90k: u64, data_len: usize) -> Vec<u8> {
    use crate::consts::pes_stream_id;
    // Determine stream_id from PID range
    let stream_id: u8 = if is_video_pid(pid) {
        pes_stream_id::VIDEO
    } else {
        pes_stream_id::PRIVATE_STREAM_1 // audio, PGS subtitle, or default
    };

    let pes_data_len = data_len + 8; // 3 header bytes + 5 PTS bytes + data
    let mut header = Vec::with_capacity(14);

    // Start code: 00 00 01 stream_id
    header.push(0x00);
    header.push(0x00);
    header.push(0x01);
    header.push(stream_id);

    // PES packet length. The unbounded form (0) is only spec-legal for
    // video; `write_frame` splits oversized 0xBD access units so a private
    // stream always fits a bounded u16 length here. The `> 65535` arm
    // remains a defensive fallback for video only.
    if stream_id == pes_stream_id::VIDEO || pes_data_len > u16::MAX as usize {
        header.push(0x00);
        header.push(0x00);
    } else {
        let len = pes_data_len as u16;
        header.push((len >> 8) as u8);
        header.push(len as u8);
    }

    // Flags: 10xx xxxx — MPEG-2, PTS present
    header.push(0x80); // marker bits
    header.push(0x80); // PTS present

    // PES header data length
    header.push(5); // 5 bytes of PTS

    // PTS (5 bytes, 33-bit timestamp with markers)
    let pts = pts_90k & 0x1_FFFF_FFFF;
    header.push(0x21 | (((pts >> 29) & 0x0E) as u8));
    header.push(((pts >> 22) & 0xFF) as u8);
    header.push(0x01 | (((pts >> 14) & 0xFE) as u8));
    header.push(((pts >> 7) & 0xFF) as u8);
    header.push(0x01 | (((pts << 1) & 0xFE) as u8));

    header
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::consts::BD_SOURCE_PACKET_BYTES;
    const VIDEO_PID: u16 = 0x1011;

    /// Parsed BD-TS packet (192 bytes total: 4 TP_extra + 4 TS header + 184 body).
    struct TsPacket {
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
            // The single non-key frame was dropped (no keyframe to anchor),
            // so finish() now reports MuxEmpty rather than producing a
            // header-only "success". The drop behaviour itself is still
            // verified by the empty packet list below.
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
        // Fix 4: a TsMuxer that never emitted a frame must NOT report a
        // clean finish — an m2ts:// sink that wrote only the FMKV header
        // (undecryptable ciphertext → no demuxable frames) would otherwise
        // be a header-only file published as a successful rip.
        let mut sink: Vec<u8> = Vec::new();
        let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
        let err = mux.finish().unwrap_err();
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::InvalidData,
            "zero-frame finish must surface MuxEmpty (E9023 → InvalidData)"
        );
        // The MuxEmpty variant carries the E9023 code, and its io::Error
        // mapping is InvalidData (matching the kind above). Asserting both
        // pins the variant ⇄ code ⇄ kind wiring without a lossy round-trip
        // (From<Error> for io::Error → from-io goes back to IoError/E5000).
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

    #[test]
    fn av_offset_preserved_with_audio_before_first_video() {
        // Audio at t=0 arrives BEFORE the first video keyframe at t=1s.
        // The global base must be seeded from the VIDEO frame so the
        // audio/video PTS offset is preserved (audio earlier ⇒ saturates to
        // 0, video lands at +1s = 90000 ticks), not both collapsed to 0.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID, AUDIO_PID]);
            // Audio frame first, at PTS 0.
            mux.write_frame(1, 0, false, &[0x0B, 0x77, 0x00, 0x00])
                .unwrap();
            // Video keyframe at PTS 1s — seeds the base.
            let idr = fake_hevc_nal(19, 100);
            mux.write_frame(0, 1_000_000_000, true, &idr).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        let video_pts = first_pts_90k(&packets, VIDEO_PID);
        let audio_pts = first_pts_90k(&packets, AUDIO_PID);
        // Video keyframe is the base ⇒ its relative PTS is 0.
        assert_eq!(video_pts, 0, "video keyframe seeds the base at t=0");
        // Audio arrived 1s earlier ⇒ saturates to 0, NOT lifted past video.
        assert_eq!(audio_pts, 0, "earlier audio saturates to 0");
        assert!(
            audio_pts <= video_pts,
            "audio must not be pulled ahead of the video base"
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
        // A private_stream_1 (0xBD) audio frame larger than the bounded PES
        // limit must be split into multiple PES, each with a non-zero
        // PES_packet_length (never the unbounded 0 form, which is illegal
        // for 0xBD).
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

    /// Concatenate the ES payloads of all packets on `pid`, stripping the
    /// PES header off each PUSI packet. A PUSI packet starts a PES whose
    /// header is `00 00 01 stream_id len len 80 80 05` + 5 PTS bytes = 14
    /// bytes for our muxer (always PTS-present, header_data_length 5).
    fn reassemble_es(packets: &[TsPacket], pid: u16) -> Vec<u8> {
        let mut out = Vec::new();
        for p in packets.iter().filter(|p| p.pid == pid) {
            if p.pusi {
                // Skip the 14-byte PES header (3 startcode + 1 stream_id +
                // 2 length + 2 flags + 1 hdr_len + 5 PTS).
                assert!(p.payload.len() >= 14, "PUSI payload holds a PES header");
                out.extend_from_slice(&p.payload[14..]);
            } else {
                out.extend_from_slice(&p.payload);
            }
        }
        out
    }

    /// A non-NAL codec must pass the ES through byte-for-byte: MPEG-2 and
    /// VC-1 are not NAL-based, so their ES already IS the wire format and
    /// `length_prefixed_to_annex_b` would mangle it.
    ///
    /// The payload is deliberately length-prefix SHAPED (a big-endian length
    /// followed by that many bytes) so the conversion, if wrongly applied, rewrites
    /// the leading four bytes into a `00 00 00 01` start code. That makes the two
    /// paths produce visibly different bytes; a payload the converter happened to
    /// leave alone would let a mutant pass.
    ///
    /// Mutation: delete the `set_video_codec` call, or make Mpeg2 report as NAL,
    /// and the emitted ES gains a start code -> this fails.
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

    /// A non-NAL video track must still arm `params_written`, or every later
    /// non-keyframe would fail the drop guard and silently vanish — the same class
    /// of bug `empty_data_keyframe_arms_params_so_later_frames_survive` guards on
    /// the NAL path.
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
        // = 90_000 ticks. The first (base) video frame rebases to 0, so use
        // a second frame at a known offset and check its encoded PTS.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID]);
            let idr = fake_hevc_nal(19, 50);
            mux.write_frame(0, 0, true, &idr).unwrap(); // base = 0
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
        assert_eq!(pts, 90_000, "1s offset encodes to 90000 ticks @ 90 kHz");
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
        // A frame earlier than the base (negative relative PTS) must encode
        // PTS 0, never an underflowed huge value. Audio at t=0 before a
        // video keyframe at t=2s: base=video, audio relative = -2s → 0.
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[VIDEO_PID, AUDIO_PID]);
            mux.write_frame(1, 0, false, &[0x0B, 0x77, 0x00, 0x00])
                .unwrap();
            let idr = fake_hevc_nal(19, 50);
            mux.write_frame(0, 2_000_000_000, true, &idr).unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        assert_eq!(
            first_pts_90k(&packets, AUDIO_PID),
            0,
            "earlier audio saturates to 0"
        );
    }

    #[test]
    fn no_base_seeded_by_audio_only_stream() {
        // If only audio frames are written (no video), base_pts_ns is never
        // seeded by them; each frame rebases to itself via unwrap_or(pts_ns),
        // so the first audio frame lands at relative 0. Proves audio never
        // seeds the global base (which would corrupt later A/V offsets).
        let mut sink: Vec<u8> = Vec::new();
        {
            let mut mux = TsMuxer::new(&mut sink, &[AUDIO_PID]);
            // First audio frame at 5s.
            mux.write_frame(0, 5_000_000_000, false, &[0x01, 0x02])
                .unwrap();
            mux.finish().unwrap();
        }
        let packets = parse_bd_ts(&sink);
        // With no video base, base = unwrap_or(pts_ns) = this frame's pts,
        // so relative PTS is 0.
        assert_eq!(first_pts_90k(&packets, AUDIO_PID), 0);
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

    /// MEASURED: the Annex-B conversion buffer must be REUSED across video
    /// frames, not allocated per frame. Both the allocation's address and its
    /// capacity are unchanged after the second and third same-sized frames — if
    /// the conversion allocated a fresh `Vec` per frame (the old
    /// `Vec::with_capacity(data.len() + 1024)`), the buffer left on the muxer
    /// would be empty with zero capacity, and each frame would pay an
    /// allocate/first-touch/free cycle over the whole ~310 KB frame.
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
}
