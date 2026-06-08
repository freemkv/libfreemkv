//! BD Transport Stream muxer — PES frames → 192-byte BD-TS packets.
//!
//! Takes PES frames and writes them as BD-TS (Blu-ray transport stream)
//! packets. Each frame is wrapped in a PES header, split into TS packets,
//! and prepended with the 4-byte TP_extra_header.

use super::hevc::{hvcc_to_annex_b, length_prefixed_to_annex_b};
use std::io::{self, Write};

const SYNC_BYTE: u8 = 0x47;
const TS_PAYLOAD: usize = 184;

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
    /// Global PTS origin (nanoseconds), seeded by the FIRST video frame so
    /// the audio/video offset is preserved. Frames that arrive before it
    /// is set saturate to 0.
    base_pts_ns: Option<i64>,
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
            base_pts_ns: None,
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

        // For video: convert length-prefixed NALUs to Annex B (start codes).
        // Prepend codec_private parameter sets on the FIRST keyframe only.
        //
        // Arm `params_written` on the first video keyframe regardless of
        // whether it carries data: an empty-data keyframe still anchors
        // the stream, and leaving the flag unset would make every later
        // non-key frame fail the drop guard above and silently vanish.
        // For non-video the ES bytes pass through unchanged, so borrow
        // `data` directly rather than copying it; only video needs an
        // owned Annex-B conversion buffer.
        let es_data: std::borrow::Cow<'_, [u8]> = if is_video {
            let mut annex_b = Vec::new();
            if keyframe && !self.params_written[track] {
                if let Some(ref cp) = self.codec_privates[track] {
                    if let Some(params) = hvcc_to_annex_b(cp) {
                        annex_b.extend_from_slice(&params);
                    }
                }
                self.params_written[track] = true;
            }
            annex_b.extend_from_slice(&length_prefixed_to_annex_b(data));
            std::borrow::Cow::Owned(annex_b)
        } else {
            std::borrow::Cow::Borrowed(data)
        };

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
        if is_video || es_data.len() <= MAX_BD_PES_PAYLOAD {
            self.write_pes_chain(track, pid, pts_90k, is_video, keyframe, &es_data)?;
        } else {
            let mut first_pes = true;
            for chunk in es_data.chunks(MAX_BD_PES_PAYLOAD) {
                self.write_pes_chain(track, pid, pts_90k, is_video, keyframe && first_pes, chunk)?;
                first_pes = false;
            }
        }
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
            // i.e. af_bytes + payload_len = TS_PAYLOAD (184).
            // RAI on first packet of a keyframe video PES requires AF with flags=0x40.
            let want_rai = first && keyframe && is_video;

            // Pick payload_len and af_bytes per case.
            let (af_bytes, payload_len): (usize, usize) = if want_rai {
                // Minimum AF = 2 bytes (length=1, flags=0x40). Payload caps at 182.
                let max_payload = TS_PAYLOAD - 2;
                let p = remaining.min(max_payload);
                (TS_PAYLOAD - p, p)
            } else if remaining >= TS_PAYLOAD {
                (0, TS_PAYLOAD) // no AF, full payload
            } else {
                // Stuffing-only AF, payload = remaining.
                (TS_PAYLOAD - remaining, remaining)
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
    pub fn finish(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Build a PES packet header for a BD stream.
fn build_pes_header(pid: u16, pts_90k: u64, data_len: usize) -> Vec<u8> {
    // Determine stream_id from PID range
    let stream_id: u8 = if is_video_pid(pid) {
        0xE0 // video
    } else {
        0xBD // audio, PGS subtitle, or default (private stream 1)
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
    if stream_id == 0xE0 || pes_data_len > 65535 {
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

    const BD_PACKET_SIZE: usize = 192;
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
        for chunk in buf.chunks(BD_PACKET_SIZE) {
            if chunk.len() != BD_PACKET_SIZE {
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
            mux.finish().unwrap();
        }
        // Nothing should be emitted for that PID.
        let packets = parse_bd_ts(&sink);
        assert!(
            !packets.iter().any(|p| p.pid == VIDEO_PID),
            "non-key before first keyframe must be dropped"
        );
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
}
