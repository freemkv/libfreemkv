//! BD Transport Stream muxer — PES frames → 192-byte BD-TS packets.
//!
//! Takes PES frames and writes them as BD-TS (Blu-ray transport stream)
//! packets. Each frame is wrapped in a PES header, split into TS packets,
//! and prepended with the 4-byte TP_extra_header.

use std::io::{self, Write};

const SYNC_BYTE: u8 = 0x47;
const TS_PAYLOAD: usize = 184;

pub struct TsMuxer<W: Write> {
    writer: W,
    pids: Vec<u16>,
    continuity: Vec<u8>,                  // per-PID continuity counter (0-15)
    codec_privates: Vec<Option<Vec<u8>>>, // per-track codec_private (for video parameter sets)
    params_written: Vec<bool>,            // per-track: have we written parameter sets?
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
    pub fn set_codec_private(&mut self, track: usize, data: Vec<u8>) {
        if track < self.codec_privates.len() {
            self.codec_privates[track] = Some(data);
        }
    }

    /// Write a PES frame as BD-TS packets.
    /// Video frame data is expected as length-prefixed NALUs (MKV/PES format)
    /// and is converted to Annex B for transport stream.
    pub fn write_frame(
        &mut self,
        track: usize,
        pts_ns: i64,
        keyframe: bool,
        data: &[u8],
    ) -> io::Result<()> {
        if track >= self.pids.len() {
            return Ok(()); // unknown track, skip
        }
        let pid = self.pids[track];
        let is_video = (0x1011..=0x101F).contains(&pid);

        // Drop non-key video before any keyframe — decoder has no IDR or
        // parameter sets to anchor on.
        if is_video && !keyframe && !self.params_written[track] {
            return Ok(());
        }

        let base = *self.base_pts_ns.get_or_insert(pts_ns);
        let pts_ns = pts_ns - base;

        // For video: convert length-prefixed NALUs to Annex B (start codes).
        // Prepend codec_private parameter sets on the FIRST keyframe only.
        let es_data = if is_video && !data.is_empty() {
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
            annex_b
        } else {
            data.to_vec()
        };

        // Build PES packet: header + data
        let pts_90k = if pts_ns >= 0 {
            (pts_ns as u64).saturating_mul(9) / 100_000
        } else {
            0
        };
        let pes_header = build_pes_header(pid, pts_90k, es_data.len());
        let pes_packet = [&pes_header[..], &es_data[..]].concat();

        // Split into TS packets
        let mut offset = 0;
        let mut first = true;
        while offset < pes_packet.len() {
            let remaining = pes_packet.len() - offset;

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

            self.writer
                .write_all(&pes_packet[offset..offset + payload_len])?;

            offset += payload_len;
            first = false;
        }

        Ok(())
    }

    pub fn finish(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Build a PES packet header for a BD stream.
fn build_pes_header(pid: u16, pts_90k: u64, data_len: usize) -> Vec<u8> {
    // Determine stream_id from PID range
    let stream_id: u8 = if (0x1011..=0x101F).contains(&pid) {
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

    // PES packet length (0 = unbounded for video or if too large for u16)
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

/// Extract NAL arrays from HEVCDecoderConfigurationRecord and convert to Annex B.
/// Returns VPS + SPS + PPS as Annex B NAL units (00 00 00 01 + NAL).
fn hvcc_to_annex_b(hvcc: &[u8]) -> Option<Vec<u8>> {
    // HEVCDecoderConfigurationRecord: 22 bytes header, then NAL arrays
    if hvcc.len() < 23 {
        return None;
    }
    let num_arrays = hvcc[22] as usize;
    let mut out = Vec::new();
    let mut offset = 23;

    for _ in 0..num_arrays {
        if offset + 3 > hvcc.len() {
            break;
        }
        // array: 1 byte (completeness + NAL type), 2 bytes (numNalus)
        let _nal_type = hvcc[offset] & 0x3F;
        let num_nalus = u16::from_be_bytes([hvcc[offset + 1], hvcc[offset + 2]]) as usize;
        offset += 3;

        for _ in 0..num_nalus {
            if offset + 2 > hvcc.len() {
                break;
            }
            let nal_len = u16::from_be_bytes([hvcc[offset], hvcc[offset + 1]]) as usize;
            offset += 2;
            if offset + nal_len > hvcc.len() {
                break;
            }
            out.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
            out.extend_from_slice(&hvcc[offset..offset + nal_len]);
            offset += nal_len;
        }
    }

    if out.is_empty() { None } else { Some(out) }
}

/// Convert length-prefixed NALUs (4-byte BE length + NAL) to Annex B
/// (00 00 00 01 + NAL). Used for video elementary streams in TS.
fn length_prefixed_to_annex_b(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len());
    let mut offset = 0;
    while offset + 4 <= data.len() {
        let len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;
        if offset + len > data.len() {
            break;
        }
        out.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
        out.extend_from_slice(&data[offset..offset + len]);
        offset += len;
    }
    // If data doesn't look like length-prefixed NALs (no valid parse),
    // return original data unchanged — it may already be Annex B.
    if out.is_empty() && !data.is_empty() {
        return data.to_vec();
    }
    out
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
            mux.set_codec_private(0, hvcc);
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
}
