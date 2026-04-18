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
    pub fn write_frame(&mut self, track: usize, pts_ns: i64, data: &[u8]) -> io::Result<()> {
        if track >= self.pids.len() {
            return Ok(()); // unknown track, skip
        }
        let base = *self.base_pts_ns.get_or_insert(pts_ns);
        let pts_ns = pts_ns - base;

        let pid = self.pids[track];
        let is_video = (0x1011..=0x101F).contains(&pid);

        // For video: convert length-prefixed NALUs to Annex B (start codes)
        // On first keyframe, prepend parameter sets from codec_private
        let es_data = if is_video && !data.is_empty() {
            let mut annex_b = Vec::new();
            // Prepend codec_private parameter sets on first keyframe
            if !self.params_written[track] {
                if let Some(ref cp) = self.codec_privates[track] {
                    if let Some(params) = hvcc_to_annex_b(cp) {
                        annex_b.extend_from_slice(&params);
                        self.params_written[track] = true;
                    }
                }
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
            let payload_len = remaining.min(TS_PAYLOAD);
            let need_stuffing = payload_len < TS_PAYLOAD;

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
            ts_header[3] = 0x10 | cc; // no adaptation, has payload

            if need_stuffing {
                // Adaptation field for stuffing
                let stuff_len = TS_PAYLOAD - payload_len;
                ts_header[3] = 0x30 | cc; // adaptation + payload

                self.writer.write_all(&tp_extra)?;
                self.writer.write_all(&ts_header)?;

                // Write adaptation field: length byte + flags byte + 0xFF padding
                // stuff_len == 1: AF length = 0 (just the length byte, no flags)
                // stuff_len >= 2: AF length = stuff_len-1, flags = 0, rest 0xFF
                static STUFF_FF: [u8; 184] = [0xFF; 184];
                if stuff_len == 1 {
                    self.writer.write_all(&[0u8])?; // adaptation_field_length = 0
                } else {
                    self.writer.write_all(&[(stuff_len - 1) as u8])?; // AF length
                    self.writer.write_all(&[0u8])?; // flags
                    if stuff_len > 2 {
                        self.writer.write_all(&STUFF_FF[..stuff_len - 2])?;
                    }
                }
                self.writer
                    .write_all(&pes_packet[offset..offset + payload_len])?;
            } else {
                self.writer.write_all(&tp_extra)?;
                self.writer.write_all(&ts_header)?;
                self.writer
                    .write_all(&pes_packet[offset..offset + payload_len])?;
            }

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

    if out.is_empty() {
        None
    } else {
        Some(out)
    }
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
