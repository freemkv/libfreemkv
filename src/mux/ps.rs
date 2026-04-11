//! MPEG-2 Program Stream (PS) demuxer.
//!
//! DVDs use MPEG-2 Program Stream, which has:
//! - Pack headers (00 00 01 BA) with SCR timestamps
//! - PES packets (00 00 01 [stream_id]) with variable length
//! - System headers (00 00 01 BB)
//! - Program end code (00 00 01 B9)
//!
//! Stream IDs:
//! - 0xE0-0xEF: video (usually 0xE0)
//! - 0xC0-0xDF: MPEG audio
//! - 0xBD: private stream 1 (AC3, DTS, LPCM, subtitles via sub-stream ID)

/// Pack header start code suffix.
const PACK_HEADER_ID: u8 = 0xBA;

/// System header start code suffix.
const SYSTEM_HEADER_ID: u8 = 0xBB;

/// Program end start code suffix.
const PROGRAM_END_ID: u8 = 0xB9;

/// Private stream 1 (AC3, DTS, LPCM, subtitles).
const PRIVATE_STREAM_1: u8 = 0xBD;

/// A demuxed PES packet from the Program Stream.
#[derive(Debug, Clone)]
pub struct PsPacket {
    /// PES stream ID (0xE0 for video, 0xC0 for audio, 0xBD for private, etc.).
    pub stream_id: u8,
    /// Sub-stream ID for private stream 1 (AC3: 0x80-0x87, DTS: 0x88-0x8F,
    /// LPCM: 0xA0-0xA7, subtitles: 0x20-0x3F).
    pub sub_stream_id: Option<u8>,
    /// Presentation timestamp in 90kHz ticks.
    pub pts: Option<u64>,
    /// Decode timestamp in 90kHz ticks.
    pub dts: Option<u64>,
    /// Elementary stream payload data.
    pub data: Vec<u8>,
}

/// MPEG-2 Program Stream demuxer.
///
/// Accepts raw PS bytes via `feed()` and produces demuxed PES packets.
/// Handles non-aligned input by buffering leftover bytes between calls.
pub struct PsDemuxer {
    buffer: Vec<u8>,
}

impl Default for PsDemuxer {
    fn default() -> Self {
        Self::new()
    }
}

impl PsDemuxer {
    /// Create a new Program Stream demuxer.
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(64 * 1024),
        }
    }

    /// Feed raw MPEG-2 PS bytes, returning any completely parsed PES packets.
    pub fn feed(&mut self, data: &[u8]) -> Vec<PsPacket> {
        self.buffer.extend_from_slice(data);
        self.extract_packets()
    }

    /// Flush remaining buffered data, returning any final PES packets.
    pub fn flush(&mut self) -> Vec<PsPacket> {
        // Try to extract whatever remains. If the buffer contains an incomplete
        // PES packet we cannot parse, it will be discarded.
        let packets = self.extract_packets();
        self.buffer.clear();
        packets
    }

    /// Scan the buffer for complete start-code-delimited units and parse them.
    fn extract_packets(&mut self) -> Vec<PsPacket> {
        let mut packets = Vec::with_capacity(4);
        let mut pos = 0;

        while let Some(sc) = find_start_code(&self.buffer, pos) {

            if sc + 3 >= self.buffer.len() {
                // Not enough bytes to read the start code ID.
                break;
            }

            let code = self.buffer[sc + 3];

            match code {
                PROGRAM_END_ID => {
                    // 00 00 01 B9 — 4 bytes, no payload.
                    pos = sc + 4;
                }
                PACK_HEADER_ID => {
                    // Pack header: need at least 14 bytes for MPEG-2 pack.
                    if sc + 14 > self.buffer.len() {
                        break; // wait for more data
                    }
                    // MPEG-2 packs have bit pattern 01 in bits 7-6 of byte 4.
                    let stuffing = (self.buffer[sc + 13] & 0x07) as usize;
                    let pack_len = 14 + stuffing;
                    if sc + pack_len > self.buffer.len() {
                        break;
                    }
                    pos = sc + pack_len;
                }
                SYSTEM_HEADER_ID => {
                    // System header: 00 00 01 BB [length:2] ...
                    if sc + 6 > self.buffer.len() {
                        break;
                    }
                    let header_len =
                        ((self.buffer[sc + 4] as usize) << 8) | self.buffer[sc + 5] as usize;
                    let total = 6 + header_len;
                    if sc + total > self.buffer.len() {
                        break;
                    }
                    pos = sc + total;
                }
                id if is_pes_stream_id(id) => {
                    // PES packet: 00 00 01 [stream_id] [length:2] ...
                    if sc + 6 > self.buffer.len() {
                        break;
                    }
                    let pes_packet_len =
                        ((self.buffer[sc + 4] as usize) << 8) | self.buffer[sc + 5] as usize;

                    // Total bytes = 6 (start code + stream_id + length) + pes_packet_len.
                    // A length of 0 means unbounded (video streams); in that case we need
                    // to find the next start code to delimit the packet.
                    let end = if pes_packet_len == 0 {
                        // Find the next start code after this one.
                        match find_start_code(&self.buffer, sc + 4) {
                            Some(next_sc) => next_sc,
                            None => break, // wait for more data
                        }
                    } else {
                        let e = sc + 6 + pes_packet_len;
                        if e > self.buffer.len() {
                            break; // wait for more data
                        }
                        e
                    };

                    if let Some(pkt) = parse_pes_packet(&self.buffer[sc..end]) {
                        packets.push(pkt);
                    }
                    pos = end;
                }
                _ => {
                    // Unknown start code — skip past it.
                    pos = sc + 4;
                }
            }
        }

        if pos > 0 {
            self.buffer.drain(..pos);
        }

        packets
    }
}

/// Check whether a start code byte is a valid PES stream ID that carries payload.
fn is_pes_stream_id(id: u8) -> bool {
    // Video: 0xE0-0xEF, MPEG audio: 0xC0-0xDF, private stream 1: 0xBD,
    // private stream 2: 0xBF, padding: 0xBE, ECM/EMM etc.
    // We parse anything in the PES range.
    matches!(id, 0xBD..=0xEF)
}

/// Parse a single PES packet from a byte slice that starts at the start code.
fn parse_pes_packet(data: &[u8]) -> Option<PsPacket> {
    // Minimum: 00 00 01 [id] [len:2] = 6 bytes
    if data.len() < 6 {
        return None;
    }
    if data[0] != 0x00 || data[1] != 0x00 || data[2] != 0x01 {
        return None;
    }

    let stream_id = data[3];

    // Padding stream — skip entirely.
    if stream_id == 0xBE {
        return None;
    }

    // Streams without standard PES header extension.
    if stream_id == 0xBF {
        let payload = if data.len() > 6 { &data[6..] } else { &[] };
        return Some(PsPacket {
            stream_id,
            sub_stream_id: None,
            pts: None,
            dts: None,
            data: payload.to_vec(),
        });
    }

    // Standard PES header: [6]=flags1, [7]=flags2, [8]=header_data_length
    if data.len() < 9 {
        return None;
    }

    let pts_dts_flags = (data[7] >> 6) & 0x03;
    let header_data_len = data[8] as usize;
    let header_end = 9 + header_data_len;

    if header_end > data.len() {
        return None;
    }

    let mut pts = None;
    let mut dts = None;

    if pts_dts_flags >= 2 && data.len() >= 14 {
        pts = Some(parse_pts(&data[9..14]));
    }
    if pts_dts_flags == 3 && data.len() >= 19 {
        dts = Some(parse_pts(&data[14..19]));
    }

    let payload = &data[header_end..];

    // For private stream 1, the first payload byte is the sub-stream ID.
    let (sub_stream_id, es_data) = if stream_id == PRIVATE_STREAM_1 && !payload.is_empty() {
        (Some(payload[0]), payload[1..].to_vec())
    } else {
        (None, payload.to_vec())
    };

    Some(PsPacket {
        stream_id,
        sub_stream_id,
        pts,
        dts,
        data: es_data,
    })
}

/// Parse a 5-byte PTS/DTS timestamp field (33 bits at 90kHz).
///
/// Layout:
/// ```text
/// byte0: [marker_4bits][bit32][marker_1]
/// byte1: [bits 31..24]
/// byte2: [bits 23..15][marker_1]
/// byte3: [bits 14..7]
/// byte4: [bits 6..0][marker_1]
/// ```
fn parse_pts(buf: &[u8]) -> u64 {
    debug_assert!(buf.len() >= 5);
    let b0 = buf[0] as u64;
    let b1 = buf[1] as u64;
    let b2 = buf[2] as u64;
    let b3 = buf[3] as u64;
    let b4 = buf[4] as u64;

    ((b0 >> 1) & 0x07) << 30 | b1 << 22 | (b2 >> 1) << 15 | b3 << 7 | b4 >> 1
}

/// Find the position of the next start code (00 00 01) at or after `from`.
fn find_start_code(data: &[u8], from: usize) -> Option<usize> {
    if data.len() < from + 3 {
        return None;
    }
    (from..data.len() - 2).find(|&i| data[i] == 0x00 && data[i + 1] == 0x00 && data[i + 2] == 0x01)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Pack header detection ---

    #[test]
    fn detect_pack_header() {
        let mut demuxer = PsDemuxer::new();

        // MPEG-2 pack header: 14 bytes, stuffing_length = 0
        let mut pack = vec![
            0x00, 0x00, 0x01, 0xBA, // start code
            0x44, 0x00, 0x04, 0x00, 0x04, 0x01, // SCR (6 bytes)
            0x01, 0x89, 0xC3, // mux_rate (3 bytes)
            0xF8, // stuffing_length = 0 (lower 3 bits)
        ];

        // Follow with a PES packet so we have a delimiter
        pack.extend_from_slice(&[
            0x00, 0x00, 0x01, 0xE0, // video stream
            0x00, 0x08, // length = 8
            0x80, 0x00, 0x00, // flags: no PTS/DTS, header_data_length = 0
            0xAA, 0xBB, 0xCC, 0xDD, 0xEE, // payload (5 bytes)
        ]);

        let packets = demuxer.feed(&pack);
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].stream_id, 0xE0);
        assert_eq!(packets[0].data, vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE]);
    }

    #[test]
    fn pack_header_with_stuffing() {
        let mut demuxer = PsDemuxer::new();

        // Pack header with 3 stuffing bytes
        let mut data = vec![
            0x00, 0x00, 0x01, 0xBA, 0x44, 0x00, 0x04, 0x00, 0x04, 0x01, 0x01, 0x89, 0xC3,
            0xFB, // stuffing_length = 3
            0xFF, 0xFF, 0xFF, // stuffing bytes
        ];

        // Followed by a PES packet
        data.extend_from_slice(&[
            0x00, 0x00, 0x01, 0xC0, // audio stream
            0x00, 0x05, // length = 5
            0x80, 0x00, 0x00, // flags: no PTS, header_data_len=0
            0x11, 0x22, // payload
        ]);

        let packets = demuxer.feed(&data);
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].stream_id, 0xC0);
        assert_eq!(packets[0].data, vec![0x11, 0x22]);
    }

    // --- PES header + PTS parsing ---

    #[test]
    fn pes_header_with_pts() {
        let mut demuxer = PsDemuxer::new();

        // PTS = 90000 (1 second at 90kHz)
        // 90000 = 0x15F90
        // bit32=0, bits 29-15 = 0x0002BF, bits 14-0 = 0x1F90
        // byte0: 0010_0_1 = 0x21 ... actually let's encode properly:
        //
        // pts = 90000
        // byte0: (0010 << 4) | ((pts >> 29) & 0x0E) | 1
        //      = 0x20 | ((90000 >> 29) & 0x0E) | 1 = 0x20 | 0 | 1 = 0x21
        // byte1: (pts >> 22) & 0xFF = (90000 >> 22) & 0xFF = 0
        // byte2: ((pts >> 14) & 0xFE) | 1 = ((90000 >> 14) & 0xFE) | 1 = (0x0A & 0xFE) | 1 = 0x0B
        // byte3: (pts >> 7) & 0xFF = (90000 >> 7) & 0xFF = (703) & 0xFF = 0xBF
        // byte4: ((pts & 0x7F) << 1) | 1 = ((90000 & 0x7F) << 1) | 1 = (0x10 << 1) | 1 = 0x21

        let pts_bytes = encode_pts(90000, 0x20);

        let mut data = vec![
            0x00, 0x00, 0x01, 0xE0, // video stream
            0x00, 0x0D, // length = 13
            0x80, 0x80, 0x05, // flags: PTS only, header_data_len=5
        ];
        data.extend_from_slice(&pts_bytes);
        data.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00]); // payload

        // Add a delimiter
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]); // program end

        let packets = demuxer.feed(&data);
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].stream_id, 0xE0);
        assert_eq!(packets[0].pts, Some(90000));
        assert!(packets[0].dts.is_none());
        assert_eq!(packets[0].data, vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00]);
    }

    #[test]
    fn pes_header_with_pts_and_dts() {
        let mut demuxer = PsDemuxer::new();

        let pts_bytes = encode_pts(180000, 0x30); // PTS marker = 0x30
        let dts_bytes = encode_pts(90000, 0x10); // DTS marker = 0x10

        let mut data = vec![
            0x00, 0x00, 0x01, 0xE0, 0x00, 0x11, // length = 17
            0x80, 0xC0, 0x0A, // flags: PTS+DTS, header_data_len=10
        ];
        data.extend_from_slice(&pts_bytes);
        data.extend_from_slice(&dts_bytes);
        data.extend_from_slice(&[0xCA, 0xFE]); // payload

        data.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]);

        let packets = demuxer.feed(&data);
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].pts, Some(180000));
        assert_eq!(packets[0].dts, Some(90000));
    }

    // --- Private stream 1 sub-stream extraction ---

    #[test]
    fn private_stream_1_ac3_substream() {
        let mut demuxer = PsDemuxer::new();

        let mut data = vec![
            0x00, 0x00, 0x01, 0xBD, // private stream 1
            0x00, 0x08, // length = 8
            0x80, 0x00, 0x00, // no PTS, header_data_len=0
            0x80, // sub-stream ID: AC3 stream 0
            0xAA, 0xBB, 0xCC, 0xDD, // AC3 payload
        ];

        data.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]);

        let packets = demuxer.feed(&data);
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].stream_id, 0xBD);
        assert_eq!(packets[0].sub_stream_id, Some(0x80));
        assert_eq!(packets[0].data, vec![0xAA, 0xBB, 0xCC, 0xDD]);
    }

    #[test]
    fn private_stream_1_dts_substream() {
        let mut demuxer = PsDemuxer::new();

        let mut data = vec![
            0x00, 0x00, 0x01, 0xBD, 0x00, 0x06, // length = 6
            0x80, 0x00, 0x00, 0x88, // sub-stream ID: DTS stream 0
            0x11, 0x22,
        ];
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]);

        let packets = demuxer.feed(&data);
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].sub_stream_id, Some(0x88));
    }

    #[test]
    fn private_stream_1_subtitle_substream() {
        let mut demuxer = PsDemuxer::new();

        let mut data = vec![
            0x00, 0x00, 0x01, 0xBD, 0x00, 0x06, 0x80, 0x00, 0x00,
            0x20, // sub-stream ID: subtitle stream 0
            0xFF, 0xFE,
        ];
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]);

        let packets = demuxer.feed(&data);
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].sub_stream_id, Some(0x20));
    }

    #[test]
    fn private_stream_1_lpcm_substream() {
        let mut demuxer = PsDemuxer::new();

        let mut data = vec![
            0x00, 0x00, 0x01, 0xBD, 0x00, 0x06, 0x80, 0x00, 0x00,
            0xA0, // sub-stream ID: LPCM stream 0
            0x01, 0x02,
        ];
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]);

        let packets = demuxer.feed(&data);
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].sub_stream_id, Some(0xA0));
    }

    // --- Incremental feeding ---

    #[test]
    fn incremental_feed() {
        let mut demuxer = PsDemuxer::new();

        let mut full = vec![
            0x00, 0x00, 0x01, 0xE0, 0x00, 0x06, // length = 6
            0x80, 0x00, 0x00, // no PTS, header_data_len=0
            0xAA, 0xBB, 0xCC,
        ];
        full.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]);

        // Feed in two halves
        let mid = full.len() / 2;
        let p1 = demuxer.feed(&full[..mid]);
        assert!(p1.is_empty(), "first half should not produce packets");

        let p2 = demuxer.feed(&full[mid..]);
        assert_eq!(p2.len(), 1);
        assert_eq!(p2[0].data, vec![0xAA, 0xBB, 0xCC]);
    }

    // --- Multiple PES packets ---

    #[test]
    fn multiple_pes_packets() {
        let mut demuxer = PsDemuxer::new();

        let mut data = Vec::new();

        // First PES: video
        data.extend_from_slice(&[
            0x00, 0x00, 0x01, 0xE0, 0x00, 0x05, 0x80, 0x00, 0x00, 0x11, 0x22,
        ]);

        // Second PES: audio
        data.extend_from_slice(&[
            0x00, 0x00, 0x01, 0xC0, 0x00, 0x05, 0x80, 0x00, 0x00, 0x33, 0x44,
        ]);

        // Delimiter
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]);

        let packets = demuxer.feed(&data);
        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0].stream_id, 0xE0);
        assert_eq!(packets[1].stream_id, 0xC0);
    }

    // --- PTS parsing edge cases ---

    #[test]
    fn pts_zero() {
        // PTS = 0 encoded
        let pts = parse_pts(&encode_pts(0, 0x20));
        assert_eq!(pts, 0);
    }

    #[test]
    fn pts_large_value() {
        // Test a large PTS value (close to 33-bit max)
        let val: u64 = (1 << 32) - 1; // 0xFFFFFFFF
        let encoded = encode_pts(val, 0x20);
        let decoded = parse_pts(&encoded);
        assert_eq!(decoded, val);
    }

    // --- Helper: encode PTS for tests ---

    fn encode_pts(pts: u64, marker_prefix: u8) -> [u8; 5] {
        let mut buf = [0u8; 5];
        buf[0] = marker_prefix | (((pts >> 30) as u8) & 0x07) << 1 | 1;
        buf[1] = ((pts >> 22) & 0xFF) as u8;
        buf[2] = (((pts >> 15) & 0x7F) as u8) << 1 | 1;
        buf[3] = ((pts >> 7) & 0xFF) as u8;
        buf[4] = (((pts) & 0x7F) as u8) << 1 | 1;
        buf
    }
}
