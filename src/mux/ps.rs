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

use super::codec::startcode::find_start_code;

/// Pack header start code suffix.
const PACK_HEADER_ID: u8 = 0xBA;

/// System header start code suffix.
const SYSTEM_HEADER_ID: u8 = 0xBB;

/// Program end start code suffix.
const PROGRAM_END_ID: u8 = 0xB9;

/// Private stream 1 (AC3, DTS, LPCM, subtitles).
const PRIVATE_STREAM_1: u8 = 0xBD;

/// Hard cap on the demuxer's reassembly buffer. A length-0 (unbounded) video
/// PES is delimited by the next PS-layer boundary; if a corrupt stream declares
/// an unbounded PES and never follows it with a boundary, `feed()` would
/// otherwise accumulate the entire input. Past this cap we force the in-progress
/// unbounded PES to flush at the buffer end so untrusted input cannot drive
/// unbounded allocation. A real DVD pack/PES is at most a few KB; this leaves
/// generous slack while still bounding worst-case memory.
const MAX_PS_BUFFER: usize = 4 * 1024 * 1024;

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

/// Canonical DVD video PID. DVD-Video carries a single MPEG-2 video
/// elementary stream; both the scanner and the muxer use this PID.
pub const DVD_VIDEO_PID: u16 = 0xE0;

/// Canonical PID for a `private_stream_1` audio stream identified by its
/// on-wire sub-stream id. Returns `None` for sub-ids outside the AC-3 /
/// DTS / LPCM audio ranges.
///
/// The PID is `0xBD00 | sub_stream_id`, which is unique per sub-stream id
/// (AC-3 / DTS `0x80..=0x8F`, LPCM `0xA0..=0xA7`). Unlike the old
/// per-codec relative arithmetic, distinct sub-ids therefore always yield
/// distinct PIDs — so a mixed-codec title (e.g. AC-3 + DTS, whose sub-ids
/// are 0x80 and 0x88) can never collide on one PID. This is the single
/// source of truth shared with `Disc::scan_dvd_titles`
/// (`src/disc/dvd.rs`), which sets each `AudioStream.pid` from the same
/// function so demuxer output routes through the title's `pid_to_track`.
pub fn dvd_audio_pid(sub_stream_id: u8) -> Option<u16> {
    match sub_stream_id {
        0x80..=0x8F | 0xA0..=0xA7 => Some(0xBD00 | sub_stream_id as u16),
        _ => None,
    }
}

/// Canonical PID for a VobSub subtitle stream identified by its on-wire
/// sub-stream id (`0x20..=0x3F`). The PID is the sub-id itself (identity),
/// which never overlaps the `0xBD..` audio PID space.
pub fn dvd_subtitle_pid(sub_stream_id: u8) -> Option<u16> {
    match sub_stream_id {
        0x20..=0x3F => Some(sub_stream_id as u16),
        _ => None,
    }
}

impl PsPacket {
    /// Map this packet to the canonical DVD PID assigned by
    /// `Disc::scan_dvd_titles` (`src/disc/dvd.rs`), so demux output can
    /// be looked up in the title's `pid_to_track` map.
    ///
    /// Routes by the REAL on-wire `(stream_id, sub_stream_id)` via the
    /// shared [`dvd_audio_pid`] / [`dvd_subtitle_pid`] tables the scanner
    /// also uses — never per-codec relative arithmetic, which collided on
    /// mixed-codec audio (AC-3 0x80 and DTS 0x88 both mapping to 0xBD00).
    ///
    /// Returns `None` for stream/sub-stream combinations the DVD title
    /// scanner does not assign a PID to (e.g. MPEG audio 0xC0-0xDF,
    /// private stream 2, unrecognized sub-stream ranges). The caller is
    /// expected to WARN-and-drop in that case rather than silently
    /// mis-routing the packet.
    pub fn dvd_pid(&self) -> Option<u16> {
        match self.stream_id {
            0xE0..=0xEF => Some(DVD_VIDEO_PID),
            0xBD => {
                let sub = self.sub_stream_id?;
                dvd_audio_pid(sub).or_else(|| dvd_subtitle_pid(sub))
            }
            _ => None,
        }
    }
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
        self.extract_packets(false)
    }

    /// Flush remaining buffered data, returning any final PES packets.
    pub fn flush(&mut self) -> Vec<PsPacket> {
        // At EOF, an unbounded (length 0) PES with no trailing start code is
        // a complete-but-unterminated final packet — emit it rather than
        // dropping the tail of the last frame. Genuinely incomplete packets
        // (a length-bounded PES short of its declared size) are still
        // discarded.
        let packets = self.extract_packets(true);
        self.buffer.clear();
        packets
    }

    /// Scan the buffer for complete start-code-delimited units and parse
    /// them. When `flushing` is true, a trailing unbounded PES that has no
    /// following start code is emitted using the rest of the buffer as its
    /// payload (EOF terminates it).
    fn extract_packets(&mut self, flushing: bool) -> Vec<PsPacket> {
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
                    // DVD-Video is always MPEG-2 PS, so every 0xBA is treated
                    // as a 14-byte MPEG-2 pack: the low 3 bits of byte 13 are
                    // pack_stuffing_length. (An MPEG-1 pack would be 12 bytes
                    // with no stuffing field, but DVD never emits one.)
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
                    // A length of 0 means unbounded (video streams); in that
                    // case the packet runs to the next PS-LAYER boundary (pack /
                    // system header / program end / next PES), NOT the next raw
                    // start code — the video ES payload is itself full of
                    // 00 00 01 xx codes that would otherwise cut the PES short.
                    let end = if pes_packet_len == 0 {
                        match find_ps_boundary(&self.buffer, sc + 4) {
                            Some(next) => next,
                            // At EOF the rest of the buffer is this PES's
                            // payload — emit it.
                            None if flushing => self.buffer.len(),
                            None => {
                                // No boundary buffered yet. Normally wait for
                                // more data, but a corrupt stream could declare
                                // an unbounded PES followed by endless non-
                                // boundary bytes — bounding the buffer here
                                // stops untrusted input forcing unbounded
                                // allocation. Past the cap, flush what we have.
                                if self.buffer.len() - sc > MAX_PS_BUFFER {
                                    self.buffer.len()
                                } else {
                                    break; // wait for more data
                                }
                            }
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

/// Find the next PS-layer unit boundary at or after `from`: a start code whose
/// ID byte is a pack (0xBA), system header (0xBB), program-end (0xB9), or a
/// payload-carrying PES stream ID (0xBD..=0xEF).
///
/// A length-0 (unbounded) video PES must be delimited by the next PS-layer unit
/// — NOT by the next raw `00 00 01`. The MPEG-2 video elementary stream inside
/// the PES is itself full of `00 00 01 xx` start codes (picture 0x00, slices
/// 0x01..=0xAF, GOP 0xB8, sequence 0xB3); a plain start-code scan would cut the
/// PES inside its own payload and re-scan the discarded video bytes as bogus PS
/// units. Restricting the search to PS-layer IDs (>= 0xB9, excluding the video
/// ES codes below it) frames the unbounded PES at the right boundary.
fn find_ps_boundary(data: &[u8], from: usize) -> Option<usize> {
    let mut pos = from;
    while let Some(sc) = find_start_code(data, pos) {
        if sc + 3 >= data.len() {
            return None;
        }
        let id = data[sc + 3];
        if id == PACK_HEADER_ID
            || id == SYSTEM_HEADER_ID
            || id == PROGRAM_END_ID
            || is_pes_stream_id(id)
        {
            return Some(sc);
        }
        pos = sc + 4;
    }
    None
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

    // The PTS (5 bytes at data[9..14]) and DTS (5 bytes at data[14..19])
    // live INSIDE the PES header, so gate on header_data_len covering them
    // (>=5 for PTS, >=10 for PTS+DTS), not merely on total length. A
    // non-conformant packet that sets the flags but declares a too-short
    // header would otherwise read payload bytes as a bogus timestamp.
    if pts_dts_flags >= 2 && header_data_len >= 5 && data.len() >= 14 {
        pts = Some(parse_pts(&data[9..14]));
    }
    if pts_dts_flags == 3 && header_data_len >= 10 && data.len() >= 19 {
        dts = Some(parse_pts(&data[14..19]));
    }

    let payload = &data[header_end..];

    // For private stream 1, the first payload byte is the sub-stream ID,
    // followed by a sub-header whose length depends on the sub-stream type.
    let (sub_stream_id, es_data) = if stream_id == PRIVATE_STREAM_1 && !payload.is_empty() {
        let sub_id = payload[0];
        let skip = match sub_id {
            0x80..=0x8F => 4, // AC3/DTS: sub_id + frame_count + access_unit_ptr(2)
            0xA0..=0xA7 => 7, // LPCM: sub_id + frames + ptr(2) + emphasis + quant_freq + channels
            _ => 1,
        };
        let start = skip.min(payload.len());
        (Some(sub_id), payload[start..].to_vec())
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
/// Layout (ISO/IEC 13818-1 Table 2-17):
/// ```text
/// byte0: [prefix:4][pts 32..30:3][marker:1]
/// byte1: [pts 29..22:8]
/// byte2: [pts 21..15:7][marker:1]
/// byte3: [pts 14..7:8]
/// byte4: [pts 6..0:7][marker:1]
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

        // AC3 sub-header: sub_id(1) + frame_count(1) + access_unit_ptr(2) = 4 bytes
        let mut data = vec![
            0x00, 0x00, 0x01, 0xBD, // private stream 1
            0x00, 0x0B, // length = 11
            0x80, 0x00, 0x00, // no PTS, header_data_len=0
            0x80, // sub-stream ID: AC3 stream 0
            0x01, 0x00, 0x02, // frame_count + access_unit_ptr (sub-header bytes)
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

        // DTS sub-header: sub_id(1) + frame_count(1) + access_unit_ptr(2) = 4 bytes
        let mut data = vec![
            0x00, 0x00, 0x01, 0xBD, 0x00, 0x09, // length = 9
            0x80, 0x00, 0x00, // no PTS, header_data_len=0
            0x88, // sub-stream ID: DTS stream 0
            0x01, 0x00, 0x00, // sub-header (frame_count + access_unit_ptr)
            0x11, 0x22,
        ];
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]);

        let packets = demuxer.feed(&data);
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].sub_stream_id, Some(0x88));
        assert_eq!(packets[0].data, vec![0x11, 0x22]);
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

        // LPCM sub-header: sub_id(1) + frames(1) + ptr(2) + emphasis(1) + quant_freq(1) + channels(1) = 7 bytes
        let mut data = vec![
            0x00, 0x00, 0x01, 0xBD, 0x00, 0x0C, // length = 12
            0x80, 0x00, 0x00, // no PTS, header_data_len=0
            0xA0, // sub-stream ID: LPCM stream 0
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // LPCM sub-header (6 bytes after sub_id)
            0x01, 0x02, // LPCM payload
        ];
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]);

        let packets = demuxer.feed(&data);
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].sub_stream_id, Some(0xA0));
        assert_eq!(packets[0].data, vec![0x01, 0x02]);
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

    #[test]
    fn flush_emits_trailing_unbounded_video_pes() {
        let mut demuxer = PsDemuxer::new();
        // Unbounded (length 0) video PES with no trailing start code — the
        // common EOF case. feed() must not emit it (awaiting a delimiter),
        // but flush() must emit the tail rather than discarding it.
        let data = vec![
            0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, // video, length 0 (unbounded)
            0x80, 0x00, 0x00, // no PTS, header_data_len = 0
            0xAA, 0xBB, 0xCC, 0xDD,
        ];
        let fed = demuxer.feed(&data);
        assert!(fed.is_empty(), "unbounded PES not emitted until delimited");
        let flushed = demuxer.flush();
        assert_eq!(flushed.len(), 1, "flush emits the trailing PES");
        assert_eq!(flushed[0].stream_id, 0xE0);
        assert_eq!(flushed[0].data, vec![0xAA, 0xBB, 0xCC, 0xDD]);
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

    // --- unbounded (length-0) video PES framing ---

    #[test]
    fn unbounded_video_pes_not_cut_by_embedded_start_codes() {
        // A length-0 video PES whose ES payload contains embedded MPEG start
        // codes (picture 0x00, slice 0x01, GOP 0xB8, sequence 0xB3) must be
        // delimited by the NEXT PS-layer boundary (here a program-end 0xB9),
        // not by the first embedded 00 00 01 inside the payload.
        let mut demuxer = PsDemuxer::new();

        let mut data = vec![
            0x00, 0x00, 0x01, 0xE0, // video stream
            0x00, 0x00, // length = 0 (unbounded)
            0x80, 0x00, 0x00, // flags: no PTS, header_data_len = 0
        ];
        // ES payload with embedded MPEG-2 start codes.
        let payload = [
            0x00, 0x00, 0x01, 0xB3, // sequence header
            0x11, 0x22, 0x00, 0x00, 0x01, 0x00, // picture start code
            0x33, 0x44, 0x00, 0x00, 0x01, 0x01, // slice
            0x55, 0x66,
        ];
        data.extend_from_slice(&payload);
        // PS-layer boundary that closes the unbounded PES.
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]);

        let packets = demuxer.feed(&data);
        assert_eq!(packets.len(), 1, "one PES, not several payload fragments");
        assert_eq!(packets[0].stream_id, 0xE0);
        // The whole ES payload survives — none of it discarded as bogus units.
        assert_eq!(packets[0].data, payload.to_vec());
    }

    #[test]
    fn unbounded_video_pes_waits_for_boundary() {
        // Without a following PS-layer boundary the unbounded PES is held
        // (waiting for more data), not emitted truncated.
        let mut demuxer = PsDemuxer::new();
        let mut data = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        data.extend_from_slice(&[0x00, 0x00, 0x01, 0x00, 0xAA, 0xBB]); // picture SC, no PS boundary
        let packets = demuxer.feed(&data);
        assert!(packets.is_empty(), "no PS boundary yet → hold the PES");
    }

    #[test]
    fn unbounded_video_pes_buffer_is_bounded() {
        // A corrupt stream declaring an unbounded PES followed by endless
        // non-boundary bytes must not grow the buffer without limit.
        let mut demuxer = PsDemuxer::new();
        let header = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        let packets = demuxer.feed(&header);
        assert!(packets.is_empty());
        // Feed >MAX_PS_BUFFER of bytes containing no PS-layer boundary.
        let chunk = vec![0x55u8; 1024 * 1024];
        let mut emitted = 0;
        for _ in 0..(MAX_PS_BUFFER / chunk.len() + 4) {
            emitted += demuxer.feed(&chunk).len();
        }
        assert!(
            demuxer.buffer.len() <= MAX_PS_BUFFER + chunk.len(),
            "buffer grew to {} (cap {})",
            demuxer.buffer.len(),
            MAX_PS_BUFFER
        );
        // The force-flush emits the over-long PES rather than accumulating it.
        assert!(emitted >= 1, "over-cap unbounded PES is force-flushed");
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

    // --- DVD PID mapping (track-routing collision regression) ---

    fn mk(stream_id: u8, sub: Option<u8>) -> PsPacket {
        PsPacket {
            stream_id,
            sub_stream_id: sub,
            pts: None,
            dts: None,
            data: vec![0xAA],
        }
    }

    #[test]
    fn dvd_pid_matches_scanner_assignment() {
        // Video → 0xE0 (matches dvd.rs VideoStream pid).
        assert_eq!(mk(0xE0, None).dvd_pid(), Some(DVD_VIDEO_PID));
        // PID = 0xBD00 | sub_stream_id — unique per sub-id, no collision.
        assert_eq!(mk(0xBD, Some(0x80)).dvd_pid(), Some(0xBD80)); // AC-3 #0
        assert_eq!(mk(0xBD, Some(0x81)).dvd_pid(), Some(0xBD81)); // AC-3 #1
        assert_eq!(mk(0xBD, Some(0x88)).dvd_pid(), Some(0xBD88)); // DTS  #0
        assert_eq!(mk(0xBD, Some(0xA0)).dvd_pid(), Some(0xBDA0)); // LPCM #0
        // VobSub subtitle 0x20/0x21 → 0x20 / 0x21 (identity).
        assert_eq!(mk(0xBD, Some(0x20)).dvd_pid(), Some(0x20));
        assert_eq!(mk(0xBD, Some(0x21)).dvd_pid(), Some(0x21));
        // Unmappable: MPEG audio, private stream 2, bogus sub-id.
        assert_eq!(mk(0xC0, None).dvd_pid(), None);
        assert_eq!(mk(0xBF, None).dvd_pid(), None);
        assert_eq!(mk(0xBD, Some(0x10)).dvd_pid(), None);
    }

    #[test]
    fn mixed_codec_audio_does_not_collide() {
        // The core regression: a title mixing AC-3 (0x80), DTS (0x88) and
        // LPCM (0xA0) audio. The old per-codec relative arithmetic mapped
        // all three to 0xBD00. They must now get distinct PIDs that match
        // what dvd.rs assigns from the same dvd_audio_pid() table.
        let ac3 = mk(0xBD, Some(0x80)).dvd_pid().unwrap();
        let dts = mk(0xBD, Some(0x88)).dvd_pid().unwrap();
        let lpcm = mk(0xBD, Some(0xA0)).dvd_pid().unwrap();
        assert_ne!(ac3, dts, "AC-3 and DTS must not collide");
        assert_ne!(ac3, lpcm, "AC-3 and LPCM must not collide");
        assert_ne!(dts, lpcm, "DTS and LPCM must not collide");

        // Scanner side uses the same table; build a pid_to_track for a
        // mixed-codec title [video, AC-3, DTS, LPCM, sub] and route every
        // PS packet to its own distinct track.
        let pid_to_track: Vec<(u16, usize)> = vec![
            (DVD_VIDEO_PID, 0),
            (dvd_audio_pid(0x80).unwrap(), 1),
            (dvd_audio_pid(0x88).unwrap(), 2),
            (dvd_audio_pid(0xA0).unwrap(), 3),
            (dvd_subtitle_pid(0x20).unwrap(), 4),
        ];
        let route = |p: PsPacket| -> Option<usize> {
            let pid = p.dvd_pid()?;
            pid_to_track
                .iter()
                .find(|(x, _)| *x == pid)
                .map(|(_, t)| *t)
        };
        assert_eq!(route(mk(0xE0, None)), Some(0));
        assert_eq!(route(mk(0xBD, Some(0x80))), Some(1)); // AC-3 → its own track
        assert_eq!(route(mk(0xBD, Some(0x88))), Some(2)); // DTS  → its own track
        assert_eq!(route(mk(0xBD, Some(0xA0))), Some(3)); // LPCM → its own track
        assert_eq!(route(mk(0xBD, Some(0x20))), Some(4)); // sub  → its own track
    }

    #[test]
    fn subtitle_does_not_collide_with_audio_track() {
        // Subtitle sub-id 0x20 routes to its own subtitle PID (0x20),
        // distinct from any audio PID (0xBD80+).
        let audio0 = mk(0xBD, Some(0x80)).dvd_pid().unwrap(); // 0xBD80
        let sub0 = mk(0xBD, Some(0x20)).dvd_pid().unwrap(); // 0x20
        assert_ne!(
            audio0, sub0,
            "subtitle sub-id 0x20 must NOT map to the audio PID"
        );

        let pid_to_track: Vec<(u16, usize)> = vec![
            (DVD_VIDEO_PID, 0),
            (dvd_audio_pid(0x80).unwrap(), 1),
            (dvd_audio_pid(0x81).unwrap(), 2),
            (dvd_subtitle_pid(0x20).unwrap(), 3),
            (dvd_subtitle_pid(0x21).unwrap(), 4),
        ];
        let route = |p: PsPacket| -> Option<usize> {
            let pid = p.dvd_pid()?;
            pid_to_track
                .iter()
                .find(|(x, _)| *x == pid)
                .map(|(_, t)| *t)
        };
        assert_eq!(route(mk(0xE0, None)), Some(0));
        assert_eq!(route(mk(0xBD, Some(0x80))), Some(1));
        assert_eq!(route(mk(0xBD, Some(0x81))), Some(2));
        assert_eq!(route(mk(0xBD, Some(0x20))), Some(3)); // sub0 → track 3, NOT 1
        assert_eq!(route(mk(0xBD, Some(0x21))), Some(4)); // sub1 → track 4, NOT 2
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

    // ════════════════════════════════════════════════════════════════════
    // Added hardening tests
    // ════════════════════════════════════════════════════════════════════

    /// Program-end start code (00 00 01 B9) — used as a delimiter so a
    /// bounded or unbounded PES preceding it is fully framed.
    const PROGRAM_END: [u8; 4] = [0x00, 0x00, 0x01, 0xB9];

    // ── parse_pts: full 33-bit field round trip (ISO 13818-1 Table 2-17) ──

    #[test]
    fn parse_pts_max_33bit() {
        // The PTS field is exactly 33 bits; 2^33-1 must round-trip — a
        // truncated shift/mask would lose the top bits.
        let max = (1u64 << 33) - 1;
        assert_eq!(parse_pts(&encode_pts(max, 0x20)), max);
    }

    #[test]
    fn parse_pts_ignores_marker_bits_in_value() {
        // The marker bits (LSB of bytes 0,2,4) are NOT part of the 33-bit
        // value. Two encodings differing only in marker bits decode equal.
        let v = 0x1_2345_6789u64 & ((1 << 33) - 1);
        let a = encode_pts(v, 0x20);
        let mut b = a;
        // markers are already 1; the value bits must dominate regardless.
        b[0] |= 0x01;
        b[2] |= 0x01;
        b[4] |= 0x01;
        assert_eq!(parse_pts(&a), v);
        assert_eq!(parse_pts(&b), v);
    }

    // ── pack header (0xBA) framing ────────────────────────────────────────

    #[test]
    fn pack_header_waits_for_full_14_bytes() {
        // A pack header needs 14 bytes (MPEG-2). A buffer with only the
        // start code + a few bytes must NOT advance past it — the demuxer
        // waits for more data rather than misframing.
        let mut demuxer = PsDemuxer::new();
        // 00 00 01 BA then only 6 of the 10 remaining pack bytes.
        let partial = vec![0x00, 0x00, 0x01, 0xBA, 0x44, 0x00, 0x04, 0x00, 0x04, 0x01];
        let p = demuxer.feed(&partial);
        assert!(p.is_empty());
        // Now supply the rest of the pack (stuffing=0) plus a PES + delimiter.
        let mut rest = vec![0x01, 0x89, 0xC3, 0xF8]; // mux_rate(3) + stuffing byte
        rest.extend_from_slice(&[
            0x00, 0x00, 0x01, 0xE0, 0x00, 0x05, 0x80, 0x00, 0x00, 0xAB, 0xCD,
        ]);
        rest.extend_from_slice(&PROGRAM_END);
        let p2 = demuxer.feed(&rest);
        assert_eq!(p2.len(), 1, "PES after a now-complete pack header parses");
        assert_eq!(p2[0].data, vec![0xAB, 0xCD]);
    }

    #[test]
    fn pack_header_stuffing_length_consumed() {
        // pack_stuffing_length = low 3 bits of byte 13 (ISO 13818-1
        // §2.5.3.4). The demuxer must skip exactly 14 + stuffing bytes. The
        // stuffing region here holds a DECOY PES start code (00 00 01 E0…);
        // if the stuffing count is under-consumed the scanner would re-sync
        // onto that decoy and emit a bogus PES. Correct skip lands directly
        // on the REAL PES.
        let mut demuxer = PsDemuxer::new();
        let mut data = vec![
            0x00, 0x00, 0x01, 0xBA, 0x44, 0x00, 0x04, 0x00, 0x04, 0x01, 0x01, 0x89, 0xC3,
            0xFD, // stuffing_length = 5 (low 3 bits of 0xFD = 0b101)
            // 5 stuffing bytes containing a decoy PES start code.
            0x00, 0x00, 0x01, 0xE0, 0xDE,
        ];
        // Real PES carries 0x11 0x22; the decoy (if mis-parsed) would carry
        // garbage with a different/short payload.
        data.extend_from_slice(&[
            0x00, 0x00, 0x01, 0xE0, 0x00, 0x05, 0x80, 0x00, 0x00, 0x11, 0x22,
        ]);
        data.extend_from_slice(&PROGRAM_END);
        let p = demuxer.feed(&data);
        assert_eq!(p.len(), 1, "exactly the real PES; the decoy was skipped");
        assert_eq!(p[0].data, vec![0x11, 0x22]);
    }

    // ── system header (0xBB) framing ──────────────────────────────────────

    #[test]
    fn system_header_length_skipped() {
        // System header: 00 00 01 BB [header_length:2] body. The demuxer
        // must skip 6 + header_length bytes (ISO 13818-1 §2.5.3.5), even
        // though the body contains bytes that look like PES IDs.
        let mut demuxer = PsDemuxer::new();
        let body = [0x00, 0x00, 0x01, 0xE0, 0xFF, 0xFF]; // decoy PES-looking bytes
        let mut data = vec![0x00, 0x00, 0x01, 0xBB];
        data.extend_from_slice(&(body.len() as u16).to_be_bytes());
        data.extend_from_slice(&body);
        // Real PES after the system header.
        data.extend_from_slice(&[
            0x00, 0x00, 0x01, 0xC0, 0x00, 0x05, 0x80, 0x00, 0x00, 0x33, 0x44,
        ]);
        data.extend_from_slice(&PROGRAM_END);
        let p = demuxer.feed(&data);
        assert_eq!(
            p.len(),
            1,
            "decoy bytes inside system header not parsed as PES"
        );
        assert_eq!(p[0].stream_id, 0xC0);
        assert_eq!(p[0].data, vec![0x33, 0x44]);
    }

    #[test]
    fn system_header_waits_for_full_body() {
        // System header declaring a body longer than buffered must not
        // advance — wait for more data.
        let mut demuxer = PsDemuxer::new();
        let mut data = vec![0x00, 0x00, 0x01, 0xBB, 0x00, 0x20]; // len=32
        data.extend_from_slice(&[0xAA; 4]); // only 4 of 32 body bytes
        assert!(demuxer.feed(&data).is_empty());
    }

    // ── PES length / boundary handling ────────────────────────────────────

    #[test]
    fn bounded_pes_waits_for_full_declared_length() {
        // A PES with a non-zero PES_packet_length must not be emitted until
        // all 6 + length bytes are buffered — never emit a short frame.
        let mut demuxer = PsDemuxer::new();
        // length = 5 → total 11 bytes, supply only 9.
        let head = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x05, 0x80, 0x00, 0x00];
        assert!(demuxer.feed(&head).is_empty());
        // supply the remaining 2 payload bytes.
        let p = demuxer.feed(&[0xEE, 0xFF]);
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].data, vec![0xEE, 0xFF]);
    }

    #[test]
    fn padding_stream_0xbe_is_dropped() {
        // Padding stream (0xBE) carries no ES (ISO 13818-1 Table 2-22) and
        // must produce no PsPacket — only the real PES survives.
        let mut demuxer = PsDemuxer::new();
        let mut data = vec![0x00, 0x00, 0x01, 0xBE, 0x00, 0x04, 0xFF, 0xFF, 0xFF, 0xFF];
        data.extend_from_slice(&[
            0x00, 0x00, 0x01, 0xE0, 0x00, 0x05, 0x80, 0x00, 0x00, 0x01, 0x02,
        ]);
        data.extend_from_slice(&PROGRAM_END);
        let p = demuxer.feed(&data);
        assert_eq!(p.len(), 1, "padding stream dropped; only real PES emitted");
        assert_eq!(p[0].stream_id, 0xE0);
    }

    #[test]
    fn private_stream_2_0xbf_has_no_pes_extension() {
        // private_stream_2 (0xBF) carries no standard PES header extension
        // (ISO 13818-1 Table 2-22): the bytes after the 6-byte prefix are
        // raw payload, NOT flags/header_data_length. No PTS, no sub-stream.
        let mut demuxer = PsDemuxer::new();
        let mut data = vec![0x00, 0x00, 0x01, 0xBF, 0x00, 0x04, 0xDE, 0xAD, 0xBE, 0xEF];
        data.extend_from_slice(&PROGRAM_END);
        let p = demuxer.feed(&data);
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].stream_id, 0xBF);
        assert_eq!(p[0].pts, None, "0xBF carries no PTS");
        assert_eq!(p[0].sub_stream_id, None);
        assert_eq!(p[0].data, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn unknown_start_code_is_skipped_not_parsed() {
        // A start code with an ID outside the known PS-layer set
        // (e.g. 0xB0, reserved) must be skipped 4 bytes and not derail
        // the following real PES.
        let mut demuxer = PsDemuxer::new();
        let mut data = vec![0x00, 0x00, 0x01, 0xB0]; // unknown/reserved code
        data.extend_from_slice(&[
            0x00, 0x00, 0x01, 0xE0, 0x00, 0x05, 0x80, 0x00, 0x00, 0x9A, 0xBC,
        ]);
        data.extend_from_slice(&PROGRAM_END);
        let p = demuxer.feed(&data);
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].data, vec![0x9A, 0xBC]);
    }

    // ── private_stream_1 sub-header skip lengths ──────────────────────────

    #[test]
    fn private_stream_1_unknown_subid_skips_one_byte() {
        // For a private_stream_1 sub-id outside the AC3/DTS/LPCM ranges the
        // skip is 1 (just the sub-id byte). All remaining bytes are ES.
        let mut demuxer = PsDemuxer::new();
        let mut data = vec![
            0x00, 0x00, 0x01, 0xBD, 0x00, 0x06, 0x80, 0x00, 0x00, //
            0x70, // sub-id outside known ranges → skip 1
            0x55, 0x66,
        ];
        data.extend_from_slice(&PROGRAM_END);
        let p = demuxer.feed(&data);
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].sub_stream_id, Some(0x70));
        assert_eq!(p[0].data, vec![0x55, 0x66], "only sub-id byte skipped");
    }

    #[test]
    fn private_stream_1_short_payload_does_not_underflow_skip() {
        // If the sub-header skip exceeds the payload length, `skip.min(len)`
        // clamps so ES is empty rather than panicking on an out-of-range
        // slice. AC3 skip is 4 but only 2 payload bytes present.
        let mut demuxer = PsDemuxer::new();
        let mut data = vec![
            0x00, 0x00, 0x01, 0xBD, 0x00, 0x04, 0x80, 0x00, 0x00, //
            0x80, // AC3 sub-id, skip=4
            0x01, // only 1 byte after sub-id (total payload 2 < skip 4)
        ];
        data.extend_from_slice(&PROGRAM_END);
        let p = demuxer.feed(&data);
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].sub_stream_id, Some(0x80));
        assert!(
            p[0].data.is_empty(),
            "clamped skip yields empty ES, no panic"
        );
    }

    // ── dvd_audio_pid / dvd_subtitle_pid range boundaries ─────────────────

    #[test]
    fn dvd_audio_pid_range_boundaries() {
        // AC3/DTS audio sub-ids 0x80..=0x8F and LPCM 0xA0..=0xA7 map to
        // 0xBD00|sub. Just-outside values must return None.
        assert_eq!(dvd_audio_pid(0x80), Some(0xBD80));
        assert_eq!(dvd_audio_pid(0x8F), Some(0xBD8F));
        assert_eq!(dvd_audio_pid(0xA0), Some(0xBDA0));
        assert_eq!(dvd_audio_pid(0xA7), Some(0xBDA7));
        // Boundaries just outside the ranges.
        assert_eq!(dvd_audio_pid(0x7F), None);
        assert_eq!(dvd_audio_pid(0x90), None);
        assert_eq!(dvd_audio_pid(0x9F), None);
        assert_eq!(dvd_audio_pid(0xA8), None);
    }

    #[test]
    fn dvd_subtitle_pid_range_boundaries() {
        // VobSub subtitle sub-ids 0x20..=0x3F map to the identity PID.
        assert_eq!(dvd_subtitle_pid(0x20), Some(0x20));
        assert_eq!(dvd_subtitle_pid(0x3F), Some(0x3F));
        assert_eq!(dvd_subtitle_pid(0x1F), None);
        assert_eq!(dvd_subtitle_pid(0x40), None);
    }

    #[test]
    fn dvd_pid_all_video_stream_ids_map_to_video() {
        // ISO 13818-1: 0xE0..=0xEF are all video streams. DVD collapses
        // them onto the single canonical video PID.
        for sid in 0xE0u8..=0xEF {
            assert_eq!(
                mk(sid, None).dvd_pid(),
                Some(DVD_VIDEO_PID),
                "stream_id {sid:#04x} must map to video"
            );
        }
    }

    // ── flushing semantics ────────────────────────────────────────────────

    #[test]
    fn flush_discards_incomplete_bounded_pes() {
        // A bounded PES short of its declared length is genuinely incomplete
        // and must be DROPPED at flush — not emitted with a truncated payload.
        let mut demuxer = PsDemuxer::new();
        // length=10 but only 2 payload bytes supplied.
        let head = vec![
            0x00, 0x00, 0x01, 0xE0, 0x00, 0x0A, 0x80, 0x00, 0x00, 0xAA, 0xBB,
        ];
        assert!(demuxer.feed(&head).is_empty());
        let flushed = demuxer.flush();
        assert!(
            flushed.is_empty(),
            "incomplete bounded PES must not be emitted on flush"
        );
    }

    #[test]
    fn empty_feed_then_flush_is_empty() {
        // No input at all → nothing to emit, no panic.
        let mut demuxer = PsDemuxer::new();
        assert!(demuxer.feed(&[]).is_empty());
        assert!(demuxer.flush().is_empty());
    }

    #[test]
    fn pes_header_data_length_skips_pts_when_flag_unset() {
        // If pts_dts_flags == 0 the 5 "PTS" bytes after the fixed header are
        // ES, not a timestamp. A PES with header_data_length=0 and no PTS
        // flag must surface no PTS and keep all payload bytes.
        let mut demuxer = PsDemuxer::new();
        let mut data = vec![
            0x00, 0x00, 0x01, 0xE0, 0x00, 0x06, 0x80, 0x00, 0x00, 0x21, 0x00, 0x01,
        ];
        // 0x21 0x00 0x01 look like the start of a PTS field but must NOT be
        // parsed as one (flags2 = 0x00 ⇒ no PTS).
        data.extend_from_slice(&PROGRAM_END);
        let p = demuxer.feed(&data);
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].pts, None);
        assert_eq!(p[0].data, vec![0x21, 0x00, 0x01]);
    }

    #[test]
    fn unbounded_video_pes_framed_by_next_pes_not_embedded_audio_code() {
        // An unbounded (length 0) video PES must be delimited by the next
        // PS-layer unit. A following AUDIO PES (0xC0) is a valid boundary,
        // so the video ES must include its embedded 00 00 01 00 picture
        // code but stop at the audio PES start.
        let mut demuxer = PsDemuxer::new();
        let mut data = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        let video_payload = [0x11, 0x00, 0x00, 0x01, 0x00, 0x22]; // embedded picture SC
        data.extend_from_slice(&video_payload);
        // Next PS-layer unit: an audio PES (bounded).
        data.extend_from_slice(&[
            0x00, 0x00, 0x01, 0xC0, 0x00, 0x05, 0x80, 0x00, 0x00, 0x99, 0x88,
        ]);
        data.extend_from_slice(&PROGRAM_END);
        let p = demuxer.feed(&data);
        assert_eq!(p.len(), 2, "video PES + audio PES");
        assert_eq!(p[0].stream_id, 0xE0);
        assert_eq!(
            p[0].data, video_payload,
            "video ES keeps its embedded start code, stops at the audio PES"
        );
        assert_eq!(p[1].stream_id, 0xC0);
        assert_eq!(p[1].data, vec![0x99, 0x88]);
    }
}
