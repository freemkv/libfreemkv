//! BD Transport Stream demuxer.
//!
//! Blu-ray uses 192-byte TS packets (not standard 188):
//! - 4-byte TP_extra_header (arrival timestamp + copy permission)
//! - 188-byte standard MPEG-TS packet
//!
//! This demuxer extracts PES packets from selected PIDs, with PTS/DTS timestamps.

/// BD transport stream packet size (4-byte extra header + 188-byte TS).
const BD_TS_PACKET_SIZE: usize = 192;

/// Standard TS packet size.
const TS_PACKET_SIZE: usize = 188;

/// TS sync byte.
const SYNC_BYTE: u8 = 0x47;

/// A reassembled PES packet with timestamp info.
#[derive(Debug)]
pub struct PesPacket {
    /// MPEG-TS PID this packet belongs to.
    pub pid: u16,
    /// Presentation timestamp in 90kHz ticks (if present).
    pub pts: Option<i64>,
    /// Decode timestamp in 90kHz ticks (if present).
    pub dts: Option<i64>,
    /// Elementary stream data (video frame, audio frame, subtitle segment, etc.).
    pub data: Vec<u8>,
}

/// Per-PID PES reassembly state.
struct PesAssembler {
    pid: u16,
    buffer: Vec<u8>,
    pts: Option<i64>,
    dts: Option<i64>,
    active: bool,
}

impl PesAssembler {
    fn new(pid: u16) -> Self {
        Self {
            pid,
            buffer: Vec::with_capacity(256 * 1024),
            pts: None,
            dts: None,
            active: false,
        }
    }

    /// Start a new PES packet. Returns the completed previous packet (if any).
    fn start(&mut self, pts: Option<i64>, dts: Option<i64>) -> Option<PesPacket> {
        let completed = if self.active && !self.buffer.is_empty() {
            Some(PesPacket {
                pid: self.pid,
                pts: self.pts,
                dts: self.dts,
                data: std::mem::replace(&mut self.buffer, Vec::with_capacity(256 * 1024)),
            })
        } else {
            self.buffer.clear();
            None
        };
        self.pts = pts;
        self.dts = dts;
        self.active = true;
        completed
    }

    /// Append payload data to the current PES packet.
    fn push(&mut self, data: &[u8]) {
        if self.active {
            self.buffer.extend_from_slice(data);
        }
    }

    /// Flush remaining data as a PES packet.
    fn flush(&mut self) -> Option<PesPacket> {
        if self.active && !self.buffer.is_empty() {
            self.active = false;
            Some(PesPacket {
                pid: self.pid,
                pts: self.pts,
                dts: self.dts,
                data: std::mem::take(&mut self.buffer),
            })
        } else {
            None
        }
    }
}

/// BD Transport Stream demuxer.
pub struct TsDemuxer {
    assemblers: Vec<PesAssembler>,
    pid_index: Vec<i16>, // PID → index into assemblers, -1 = not tracked
    remainder: Vec<u8>,  // leftover bytes from previous feed() call
}

impl TsDemuxer {
    /// Create a new demuxer tracking the given PIDs.
    ///
    /// Allocates a flat lookup table of `i16` slots — one per possible PID
    /// up to `max(8192, max_pid + 1)`. The 8192 floor matches the BD-TS
    /// 13-bit PID space (0..0x1FFF); the variable upper bound exists for
    /// DVD program streams which may use 16-bit stream IDs above 8191.
    /// Worst-case allocation is `u16::MAX × 2 bytes ≈ 128 KB` — bounded by
    /// the type, so adversarial input can't drive this beyond predictable
    /// limits. Empty `pids` yields max_pid 0; the floor still produces a
    /// valid (wholly-unused) table.
    pub fn new(pids: &[u16]) -> Self {
        let max_pid = pids.iter().copied().max().unwrap_or(0) as usize;
        let table_size = (max_pid + 1).max(8192);
        let mut pid_index = vec![-1i16; table_size];
        let mut assemblers = Vec::with_capacity(pids.len());
        for (i, &pid) in pids.iter().enumerate() {
            pid_index[pid as usize] = i as i16;
            assemblers.push(PesAssembler::new(pid));
        }
        Self {
            assemblers,
            pid_index,
            remainder: Vec::new(),
        }
    }

    /// Feed a chunk of BD transport stream data. Handles non-192-byte-aligned input
    /// by buffering leftover bytes between calls. Returns completed PES packets.
    pub fn feed(&mut self, data: &[u8]) -> Vec<PesPacket> {
        let mut completed = Vec::with_capacity(4);

        // Prepend any remainder from previous call
        let mut combined: Vec<u8> = Vec::new();
        let work: &[u8] = if !self.remainder.is_empty() {
            combined.reserve(self.remainder.len() + data.len());
            combined.extend_from_slice(&self.remainder);
            combined.extend_from_slice(data);
            self.remainder.clear();
            &combined
        } else {
            data
        };

        let mut offset = 0;

        while offset + BD_TS_PACKET_SIZE <= work.len() {
            let packet = &work[offset..offset + BD_TS_PACKET_SIZE];
            offset += BD_TS_PACKET_SIZE;

            // Skip 4-byte TP_extra_header, check sync byte
            if packet[4] != SYNC_BYTE {
                continue;
            }

            let ts = &packet[4..]; // 188-byte standard TS packet

            // Parse TS header
            let pid = (((ts[1] & 0x1F) as u16) << 8) | ts[2] as u16;
            let pusi = ts[1] & 0x40 != 0; // Payload Unit Start Indicator
            let adaptation = (ts[3] >> 4) & 0x03;

            // Check if we're tracking this PID
            let idx = if (pid as usize) < self.pid_index.len() {
                self.pid_index[pid as usize]
            } else {
                -1
            };
            if idx < 0 {
                continue;
            }
            let asm = &mut self.assemblers[idx as usize];

            // Find payload start (skip adaptation field if present)
            let payload_start = if adaptation == 0x03 || adaptation == 0x02 {
                // Adaptation field present
                let af_len = ts[4] as usize;
                if af_len > 183 {
                    continue; // Malformed: AF length exceeds TS payload
                }
                5 + af_len
            } else {
                4
            };

            if payload_start >= TS_PACKET_SIZE {
                continue;
            }

            // No payload
            if adaptation == 0x02 {
                continue;
            }

            let payload = &ts[payload_start..];

            if pusi {
                // New PES packet starts here — parse PES header
                let (pts, dts, pes_data_start) = parse_pes_header(payload);
                if let Some(prev) = asm.start(pts, dts) {
                    completed.push(prev);
                }
                if pes_data_start < payload.len() {
                    asm.push(&payload[pes_data_start..]);
                }
            } else {
                // Continuation of current PES packet
                asm.push(payload);
            }
        }

        // Save leftover bytes for next call (cap at one packet to prevent unbounded growth)
        if offset < work.len() {
            let leftover = &work[offset..];
            if leftover.len() < BD_TS_PACKET_SIZE {
                self.remainder.extend_from_slice(leftover);
            } else {
                // More than one full packet leftover — something is wrong, discard
                self.remainder.clear();
            }
        }

        completed
    }

    /// Flush all assemblers, returning any remaining PES packets.
    pub fn flush(&mut self) -> Vec<PesPacket> {
        let mut completed = Vec::new();
        for asm in &mut self.assemblers {
            if let Some(pkt) = asm.flush() {
                completed.push(pkt);
            }
        }
        completed
    }
}

/// Parse a PES packet header, extracting PTS and DTS.
/// Returns (pts, dts, offset_to_elementary_stream_data).
fn parse_pes_header(data: &[u8]) -> (Option<i64>, Option<i64>, usize) {
    // PES packet: 00 00 01 [stream_id] [length:2] [flags...]
    if data.len() < 9 || data[0] != 0x00 || data[1] != 0x00 || data[2] != 0x01 {
        return (None, None, 0);
    }

    let stream_id = data[3];

    // Some stream IDs don't have the standard PES header extension
    // (program_stream_map, padding, private_stream_2, ECM, EMM, etc.)
    if stream_id == 0xBC
        || stream_id == 0xBE
        || stream_id == 0xBF
        || stream_id == 0xF0
        || stream_id == 0xF1
        || stream_id == 0xFF
    {
        return (None, None, 6);
    }

    // Standard PES header: [6] = flags1, [7] = flags2, [8] = header_data_length
    if data.len() < 9 {
        return (None, None, 6);
    }

    let pts_dts_flags = (data[7] >> 6) & 0x03;
    let header_data_len = data[8] as usize;
    let data_start = (9 + header_data_len).min(data.len());

    let mut pts = None;
    let mut dts = None;

    if pts_dts_flags >= 2 && header_data_len >= 5 && data.len() >= 14 {
        pts = parse_timestamp(&data[9..14]);
    }
    if pts_dts_flags == 3 && header_data_len >= 10 && data.len() >= 19 {
        dts = parse_timestamp(&data[14..19]);
    }

    (pts, dts, data_start)
}

/// Parse a 5-byte PTS/DTS timestamp (33 bits in 90kHz).
/// Validates marker bits per MPEG-2 spec. Returns None on invalid encoding.
fn parse_timestamp(data: &[u8]) -> Option<i64> {
    if data.len() < 5 {
        return None;
    }
    // Validate marker bits: byte 2 bit 0 and byte 4 bit 0 must be 1
    if (data[2] & 0x01) == 0 || (data[4] & 0x01) == 0 {
        return None;
    }
    let b0 = data[0] as i64;
    let b1 = data[1] as i64;
    let b2 = data[2] as i64;
    let b3 = data[3] as i64;
    let b4 = data[4] as i64;

    Some(((b0 >> 1) & 0x07) << 30 | b1 << 22 | (b2 >> 1) << 15 | b3 << 7 | b4 >> 1)
}

// ============================================================
// Stream scanning (PAT/PMT → stream list)
// ============================================================

/// Scan BD-TS data for streams by parsing PAT and PMT tables.
/// Returns None if no valid program is found.
pub fn scan_streams(data: &[u8]) -> Option<Vec<crate::disc::Stream>> {
    use crate::disc::*;

    // Pass 1: find PMT PID from PAT
    let mut pat_pmt_pid: Option<u16> = None;
    let mut offset = 0;
    while offset + BD_TS_PACKET_SIZE <= data.len() {
        if data[offset + 4] != SYNC_BYTE {
            offset += 1;
            continue;
        }
        let pid = (((data[offset + 5] & 0x1F) as u16) << 8) | data[offset + 6] as u16;
        let pusi = data[offset + 5] & 0x40 != 0;

        if pid == 0 && pusi {
            let payload_start = offset + 4 + 4;
            if payload_start + 12 < data.len() {
                let pointer = data[payload_start] as usize;
                let pat_start = payload_start + 1 + pointer;
                if pat_start + 12 < data.len() && data[pat_start] == 0x00 {
                    let section_len = (((data[pat_start + 1] & 0x0F) as usize) << 8)
                        | data[pat_start + 2] as usize;
                    let entries_start = pat_start + 8;
                    if section_len < 4 {
                        offset += BD_TS_PACKET_SIZE;
                        continue;
                    }
                    let entries_end = pat_start + 3 + section_len - 4;
                    let mut e = entries_start;
                    while e + 4 <= data.len() && e < entries_end {
                        let prog_num = ((data[e] as u16) << 8) | data[e + 1] as u16;
                        let p = (((data[e + 2] & 0x1F) as u16) << 8) | data[e + 3] as u16;
                        if prog_num != 0 {
                            pat_pmt_pid = Some(p);
                            break;
                        }
                        e += 4;
                    }
                }
            }
        }
        offset += BD_TS_PACKET_SIZE;
    }

    let pmt_pid = pat_pmt_pid?;

    // Pass 2: parse PMT for stream entries
    let mut streams = Vec::new();
    offset = 0;
    while offset + BD_TS_PACKET_SIZE <= data.len() {
        if data[offset + 4] != SYNC_BYTE {
            offset += 1;
            continue;
        }
        let pid = (((data[offset + 5] & 0x1F) as u16) << 8) | data[offset + 6] as u16;
        let pusi = data[offset + 5] & 0x40 != 0;

        if pid == pmt_pid && pusi {
            let payload_start = offset + 4 + 4;
            if payload_start + 1 >= data.len() {
                offset += BD_TS_PACKET_SIZE;
                continue;
            }
            let pointer = data[payload_start] as usize;
            let pmt_start = payload_start + 1 + pointer;
            if pmt_start + 12 >= data.len() {
                offset += BD_TS_PACKET_SIZE;
                continue;
            }
            if data[pmt_start] != 0x02 {
                offset += BD_TS_PACKET_SIZE;
                continue;
            }

            let section_len =
                (((data[pmt_start + 1] & 0x0F) as usize) << 8) | data[pmt_start + 2] as usize;
            let prog_info_len =
                (((data[pmt_start + 10] & 0x0F) as usize) << 8) | data[pmt_start + 11] as usize;
            let mut pos = pmt_start + 12 + prog_info_len;
            let end = pmt_start + 3 + section_len - 4;

            while pos + 5 <= data.len() && pos < end {
                let stream_type = data[pos];
                let es_pid = (((data[pos + 1] & 0x1F) as u16) << 8) | data[pos + 2] as u16;
                let es_info_len = (((data[pos + 3] & 0x0F) as usize) << 8) | data[pos + 4] as usize;

                let stream = match stream_type {
                    0x1B => Some(Stream::Video(VideoStream {
                        pid: es_pid,
                        codec: Codec::H264,
                        resolution: Resolution::R1080p,
                        frame_rate: FrameRate::Unknown,
                        hdr: HdrFormat::Sdr,
                        color_space: ColorSpace::Bt709,
                        secondary: false,
                        label: String::new(),
                    })),
                    0x24 => Some(Stream::Video(VideoStream {
                        pid: es_pid,
                        codec: Codec::Hevc,
                        resolution: Resolution::R2160p,
                        frame_rate: FrameRate::Unknown,
                        hdr: HdrFormat::Sdr,
                        color_space: ColorSpace::Bt709,
                        secondary: false,
                        label: String::new(),
                    })),
                    0xEA => Some(Stream::Video(VideoStream {
                        pid: es_pid,
                        codec: Codec::Vc1,
                        resolution: Resolution::R1080p,
                        frame_rate: FrameRate::Unknown,
                        hdr: HdrFormat::Sdr,
                        color_space: ColorSpace::Bt709,
                        secondary: false,
                        label: String::new(),
                    })),
                    0x02 => Some(Stream::Video(VideoStream {
                        pid: es_pid,
                        codec: Codec::Mpeg2,
                        resolution: Resolution::R1080i,
                        frame_rate: FrameRate::Unknown,
                        hdr: HdrFormat::Sdr,
                        color_space: ColorSpace::Bt709,
                        secondary: false,
                        label: String::new(),
                    })),
                    0x81 => Some(Stream::Audio(AudioStream {
                        pid: es_pid,
                        codec: Codec::Ac3,
                        channels: AudioChannels::Surround51,
                        language: "und".into(),
                        sample_rate: SampleRate::S48,
                        secondary: false,
                        purpose: crate::disc::LabelPurpose::Normal,
                        label: String::new(),
                    })),
                    0x83 => Some(Stream::Audio(AudioStream {
                        pid: es_pid,
                        codec: Codec::TrueHd,
                        channels: AudioChannels::Surround51,
                        language: "und".into(),
                        sample_rate: SampleRate::S48,
                        secondary: false,
                        purpose: crate::disc::LabelPurpose::Normal,
                        label: String::new(),
                    })),
                    0x84 | 0xA1 => Some(Stream::Audio(AudioStream {
                        pid: es_pid,
                        codec: Codec::Ac3Plus,
                        channels: AudioChannels::Surround51,
                        language: "und".into(),
                        sample_rate: SampleRate::S48,
                        secondary: false,
                        purpose: crate::disc::LabelPurpose::Normal,
                        label: String::new(),
                    })),
                    0x85 | 0x86 => Some(Stream::Audio(AudioStream {
                        pid: es_pid,
                        codec: Codec::DtsHdMa,
                        channels: AudioChannels::Surround51,
                        language: "und".into(),
                        sample_rate: SampleRate::S48,
                        secondary: false,
                        purpose: crate::disc::LabelPurpose::Normal,
                        label: String::new(),
                    })),
                    0x82 => Some(Stream::Audio(AudioStream {
                        pid: es_pid,
                        codec: Codec::Dts,
                        channels: AudioChannels::Surround51,
                        language: "und".into(),
                        sample_rate: SampleRate::S48,
                        secondary: false,
                        purpose: crate::disc::LabelPurpose::Normal,
                        label: String::new(),
                    })),
                    0x90 => Some(Stream::Subtitle(SubtitleStream {
                        pid: es_pid,
                        codec: Codec::Pgs,
                        language: "und".into(),
                        forced: false,
                        qualifier: crate::disc::LabelQualifier::None,
                        codec_data: None,
                    })),
                    _ => None,
                };

                if let Some(s) = stream {
                    streams.push(s);
                }
                pos += 5 + es_info_len;
            }
            break;
        }
        offset += BD_TS_PACKET_SIZE;
    }

    if streams.is_empty() {
        None
    } else {
        Some(streams)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_timestamp() {
        // Example: PTS = 0 → encoded as 21 00 01 00 01
        let data = [0x21, 0x00, 0x01, 0x00, 0x01];
        assert_eq!(parse_timestamp(&data), Some(0));

        // Example: PTS = 90000 (1 second at 90kHz)
        // Manual encoding: 33 bits = 0x00015F90
        // This is just a sanity check that the parser doesn't crash
        let data2 = [0x21, 0x00, 0x07, 0xE9, 0x01]; // approximate
        let pts = parse_timestamp(&data2);
        assert!(pts.is_some() && pts.unwrap() >= 0);

        // Invalid marker bits → returns None
        let bad = [0x00, 0x00, 0x00, 0x00, 0x00]; // marker bits wrong
        assert_eq!(parse_timestamp(&bad), None);
    }

    #[test]
    fn test_demuxer_empty() {
        let mut demux = TsDemuxer::new(&[0x1011]);
        let result = demux.feed(&[]);
        assert!(result.is_empty());
    }
}
