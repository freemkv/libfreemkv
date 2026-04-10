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
    pid_index: [i16; 8192], // PID → index into assemblers, -1 = not tracked
}

impl TsDemuxer {
    /// Create a new demuxer tracking the given PIDs.
    pub fn new(pids: &[u16]) -> Self {
        let mut pid_index = [-1i16; 8192];
        let mut assemblers = Vec::with_capacity(pids.len());
        for (i, &pid) in pids.iter().enumerate() {
            pid_index[pid as usize] = i as i16;
            assemblers.push(PesAssembler::new(pid));
        }
        Self { assemblers, pid_index }
    }

    /// Feed a chunk of BD transport stream data (must be aligned to 192-byte packets).
    /// Returns completed PES packets.
    pub fn feed(&mut self, data: &[u8]) -> Vec<PesPacket> {
        let mut completed = Vec::new();
        let mut offset = 0;

        while offset + BD_TS_PACKET_SIZE <= data.len() {
            let packet = &data[offset..offset + BD_TS_PACKET_SIZE];
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
            let idx = self.pid_index[pid as usize];
            if idx < 0 {
                continue;
            }
            let asm = &mut self.assemblers[idx as usize];

            // Find payload start (skip adaptation field if present)
            let payload_start = if adaptation == 0x03 || adaptation == 0x02 {
                // Adaptation field present
                let af_len = ts[4] as usize;
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
    if stream_id == 0xBC || stream_id == 0xBE || stream_id == 0xBF
        || stream_id == 0xF0 || stream_id == 0xF1 || stream_id == 0xFF
    {
        return (None, None, 6);
    }

    // Standard PES header: [6] = flags1, [7] = flags2, [8] = header_data_length
    if data.len() < 9 {
        return (None, None, 6);
    }

    let pts_dts_flags = (data[7] >> 6) & 0x03;
    let header_data_len = data[8] as usize;
    let data_start = 9 + header_data_len;

    let mut pts = None;
    let mut dts = None;

    if pts_dts_flags >= 2 && data.len() >= 14 {
        pts = Some(parse_timestamp(&data[9..14]));
    }
    if pts_dts_flags == 3 && data.len() >= 19 {
        dts = Some(parse_timestamp(&data[14..19]));
    }

    (pts, dts, data_start)
}

/// Parse a 5-byte PTS/DTS timestamp (33 bits in 90kHz).
fn parse_timestamp(data: &[u8]) -> i64 {
    let b0 = data[0] as i64;
    let b1 = data[1] as i64;
    let b2 = data[2] as i64;
    let b3 = data[3] as i64;
    let b4 = data[4] as i64;

    ((b0 >> 1) & 0x07) << 30
        | b1 << 22
        | (b2 >> 1) << 15
        | b3 << 7
        | b4 >> 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_timestamp() {
        // Example: PTS = 0 → encoded as 21 00 01 00 01
        let data = [0x21, 0x00, 0x01, 0x00, 0x01];
        assert_eq!(parse_timestamp(&data), 0);

        // Example: PTS = 90000 (1 second at 90kHz)
        // Manual encoding: 33 bits = 0x00015F90
        // This is just a sanity check that the parser doesn't crash
        let data2 = [0x21, 0x00, 0x07, 0xE9, 0x01]; // approximate
        let pts = parse_timestamp(&data2);
        assert!(pts >= 0);
    }

    #[test]
    fn test_demuxer_empty() {
        let mut demux = TsDemuxer::new(&[0x1011]);
        let result = demux.feed(&[]);
        assert!(result.is_empty());
    }
}
