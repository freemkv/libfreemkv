//! Dolby TrueHD / Atmos elementary stream parser.
//!
//! BD-TS TrueHD PES packets contain interleaved AC-3 + TrueHD access units.
//! Access units span PES boundaries — must buffer and reassemble.
//!
//! TrueHD access unit header (4 bytes):
//!   bytes 0-1: top nibble = MLP check/access-unit nibble, lower 12 bits =
//!              access-unit length in 2-byte words
//!   bytes 2-3: timing value
//!   bytes 4..: substream data (major sync 0xF8726FBA may appear at offset 4)
//!
//! AC-3 frames (interleaved, same PID): start with sync word 0x0B77.
//! We skip AC-3 frames and only emit TrueHD access units.

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// Duration of one TrueHD access unit in nanoseconds (1/1200 second).
const AU_DURATION_NS: i64 = 833_333;

/// Hard cap on the reassembly buffer. A valid TrueHD/MAT access unit is
/// well under 32 KiB; if the buffer grows far past that without yielding a
/// frame the stream is malformed, so we drop it and resync rather than grow
/// without bound. Parity with the AC-3 / DTS / PGS caps.
const MAX_TRUEHD_BUF: usize = 256 * 1024;

pub struct TrueHdParser {
    buf: Vec<u8>,
    next_pts_ns: i64,
}

impl Default for TrueHdParser {
    fn default() -> Self {
        Self::new()
    }
}

impl TrueHdParser {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(32768),
            next_pts_ns: 0,
        }
    }

    /// Size (bytes) of the AC-3 frame at the buffer head.
    ///
    /// Distinguishes three cases the caller must treat differently:
    /// - `Unmappable`: the header's fscod/frmsizecod don't map to a real frame
    ///   size (reserved fscod==3, or frmsizecod >= 38). The caller must drain
    ///   and resync, NOT wait for more data — waiting would stall forever.
    /// - `NeedMore`: a valid size, but the frame isn't fully buffered yet.
    /// - `Frame(n)`: a complete `n`-byte AC-3 frame is buffered.
    ///
    /// Frame sizing reuses `ac3::ac3_frame_size` so the AC-3 size table has a
    /// single source of truth shared with the AC-3 parser; a returned `0` there
    /// (reserved fscod or out-of-range frmsizecod) is the unmappable case.
    fn ac3_frame_at_head(&self) -> Ac3Size {
        if self.buf.len() < 6 {
            return Ac3Size::NeedMore;
        }
        let frame_bytes = super::ac3::ac3_frame_size(&self.buf);
        if frame_bytes == 0 {
            // Reserved fscod or out-of-range frmsizecod → unmappable header.
            return Ac3Size::Unmappable;
        }
        if self.buf.len() < frame_bytes {
            return Ac3Size::NeedMore;
        }
        Ac3Size::Frame(frame_bytes)
    }
}

/// Secondary validation for an AC-3 frame of `frame_bytes` at the buffer head:
/// is its computed end a plausible boundary? Accept when the frame fills the
/// rest of the buffer, or the bytes that follow start another AC-3 sync
/// (0x0B77) or a plausible TrueHD access unit (non-zero 12-bit length within
/// the 32 KiB cap). If none holds, the leading 0x0B77 is more likely a TrueHD
/// AU header that happens to look like AC-3, so the AC-3 reading is rejected.
fn ac3_boundary_corroborated(buf: &[u8], frame_bytes: usize) -> bool {
    if frame_bytes >= buf.len() {
        // The AC-3 frame is fully buffered and ends the data — consistent.
        return true;
    }
    let tail = &buf[frame_bytes..];
    if tail.len() < 2 {
        // Not enough following bytes to judge; accept (the next call will see
        // the continuation).
        return true;
    }
    // Another AC-3 sync immediately after?
    if tail[0] == 0x0B && tail[1] == 0x77 {
        return true;
    }
    // A plausible TrueHD AU header after? (non-zero 12-bit length, <= 32 KiB)
    let next_words = (((tail[0] as usize) << 8) | tail[1] as usize) & 0xFFF;
    next_words != 0 && next_words * 2 <= 32768
}

/// Outcome of sizing the AC-3 frame at the TrueHD buffer head.
enum Ac3Size {
    /// fscod/frmsizecod don't map to a real frame size — resync, don't wait.
    Unmappable,
    /// A valid size, but the frame is not fully buffered yet.
    NeedMore,
    /// A complete `n`-byte AC-3 frame is buffered.
    Frame(usize),
}

impl CodecParser for TrueHdParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        // Capture the PTS base ONLY at an access-unit boundary, i.e. when no AU
        // is mid-assembly in `buf`. TrueHD access units span PES packets; a PES
        // that merely continues an AU already in progress carries its own (later)
        // PTS, which must NOT override the running per-AU timestamp. Adopting it
        // mid-AU would snap that AU's PTS backward/forward and break the
        // monotonic +AU_DURATION_NS cadence (A/V drift). Once the buffer is empty
        // the next PES legitimately begins a new AU and seeds the base.
        if self.buf.is_empty() {
            if let Some(pts) = pes.pts {
                self.next_pts_ns = pts_to_ns(pts);
            }
        }

        self.buf.extend_from_slice(&pes.data);

        let mut frames = Vec::new();

        loop {
            if self.buf.len() < 4 {
                break;
            }

            // AC-3 frame (interleaved): starts with sync word 0x0B77.
            //
            // 0x0B 0x77 is also a legal TrueHD AU header (check-nibble 0,
            // length-high-bits 0xB → length 0xB77 words). To avoid an AC-3
            // misread stealing a real TrueHD AU, an AC-3 frame is only accepted
            // when its computed end is corroborated by what follows: end of
            // buffer (frame fills the rest), another AC-3 sync, or a plausible
            // TrueHD AU header. If none holds, this is treated as a TrueHD AU.
            if self.buf[0] == 0x0B && self.buf[1] == 0x77 {
                match self.ac3_frame_at_head() {
                    Ac3Size::Unmappable => {
                        // Permanently unmappable header at the head would stall
                        // the parser forever; resync by dropping 2 bytes so one
                        // bad frame costs one frame, not the whole buffer.
                        self.buf.drain(..2);
                        continue;
                    }
                    Ac3Size::NeedMore => break, // wait for the rest of the frame
                    Ac3Size::Frame(skip) => {
                        if ac3_boundary_corroborated(&self.buf, skip) {
                            self.buf.drain(..skip);
                            continue;
                        }
                        // Not corroborated — fall through and interpret the
                        // 0x0B77 bytes as a TrueHD access unit instead.
                    }
                }
            }

            // TrueHD access unit: lower 12 bits of first 2 bytes = length in words
            let unit_words = (((self.buf[0] as usize) << 8) | self.buf[1] as usize) & 0xFFF;
            if unit_words == 0 {
                // A zero-length AU is malformed/padding. The AU header is 4 bytes
                // (length + timing); drain the whole header, not just the length
                // word, otherwise the timing bytes get misread as the next
                // length word and produce a spurious parse on the next iteration.
                self.buf.drain(..4);
                continue;
            }
            // unit_words is masked to 12 bits, so unit_bytes <= 4095 * 2 = 8190;
            // no separate oversize-resync guard is reachable.
            let unit_bytes = unit_words * 2;
            if self.buf.len() < unit_bytes {
                break; // incomplete access unit, wait for more data
            }

            let is_major_sync = unit_bytes >= 8
                && (u32::from_be_bytes([self.buf[4], self.buf[5], self.buf[6], self.buf[7]])
                    & 0xFFFF_FFFE)
                    == 0xF872_6FBA;

            frames.push(Frame {
                pts_ns: self.next_pts_ns,
                keyframe: is_major_sync,
                data: self.buf[..unit_bytes].to_vec(),
                duration_ns: None,
            });
            self.buf.drain(..unit_bytes);
            self.next_pts_ns += AU_DURATION_NS;
        }

        // Bound memory on malformed input: a stream that never yields a
        // complete frame must not grow the buffer without limit.
        if self.buf.len() > MAX_TRUEHD_BUF {
            self.buf.clear();
        }

        frames
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Per-bit channel counts for the TrueHD 8-channel and 6-channel presentation
/// channel-assignment masks (per the MLP spec / FFmpeg `thd_channels`). Some
/// bits denote a stereo pair (2), others a single channel (1).
const THD_8CH: [u8; 13] = [2, 1, 1, 2, 2, 2, 2, 1, 1, 2, 2, 1, 1];
const THD_6CH: [u8; 5] = [2, 1, 1, 2, 1];

/// Decode the true channel count from a TrueHD major-sync `format_info` word
/// (the 32 bits immediately after the 0xF8726FBA sync). Returns the richest
/// presentation's channel count — the 8-channel (e.g. 7.1) presentation when
/// present, else the 6-channel (5.1) one. This is the real layout that the MPLS
/// `audio_format` base field (often 5.1 even on a 7.1/Atmos track) understates.
pub fn truehd_channels(format_info: u32) -> Option<u8> {
    let ch8 = (format_info & 0x1FFF) as u16; // 8ch_presentation_channel_assignment (13 bits)
    let ch6 = ((format_info >> 15) & 0x1F) as u16; // 6ch_presentation_channel_assignment (5 bits)
    let count = |mask: u16, tbl: &[u8]| -> u8 {
        tbl.iter()
            .enumerate()
            .filter(|(i, _)| mask & (1 << i) != 0)
            .map(|(_, &c)| c)
            .sum()
    };
    if ch8 != 0 {
        Some(count(ch8, &THD_8CH))
    } else if ch6 != 0 {
        Some(count(ch6, &THD_6CH))
    } else {
        None
    }
}

/// Scan a demuxed TrueHD elementary-stream chunk for the first major sync and
/// decode its true channel count. The stream may interleave AC-3; we scan for
/// the major-sync word anywhere and read the following `format_info`.
pub fn truehd_channels_from_stream(data: &[u8]) -> Option<u8> {
    let mut p = 0;
    while p + 8 <= data.len() {
        let w = u32::from_be_bytes([data[p], data[p + 1], data[p + 2], data[p + 3]]);
        if (w & 0xFFFF_FFFE) == 0xF872_6FBA {
            let fi = u32::from_be_bytes([data[p + 4], data[p + 5], data[p + 6], data[p + 7]]);
            return truehd_channels(fi);
        }
        p += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::ts::PesPacket;

    fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
        PesPacket {
            pid: 0x1100,
            pts,
            dts: None,
            data,
        }
    }

    fn make_truehd_unit(size_bytes: usize) -> Vec<u8> {
        let words = size_bytes / 2;
        let mut data = vec![0u8; size_bytes];
        data[0] = ((words >> 8) & 0x0F) as u8;
        data[1] = (words & 0xFF) as u8;
        data
    }

    fn make_ac3_frame() -> Vec<u8> {
        // Minimal AC-3 frame: sync 0x0B77, fscod=0 (48kHz), frmsizecod=0 (64 words = 128 bytes)
        let mut data = vec![0u8; 128];
        data[0] = 0x0B;
        data[1] = 0x77;
        data[4] = 0x00; // fscod=0, frmsizecod=0
        data
    }

    #[test]
    fn parse_empty_pes() {
        let mut parser = TrueHdParser::new();
        let pes = make_pes(Vec::new(), Some(0));
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn parse_single_unit() {
        let mut parser = TrueHdParser::new();
        let unit = make_truehd_unit(200);
        let pes = make_pes(unit, Some(90000));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data.len(), 200);
    }

    #[test]
    fn parse_unit_spanning_two_pes() {
        let mut parser = TrueHdParser::new();
        let unit = make_truehd_unit(200);
        let mid = 100;

        let pes1 = make_pes(unit[..mid].to_vec(), Some(90000));
        assert!(parser.parse(&pes1).is_empty());

        let pes2 = make_pes(unit[mid..].to_vec(), Some(93000));
        let frames = parser.parse(&pes2);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data.len(), 200);
    }

    #[test]
    fn parse_multiple_units_incrementing_pts() {
        let mut parser = TrueHdParser::new();
        let mut data = make_truehd_unit(100);
        data.extend_from_slice(&make_truehd_unit(120));
        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].data.len(), 100);
        assert_eq!(frames[1].data.len(), 120);
        assert_eq!(frames[1].pts_ns - frames[0].pts_ns, AU_DURATION_NS);
    }

    #[test]
    fn skip_interleaved_ac3() {
        let mut parser = TrueHdParser::new();
        let ac3 = make_ac3_frame();
        let truehd = make_truehd_unit(200);
        let mut data = ac3;
        data.extend_from_slice(&truehd);
        let pes = make_pes(data, Some(90000));
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data.len(), 200);
    }

    #[test]
    fn continuation_pes_pts_does_not_override_au_in_progress() {
        // An AU split across two PES packets: the FIRST PES (pts 90000) begins
        // the AU; the SECOND PES (pts 99999, a later timestamp) merely continues
        // it. The emitted AU must keep the first PES's PTS, not adopt the
        // continuation PES's later timestamp.
        let mut parser = TrueHdParser::new();
        let unit = make_truehd_unit(200);
        let mid = 100;

        let pes1 = make_pes(unit[..mid].to_vec(), Some(90000));
        assert!(parser.parse(&pes1).is_empty(), "AU held mid-assembly");

        // Continuation PES carries a later PTS that must be ignored for this AU.
        let pes2 = make_pes(unit[mid..].to_vec(), Some(99999));
        let frames = parser.parse(&pes2);
        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0].pts_ns,
            pts_to_ns(90000),
            "AU keeps the PTS of the PES that began it, not the continuation PES"
        );
    }

    #[test]
    fn new_au_after_empty_buffer_takes_new_pes_pts() {
        // After an AU fully drains (buffer empty), the next PES legitimately
        // seeds a fresh PTS base.
        let mut parser = TrueHdParser::new();
        let f1 = parser.parse(&make_pes(make_truehd_unit(200), Some(90000)));
        assert_eq!(f1.len(), 1);
        assert_eq!(f1[0].pts_ns, pts_to_ns(90000));

        // Buffer is now empty; a new PES with a new PTS starts a new AU.
        let f2 = parser.parse(&make_pes(make_truehd_unit(200), Some(180000)));
        assert_eq!(f2.len(), 1);
        assert_eq!(
            f2[0].pts_ns,
            pts_to_ns(180000),
            "new AU after empty buffer adopts the new PES PTS"
        );
    }

    #[test]
    fn zero_length_au_drains_full_header() {
        // A zero-length AU header (4 bytes: length=0 + timing) must be skipped
        // whole. If only 2 bytes were drained the timing bytes would be misread
        // as a bogus length word. Here the timing bytes are 0x01 0x90 (= 0x190 =
        // 400 words = 800 bytes) which, if misread, would stall the parser
        // waiting for 800 bytes that never come. Draining 4 lets the following
        // real unit parse.
        let mut parser = TrueHdParser::new();
        let mut data = vec![0x00, 0x00, 0x01, 0x90]; // length=0, timing=0x0190
        data.extend_from_slice(&make_truehd_unit(200));
        let frames = parser.parse(&make_pes(data, Some(90000)));
        assert_eq!(frames.len(), 1, "real unit parses after zero-length header");
        assert_eq!(frames[0].data.len(), 200);
    }

    #[test]
    fn unmappable_ac3_header_resyncs_not_stalls() {
        // A permanently unmappable 0x0B77 header at the buffer head (reserved
        // fscod==3) must NOT stall the parser. It used to be treated as
        // "incomplete, wait" and break forever, dropping every following AU.
        // Now it resyncs (drains 2 bytes) so a clean TrueHD unit behind it is
        // eventually emitted.
        let mut parser = TrueHdParser::new();
        // Unmappable AC-3-looking head: 0x0B77, byte4 fscod=3 (0xC0).
        let mut data = vec![0x0B, 0x77, 0x00, 0x00, 0xC0, 0x00];
        // A clean TrueHD AU follows.
        data.extend_from_slice(&make_truehd_unit(200));
        let frames = parser.parse(&make_pes(data, Some(90000)));
        assert_eq!(
            frames.len(),
            1,
            "TrueHD AU behind a bad header is recovered"
        );
        assert_eq!(frames[0].data.len(), 200);
        assert!(parser.buf.is_empty(), "buffer fully consumed, no stall");
    }

    #[test]
    fn truehd_au_with_0b77_head_not_stolen_by_ac3() {
        // A TrueHD AU whose first two bytes are 0x0B 0x77 (length 0xB77 = 2935
        // words = 5870 bytes) must NOT be misrouted to the AC-3 path. The AC-3
        // size for this header (fscod from byte4) would close the boundary in
        // the wrong place; the secondary corroboration rejects it because the
        // computed AC-3 end is not followed by another AC-3 sync / TrueHD AU.
        let mut parser = TrueHdParser::new();
        // 5870-byte AU starting with 0x0B 0x77. Byte 4 = 0x00 → AC-3 would
        // size it as fscod=0, frmsizecod=0 → 128 bytes. The bytes at offset 128
        // are zeros (next_words==0) → not corroborated → kept as TrueHD.
        let mut unit = vec![0u8; 5870];
        unit[0] = 0x0B; // 0xB high nibble of the 12-bit length, check nibble 0
        unit[1] = 0x77; // low byte of length 0xB77
        let frames = parser.parse(&make_pes(unit, Some(90000)));
        assert_eq!(frames.len(), 1, "0x0B77-headed TrueHD AU kept whole");
        assert_eq!(
            frames[0].data.len(),
            5870,
            "AU sized by TrueHD length, not AC-3 frame size"
        );
    }

    #[test]
    fn codec_private_none() {
        let parser = TrueHdParser::new();
        assert!(parser.codec_private().is_none());
    }

    #[test]
    fn truehd_channels_71_from_8ch_presentation() {
        // 8ch presentation assignment bits 0-4 (LR,C,LFE,LsRs,back-LR) = 2+1+1+2+2 = 8.
        let format_info = 0x1F; // low 13 bits = 0x1F
        assert_eq!(truehd_channels(format_info), Some(8));
    }

    #[test]
    fn truehd_channels_51_from_6ch_presentation() {
        // No 8ch presentation; 6ch bits 0-3 (LR,C,LFE,LsRs) = 2+1+1+2 = 6.
        let format_info = 0xF << 15; // 6ch field = 0xF, 8ch field = 0
        assert_eq!(truehd_channels(format_info), Some(6));
    }

    #[test]
    fn truehd_channels_scan_finds_major_sync() {
        // [junk][major sync 0xF8726FBA][format_info: 8ch=0x1F -> 7.1]
        let mut data = vec![0xAA, 0xBB];
        data.extend_from_slice(&0xF872_6FBAu32.to_be_bytes());
        data.extend_from_slice(&0x0000_001Fu32.to_be_bytes());
        assert_eq!(truehd_channels_from_stream(&data), Some(8));
    }
}
