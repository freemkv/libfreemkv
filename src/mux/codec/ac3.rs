//! AC3 (Dolby Digital) / EAC3 (Dolby Digital Plus) frame parser.
//!
//! AC3 frames are self-contained and always start with syncword 0x0B77.
//! Buffers across PES boundaries so frames that span two PES packets
//! are emitted complete, not truncated.

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// Sample rates indexed by fscod (0=48kHz, 1=44.1kHz, 2=32kHz). fscod=3 is
/// reserved in AC-3 and signals "fscod2" (reduced rates) in E-AC-3; we treat
/// the base rate as 48 kHz in that case for duration purposes.
const SAMPLE_RATES: [u32; 4] = [48_000, 44_100, 32_000, 48_000];

/// AC-3 (legacy) always carries 6 audio blocks × 256 samples = 1536 samples.
const AC3_SAMPLES_PER_FRAME: u32 = 1536;

/// Hard cap on the carry-over buffer. An AC-3/E-AC-3 frame is at most 8192
/// bytes (the `frame_size > 8192` reject below), so a single straddling frame
/// plus a little slack never needs more than this. If the buffer grows past
/// the cap without yielding a frame (pathological / never-syncing input) we
/// drop it and resync rather than accumulate one PES worth of data per call
/// for the whole title.
const MAX_AC3_BUF: usize = 64 * 1024;

pub struct Ac3Parser {
    /// Leftover bytes from previous PES (incomplete frame at end).
    buf: Vec<u8>,
    /// PTS (ns) to stamp on the frame that begins the carry-over `buf` — i.e.
    /// the running per-frame PTS at the point the partial tail was retained.
    /// Used by `flush()` to time the final buffered frame at EOS.
    flush_pts_ns: i64,
}

impl Default for Ac3Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Ac3Parser {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(4096),
            flush_pts_ns: 0,
        }
    }
}

impl CodecParser for Ac3Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }

        // Base PTS for the FIRST frame emitted from this call. Each subsequent
        // frame in the same call advances by the previous frame's duration, so a
        // PES that carries several AC-3 frames stamps a monotonically increasing
        // PTS per frame instead of the same PES timestamp on all of them (which
        // collapses their timecodes and drifts A/V).
        let base_pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);

        // Prepend leftover from previous PES
        self.buf.extend_from_slice(&pes.data);

        let data = &self.buf;
        let mut frames = Vec::new();
        let mut pos = 0;
        // Running PTS for the next frame to emit in this call.
        let mut frame_pts_ns = base_pts_ns;

        while pos < data.len() {
            let sync = find_ac3_sync(&data[pos..]);
            let start = match sync {
                Some(offset) => pos + offset,
                None => break,
            };

            let remaining = &data[start..];

            if remaining.len() < 6 {
                // Not enough data to determine frame size — keep for next PES
                break;
            }

            let bsid = get_bsid(remaining);
            let frame_size = if bsid >= 11 {
                eac3_frame_size(remaining)
            } else {
                ac3_frame_size(remaining)
            };

            if frame_size == 0 || frame_size > 8192 {
                // Invalid frame size — skip this sync word
                pos = start + 2;
                continue;
            }

            if start + frame_size > data.len() {
                // Incomplete frame — keep for next PES
                break;
            }

            let duration_ns = frame_duration_ns(remaining, bsid);
            frames.push(Frame {
                pts_ns: frame_pts_ns,
                keyframe: true,
                data: data[start..start + frame_size].to_vec(),
                duration_ns: Some(duration_ns),
            });
            frame_pts_ns += duration_ns as i64;
            pos = start + frame_size;
        }

        // Keep unconsumed data for the next call. `pos` is the start of the
        // unconsumed region: either a partial frame that straddles this PES
        // boundary — which, by construction, begins at a syncword (every byte
        // before `pos` was emitted as a frame or skipped as pre-sync junk) — or
        // trailing bytes too short to size/complete a frame. Carry from `pos`,
        // NOT from the next syncword: discarding bytes between `pos` and the
        // next sync would drop the partial frame we are deliberately keeping
        // across the boundary.
        let keep_from = if pos < data.len() {
            // A syncword at/after `pos` marks the carry-over start (anything
            // before it is junk with no sync). With no full sync, retain the
            // whole tail — including a lone trailing 0x0B that may be the first
            // half of a syncword split across the PES boundary.
            match find_ac3_sync(&data[pos..]) {
                Some(o) => pos + o,
                None if data.last() == Some(&0x0B) => data.len() - 1,
                None => data.len(),
            }
        } else {
            data.len()
        };

        if keep_from < data.len() {
            let tail = &data[keep_from..];
            if tail.len() > MAX_AC3_BUF {
                // No frame could be parsed out of a buffer this large — this is
                // not valid AC-3 here. Drop it and resync on the next PES rather
                // than grow without bound on pathological input.
                self.buf.clear();
            } else {
                self.buf = tail.to_vec();
                // The carried partial frame, when later completed and emitted by
                // flush() at EOS, is timed at the running per-frame PTS reached
                // here (the PTS of the next frame in presentation order).
                self.flush_pts_ns = frame_pts_ns;
            }
        } else {
            self.buf.clear();
        }

        frames
    }

    fn flush(&mut self) -> Vec<Frame> {
        // End of stream: emit a complete final frame still buffered. During
        // streaming a final frame may sit in `buf` with no following PES to
        // complete/confirm it; without this drain the last ~32 ms of audio is
        // dropped at EOS (mirrors dts.rs::flush). Only a fully-sized frame at a
        // syncword is emitted; a partial/garbage tail is discarded.
        let buf = std::mem::take(&mut self.buf);
        let Some(off) = find_ac3_sync(&buf) else {
            return Vec::new();
        };
        let frame = &buf[off..];
        if frame.len() < 6 {
            return Vec::new();
        }
        let bsid = get_bsid(frame);
        let frame_size = if bsid >= 11 {
            eac3_frame_size(frame)
        } else {
            ac3_frame_size(frame)
        };
        if frame_size == 0 || frame_size > 8192 || off + frame_size > buf.len() {
            return Vec::new();
        }
        let duration_ns = frame_duration_ns(frame, bsid);
        vec![Frame {
            pts_ns: self.flush_pts_ns,
            keyframe: true,
            data: buf[off..off + frame_size].to_vec(),
            duration_ns: Some(duration_ns),
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Number of samples per E-AC-3 frame from numblkscod (audio blocks × 256).
fn eac3_samples_per_frame(data: &[u8]) -> u32 {
    if data.len() < 5 {
        return AC3_SAMPLES_PER_FRAME;
    }
    // E-AC-3 byte 4: fscod(2) | numblkscod(2) | ... — but only when fscod != 3.
    // When fscod == 3 (fscod2 / reduced rate), numblks is fixed at 6.
    let fscod = (data[4] >> 6) & 0x03;
    if fscod == 0x03 {
        return 6 * 256;
    }
    let numblkscod = (data[4] >> 4) & 0x03;
    let numblks = match numblkscod {
        0 => 1,
        1 => 2,
        2 => 3,
        _ => 6,
    };
    numblks * 256
}

/// Sample rate (Hz) of an AC-3/E-AC-3 frame from its fscod field (byte 4 bits 7-6).
fn frame_sample_rate(data: &[u8]) -> u32 {
    if data.len() < 5 {
        return SAMPLE_RATES[0];
    }
    SAMPLE_RATES[((data[4] >> 6) & 0x03) as usize]
}

/// Duration of one AC-3/E-AC-3 frame in nanoseconds: samples_per_frame /
/// sample_rate. AC-3 is always 1536 samples; E-AC-3 derives from numblkscod.
fn frame_duration_ns(data: &[u8], bsid: u8) -> u64 {
    let samples = if bsid >= 11 {
        eac3_samples_per_frame(data)
    } else {
        AC3_SAMPLES_PER_FRAME
    } as u64;
    let rate = frame_sample_rate(data) as u64;
    // samples / rate seconds → ns, rounded to nearest.
    (samples * 1_000_000_000 + rate / 2) / rate
}

/// Find AC3/E-AC-3 syncword (0x0B77) in data.
fn find_ac3_sync(data: &[u8]) -> Option<usize> {
    (0..data.len().saturating_sub(1)).find(|&i| data[i] == 0x0B && data[i + 1] == 0x77)
}

/// Extract bsid from an AC-3/E-AC-3 frame starting at the syncword.
/// bsid is at byte 5, bits 7..3.
pub fn get_bsid(data: &[u8]) -> u8 {
    if data.len() < 6 {
        return 0;
    }
    (data[5] >> 3) & 0x1F
}

/// Calculate E-AC-3 frame size in bytes from the frmsiz field.
fn eac3_frame_size(data: &[u8]) -> usize {
    if data.len() < 4 {
        return 0;
    }
    let frmsiz = ((data[2] as usize & 0x07) << 8) | data[3] as usize;
    (frmsiz + 1) * 2
}

/// Calculate AC-3 frame size in bytes from fscod and frmsizecod.
fn ac3_frame_size(data: &[u8]) -> usize {
    if data.len() < 5 {
        return 0;
    }
    let fscod = (data[4] >> 6) & 0x03;
    let frmsizecod = (data[4] & 0x3F) as usize;
    if frmsizecod >= AC3_FRAME_SIZES.len() {
        return 0;
    }
    let words = AC3_FRAME_SIZES[frmsizecod];
    match fscod {
        0 => words[0] * 2,
        1 => words[1] * 2,
        2 => words[2] * 2,
        _ => 0,
    }
}

/// AC-3 frame size table: [frmsizecod] -> [48kHz words, 44.1kHz words, 32kHz words]
const AC3_FRAME_SIZES: [[usize; 3]; 38] = [
    [64, 69, 96],
    [64, 70, 96],
    [80, 87, 120],
    [80, 88, 120],
    [96, 104, 144],
    [96, 105, 144],
    [112, 121, 168],
    [112, 122, 168],
    [128, 139, 192],
    [128, 140, 192],
    [160, 174, 240],
    [160, 175, 240],
    [192, 208, 288],
    [192, 209, 288],
    [224, 243, 336],
    [224, 244, 336],
    [256, 278, 384],
    [256, 279, 384],
    [320, 348, 480],
    [320, 349, 480],
    [384, 417, 576],
    [384, 418, 576],
    [448, 487, 672],
    [448, 488, 672],
    [512, 557, 768],
    [512, 558, 768],
    [640, 696, 960],
    [640, 697, 960],
    [768, 835, 1152],
    [768, 836, 1152],
    [896, 975, 1344],
    [896, 976, 1344],
    [1024, 1114, 1536],
    [1024, 1115, 1536],
    [1152, 1253, 1728],
    [1152, 1254, 1728],
    [1280, 1393, 1920],
    [1280, 1394, 1920],
];

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ac3_frame(fscod: u8, frmsizecod: u8) -> Vec<u8> {
        let size = AC3_FRAME_SIZES[frmsizecod as usize][fscod as usize] * 2;
        let mut frame = vec![0u8; size];
        frame[0] = 0x0B;
        frame[1] = 0x77;
        frame[4] = (fscod << 6) | frmsizecod;
        frame[5] = 0x08 << 3; // bsid = 8 (AC-3)
        frame
    }

    #[test]
    fn parse_empty_pes() {
        let mut parser = Ac3Parser::new();
        let pes = PesPacket {
            pid: 0,
            pts: None,
            dts: None,
            data: vec![],
        };
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn parse_single_frame() {
        let mut parser = Ac3Parser::new();
        let frame_data = make_ac3_frame(0, 2); // 48kHz, 80 words = 160 bytes
        let pes = PesPacket {
            pid: 0,
            pts: Some(90000),
            dts: None,
            data: frame_data.clone(),
        };
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data.len(), 160);
    }

    #[test]
    fn parse_frame_spanning_two_pes() {
        let mut parser = Ac3Parser::new();
        let frame_data = make_ac3_frame(0, 2); // 160 bytes
        let mid = 80;

        // First PES: first half of frame
        let pes1 = PesPacket {
            pid: 0,
            pts: Some(90000),
            dts: None,
            data: frame_data[..mid].to_vec(),
        };
        let frames1 = parser.parse(&pes1);
        assert!(frames1.is_empty(), "partial frame should not emit");

        // Second PES: second half
        let pes2 = PesPacket {
            pid: 0,
            pts: Some(93000),
            dts: None,
            data: frame_data[mid..].to_vec(),
        };
        let frames2 = parser.parse(&pes2);
        assert_eq!(frames2.len(), 1);
        assert_eq!(frames2[0].data.len(), 160);
    }

    #[test]
    fn skip_garbage_before_sync() {
        let mut parser = Ac3Parser::new();
        let frame_data = make_ac3_frame(0, 2);
        let mut data = vec![0xDE, 0xAD, 0xBE, 0xEF]; // garbage
        data.extend_from_slice(&frame_data);
        let pes = PesPacket {
            pid: 0,
            pts: None,
            dts: None,
            data,
        };
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data.len(), 160);
    }

    #[test]
    fn sync_word_split_across_pes_is_preserved() {
        // A frame whose 0x0B77 syncword straddles the PES boundary (0x0B at the
        // tail of PES 1, 0x77 at the head of PES 2) must still be emitted whole.
        // Previously the lone trailing 0x0B was dropped and the frame lost.
        let mut parser = Ac3Parser::new();
        let frame_data = make_ac3_frame(0, 2); // 160 bytes, starts with 0x0B 0x77

        // PES 1: a complete frame, then a single 0x0B (first half of next sync).
        let mut pes1_data = frame_data.clone();
        pes1_data.push(0x0B);
        let pes1 = PesPacket {
            pid: 0,
            pts: Some(90000),
            dts: None,
            data: pes1_data,
        };
        let frames1 = parser.parse(&pes1);
        assert_eq!(frames1.len(), 1, "first complete frame emitted");

        // PES 2: 0x77 (second half of sync) + rest of the second frame.
        let mut pes2_data = vec![0x77];
        pes2_data.extend_from_slice(&frame_data[2..]);
        let pes2 = PesPacket {
            pid: 0,
            pts: Some(93000),
            dts: None,
            data: pes2_data,
        };
        let frames2 = parser.parse(&pes2);
        assert_eq!(frames2.len(), 1, "split-sync frame must be recovered");
        assert_eq!(frames2[0].data.len(), 160);
    }

    #[test]
    fn buffer_stays_bounded_across_many_garbage_pes() {
        // Finding 14: the carry-over buffer must never grow without bound. Feed
        // many large PES packets that contain no usable frame and assert the
        // retained buffer stays tiny — carry-from-`pos` drops all pre-sync junk,
        // and a never-completing frame is bounded by the 8192-byte frame cap and
        // the MAX_AC3_BUF resync guard.
        let mut parser = Ac3Parser::new();
        for i in 0..256 {
            // Vary the trailing byte so we also exercise the lone-0x0B retain.
            let mut data = vec![0x55u8; 8192];
            if i % 3 == 0 {
                *data.last_mut().unwrap() = 0x0B;
            }
            let pes = PesPacket {
                pid: 0,
                pts: None,
                dts: None,
                data,
            };
            let frames = parser.parse(&pes);
            assert!(frames.is_empty());
            assert!(
                parser.buf.len() <= MAX_AC3_BUF,
                "buffer grew to {} (cap {})",
                parser.buf.len(),
                MAX_AC3_BUF
            );
        }
        // After all that garbage the retained tail is at most a single partial
        // syncword byte — never an accumulation of whole PES packets.
        assert!(parser.buf.len() <= 1, "retained {} bytes", parser.buf.len());
    }

    #[test]
    fn split_sync_below_cap_is_still_retained() {
        // The cap must not break the normal split-sync straddle: a short tail
        // ending in 0x0B (well under the cap) is retained so the next PES can
        // complete the syncword.
        let mut parser = Ac3Parser::new();
        let data = vec![0x00, 0x00, 0x0B];
        let pes = PesPacket {
            pid: 0,
            pts: None,
            dts: None,
            data,
        };
        assert!(parser.parse(&pes).is_empty());
        assert_eq!(parser.buf, vec![0x0B], "lone trailing 0x0B retained");
    }

    #[test]
    fn flush_emits_complete_buffered_frame_at_eos() {
        // A complete final frame sitting in the carry-over buffer with no
        // following PES must be drained by flush() at EOS — the bug was that
        // ac3 inherited the no-op default flush and dropped the last frame.
        let mut parser = Ac3Parser::new();
        let frame_data = make_ac3_frame(0, 2);
        parser.buf = frame_data.clone();
        parser.flush_pts_ns = pts_to_ns(99000);
        let f = parser.flush();
        assert_eq!(f.len(), 1, "complete buffered frame drained at EOS");
        assert_eq!(f[0].data.len(), 160);
        assert_eq!(f[0].pts_ns, pts_to_ns(99000), "flush uses carried PTS");
        assert!(f[0].duration_ns.is_some(), "flush sets duration");
        assert!(parser.buf.is_empty(), "buffer consumed by flush");
    }

    #[test]
    fn flush_carries_running_pts_from_partial_tail() {
        // After a full frame emits in parse, the partial next frame held in the
        // buffer is timed at the running per-frame PTS; flush completing it must
        // use that, not the original PES base.
        let mut parser = Ac3Parser::new();
        let frame_data = make_ac3_frame(0, 2);
        let mut data = frame_data.clone();
        data.extend_from_slice(&frame_data[..40]); // partial frame 2 held
        let pes = PesPacket {
            pid: 0,
            pts: Some(90000),
            dts: None,
            data,
        };
        let f = parser.parse(&pes);
        assert_eq!(f.len(), 1, "frame 1 emitted in parse");
        let dur = f[0].duration_ns.unwrap() as i64;
        // The held partial's flush PTS should be base + one frame duration.
        assert_eq!(parser.flush_pts_ns, pts_to_ns(90000) + dur);
    }

    #[test]
    fn flush_drops_partial_tail() {
        // A partial frame (cannot be sized/completed) at EOS is dropped, not
        // emitted truncated.
        let mut parser = Ac3Parser::new();
        let frame_data = make_ac3_frame(0, 2);
        parser.buf = frame_data[..80].to_vec(); // half a frame
        assert!(parser.flush().is_empty(), "partial tail dropped");
    }

    #[test]
    fn per_frame_pts_increments_within_one_pes() {
        // Two AC-3 frames in a single PES must get distinct, increasing PTS —
        // one per frame, not the single PES timestamp on both.
        let mut parser = Ac3Parser::new();
        let frame_data = make_ac3_frame(0, 2); // 48kHz, 1536 samples
        let mut data = frame_data.clone();
        data.extend_from_slice(&frame_data);
        let pes = PesPacket {
            pid: 0,
            pts: Some(90000),
            dts: None,
            data,
        };
        let f = parser.parse(&pes);
        assert_eq!(f.len(), 2);
        assert_eq!(f[0].pts_ns, pts_to_ns(90000), "frame 0 uses PES base PTS");
        // 1536 samples @ 48kHz = 32 ms = 32_000_000 ns.
        let expect = 1536u64 * 1_000_000_000 / 48_000;
        assert_eq!(f[0].duration_ns, Some(expect));
        assert_eq!(
            f[1].pts_ns - f[0].pts_ns,
            expect as i64,
            "frame 1 PTS advances by one frame duration, not equal to frame 0"
        );
    }

    #[test]
    fn frame_duration_ac3_48khz() {
        // AC-3 @ 48kHz: 1536 / 48000 s = 32 ms.
        let frame = make_ac3_frame(0, 2);
        let bsid = get_bsid(&frame);
        assert!(bsid < 11, "test frame is legacy AC-3");
        assert_eq!(frame_duration_ns(&frame, bsid), 32_000_000);
    }

    #[test]
    fn ac3_frame_size_table() {
        // fscod=0 (48kHz), frmsizecod=0: 64 words = 128 bytes
        assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x00, 0x40]), 128);
        // fscod=0 (48kHz), frmsizecod=2: 80 words = 160 bytes
        assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x02, 0x40]), 160);
    }
}
