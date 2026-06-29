//! AC3 (Dolby Digital) / EAC3 (Dolby Digital Plus) frame parser.
//!
//! AC3 frames are self-contained and always start with syncword 0x0B77.
//! Buffers across PES boundaries so frames that span two PES packets
//! are emitted complete, not truncated.

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

/// Sample rates indexed by fscod (0=48kHz, 1=44.1kHz, 2=32kHz). fscod=3 is
/// reserved in AC-3; in E-AC-3 it signals "fscod2" (reduced rates: 24/22.05/16
/// kHz, selected by byte-4 bits [5:4]). `frame_sample_rate` decodes fscod2 in
/// the E-AC-3 case; this table's index-3 entry (48 kHz) is only the fallback
/// when the header is too short to read fscod2.
const SAMPLE_RATES: [u32; 4] = [48_000, 44_100, 32_000, 48_000];

/// E-AC-3 reduced sample rates indexed by fscod2 (byte-4 bits [5:4]), used when
/// fscod==3. Index 3 is reserved; we fall back to 48 kHz for it.
const EAC3_REDUCED_RATES: [u32; 4] = [24_000, 22_050, 16_000, 48_000];

/// Minimum byte length of a valid (E-)AC-3 frame. A real E-AC-3 frame must carry
/// at least the syncword (2) + BSI header (~4) before any audio. `eac3_frame_size`
/// returns `(frmsiz + 1) * 2`, so frmsiz=0/1 yield 2/4-byte "frames" that are
/// sub-header junk; rejecting anything below this guards against emitting them.
const MIN_FRAME_BYTES: usize = 6;

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

            if !(MIN_FRAME_BYTES..=8192).contains(&frame_size) {
                // Invalid/sub-header frame size (e.g. an E-AC-3 frmsiz of 0/1
                // sizing to a 2/4-byte fragment) — skip this sync word.
                pos = start + 2;
                continue;
            }

            if start + frame_size > data.len() {
                // Incomplete frame — keep for next PES
                break;
            }

            let duration_ns = frame_duration_ns(remaining, bsid);
            frames.push(Frame {
                discontinuity: false,
                coding: None,
                source: None,
                pts_ns: frame_pts_ns,
                keyframe: true,
                data: data[start..start + frame_size].to_vec(),
                duration_ns: Some(duration_ns),
            });
            frame_pts_ns += duration_ns as i64;
            pos = start + frame_size;
        }

        // Keep unconsumed data for the next call. `pos` is the start of the
        // last unprocessed search region. On the `start + frame_size > len`
        // break it sits exactly at the straddling frame's syncword; on the
        // `remaining.len() < 6` break it is the value from the top of that
        // iteration, with the syncword possibly sitting after some pre-sync
        // junk — so the re-scan below (from `pos`, NOT a recomputed sync) is
        // required to locate the carry-over syncword. Carry from `pos`, NOT
        // from the next syncword: discarding bytes between `pos` and the next
        // sync would drop the partial frame we are deliberately keeping across
        // the boundary.
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
                tracing::debug!(
                    target: "mux",
                    "ac3: carry-over buffer exceeded {} bytes without a frame; dropping and resyncing",
                    MAX_AC3_BUF
                );
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
        if !(MIN_FRAME_BYTES..=8192).contains(&frame_size) || off + frame_size > buf.len() {
            return Vec::new();
        }
        let duration_ns = frame_duration_ns(frame, bsid);
        vec![Frame {
            discontinuity: false,
            coding: None,
            source: None,
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

/// Sample rate (Hz) of an AC-3/E-AC-3 frame from its fscod field (byte 4 bits
/// 7-6). For E-AC-3 (`bsid >= 11`) an fscod of 3 selects a reduced rate via
/// fscod2 (byte 4 bits [5:4]); decoding it keeps the frame duration correct
/// instead of mistiming reduced-rate frames at 48 kHz (A/V drift).
fn frame_sample_rate(data: &[u8], bsid: u8) -> u32 {
    if data.len() < 5 {
        return SAMPLE_RATES[0];
    }
    let fscod = (data[4] >> 6) & 0x03;
    if fscod == 0x03 && bsid >= 11 {
        let fscod2 = (data[4] >> 4) & 0x03;
        return EAC3_REDUCED_RATES[fscod2 as usize];
    }
    SAMPLE_RATES[fscod as usize]
}

/// Duration of one AC-3/E-AC-3 frame in nanoseconds: samples_per_frame /
/// sample_rate. AC-3 is always 1536 samples; E-AC-3 derives from numblkscod.
fn frame_duration_ns(data: &[u8], bsid: u8) -> u64 {
    let samples = if bsid >= 11 {
        eac3_samples_per_frame(data)
    } else {
        AC3_SAMPLES_PER_FRAME
    } as u64;
    let rate = frame_sample_rate(data, bsid) as u64;
    // samples / rate seconds → ns, rounded to nearest.
    (samples * 1_000_000_000 + rate / 2) / rate
}

/// Base channel count per AC-3 `acmod` (A/52 Table 5.8), BEFORE the LFE.
/// Index is the 3-bit acmod value; add 1 when `lfeon` is set.
///
/// ```text
///   0 = 1+1 (Ch1, Ch2)  -> 2     4 = 2/1 (L,R,S)        -> 3
///   1 = 1/0 (C, mono)   -> 1     5 = 3/1 (L,C,R,S)      -> 4
///   2 = 2/0 (L, R)      -> 2     6 = 2/2 (L,R,SL,SR)    -> 4
///   3 = 3/0 (L,C,R)     -> 3     7 = 3/2 (L,C,R,SL,SR)  -> 5
/// ```
const ACMOD_CHANNELS: [u8; 8] = [2, 1, 2, 3, 3, 4, 4, 5];

/// Decode the channel count of an (E-)AC-3 frame from its bitstream `acmod` and
/// `lfeon`, starting at the 0x0B77 syncword. Returns `None` when the frame is
/// too short to carry the BSI bits.
///
/// This is the AUTHORITATIVE channel count for the track header: the DVD IFO
/// `audio_attr_t.channels` nibble is a well-known unreliable/stale field, so
/// the muxer prefers this over the IFO-claimed count (mirrors MakeMKV /
/// HandBrake, which never trust the IFO audio nibble). LFE adds one channel
/// (e.g. acmod=7 + lfeon → 6 = 5.1).
///
/// Bit layout from the syncword (A/52 §5.3.2 BSI):
///
/// ```text
///   byte 5: bsid(5) | bsmod(3)
///   byte 6: acmod(3) | [cmixlev(2) if acmod has a centre and acmod!=1]
///                    | [surmixlev(2) if acmod has surround]
///                    | [dsurmod(2) if acmod==2] | lfeon(1) | ...
/// ```
///
/// `acmod` therefore always occupies byte-6 bits 7-5; `lfeon` follows a
/// variable number of optional 2-bit fields, so we track the bit cursor.
pub(crate) fn acmod_channels(data: &[u8]) -> Option<u8> {
    // Need at least bytes 0..=6 to read acmod (byte 6) and its trailing
    // optional fields + lfeon (which never spills past byte 7 for any acmod).
    if data.len() < 8 {
        return None;
    }
    let bsid = get_bsid(data);
    // E-AC-3 (bsid >= 11, Annex E) uses a different BSI layout. DVD audio is
    // always legacy AC-3 (bsid <= 8); for E-AC-3 we don't decode acmod here
    // and let the caller fall back to the passed channel count.
    if bsid >= 11 {
        return None;
    }
    // Bit cursor over `data`, MSB-first, starting at byte 6 bit 7 (= bit 48).
    let mut bit = 6 * 8;
    let read = |n: usize, bit: &mut usize| -> u32 {
        let mut v = 0u32;
        for _ in 0..n {
            let byte = data[*bit / 8];
            let shift = 7 - (*bit % 8);
            v = (v << 1) | ((byte >> shift) & 1) as u32;
            *bit += 1;
        }
        v
    };
    let acmod = read(3, &mut bit) as usize;
    // cmixlev: present when acmod has a centre channel AND is not the 1/0
    // (centre-only) mode — i.e. acmod & 0x1 != 0 && acmod != 0x1.
    if (acmod & 0x1) != 0 && acmod != 0x1 {
        let _cmixlev = read(2, &mut bit);
    }
    // surmixlev: present when acmod has a surround channel (acmod & 0x4).
    if (acmod & 0x4) != 0 {
        let _surmixlev = read(2, &mut bit);
    }
    // dsurmod: present only for the 2/0 (stereo) mode.
    if acmod == 0x2 {
        let _dsurmod = read(2, &mut bit);
    }
    let lfeon = read(1, &mut bit);
    Some(ACMOD_CHANNELS[acmod] + lfeon as u8)
}

/// Find AC3/E-AC-3 syncword (0x0B77) in data.
pub(crate) fn find_ac3_sync(data: &[u8]) -> Option<usize> {
    (0..data.len().saturating_sub(1)).find(|&i| data[i] == 0x0B && data[i + 1] == 0x77)
}

/// Extract bsid from an AC-3/E-AC-3 frame starting at the syncword.
/// bsid is at byte 5, bits 7..3.
fn get_bsid(data: &[u8]) -> u8 {
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

/// Calculate AC-3 frame size in bytes from fscod and frmsizecod. Returns 0 for
/// an unmappable header (reserved fscod==3, or frmsizecod out of table range).
/// `pub(crate)` so the TrueHD parser can reuse it when skipping interleaved AC-3
/// frames instead of duplicating the size table.
pub(crate) fn ac3_frame_size(data: &[u8]) -> usize {
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
            source: None,
            pid: 0,
            pts: None,
            dts: None,
            data: vec![],
            discontinuity: false,
        };
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn parse_single_frame() {
        let mut parser = Ac3Parser::new();
        let frame_data = make_ac3_frame(0, 2); // 48kHz, 80 words = 160 bytes
        let pes = PesPacket {
            source: None,
            pid: 0,
            pts: Some(90000),
            dts: None,
            data: frame_data.clone(),
            discontinuity: false,
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
            source: None,
            pid: 0,
            pts: Some(90000),
            dts: None,
            data: frame_data[..mid].to_vec(),
            discontinuity: false,
        };
        let frames1 = parser.parse(&pes1);
        assert!(frames1.is_empty(), "partial frame should not emit");

        // Second PES: second half
        let pes2 = PesPacket {
            source: None,
            pid: 0,
            pts: Some(93000),
            dts: None,
            data: frame_data[mid..].to_vec(),
            discontinuity: false,
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
            source: None,
            pid: 0,
            pts: None,
            dts: None,
            data,
            discontinuity: false,
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
            source: None,
            pid: 0,
            pts: Some(90000),
            dts: None,
            data: pes1_data,
            discontinuity: false,
        };
        let frames1 = parser.parse(&pes1);
        assert_eq!(frames1.len(), 1, "first complete frame emitted");

        // PES 2: 0x77 (second half of sync) + rest of the second frame.
        let mut pes2_data = vec![0x77];
        pes2_data.extend_from_slice(&frame_data[2..]);
        let pes2 = PesPacket {
            source: None,
            pid: 0,
            pts: Some(93000),
            dts: None,
            data: pes2_data,
            discontinuity: false,
        };
        let frames2 = parser.parse(&pes2);
        assert_eq!(frames2.len(), 1, "split-sync frame must be recovered");
        assert_eq!(frames2[0].data.len(), 160);
    }

    #[test]
    fn buffer_stays_bounded_across_many_garbage_pes() {
        // The carry-over buffer must never grow without bound. Feed
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
                source: None,
                pid: 0,
                pts: None,
                dts: None,
                data,
                discontinuity: false,
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
            source: None,
            pid: 0,
            pts: None,
            dts: None,
            data,
            discontinuity: false,
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
            source: None,
            pid: 0,
            pts: Some(90000),
            dts: None,
            data,
            discontinuity: false,
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
            source: None,
            pid: 0,
            pts: Some(90000),
            dts: None,
            data,
            discontinuity: false,
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
    fn eac3_subheader_sized_frame_is_rejected() {
        // An E-AC-3 sync with frmsiz=0 sizes to a 2-byte "frame"; frmsiz=1 to
        // 4 bytes. Both are sub-header junk that must NOT be emitted as audio.
        // bsid must be >= 11 for the E-AC-3 sizing path. Byte 5 bits 7..3 = bsid.
        let mut parser = Ac3Parser::new();
        // Build an E-AC-3 sync: 0x0B 0x77, frmsiz=0 (bytes 2-3 low bits = 0),
        // bsid=16 (>=11) at byte 5. Pad to a few bytes so find_ac3_sync + sizing
        // run. eac3_frame_size = (0 + 1) * 2 = 2 < MIN_FRAME_BYTES.
        let mut data = vec![0x0B, 0x77, 0x00, 0x00, 0x00, 16 << 3, 0x00, 0x00];
        // Append a real AC-3 frame after the junk so we can confirm the parser
        // resyncs past the junk and still emits the valid frame.
        let good = make_ac3_frame(0, 2);
        data.extend_from_slice(&good);
        let pes = PesPacket {
            source: None,
            pid: 0,
            pts: Some(90000),
            dts: None,
            data,
            discontinuity: false,
        };
        let frames = parser.parse(&pes);
        assert_eq!(frames.len(), 1, "only the real AC-3 frame is emitted");
        assert_eq!(frames[0].data.len(), 160);
    }

    #[test]
    fn eac3_fscod2_reduced_rate_duration() {
        // E-AC-3 with fscod==3 (reduced rate) and fscod2==0 → 24 kHz, not 48.
        // bsid>=11 selects the E-AC-3 path. When fscod==3 the block count is
        // fixed at 6 → 1536 samples. Byte 4 layout: fscod(2)|fscod2(2)|...
        // fscod=3 (0b11), fscod2=0 (0b00) → byte4 = 0b1100_0000 = 0xC0.
        let data = [0x0B, 0x77, 0x00, 0x00, 0xC0, 16 << 3];
        let bsid = get_bsid(&data);
        assert!(bsid >= 11, "test frame is E-AC-3");
        // 1536 samples / 24000 Hz = 64 ms.
        assert_eq!(frame_duration_ns(&data, bsid), 64_000_000);
    }

    #[test]
    fn ac3_frame_size_table() {
        // fscod=0 (48kHz), frmsizecod=0: 64 words = 128 bytes
        assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x00, 0x40]), 128);
        // fscod=0 (48kHz), frmsizecod=2: 80 words = 160 bytes
        assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x02, 0x40]), 160);
    }

    // --- ac3_frame_size: fscod-indexed table columns + reject paths ---

    #[test]
    fn ac3_frame_size_44100_uses_second_column() {
        // ATSC A/52 Table 5.18: fscod=1 (44.1 kHz), frmsizecod=0 → 69 words.
        // byte4 = fscod(2)<<6 | frmsizecod(6) = 0b01_000000 = 0x40.
        assert_eq!(
            ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x40, 0x00]),
            69 * 2,
            "44.1kHz column (index 1), 69 words = 138 bytes"
        );
    }

    #[test]
    fn ac3_frame_size_32000_uses_third_column() {
        // A/52 Table 5.18: fscod=2 (32 kHz), frmsizecod=0 → 96 words.
        // byte4 = 0b10_000000 = 0x80.
        assert_eq!(
            ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x80, 0x00]),
            96 * 2,
            "32kHz column (index 2), 96 words = 192 bytes"
        );
    }

    #[test]
    fn ac3_frame_size_reserved_fscod3_is_unmappable() {
        // fscod=3 is RESERVED in AC-3 (A/52 §5.4.1.3). The size function must
        // return 0 (unmappable), never index the table. byte4 = 0b11_000000.
        assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0xC0, 0x00]), 0);
    }

    #[test]
    fn ac3_frame_size_frmsizecod_out_of_range_is_zero() {
        // frmsizecod has 38 valid entries (0..=37). 38..=63 are reserved.
        // frmsizecod=38 (0b100110) with fscod=0 → byte4 = 0x26. Must return 0.
        assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x26, 0x00]), 0);
        // The largest reserved code (63 = 0x3F) likewise.
        assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x3F, 0x00]), 0);
    }

    #[test]
    fn ac3_frame_size_short_input_is_zero() {
        // Fewer than 5 bytes can't carry byte 4 → 0, no panic.
        assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0]), 0);
        assert_eq!(ac3_frame_size(&[]), 0);
    }

    #[test]
    fn ac3_frame_size_max_frmsizecod_37() {
        // Last valid frmsizecod=37 (0b100101), fscod=0 → 1280 words = 2560 bytes.
        // byte4 = 0x25.
        assert_eq!(ac3_frame_size(&[0x0B, 0x77, 0, 0, 0x25, 0x00]), 1280 * 2);
    }

    // --- E-AC-3 frame sizing (frmsiz field bytes 2-3) ---

    #[test]
    fn eac3_frame_size_formula() {
        // E-AC-3 (A/52 Annex E): frmsiz = byte2[2:0]<<8 | byte3; frame bytes =
        // (frmsiz + 1) * 2. With byte2=0x07 (low 3 bits set) and byte3=0xFF,
        // frmsiz = 0x7FF = 2047 → (2048)*2 = 4096 bytes.
        assert_eq!(eac3_frame_size(&[0x0B, 0x77, 0x07, 0xFF]), 4096);
        // frmsiz=2 → (3)*2 = 6 bytes (== MIN_FRAME_BYTES).
        assert_eq!(eac3_frame_size(&[0x0B, 0x77, 0x00, 0x02]), 6);
    }

    #[test]
    fn eac3_frame_size_short_input_zero() {
        // < 4 bytes can't carry the frmsiz field → 0, no panic.
        assert_eq!(eac3_frame_size(&[0x0B, 0x77, 0x00]), 0);
    }

    #[test]
    fn eac3_frame_size_masks_byte2_to_three_bits() {
        // Only the low 3 bits of byte 2 belong to frmsiz; the upper 5 bits
        // (strmtyp/substreamid) must be masked off. byte2=0xFF, byte3=0x00 →
        // frmsiz = (0xFF & 0x07)<<8 | 0 = 0x700 = 1792 → (1793)*2 = 3586.
        assert_eq!(eac3_frame_size(&[0x0B, 0x77, 0xFF, 0x00]), (1792 + 1) * 2);
    }

    // --- get_bsid: byte 5 bits 7..3, the AC-3/E-AC-3 selector ---

    #[test]
    fn get_bsid_extracts_bits_7_3() {
        // bsid lives in byte 5 bits 7..3 (A/52 §5.3.2 BSI). 0b10101_000 = 0xA8 →
        // bsid = 0b10101 = 21.
        assert_eq!(get_bsid(&[0x0B, 0x77, 0, 0, 0, 0xA8]), 21);
        // Low 3 bits must be ignored: 0x0F (0b00001_111) → bsid = 1.
        assert_eq!(get_bsid(&[0x0B, 0x77, 0, 0, 0, 0x0F]), 1);
    }

    #[test]
    fn get_bsid_short_input_zero() {
        assert_eq!(get_bsid(&[0x0B, 0x77, 0, 0, 0]), 0);
    }

    #[test]
    fn bsid_11_is_first_eac3_value() {
        // The parser switches to E-AC-3 sizing at bsid >= 11. bsid=10 must use
        // AC-3 sizing, bsid=11 E-AC-3. byte5 = bsid<<3.
        assert_eq!(get_bsid(&[0x0B, 0x77, 0, 0, 0, 10 << 3]), 10);
        assert_eq!(get_bsid(&[0x0B, 0x77, 0, 0, 0, 11 << 3]), 11);
    }

    // --- frame_sample_rate / frame_duration: per-fscod and fscod2 ---

    #[test]
    fn ac3_duration_44100() {
        // Legacy AC-3 @ 44.1kHz: 1536 / 44100 s. fscod=1 → byte4 bits 7-6 = 01.
        // Build a real frame so the sizing path validates too.
        let frame = make_ac3_frame(1, 0); // fscod=1, frmsizecod=0
        let bsid = get_bsid(&frame);
        assert!(bsid < 11);
        // (1536 * 1e9 + 44100/2) / 44100, rounded to nearest.
        let expect = (1536u64 * 1_000_000_000 + 44_100 / 2) / 44_100;
        assert_eq!(frame_duration_ns(&frame, bsid), expect);
    }

    #[test]
    fn ac3_duration_32000() {
        // 1536 / 32000 s = 48 ms exactly.
        let frame = make_ac3_frame(2, 0); // fscod=2 (32kHz)
        let bsid = get_bsid(&frame);
        assert_eq!(frame_duration_ns(&frame, bsid), 48_000_000);
    }

    #[test]
    fn eac3_fscod2_22050_reduced_rate() {
        // E-AC-3 fscod==3, fscod2==1 → 22.05 kHz (EAC3_REDUCED_RATES[1]).
        // byte4 = fscod(11) | fscod2(01) << 4 = 0b1101_0000 = 0xD0. fscod==3
        // fixes numblks to 6 → 1536 samples.
        let data = [0x0B, 0x77, 0x00, 0x00, 0xD0, 16 << 3];
        let bsid = get_bsid(&data);
        assert!(bsid >= 11);
        let expect = (1536u64 * 1_000_000_000 + 22_050 / 2) / 22_050;
        assert_eq!(frame_duration_ns(&data, bsid), expect);
    }

    #[test]
    fn eac3_fscod2_16000_reduced_rate() {
        // fscod==3, fscod2==2 → 16 kHz. byte4 = 0b1110_0000 = 0xE0.
        let data = [0x0B, 0x77, 0x00, 0x00, 0xE0, 16 << 3];
        let bsid = get_bsid(&data);
        let expect = 1536u64 * 1_000_000_000 / 16_000; // exact
        assert_eq!(frame_duration_ns(&data, bsid), expect);
    }

    #[test]
    fn eac3_fscod2_reserved_index3_falls_back_48k() {
        // fscod==3, fscod2==3 is RESERVED; the code falls back to 48 kHz
        // (EAC3_REDUCED_RATES[3]). byte4 = 0b1111_0000 = 0xF0.
        let data = [0x0B, 0x77, 0x00, 0x00, 0xF0, 16 << 3];
        let bsid = get_bsid(&data);
        let expect = 1536u64 * 1_000_000_000 / 48_000; // 32ms
        assert_eq!(frame_duration_ns(&data, bsid), expect);
    }

    #[test]
    fn ac3_fscod3_does_not_use_fscod2_path() {
        // For LEGACY AC-3 (bsid < 11) fscod==3 is reserved; frame_sample_rate
        // must NOT take the fscod2 branch (that is E-AC-3 only) and must index
        // SAMPLE_RATES[3] = 48000 fallback. Duration = 1536/48000 = 32ms.
        let data = [0x0B, 0x77, 0x00, 0x00, 0xC0, 8 << 3]; // bsid=8 (AC-3)
        let bsid = get_bsid(&data);
        assert!(bsid < 11);
        assert_eq!(frame_duration_ns(&data, bsid), 32_000_000);
    }

    #[test]
    fn frame_sample_rate_short_input_defaults_48k() {
        // < 5 bytes → SAMPLE_RATES[0] = 48000 default (can't read fscod).
        let short = [0x0B, 0x77, 0x00, 0x00];
        let expect = 1536u64 * 1_000_000_000 / 48_000;
        assert_eq!(frame_duration_ns(&short, 8), expect);
    }

    // --- eac3_samples_per_frame: numblkscod table ---

    #[test]
    fn eac3_numblkscod_block_counts() {
        // A/52 Annex E numblkscod (byte4 bits 5-4 when fscod != 3):
        //   0→1 block, 1→2, 2→3, 3→6 blocks; each block = 256 samples.
        // fscod=0 keeps the fscod2 path off. byte4 = numblkscod << 4.
        let mk = |numblkscod: u8| [0x0B, 0x77, 0x00, 0x00, numblkscod << 4, 0x00];
        assert_eq!(
            eac3_samples_per_frame(&mk(0)),
            256,
            "numblkscod 0 → 1 block"
        );
        assert_eq!(
            eac3_samples_per_frame(&mk(1)),
            512,
            "numblkscod 1 → 2 blocks"
        );
        assert_eq!(
            eac3_samples_per_frame(&mk(2)),
            768,
            "numblkscod 2 → 3 blocks"
        );
        assert_eq!(
            eac3_samples_per_frame(&mk(3)),
            1536,
            "numblkscod 3 → 6 blocks"
        );
    }

    #[test]
    fn eac3_samples_fscod3_fixed_at_six_blocks() {
        // When fscod==3 (reduced rate), numblks is fixed at 6 regardless of the
        // numblkscod bits. byte4 = 0b11_xx_0000; set the numblkscod bits to 0
        // (would otherwise be 1 block) to prove the fscod==3 override wins.
        let data = [0x0B, 0x77, 0x00, 0x00, 0xC0, 0x00];
        assert_eq!(eac3_samples_per_frame(&data), 6 * 256);
    }

    #[test]
    fn eac3_samples_short_input_defaults_1536() {
        // < 5 bytes → AC3_SAMPLES_PER_FRAME (1536) fallback.
        assert_eq!(eac3_samples_per_frame(&[0x0B, 0x77, 0x00, 0x00]), 1536);
    }

    // --- frame acceptance / rejection at the size boundaries ---

    #[test]
    fn eac3_frame_at_min_frame_bytes_is_accepted() {
        // The smallest acceptable (E-)AC-3 frame is MIN_FRAME_BYTES = 6.
        // Build an E-AC-3 frame whose frmsiz sizes it to exactly 6 bytes
        // (frmsiz=2). bsid >= 11 selects E-AC-3 sizing. The parser must emit it.
        let mut parser = Ac3Parser::new();
        // 0x0B 0x77 | byte2=0 byte3=2 (frmsiz=2 → 6 bytes) | byte4=0 | byte5 bsid
        let mut data = vec![0x0B, 0x77, 0x00, 0x02, 0x00, 16 << 3];
        // pad to exactly 6 bytes (already 6). Then a trailing real AC-3 frame so
        // the 6-byte frame isn't a tail that needs more data.
        data.truncate(6);
        data.extend_from_slice(&make_ac3_frame(0, 2));
        let f = parser.parse(&make_eac3_pes(data));
        assert_eq!(f.len(), 2, "6-byte E-AC-3 frame accepted + following AC-3");
        assert_eq!(f[0].data.len(), 6);
    }

    #[test]
    fn eac3_max_frmsiz_frame_within_window_accepted() {
        // E-AC-3 frmsiz is an 11-bit field (3 bits of byte2 + 8 bits of byte3),
        // so its maximum value is 0x7FF = 2047 → (2048)*2 = 4096 bytes, which is
        // inside the MIN_FRAME_BYTES..=8192 accept window and must be emitted.
        let mut parser = Ac3Parser::new();
        let mut frame = vec![0u8; 4096];
        frame[0] = 0x0B;
        frame[1] = 0x77;
        frame[2] = 0x07; // frmsiz high
        frame[3] = 0xFF; // frmsiz low → 0x7FF = 2047 → 4096 bytes
        frame[5] = 16 << 3; // bsid 16 (E-AC-3)
        let f = parser.parse(&make_eac3_pes(frame));
        assert_eq!(f.len(), 1, "4096-byte E-AC-3 frame within window accepted");
        assert_eq!(f[0].data.len(), 4096);
    }

    #[test]
    fn undersized_sync_skips_two_bytes_and_resyncs() {
        // A sync whose decoded size is below MIN_FRAME_BYTES (here an E-AC-3
        // frmsiz=0 → 2-byte "frame") is rejected by skipping exactly 2 bytes
        // past the sync, then resyncing to the next real frame.
        let mut parser = Ac3Parser::new();
        let mut data = vec![0x0B, 0x77, 0x00, 0x00, 0x00, 16 << 3];
        data.extend_from_slice(&make_ac3_frame(0, 2)); // real frame follows
        let f = parser.parse(&make_eac3_pes(data));
        assert_eq!(f.len(), 1, "junk sync skipped, real frame found");
        assert_eq!(f[0].data.len(), 160);
    }

    // --- find_ac3_sync ---

    #[test]
    fn find_ac3_sync_locates_0b77() {
        assert_eq!(find_ac3_sync(&[0xFF, 0x0B, 0x77, 0x00]), Some(1));
        assert_eq!(find_ac3_sync(&[0x0B, 0x77]), Some(0));
    }

    #[test]
    fn find_ac3_sync_lone_0b_at_end_not_matched() {
        // A trailing lone 0x0B (no following 0x77) is not a complete syncword.
        // saturating_sub(1) prevents an out-of-bounds read of data[i+1].
        assert_eq!(find_ac3_sync(&[0xFF, 0xFF, 0x0B]), None);
        assert_eq!(find_ac3_sync(&[0x0B]), None);
        assert_eq!(find_ac3_sync(&[]), None);
    }

    #[test]
    fn find_ac3_sync_0b_without_77_no_false_positive() {
        // 0x0B followed by something other than 0x77 is not a sync.
        assert_eq!(find_ac3_sync(&[0x0B, 0x76, 0x0B, 0x78]), None);
    }

    // --- flush rejects an oversized declared frame ---

    #[test]
    fn flush_rejects_frame_extending_past_buffer() {
        // A buffered sync whose decoded frame size exceeds the buffered bytes
        // must be dropped by flush (never emit fewer bytes than the size field
        // declares). Build a real AC-3 header (160-byte frame) but only buffer
        // 100 bytes.
        let mut parser = Ac3Parser::new();
        let frame = make_ac3_frame(0, 2); // sizes to 160
        parser.buf = frame[..100].to_vec();
        assert!(
            parser.flush().is_empty(),
            "incomplete frame must not be emitted truncated at flush"
        );
    }

    #[test]
    fn flush_with_no_sync_is_empty() {
        // flush on a buffer with no syncword yields nothing and clears.
        let mut parser = Ac3Parser::new();
        parser.buf = vec![0xAA, 0xBB, 0xCC];
        assert!(parser.flush().is_empty());
    }

    // --- acmod_channels: channel count from the AC-3 BSI bitstream ---

    /// Build a minimal AC-3 BSI header (8 bytes) with a given acmod + lfeon.
    /// byte5 = bsid<<3 (bsmod=0); byte6 carries acmod in bits 7-5 followed by
    /// the optional mix-level fields and lfeon. We construct byte6/7 by writing
    /// bits MSB-first in the exact order acmod_channels reads them.
    fn make_bsi(acmod: u8, lfeon: bool) -> Vec<u8> {
        // Collect the bit sequence after byte 6 bit 7: acmod(3), [cmixlev(2)],
        // [surmixlev(2)], [dsurmod(2)], lfeon(1). Mix-level/dsurmod bits are
        // arbitrary (0 here) — only their PRESENCE shifts lfeon's position.
        let mut bits: Vec<u8> = Vec::new();
        for i in (0..3).rev() {
            bits.push((acmod >> i) & 1);
        }
        if (acmod & 0x1) != 0 && acmod != 0x1 {
            bits.push(0);
            bits.push(0); // cmixlev
        }
        if (acmod & 0x4) != 0 {
            bits.push(0);
            bits.push(0); // surmixlev
        }
        if acmod == 0x2 {
            bits.push(0);
            bits.push(0); // dsurmod
        }
        bits.push(lfeon as u8); // lfeon
        // Pack bits MSB-first starting at byte 6.
        let mut frame = vec![0u8; 8];
        frame[0] = 0x0B;
        frame[1] = 0x77;
        frame[5] = 8 << 3; // bsid = 8 (legacy AC-3), bsmod = 0
        for (idx, &b) in bits.iter().enumerate() {
            let bitpos = 6 * 8 + idx;
            if b != 0 {
                frame[bitpos / 8] |= 1 << (7 - (bitpos % 8));
            }
        }
        frame
    }

    #[test]
    fn acmod_channels_stereo_2_0_no_lfe() {
        // acmod=2 (2/0 L,R), no LFE → 2 channels. Verifies the channel count is
        // read from the AC-3 bitstream's acmod, independent of any IFO claim.
        // (A disc whose IFO lists 5.1 but where the wrong physical substream is
        // selected is a separate stream-SELECTION bug, not this label path —
        // tracked for rc.5.2.)
        assert_eq!(acmod_channels(&make_bsi(2, false)), Some(2));
    }

    #[test]
    fn acmod_channels_5_1() {
        // acmod=7 (3/2 L,C,R,SL,SR) + LFE → 6 channels (5.1).
        assert_eq!(acmod_channels(&make_bsi(7, true)), Some(6));
        // 3/2 without LFE → 5 channels.
        assert_eq!(acmod_channels(&make_bsi(7, false)), Some(5));
    }

    #[test]
    fn acmod_channels_mono_and_dual_mono() {
        // acmod=1 (1/0 centre/mono) → 1; with LFE → 2.
        assert_eq!(acmod_channels(&make_bsi(1, false)), Some(1));
        assert_eq!(acmod_channels(&make_bsi(1, true)), Some(2));
        // acmod=0 (1+1 dual mono) → 2 base channels.
        assert_eq!(acmod_channels(&make_bsi(0, false)), Some(2));
    }

    #[test]
    fn acmod_channels_3_0_and_2_1() {
        // Per A/52 Table 5.8: acmod 4 = 2/1, 5 = 3/1, 6 = 2/2.
        // acmod=4 (2/1 L,R,S) → 3 (surmixlev present, no centre → no cmixlev).
        assert_eq!(acmod_channels(&make_bsi(4, false)), Some(3));
        // acmod=5 (3/1 L,C,R,S) → 4 (centre → cmixlev present, surround →
        // surmixlev present). This is the regression case: index 5 was wrongly
        // 3 in ACMOD_CHANNELS, undercounting a 3/1 stream by one channel.
        assert_eq!(acmod_channels(&make_bsi(5, false)), Some(4));
        // acmod=5 (3/1) + LFE → 5; lfeon position shifts after both cmixlev
        // (centre) and surmixlev (surround) 2-bit fields.
        assert_eq!(acmod_channels(&make_bsi(5, true)), Some(5));
        // acmod=6 (2/2 L,R,SL,SR) → 4 (surmixlev present, no centre); +LFE → 5.
        assert_eq!(acmod_channels(&make_bsi(6, false)), Some(4));
        assert_eq!(acmod_channels(&make_bsi(6, true)), Some(5));
    }

    #[test]
    fn acmod_channels_short_frame_is_none() {
        // Fewer than 8 bytes cannot carry the BSI bits → None (caller falls
        // back to the IFO-claimed channel count).
        assert_eq!(acmod_channels(&[0x0B, 0x77, 0, 0, 0, 8 << 3]), None);
        assert_eq!(acmod_channels(&[]), None);
    }

    #[test]
    fn acmod_channels_eac3_is_none() {
        // E-AC-3 (bsid >= 11) uses a different BSI layout; acmod_channels
        // declines so the caller keeps the passed count.
        let mut data = make_bsi(2, false);
        data[5] = 16 << 3; // bsid = 16 (E-AC-3)
        assert_eq!(acmod_channels(&data), None);
    }

    #[test]
    fn acmod_channels_parses_real_built_frame() {
        // A frame built by make_ac3_frame (fscod/frmsizecod set, acmod bits 0)
        // decodes acmod=0 → 2 channels (dual mono), confirming the cursor lands
        // on the right bytes for a fully-formed frame, not just a stub header.
        let frame = make_ac3_frame(0, 2);
        // make_ac3_frame leaves byte 6 = 0 → acmod=0, lfeon=0 → 2 channels.
        assert_eq!(acmod_channels(&frame), Some(2));
    }

    // helper: PES with a generic pts for E-AC-3 tests
    fn make_eac3_pes(data: Vec<u8>) -> PesPacket {
        PesPacket {
            source: None,
            pid: 0,
            pts: Some(90000),
            dts: None,
            data,
            discontinuity: false,
        }
    }
}
