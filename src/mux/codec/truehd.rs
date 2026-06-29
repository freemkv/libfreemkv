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
use crate::mux::timeline::DISCONTINUITY_BACKSTEP_NS;

/// Duration of one TrueHD access unit in nanoseconds for the 48 kHz family
/// (48 / 96 / 192 kHz). `access_unit_size = 40 << (ratebits & 7)` and
/// `sample_rate = 48000 << (ratebits & 7)`; the shared shift cancels in
/// `samples_per_AU / sample_rate = 40/48000 = 1/1200 s`, so this constant is
/// exact for the whole 48 kHz family — 48, 96 and 192 kHz alike. Used as the
/// default until a major sync reveals the actual rate family.
const AU_DURATION_NS: i64 = 833_333;

/// Duration of one TrueHD access unit in nanoseconds for the 44.1 kHz family
/// (44.1 / 88.2 / 176.4 kHz): `40/44100 = 1/1102.5 s = 907_029.478… ns`. The
/// 48 kHz constant would run ~8.95 % fast on these (rare) streams.
const AU_DURATION_NS_441: i64 = 907_029;

/// Hard cap on the reassembly buffer. A valid TrueHD/MAT access unit is
/// well under 32 KiB; if the buffer grows far past that without yielding a
/// frame the stream is malformed, so we drop it and resync rather than grow
/// without bound. Parity with the AC-3 / DTS / PGS caps.
const MAX_TRUEHD_BUF: usize = 256 * 1024;

pub struct TrueHdParser {
    buf: Vec<u8>,
    next_pts_ns: i64,
    /// Per-AU PTS increment. Defaults to the 48 kHz-family value (833_333) and
    /// is refined to the 44.1 kHz-family value once the first major sync reveals
    /// the actual rate. Stays at the default for streams whose major sync is not
    /// yet seen (head of stream) — preserving byte-identical timing for the
    /// common 48 kHz case.
    au_duration_ns: i64,
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
            au_duration_ns: AU_DURATION_NS,
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

        // B1: a concealed/lost gap means the buffered TrueHD AU is TRUNCATED.
        // Splicing post-gap bytes onto it corrupts the AU framing (→ "Invalid
        // data found") and strands the PTS cadence (the non-monotonic audio-DTS
        // band at gaps). Drop the partial; with `buf` now empty the PTS-base block
        // below re-seeds the cadence from the post-gap PES, monotonic across the
        // gap. (Audio has no inter-frame refs — this is the whole audio fix.)
        if pes.discontinuity {
            self.buf.clear();
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
                // Resync to the authoritative PES PTS. TrueHD AUs are a fixed
                // sample count (40 @ 48 kHz), so the per-AU `+AU_DURATION_NS`
                // cadence is sample-accurate — more so than the disc's per-PES
                // PTS, which carries the source muxer's own rounding jitter.
                //
                // Two distinct backward steps must be handled OPPOSITELY:
                //
                // 1. Small backward jitter (sub-second PES rounding): when the
                //    buffer empties exactly on a PES boundary and that PES's PTS
                //    lands a few ticks *below* the running cadence, an
                //    unconditional reset would set the next AU's timestamp below
                //    the AU just emitted, producing non-monotonic block
                //    timestamps a muxer rejects. CLAMP to the running position so
                //    output stays strictly monotonic.
                //
                // 2. Large backward step (> DISCONTINUITY_BACKSTEP_NS): this is a
                //    clip-boundary PTS reset — the title's clips are read as one
                //    concatenated stream and a non-seamless boundary resets the
                //    source PES PTS near zero. This is NOT jitter and must NOT be
                //    clamped: clamping strands the audio at the previous clip's
                //    tail cadence, so when `TimelineContinuity` later bumps the
                //    global offset for the new epoch (driven by the video
                //    back-jump) the stranded-high audio PTS is flung ~a whole
                //    clip past the frontier, producing the non-monotonic
                //    audio-DTS band on multi-clip titles (Dune: Part Two, Top
                //    Gun). ADOPT the raw reset so the per-track raw PTS that
                //    reaches `TimelineContinuity` carries the true boundary, and
                //    the corrector rebases it exactly as it already does for the
                //    DTS / AC-3 parsers (which never clamp). Same threshold the
                //    timeline corrector uses to classify a discontinuity.
                //
                // A genuine forward gap/discontinuity is always adopted by the
                // `.max()`.
                let new = pts_to_ns(pts);
                if new < self.next_pts_ns - DISCONTINUITY_BACKSTEP_NS {
                    // Clip-boundary reset: take the raw PTS, restart the cadence.
                    self.next_pts_ns = new;
                } else {
                    // Within-clip jitter (or forward progression): stay monotonic.
                    self.next_pts_ns = self.next_pts_ns.max(new);
                }
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

            // On a major sync the 32-bit `format_info` word (immediately after
            // the 4-byte sync, i.e. AU bytes 8..12) carries the rate nibble.
            // Refine the per-AU PTS increment to the actual rate family. The
            // 48 kHz family resolves to the unchanged 833_333 default, so the
            // common case stays byte-identical; only the 44.1 kHz family shifts.
            if is_major_sync && unit_bytes >= 12 {
                let format_info =
                    u32::from_be_bytes([self.buf[8], self.buf[9], self.buf[10], self.buf[11]]);
                self.au_duration_ns = truehd_au_duration_ns(format_info);
            }

            frames.push(Frame {
                discontinuity: false,
                coding: None,
                source: None,
                pts_ns: self.next_pts_ns,
                keyframe: is_major_sync,
                data: self.buf[..unit_bytes].to_vec(),
                duration_ns: None,
            });
            self.buf.drain(..unit_bytes);
            self.next_pts_ns += self.au_duration_ns;
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

/// Real sample rate (Hz) from a TrueHD major-sync `format_info` word.
///
/// The 4-bit `ratebits` nibble sits in `format_info` bits 31..28 (the top
/// nibble), the same word `truehd_channels` reads for the channel masks. The
/// MLP rate formula is `(ratebits & 8 ? 44100 : 48000) << (ratebits & 7)`;
/// rather than evaluate it blindly this is a **strict whitelist** of the only
/// six rates that occur on real BD/UHD TrueHD. Every other code — the invalid
/// `0xF`, the formula-only `0x3`/`0xB`, and all reserved values — returns
/// `None`, so a malformed or unexpected field can never produce a wrong
/// `SamplingFrequency`; the caller falls back to its container-derived rate.
pub fn truehd_sample_rate_hz(format_info: u32) -> Option<u32> {
    match (format_info >> 28) & 0xF {
        0x0 => Some(48000),
        0x1 => Some(96000),
        0x2 => Some(192000),
        0x8 => Some(44100),
        0x9 => Some(88200),
        0xA => Some(176400),
        _ => None,
    }
}

/// Per-AU PTS increment (ns) for the rate family encoded in `format_info`.
///
/// Derived from the same whitelisted rate as [`truehd_sample_rate_hz`]: the
/// 44.1 kHz family (44.1 / 88.2 / 176.4 kHz) is `907_029` ns; everything else —
/// the entire 48 kHz family AND any unrecognised rate — keeps the exact current
/// `833_333` default, so the common case and all unknown/garbage inputs are
/// byte-identical to prior behaviour.
pub fn truehd_au_duration_ns(format_info: u32) -> i64 {
    match truehd_sample_rate_hz(format_info) {
        Some(44100) | Some(88200) | Some(176400) => AU_DURATION_NS_441,
        _ => AU_DURATION_NS,
    }
}

/// First TrueHD major sync found in a demuxed elementary-stream chunk: the
/// `format_info` word plus the Atmos signal. A single scan that the per-field
/// helpers below share, so the host probes the bitstream once for channels,
/// sample rate and Atmos.
pub struct TrueHdSyncInfo {
    /// The 32-bit word immediately after the 0xF8726FBA sync (channel masks +
    /// rate nibble). Feed to `truehd_channels` / `truehd_sample_rate_hz`.
    pub format_info: u32,
    /// `num_substreams >= 4` ⟺ a 4th (Atmos object/OAMD) substream is present.
    /// `num_substreams = msync[16] >> 4`, where `msync[0]` is the sync's 0xF8.
    /// `None` when the AU is too short to reach that byte — never guess Atmos.
    pub is_atmos: Option<bool>,
}

/// Scan a demuxed TrueHD chunk for the first major sync and return its
/// `format_info` and Atmos signal. The stream may interleave AC-3; the scan
/// advances one byte at a time and matches the sync word anywhere.
pub fn truehd_sync_info_from_stream(data: &[u8]) -> Option<TrueHdSyncInfo> {
    let mut p = 0;
    while p + 8 <= data.len() {
        let w = u32::from_be_bytes([data[p], data[p + 1], data[p + 2], data[p + 3]]);
        if (w & 0xFFFF_FFFE) == 0xF872_6FBA {
            let format_info =
                u32::from_be_bytes([data[p + 4], data[p + 5], data[p + 6], data[p + 7]]);
            // num_substreams is the top nibble of the 17th sync byte (p + 16).
            // .get() yields None — not a panic and not a false Atmos — when the
            // AU is truncated before that byte.
            let is_atmos = data.get(p + 16).map(|&b| (b >> 4) >= 4);
            return Some(TrueHdSyncInfo {
                format_info,
                is_atmos,
            });
        }
        p += 1;
    }
    None
}

/// Real sample rate (Hz) from the first major sync in a demuxed chunk, or
/// `None` if no major sync is found or its rate code is not whitelisted.
pub fn truehd_sample_rate_from_stream(data: &[u8]) -> Option<u32> {
    truehd_sync_info_from_stream(data).and_then(|s| truehd_sample_rate_hz(s.format_info))
}

/// Whether the first major sync in a demuxed chunk carries an Atmos substream.
/// `None` when no major sync is found or the AU is too short to read the
/// substream count — callers must treat `None` as "not Atmos" (never label).
pub fn truehd_is_atmos_from_stream(data: &[u8]) -> Option<bool> {
    truehd_sync_info_from_stream(data).and_then(|s| s.is_atmos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::ts::PesPacket;

    fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
        PesPacket {
            source: None,
            pid: 0x1100,
            pts,
            dts: None,
            data,
            discontinuity: false,
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
    fn discontinuity_drops_truncated_partial() {
        // B1: a partial TrueHD unit is buffered, then a concealed gap (PES marked
        // discontinuity) carries a fresh unit. The truncated partial must be
        // DROPPED — splicing it makes the length-prefixed framer emit a
        // wrong-size unit (corrupt AU framing) and re-seeds the PTS cadence from
        // the post-gap PES rather than stranding it (the non-monotonic audio-DTS
        // band at gaps).
        let mut parser = TrueHdParser::new();

        // PES 1: first 150 bytes of a 300-byte unit (length prefix says 300, only
        // 150 present) → held, nothing emitted.
        let partial = make_truehd_unit(300);
        let pes1 = make_pes(partial[..150].to_vec(), Some(90000));
        assert!(parser.parse(&pes1).is_empty(), "partial unit held");

        // Concealed gap: a fresh 200-byte unit at a forward PTS jump.
        let fresh = make_truehd_unit(200);
        let pes2 = PesPacket {
            source: None,
            pid: 0x1100,
            pts: Some(180000),
            dts: None,
            data: fresh.clone(),
            discontinuity: true,
        };
        let frames = parser.parse(&pes2);
        assert_eq!(frames.len(), 1, "exactly one clean unit across the gap");
        assert_eq!(
            frames[0].data.len(),
            200,
            "emitted unit is the fresh 200-byte one, not a 300-byte splice"
        );
        assert_eq!(
            frames[0].data, fresh,
            "unit bytes are the fresh post-gap unit"
        );
        assert_eq!(
            frames[0].pts_ns,
            pts_to_ns(180000),
            "cadence re-bases to the post-gap PTS across the cleared buffer"
        );
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
    fn pes_pts_lagging_the_au_cadence_never_emits_backward() {
        // Regression: the per-AU cadence is sample-accurate, but a PES boundary
        // can carry a PTS that lags it slightly (source-muxer rounding jitter).
        // When the buffer empties exactly on that boundary, an unconditional
        // reset to the PES PTS snapped the next AU's timestamp BELOW the AU just
        // emitted — the non-monotonic block timestamps a muxer rejects (the
        // Top Gun / Dune: Part Two case). The reset must clamp forward-only.
        let mut parser = TrueHdParser::new();
        let au = make_truehd_unit(100);
        // PES1: three complete AUs at pts 90000 — buffer empties, cadence runs
        // ahead to 90000_ns + 3*AU_DURATION_NS.
        let mut d1 = au.clone();
        d1.extend_from_slice(&au);
        d1.extend_from_slice(&au);
        let f1 = parser.parse(&make_pes(d1, Some(90000)));
        assert_eq!(f1.len(), 3);
        let last1 = f1.last().unwrap().pts_ns;
        // PES2's PTS (90001) maps to fewer ns than the running cadence — pre-fix
        // this snapped backward.
        let f2 = parser.parse(&make_pes(au.clone(), Some(90001)));
        assert_eq!(f2.len(), 1);
        assert!(
            f2[0].pts_ns >= last1,
            "AU pts must not go backward when PES PTS lags the cadence: got {} after {}",
            f2[0].pts_ns,
            last1
        );
    }

    #[test]
    fn clip_boundary_pts_reset_is_adopted_not_clamped() {
        // Regression (Dune: Part Two / Top Gun non-monotonic audio-DTS band):
        // a title's clips are read as one concatenated stream, so at a
        // non-seamless boundary the source PES PTS resets near zero — a LARGE
        // backward step (> DISCONTINUITY_BACKSTEP_NS), NOT muxer jitter. The
        // parser must ADOPT that reset (restart the cadence at the raw PTS), the
        // same way the DTS / AC-3 parsers pass raw PTS through, so the per-track
        // raw PTS reaching TimelineContinuity carries the true boundary and the
        // corrector can rebase it. Clamping it forward (the old `.max()`) stranded
        // the audio at the previous clip's tail; when the global offset later
        // bumped for the new epoch the stranded audio was flung ~a clip past the
        // frontier — the non-monotonic band.
        let mut parser = TrueHdParser::new();
        let au = make_truehd_unit(100);
        // Clip 1: an AU at PES PTS = 10s (90000 ticks/s → 900_000 ticks). Buffer
        // empties, so the next PES seeds a fresh base.
        let clip1_pts = 90_000 * 10; // 10 s in 90 kHz ticks
        let f1 = parser.parse(&make_pes(au.clone(), Some(clip1_pts)));
        assert_eq!(f1.len(), 1);
        let last1 = f1[0].pts_ns;
        assert_eq!(last1, pts_to_ns(clip1_pts));
        // Clip 2: PES PTS resets to 0 — 10 s backward, far beyond the 3 s
        // discontinuity threshold. Must be adopted, not clamped to the cadence.
        let f2 = parser.parse(&make_pes(au.clone(), Some(0)));
        assert_eq!(f2.len(), 1);
        assert_eq!(
            f2[0].pts_ns, 0,
            "clip-boundary PTS reset must be adopted raw (got {}, expected the \
             reset value 0 — clamping to the previous clip's cadence is the bug)",
            f2[0].pts_ns
        );
        assert!(
            f2[0].pts_ns < last1,
            "the reset frame must land below the previous clip's tail, not above it"
        );
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

    // --- truehd_channels: per-bit mask channel counts (MLP / FFmpeg table) ---

    #[test]
    fn truehd_channels_8ch_single_bit_counts() {
        // THD_8CH = [2,1,1,2,2,2,2,1,1,2,2,1,1]. A single set bit must yield
        // exactly that bit's channel count. Bit 0 → 2 (L/R pair), bit 1 → 1 (C),
        // bit 2 → 1 (LFE), bit 7 → 1.
        assert_eq!(truehd_channels(1 << 0), Some(2));
        assert_eq!(truehd_channels(1 << 1), Some(1));
        assert_eq!(truehd_channels(1 << 2), Some(1));
        assert_eq!(truehd_channels(1 << 7), Some(1));
    }

    #[test]
    fn truehd_channels_8ch_all_bits_set() {
        // All 13 8ch bits set = 2+1+1+2+2+2+2+1+1+2+2+1+1 = 20. ch8 field is the
        // low 13 bits (0x1FFF).
        assert_eq!(truehd_channels(0x1FFF), Some(20));
    }

    #[test]
    fn truehd_channels_6ch_used_only_when_8ch_zero() {
        // The 8ch presentation takes priority; the 6ch field (bits 15-19) is read
        // ONLY when ch8 == 0. THD_6CH = [2,1,1,2,1]. Set 6ch bit 0 (→2) while
        // 8ch is zero: 6ch field value 1 at shift 15.
        assert_eq!(truehd_channels(1 << 15), Some(2));
        // All 5 6ch bits = 2+1+1+2+1 = 7. 0x1F << 15.
        assert_eq!(truehd_channels(0x1F << 15), Some(7));
    }

    #[test]
    fn truehd_channels_8ch_wins_over_6ch_when_both_present() {
        // When BOTH fields are non-zero, the richer 8ch presentation is used.
        // 8ch = bit0 (→2), 6ch = all bits (would be 7) → result must be 2, the
        // 8ch count, proving the `if ch8 != 0` branch wins.
        let fi = (1u32 << 0) | (0x1F << 15);
        assert_eq!(truehd_channels(fi), Some(2));
    }

    #[test]
    fn truehd_channels_none_when_both_fields_zero() {
        // No presentation flags set → None (can't determine layout).
        assert_eq!(truehd_channels(0), None);
        // Bits outside both fields (e.g. bit 13, bit 14, bits 20-31) don't count
        // as a presentation and must still yield None.
        assert_eq!(truehd_channels(1 << 13), None);
        assert_eq!(truehd_channels(1 << 20), None);
    }

    // --- truehd_channels_from_stream: major-sync variant bit + scan ---

    #[test]
    fn channels_from_stream_matches_variant_sync_0xfb() {
        // The sync match masks the low bit: 0xF8726FBA & 0xFFFFFFFE == base, and
        // 0xF8726FBB (the +1 variant) matches the same masked pattern. A stream
        // carrying 0xF8726FBB must still be recognised.
        let mut data = vec![0x00];
        data.extend_from_slice(&0xF872_6FBBu32.to_be_bytes());
        data.extend_from_slice(&0x0000_001Fu32.to_be_bytes());
        assert_eq!(truehd_channels_from_stream(&data), Some(8));
    }

    #[test]
    fn channels_from_stream_none_without_major_sync() {
        // No major sync anywhere → None, no panic, scan terminates.
        let data = vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        assert_eq!(truehd_channels_from_stream(&data), None);
    }

    #[test]
    fn channels_from_stream_too_short_for_format_info() {
        // Sync present but fewer than 8 bytes total → the `p + 8 <= len` guard
        // prevents reading format_info out of bounds → None.
        let data = 0xF872_6FBAu32.to_be_bytes().to_vec(); // 4 bytes only
        assert_eq!(truehd_channels_from_stream(&data), None);
    }

    #[test]
    fn channels_from_stream_unaligned_sync() {
        // The scan advances 1 byte at a time, so a major sync at an odd offset
        // is still found. Place it at offset 3.
        let mut data = vec![0xAA, 0xBB, 0xCC];
        data.extend_from_slice(&0xF872_6FBAu32.to_be_bytes());
        data.extend_from_slice(&(0x1Fu32).to_be_bytes());
        assert_eq!(truehd_channels_from_stream(&data), Some(8));
    }

    // --- AU length field: 12-bit mask, partial AU, is_major_sync keyframe ---

    #[test]
    fn au_length_uses_low_12_bits_only() {
        // unit_words = ((b0<<8)|b1) & 0xFFF. The top 4 bits of b0 (the MLP
        // check/access-unit nibble) must NOT inflate the length. b0 = 0xF1
        // (nibble 0xF, low 0x1), b1 = 0x00 → words = 0x100 = 256 → 512 bytes.
        let mut parser = TrueHdParser::new();
        let mut unit = vec![0u8; 512];
        unit[0] = 0xF1; // high nibble 0xF must be masked off
        unit[1] = 0x00;
        let f = parser.parse(&make_pes(unit, Some(90000)));
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].data.len(),
            512,
            "length sized from low 12 bits (0x100 words), nibble masked"
        );
    }

    #[test]
    fn au_with_major_sync_is_keyframe() {
        // An AU whose bytes 4-7 hold the major sync (0xF8726FBA, low bit masked)
        // is a restart point → keyframe. Build a >=8-byte AU with the sync at
        // offset 4. words = 100 → 200 bytes.
        let mut parser = TrueHdParser::new();
        let mut unit = make_truehd_unit(200);
        unit[4..8].copy_from_slice(&0xF872_6FBAu32.to_be_bytes());
        let f = parser.parse(&make_pes(unit, Some(90000)));
        assert_eq!(f.len(), 1);
        assert!(f[0].keyframe, "major-sync AU must be flagged keyframe");
    }

    #[test]
    fn au_without_major_sync_is_not_keyframe() {
        // A plain AU (no major sync at offset 4) is not a keyframe.
        let mut parser = TrueHdParser::new();
        let f = parser.parse(&make_pes(make_truehd_unit(200), Some(90000)));
        assert_eq!(f.len(), 1);
        assert!(!f[0].keyframe);
    }

    #[test]
    fn major_sync_variant_bit_also_keyframe() {
        // The keyframe check masks the low bit (0xFFFF_FFFE), so the 0xF8726FBB
        // variant must also be detected as a major sync.
        let mut parser = TrueHdParser::new();
        let mut unit = make_truehd_unit(200);
        unit[4..8].copy_from_slice(&0xF872_6FBBu32.to_be_bytes());
        let f = parser.parse(&make_pes(unit, Some(90000)));
        assert_eq!(f.len(), 1);
        assert!(f[0].keyframe, "major-sync variant 0xFB also a keyframe");
    }

    #[test]
    fn incomplete_au_waits_does_not_emit_short() {
        // The AU length declares more bytes than buffered → parser must wait, not
        // emit a truncated AU. words=300 (0x12C) → 600 bytes declared, only 100
        // present. 300 exercises both length bytes (high nibble 0x1, low 0x2C).
        let mut parser = TrueHdParser::new();
        let mut data = vec![0u8; 100];
        let words = 300usize;
        data[0] = ((words >> 8) & 0x0F) as u8; // 0x01
        data[1] = (words & 0xFF) as u8; // 0x2C → 300 words = 600 bytes
        let f = parser.parse(&make_pes(data, Some(90000)));
        assert!(
            f.is_empty(),
            "must not emit fewer bytes than the length field"
        );
        assert_eq!(parser.buf.len(), 100, "partial AU retained");
    }

    #[test]
    fn buffer_stays_bounded_across_many_partial_pes() {
        // Malformed/never-completing input must keep the reassembly buffer
        // bounded by MAX_TRUEHD_BUF. Repeatedly feed AU fragments whose declared
        // length always exceeds what is buffered, so no AU ever completes; the
        // post-loop cap guard must clear the buffer instead of letting it grow
        // unbounded across many calls.
        let mut parser = TrueHdParser::new();
        // Each PES: a head declaring 0xFFF words (8190 bytes) but only 4096 bytes
        // present → incomplete → retained. Across many PES this would accumulate
        // without the cap.
        for _ in 0..200 {
            let mut frag = vec![0u8; 4096];
            frag[0] = 0x0F; // 0x0FFF words = 4095 → 8190 bytes declared
            frag[1] = 0xFF;
            let _ = parser.parse(&make_pes(frag, Some(0)));
            assert!(
                parser.buf.len() <= MAX_TRUEHD_BUF,
                "reassembly buffer exceeded cap: {} > {}",
                parser.buf.len(),
                MAX_TRUEHD_BUF
            );
        }
    }

    // --- ac3_boundary_corroborated: the AC-3-vs-TrueHD disambiguation ---

    #[test]
    fn ac3_corroborated_when_frame_fills_buffer() {
        // frame_bytes >= buf.len() → the AC-3 frame ends the buffer → corroborated.
        let buf = vec![0u8; 128];
        assert!(ac3_boundary_corroborated(&buf, 128));
        assert!(ac3_boundary_corroborated(&buf, 200));
    }

    #[test]
    fn ac3_corroborated_when_next_is_ac3_sync() {
        // Bytes after the frame begin with 0x0B 0x77 → another AC-3 frame →
        // corroborated.
        let mut buf = vec![0u8; 130];
        buf[128] = 0x0B;
        buf[129] = 0x77;
        assert!(ac3_boundary_corroborated(&buf, 128));
    }

    #[test]
    fn ac3_corroborated_when_next_is_plausible_truehd_au() {
        // Bytes after the frame form a plausible TrueHD AU header (non-zero
        // 12-bit length within 32 KiB) → corroborated. next_words = 0x100 = 256
        // → 512 bytes <= 32768.
        let mut buf = vec![0u8; 130];
        buf[128] = 0x01; // (0x01<<8)|0x00 & 0xFFF = 0x100
        buf[129] = 0x00;
        assert!(ac3_boundary_corroborated(&buf, 128));
    }

    #[test]
    fn ac3_not_corroborated_when_next_zero_length() {
        // Bytes after the frame are zeros → next_words == 0 → NOT a plausible
        // TrueHD AU and not an AC-3 sync → NOT corroborated (treat as TrueHD).
        let buf = vec![0u8; 130]; // all zero after frame_bytes=128
        assert!(!ac3_boundary_corroborated(&buf, 128));
    }

    #[test]
    fn ac3_corroborated_when_too_few_trailing_bytes() {
        // Fewer than 2 bytes follow the frame → can't judge → accept (next call
        // sees the continuation). frame_bytes=128, buf=129 → 1 trailing byte.
        let buf = vec![0u8; 129];
        assert!(ac3_boundary_corroborated(&buf, 128));
    }

    #[test]
    fn ac3_frame_at_head_needs_more_when_buffer_short() {
        // < 6 bytes buffered → NeedMore (can't read the AC-3 header).
        let mut parser = TrueHdParser::new();
        parser.buf = vec![0x0B, 0x77, 0x00];
        // Drive through parse: a short 0x0B77 head must wait, not emit.
        let f = parser.parse(&make_pes(vec![0x0B, 0x77, 0x00], Some(0)));
        assert!(f.is_empty());
    }

    // --- #2 sample rate from the major-sync rate nibble ---

    /// Build a `format_info` word with the given `ratebits` (top nibble) and a
    /// 7.1 8-channel mask (ch8 = 0x1F) in the low 13 bits — exactly the layout
    /// §1.A pins, so the rate nibble and the channel masks are co-located in one
    /// real word.
    fn format_info_with(ratebits: u32) -> u32 {
        ((ratebits & 0xF) << 28) | 0x1F
    }

    #[test]
    fn sample_rate_whitelist_real_rates() {
        assert_eq!(truehd_sample_rate_hz(format_info_with(0x0)), Some(48000));
        assert_eq!(truehd_sample_rate_hz(format_info_with(0x1)), Some(96000));
        assert_eq!(truehd_sample_rate_hz(format_info_with(0x2)), Some(192000));
        assert_eq!(truehd_sample_rate_hz(format_info_with(0x8)), Some(44100));
        assert_eq!(truehd_sample_rate_hz(format_info_with(0x9)), Some(88200));
        assert_eq!(truehd_sample_rate_hz(format_info_with(0xA)), Some(176400));
    }

    #[test]
    fn sample_rate_unknown_rate_falls_back_to_none() {
        // 0xF is the explicit invalid code; 0x3/0xB are formula-only and not
        // whitelisted; 0x7/0xE are reserved. None of them may produce a rate —
        // the host must fall back to its container value, never write a wrong
        // SamplingFrequency.
        for bad in [0x3u32, 0x7, 0xB, 0xC, 0xD, 0xE, 0xF] {
            assert_eq!(
                truehd_sample_rate_hz(format_info_with(bad)),
                None,
                "ratebits {bad:#x} must not yield a rate"
            );
        }
    }

    #[test]
    fn sample_rate_nibble_does_not_disturb_channel_decode() {
        // Internal-consistency guard: with the 96 kHz nibble AND a 7.1 mask in
        // the same word, the rate reads 96000 and the channels still read 8 —
        // proving the rate nibble (bits 31..28) and the channel masks
        // (bits 19..0) do not collide.
        let fi = format_info_with(0x1);
        assert_eq!(truehd_sample_rate_hz(fi), Some(96000));
        assert_eq!(truehd_channels(fi), Some(8));
    }

    #[test]
    fn sample_rate_from_stream_scans_major_sync() {
        // [junk][0xF8726FBA][format_info: ratebits=0x1 (96k), ch8=0x1F]
        let mut data = vec![0xAA, 0xBB];
        data.extend_from_slice(&0xF872_6FBAu32.to_be_bytes());
        data.extend_from_slice(&format_info_with(0x1).to_be_bytes());
        assert_eq!(truehd_sample_rate_from_stream(&data), Some(96000));
    }

    #[test]
    fn sample_rate_from_stream_none_without_sync() {
        let data = vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        assert_eq!(truehd_sample_rate_from_stream(&data), None);
    }

    // --- #3 per-AU duration: family-aware, 48 kHz family byte-identical ---

    #[test]
    fn au_duration_48k_family_unchanged() {
        // 48 / 96 / 192 kHz (ratebits 0x0/0x1/0x2) all keep the exact current
        // 833_333 constant — the common case must never shift.
        for rb in [0x0u32, 0x1, 0x2] {
            assert_eq!(truehd_au_duration_ns(format_info_with(rb)), 833_333);
        }
    }

    #[test]
    fn au_duration_441k_family_is_907029() {
        // 44.1 / 88.2 / 176.4 kHz (ratebits 0x8/0x9/0xA) → 907_029 ns.
        for rb in [0x8u32, 0x9, 0xA] {
            assert_eq!(truehd_au_duration_ns(format_info_with(rb)), 907_029);
        }
    }

    #[test]
    fn au_duration_unknown_rate_keeps_default() {
        // An unrecognised/garbage rate nibble must not pick the 44.1 k value
        // (note 0xF & 8 != 0): it falls back to the 833_333 default.
        for rb in [0x3u32, 0x7, 0xB, 0xF] {
            assert_eq!(truehd_au_duration_ns(format_info_with(rb)), 833_333);
        }
    }

    #[test]
    fn parser_44k_major_sync_sets_907029_increment() {
        // Two AUs: the first carries a major sync with ratebits=0x8 (44.1 k).
        // After the parser reads it, the per-AU PTS increment must be 907_029.
        let mut parser = TrueHdParser::new();
        let mut a1 = make_truehd_unit(200);
        a1[4..8].copy_from_slice(&0xF872_6FBAu32.to_be_bytes()); // major sync
        a1[8..12].copy_from_slice(&format_info_with(0x8).to_be_bytes()); // 44.1 k
        let mut data = a1;
        data.extend_from_slice(&make_truehd_unit(200));
        let frames = parser.parse(&make_pes(data, Some(90000)));
        assert_eq!(frames.len(), 2);
        assert_eq!(
            frames[1].pts_ns - frames[0].pts_ns,
            907_029,
            "44.1 k-family AU increments by 907_029 once the major sync is read"
        );
    }

    #[test]
    fn parser_48k_major_sync_keeps_833333_increment() {
        // Regression: a 48 k-family (ratebits=0x0) major sync keeps the exact
        // current 833_333 increment.
        let mut parser = TrueHdParser::new();
        let mut a1 = make_truehd_unit(200);
        a1[4..8].copy_from_slice(&0xF872_6FBAu32.to_be_bytes());
        a1[8..12].copy_from_slice(&format_info_with(0x0).to_be_bytes()); // 48 k
        let mut data = a1;
        data.extend_from_slice(&make_truehd_unit(200));
        let frames = parser.parse(&make_pes(data, Some(90000)));
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[1].pts_ns - frames[0].pts_ns, 833_333);
    }

    // --- #1 Atmos detection from num_substreams (msync[16] >> 4) ---

    /// Build a demuxed chunk with one major sync whose 17th sync byte (offset
    /// 16 from the 0xF8) has top nibble `num_substreams`. The AU is padded past
    /// byte 16 so the substream count is reachable.
    fn major_sync_with_substreams(num_substreams: u8) -> Vec<u8> {
        let mut data = vec![0x00, 0x00]; // leading junk; scan is byte-aligned
        let sync_off = data.len();
        data.extend_from_slice(&0xF872_6FBAu32.to_be_bytes()); // bytes [off..off+4]
        data.extend_from_slice(&format_info_with(0x0).to_be_bytes()); // format_info
        // Pad up to and including byte `sync_off + 16`.
        while data.len() <= sync_off + 16 {
            data.push(0x00);
        }
        data[sync_off + 16] = (num_substreams & 0xF) << 4;
        data
    }

    #[test]
    fn atmos_true_when_four_substreams() {
        // num_substreams = 4 → byte 16 = 0x40 → Atmos object substream present.
        let data = major_sync_with_substreams(4);
        assert_eq!(truehd_is_atmos_from_stream(&data), Some(true));
    }

    #[test]
    fn atmos_false_when_three_substreams() {
        // num_substreams = 3 (plain 7.1 TrueHD) → byte 16 = 0x30 → not Atmos.
        let data = major_sync_with_substreams(3);
        assert_eq!(truehd_is_atmos_from_stream(&data), Some(false));
    }

    #[test]
    fn atmos_none_when_au_too_short_for_substream_byte() {
        // Major sync present but the chunk ends before byte sync_off+16 → None,
        // never a false Atmos. Sync at offset 0; only format_info follows.
        let mut data = 0xF872_6FBAu32.to_be_bytes().to_vec();
        data.extend_from_slice(&format_info_with(0x0).to_be_bytes()); // 8 bytes total
        assert_eq!(truehd_is_atmos_from_stream(&data), None);
    }

    #[test]
    fn atmos_none_without_major_sync() {
        let data = vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        assert_eq!(truehd_is_atmos_from_stream(&data), None);
    }

    #[test]
    fn sync_info_combines_channels_rate_and_atmos() {
        // One scan yields all three facts: 7.1 channels, 96 kHz, 4 substreams.
        let data = {
            let mut d = vec![0x00, 0x00];
            let off = d.len();
            d.extend_from_slice(&0xF872_6FBAu32.to_be_bytes());
            d.extend_from_slice(&format_info_with(0x1).to_be_bytes()); // 96k + 7.1
            while d.len() <= off + 16 {
                d.push(0x00);
            }
            d[off + 16] = 0x40; // 4 substreams
            d
        };
        let info = truehd_sync_info_from_stream(&data).expect("major sync found");
        assert_eq!(truehd_channels(info.format_info), Some(8));
        assert_eq!(truehd_sample_rate_hz(info.format_info), Some(96000));
        assert_eq!(info.is_atmos, Some(true));
    }
}
