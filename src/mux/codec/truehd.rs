//! Dolby TrueHD / Atmos elementary stream parser.
//!
//! BD-TS TrueHD PES packets contain interleaved AC-3 + TrueHD access units,
//! which span PES boundaries — must buffer and reassemble.
//!
//! TrueHD AU header (4 bytes): bytes 0-1 top nibble = MLP check/AU nibble,
//! lower 12 bits = AU length in 2-byte words; bytes 2-3 timing value; bytes
//! 4.. substream data (major sync 0xF8726FBA may appear at offset 4).
//!
//! AC-3 frames (interleaved, same PID) start with sync 0x0B77; skipped —
//! only TrueHD access units are emitted.

use super::crc::crc16_mlp;
use super::dropgate::DropTally;
use super::{CodecParser, Frame, PesPacket, pts_to_ns};
use crate::mux::timeline::DISCONTINUITY_BACKSTEP_NS;

// Is `w` an MLP-family major sync (24-bit sig 0xF8726F; last byte 0xBA
// TrueHD or 0xBB MLP) — a decoder re-init point. Both count as restart.
fn is_mlp_major_sync(w: u32) -> bool {
    (w & 0xFFFF_FFFE) == 0xF872_6FBA
}

// Is `w` specifically the TrueHD major sync (stream type 0xBA)? Must be
// exact, not the 0xBA/0xBB mask — see docs/truehd.md for why.
fn is_truehd_major_sync(w: u32) -> bool {
    w == 0xF872_6FBA
}

// AU duration (ns), 48 kHz family (48/96/192 kHz): 40/48000 = 1/1200 s,
// exact for the whole family since the ratebits shift cancels. Default
// until a major sync reveals the actual rate.
const AU_DURATION_NS: i64 = 833_333;

// AU duration (ns), 44.1 kHz family: 40/44100 = 907_029.478.. ns. The 48
// kHz constant would run ~8.95% fast on these (rare) streams.
const AU_DURATION_NS_441: i64 = 907_029;

// Hard cap on the reassembly buffer: a valid AU is well under 32 KiB, so
// past this the stream is malformed — drop and resync. Parity with the
// AC-3/DTS/PGS caps.
const MAX_TRUEHD_BUF: usize = 256 * 1024;

pub struct TrueHdParser {
    /// Bytes assembled across PES packets, each attributable to the packet
    /// that carried it, so an access unit takes the timestamp AND the source
    /// offset of the packet covering its first byte.
    acc: super::pesbuf::PesBuf,
    next_pts_ns: i64,
    /// Per-AU PTS increment. Defaults to the 48 kHz-family value (833_333) and
    /// is refined to the 44.1 kHz-family value once the first major sync reveals
    /// the actual rate. Stays at the default for streams whose major sync is not
    /// yet seen (head of stream) — preserving byte-identical timing for the
    /// common 48 kHz case.
    au_duration_ns: i64,
    /// Keep/drop bookkeeping for the decodability gate.
    tally: DropTally,
    /// `num_substreams` from the most recent major sync — needed to size the
    /// substream directory for the per-AU parity check. `None` until the first
    /// major sync is seen (before which no AU can be parity-checked).
    num_substreams: Option<u8>,
    /// True while dropping forward to the next clean resync point. MLP/TrueHD
    /// carries filter/predictor + restart state ACROSS access units, so a corrupt
    /// AU cannot be excised in place — it poisons decoding until the next major
    /// sync re-initialises state. On corruption we set this and drop every AU
    /// until a major sync whose header CRC validates, which we then emit.
    resync_pending: bool,
}

impl Default for TrueHdParser {
    fn default() -> Self {
        Self::new()
    }
}

impl TrueHdParser {
    pub fn new() -> Self {
        Self {
            acc: super::pesbuf::PesBuf::with_capacity(32768),
            next_pts_ns: 0,
            au_duration_ns: AU_DURATION_NS,
            tally: DropTally::new("truehd"),
            num_substreams: None,
            resync_pending: false,
        }
    }

    /// Access units dropped as undecodable so far.
    pub fn dropped_frames(&self) -> u64 {
        self.tally.dropped_frames()
    }

    /// Total decoded duration (ns) of dropped access units.
    pub fn dropped_duration_ns(&self) -> u64 {
        self.tally.dropped_duration_ns()
    }

    // Decide whether an AU is corrupt, updating `num_substreams` from a
    // valid major sync. See docs/truehd.md#au_check-corruption-decision.
    fn au_check(&mut self, au: &[u8], is_major_sync: bool) -> AuCheck {
        let mut header_size = 4;
        let mut format_info = None;
        if is_major_sync {
            let ms = &au[4..];
            let Some(mshdr) = mlp_major_sync_header_size(ms) else {
                // A major sync too short to hold its header can't be CRC-validated
                // — NOT a safe resync/re-init point. Treat as unverifiable, not a
                // clean major sync.
                return AuCheck::Unverifiable;
            };
            if !mlp_major_sync_crc_ok(ms, mshdr) {
                // A failing checksum is only trustworthy once a validated baseline
                // (num_substreams from a prior clean major sync) exists — before
                // that, arming drop-forward risks silently dropping the whole track.
                if self.num_substreams.is_some() {
                    return AuCheck::Corrupt; // real corruption vs a proven baseline
                }
                return AuCheck::Unverifiable; // no baseline yet — keep, don't nuke the track
            }
            self.num_substreams = mlp_num_substreams(ms);
            header_size += mshdr;
            // format_info is only trustworthy once the major sync's CRC has
            // validated (above), and only for stream type 0xBA: an MLP (0xBB)
            // major sync's next word isn't the TrueHD layout, so leave it `None` there.
            if au.len() >= 12
                && is_truehd_major_sync(u32::from_be_bytes([au[4], au[5], au[6], au[7]]))
            {
                format_info = Some(u32::from_be_bytes([au[8], au[9], au[10], au[11]]));
            }
        }
        let Some(nss) = self.num_substreams else {
            return AuCheck::Unverifiable; // no major sync seen yet — can't check parity
        };
        let Some(shs) = mlp_substr_header_size(au, header_size, nss) else {
            return AuCheck::Unverifiable; // directory runs off the AU — can't judge
        };
        if !mlp_parity_ok(au, header_size, shs) {
            return AuCheck::Corrupt;
        }
        if is_major_sync {
            AuCheck::ValidMajorSync { format_info }
        } else {
            AuCheck::Ok
        }
    }

    // Size (bytes) of the AC-3 frame at the buffer head: Unmappable/NeedMore/
    // Frame(n). See docs/truehd.md#ac3_frame_at_head-ac-3-size-lookup.
    fn ac3_frame_at_head(&self) -> Ac3Size {
        if self.acc.len() < 6 {
            return Ac3Size::NeedMore;
        }
        let frame_bytes = super::ac3::ac3_frame_size(self.acc.as_slice());
        if frame_bytes == 0 {
            // Reserved fscod or out-of-range frmsizecod → unmappable header.
            return Ac3Size::Unmappable;
        }
        if self.acc.len() < frame_bytes {
            return Ac3Size::NeedMore;
        }
        Ac3Size::Frame(frame_bytes)
    }
}

// Secondary validation: is the AC-3 frame's computed end a plausible
// boundary? See docs/truehd.md#ac3_boundary_corroborated.
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

/// Decodability verdict for one TrueHD/MLP access unit.
enum AuCheck {
    /// Verified undecodable: a major-sync header whose CRC failed, or any AU
    /// whose substream-directory parity failed. Feeds the poison verdict.
    Corrupt,
    /// A CRC-validated major sync — a safe re-init / resync point. `format_info`
    /// (AU bytes 8..12, present when the AU is long enough) is trustworthy here,
    /// so the caller refines the PTS cadence ONLY from this validated path.
    ValidMajorSync { format_info: Option<u32> },
    /// A valid (parity-OK) non-major-sync access unit.
    Ok,
    /// Cannot be judged — a major sync too short to hold/CRC its header, or a
    /// stream head before any major sync established `num_substreams`. Never
    /// dropped on its own, and never treated as a clean resync point.
    Unverifiable,
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

// --- MLP/TrueHD access-unit integrity (per the MLP/TrueHD bitstream spec) ---

// Major-sync header size: base 28 + `2 + extensions*2` when the extension
// flag is set. See docs/truehd.md#mlp_major_sync_header_size.
fn mlp_major_sync_header_size(ms: &[u8]) -> Option<usize> {
    if ms.len() < 28 {
        return None;
    }
    let mut size = 28;
    if ms[25] & 1 != 0 {
        size += 2 + ((ms[26] >> 4) as usize) * 2;
    }
    if ms.len() < size {
        return None;
    }
    Some(size)
}

// Validate the major-sync header checksum (CRC-16, poly 0x002D, byte-
// reversed vs standard). See docs/truehd.md#mlp_major_sync_crc_ok.
fn mlp_major_sync_crc_ok(ms: &[u8], mshdr: usize) -> bool {
    if mshdr < 4 || ms.len() < mshdr {
        return false;
    }
    // checksum16(buf,n) = crc16_2D(buf,n-2) ^ read_le16(buf+n-2), evaluated with
    // n=mshdr-2 against read_le16(buf+mshdr-2). `crc16_mlp` yields bytes in the
    // opposite order to a standard LE CRC, so swap_bytes() before XOR/compare.
    let checksum = crc16_mlp(&ms[..mshdr - 4]).swap_bytes()
        ^ u16::from_le_bytes([ms[mshdr - 4], ms[mshdr - 3]]);
    checksum == u16::from_le_bytes([ms[mshdr - 2], ms[mshdr - 1]])
}

/// `num_substreams` from a major-sync header: it sits at bit 128 (byte 16, top
/// nibble) for both MLP (0xbb) and TrueHD (0xba) — the fields before it total
/// the same 128 bits in either layout.
fn mlp_num_substreams(ms: &[u8]) -> Option<u8> {
    ms.get(16).map(|&b| b >> 4)
}

/// Size in bytes of the substream directory that follows the AU header: each of
/// the `num_substreams` entries is 2 bytes, plus 2 more when its extraword flag
/// (entry's top bit) is set. `None` if the directory runs past the AU.
fn mlp_substr_header_size(au: &[u8], header_size: usize, num_substreams: u8) -> Option<usize> {
    let mut off = header_size;
    let mut shs = 0;
    for _ in 0..num_substreams {
        if off + 2 > au.len() {
            return None;
        }
        let extraword = au[off] & 0x80 != 0;
        shs += 2;
        off += 2;
        if extraword {
            shs += 2;
            off += 2;
        }
    }
    Some(shs)
}

/// MLP/TrueHD AU-header parity check: the XOR of the 4-byte AU header with the
/// substream directory, folded, must have its two nibbles XOR to 0xF.
fn mlp_parity_ok(au: &[u8], header_size: usize, substr_header_size: usize) -> bool {
    let end = header_size + substr_header_size;
    if end > au.len() {
        return false;
    }
    let xor_fold = |d: &[u8]| d.iter().fold(0u8, |a, &b| a ^ b);
    let p = xor_fold(&au[0..4]) ^ xor_fold(&au[header_size..end]);
    ((p >> 4) ^ p) & 0xF == 0xF
}

impl CodecParser for TrueHdParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        // B1: a concealed/lost gap means the buffered TrueHD AU is TRUNCATED.
        // Splicing post-gap bytes onto it corrupts framing; drop the partial so
        // PTS re-seeds from the post-gap PES. Handled before the empty-data guard (defensive).
        if pes.discontinuity {
            self.acc.clear();
            // Unlike AC-3/DTS, MLP/TrueHD carries state across AUs, so resuming
            // stale state after a gap desyncs a decoder. Arm drop-forward to the
            // next CRC-valid major sync, only once a baseline exists (see au_check).
            if self.num_substreams.is_some() {
                self.resync_pending = true;
            }
        }
        if pes.data.is_empty() {
            return Vec::new();
        }

        // Capture the PTS base only at an AU boundary (buf empty): a PES that
        // merely continues an AU in progress carries a later PTS that must not
        // override the running timestamp, or it breaks the monotonic +AU_DURATION_NS cadence.
        if self.acc.is_empty()
            && let Some(pts) = pes.pts
        {
            // Resync to the authoritative PES PTS (sample-accurate vs the source
            // muxer's rounding jitter). Small backward jitter clamps to stay
            // monotonic; a large backward step (clip boundary) is adopted raw.
            let new = pts_to_ns(pts);
            if new < self.next_pts_ns - DISCONTINUITY_BACKSTEP_NS {
                // Clip-boundary reset: take the raw PTS, restart the cadence.
                self.next_pts_ns = new;
            } else {
                // Within-clip jitter (or forward progression): stay monotonic.
                self.next_pts_ns = self.next_pts_ns.max(new);
            }
        }

        self.acc.push(pes);

        let mut frames = Vec::new();

        loop {
            if self.acc.len() < 4 {
                break;
            }

            // AC-3 frame (interleaved): starts with sync 0x0B77, which is also a
            // legal TrueHD AU header. To avoid stealing a real TrueHD AU, an AC-3
            // frame is accepted only when its end is corroborated by what follows.
            if self.acc.as_slice()[0] == 0x0B && self.acc.as_slice()[1] == 0x77 {
                match self.ac3_frame_at_head() {
                    Ac3Size::Unmappable => {
                        // Permanently unmappable header at the head would stall
                        // the parser forever; resync by dropping 2 bytes so one
                        // bad frame costs one frame, not the whole buffer.
                        self.acc.drain(2);
                        continue;
                    }
                    Ac3Size::NeedMore => break, // wait for the rest of the frame
                    Ac3Size::Frame(skip) => {
                        if ac3_boundary_corroborated(self.acc.as_slice(), skip) {
                            self.acc.drain(skip);
                            continue;
                        }
                        // Not corroborated — fall through and interpret the
                        // 0x0B77 bytes as a TrueHD access unit instead.
                    }
                }
            }

            // TrueHD access unit: lower 12 bits of first 2 bytes = length in words
            let unit_words = (((self.acc.as_slice()[0] as usize) << 8)
                | self.acc.as_slice()[1] as usize)
                & 0xFFF;
            if unit_words == 0 {
                // A zero-length AU is malformed/padding. Drain the whole 4-byte
                // header, not just the length word, or the timing bytes get
                // misread as the next length word — a spurious parse next iteration.
                self.acc.drain(4);
                continue;
            }
            // unit_words is masked to 12 bits, so unit_bytes <= 4095 * 2 = 8190;
            // no separate oversize-resync guard is reachable.
            let unit_bytes = unit_words * 2;
            if self.acc.len() < unit_bytes {
                break; // incomplete access unit, wait for more data
            }

            // Restart-point question: either stream type (0xBA TrueHD, 0xBB MLP)
            // is a decoder re-init point, so both count as major sync here.
            // Decoding format_info with the TrueHD layout is gated on 0xBA alone.
            let is_major_sync = unit_bytes >= 8
                && is_mlp_major_sync(u32::from_be_bytes([
                    self.acc.as_slice()[4],
                    self.acc.as_slice()[5],
                    self.acc.as_slice()[6],
                    self.acc.as_slice()[7],
                ]));

            // Decodability gate: MLP/TrueHD decode state persists across AUs, so
            // a corrupt AU is dropped FORWARD to the next validated major sync
            // rather than excised in place: a drop is a silence gap, never a shift.
            let au = self.acc.as_slice()[..unit_bytes].to_vec();
            let pts = self.next_pts_ns;
            // Read BEFORE the drain below: this unit's source is the packet
            // covering the CURRENT front, not the next unit's.
            let au_src = self.acc.front().source;
            let mut emit_keyframe: Option<bool> = None; // Some(is_keyframe) => emit
            let mut drop_reason: Option<(&'static str, bool)> = None; // (reason, verified)

            if self.tally.is_poisoned() {
                // Whole track already judged dead — collateral drop (does not
                // re-feed the poison verdict).
                drop_reason = Some(("track-poisoned", false));
            } else {
                match self.au_check(&au, is_major_sync) {
                    AuCheck::ValidMajorSync { format_info } => {
                        // The rate nibble is trustworthy only now that the major
                        // sync's CRC has validated. Refine the per-AU PTS
                        // increment (48 kHz family stays the 833_333 default).
                        if let Some(fi) = format_info {
                            self.au_duration_ns = truehd_au_duration_ns(fi);
                        }
                        // A validated major sync is the ONLY clean resync point.
                        self.resync_pending = false;
                        emit_keyframe = Some(true);
                    }
                    AuCheck::Corrupt => {
                        if self.resync_pending {
                            // Part of the current drop-forward run — collateral.
                            drop_reason = Some(("resync", false));
                        } else {
                            // The trigger: one verified corruption that starts the
                            // drop-forward. Only this counts toward poison.
                            let r = if is_major_sync {
                                "major-sync-crc"
                            } else {
                                "parity"
                            };
                            drop_reason = Some((r, true));
                            self.resync_pending = true;
                        }
                    }
                    AuCheck::Ok => {
                        if self.resync_pending {
                            // Decode state is invalid until the next validated
                            // major sync, so even a parity-OK AU is undecodable
                            // here — collateral drop.
                            drop_reason = Some(("resync", false));
                        } else {
                            emit_keyframe = Some(false);
                        }
                    }
                    AuCheck::Unverifiable => {
                        if self.resync_pending {
                            // Not a validated major sync — do NOT clear the resync
                            // on it; keep dropping forward.
                            drop_reason = Some(("resync", false));
                        } else {
                            // Head of stream / too-short AU: keep (never drop what
                            // we cannot verify).
                            emit_keyframe = Some(is_major_sync);
                        }
                    }
                }
            }

            if let Some(keyframe) = emit_keyframe {
                self.tally.record_kept();
                frames.push(Frame {
                    discontinuity: false,
                    coding: None,
                    source: au_src,
                    pts_ns: pts,
                    keyframe,
                    data: au,
                    duration_ns: None,
                });
            } else if let Some((reason, verified)) = drop_reason {
                if verified {
                    self.tally
                        .record_drop(pts, self.au_duration_ns, au.len(), reason);
                } else {
                    self.tally
                        .record_collateral_drop(pts, self.au_duration_ns, au.len(), reason);
                }
            }
            self.acc.drain(unit_bytes);
            self.next_pts_ns += self.au_duration_ns;
        }

        // Bound memory on malformed input: a stream that never yields a
        // complete frame must not grow the buffer without limit.
        if self.acc.len() > MAX_TRUEHD_BUF {
            self.acc.clear();
        }

        frames
    }

    fn flush(&mut self) -> Vec<Frame> {
        self.tally.log_summary();
        Vec::new()
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Per-bit channel counts for the TrueHD 8-channel and 6-channel presentation
/// channel-assignment masks (per the MLP/TrueHD bitstream spec). Some
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
        // 0xBA only: `truehd_channels` reads the TrueHD `format_info` channel
        // masks, which an MLP (0xBB) major sync does not carry.
        if is_truehd_major_sync(w) {
            let fi = u32::from_be_bytes([data[p + 4], data[p + 5], data[p + 6], data[p + 7]]);
            return truehd_channels(fi);
        }
        p += 1;
    }
    None
}

/// Real sample rate (Hz) from a TrueHD major-sync `format_info` word.
///
/// The 4-bit `ratebits` nibble sits in `format_info` bits 31..28, the same
/// word `truehd_channels` reads for the channel masks. This is a **strict
/// whitelist** of the six rates that occur on real BD/UHD TrueHD; every
/// other code returns `None` so a malformed field can never produce a wrong
/// `SamplingFrequency` — the caller falls back to its container-derived
/// rate. See docs/truehd.md for the full rate formula and rationale.
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
        // 0xBA only: `format_info` (and the num_substreams/Atmos nibble) are the
        // TrueHD layout, not MLP's.
        if is_truehd_major_sync(w) {
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

    // Non-degenerate substream count/directory size test — see
    // docs/truehd.md#mlp_substr_header_size-test-rationale.
    #[test]
    fn mlp_num_substreams_is_the_top_nibble_of_major_sync_byte_16() {
        // Byte 16 of the major sync: top nibble is num_substreams, bottom nibble
        // is a different field, so it must not leak into the answer.
        for n in 0..16u8 {
            let mut ms = vec![0u8; 17];
            ms[16] = (n << 4) | 0x0F;
            assert_eq!(
                mlp_num_substreams(&ms),
                Some(n),
                "num_substreams is the high nibble only"
            );
        }
        // Real counts: 1 substream for 2.0/5.1 core-only, 4 for the 7.1/Atmos
        // layouts this crate has to mux.
        let mut ms = vec![0u8; 20];
        ms[16] = 0x40;
        assert_eq!(mlp_num_substreams(&ms), Some(4));

        // A major sync too short to contain byte 16 yields no answer — never a
        // defaulted count, which would arm the parity check against garbage.
        assert_eq!(mlp_num_substreams(&[0u8; 16]), None);
    }

    #[test]
    fn mlp_substr_header_size_counts_the_extraword_entries() {
        // Directory entry: 2 bytes, plus 2 more when the entry's top bit
        // (extraword) is set. Build a 4-substream directory that mixes both.
        const HDR: usize = 4;
        let mut au = vec![0xAAu8; HDR];
        au.extend_from_slice(&[0x00, 0x11]); // plain          → 2
        au.extend_from_slice(&[0x80, 0x22, 0x01, 0x02]); // extraword → 4
        au.extend_from_slice(&[0x00, 0x33]); // plain          → 2
        au.extend_from_slice(&[0x80, 0x44, 0x03, 0x04]); // extraword → 4
        au.extend_from_slice(&[0xFFu8; 8]); // payload past the directory
        assert_eq!(
            mlp_substr_header_size(&au, HDR, 4),
            Some(12),
            "2 + 4 + 2 + 4"
        );

        // Same AU, fewer declared substreams → only that many entries counted.
        assert_eq!(mlp_substr_header_size(&au, HDR, 1), Some(2));
        assert_eq!(mlp_substr_header_size(&au, HDR, 2), Some(6));
        assert_eq!(mlp_substr_header_size(&au, HDR, 0), Some(0));

        // An all-plain directory is 2 bytes per substream.
        let plain = vec![0x00u8; HDR + 8];
        assert_eq!(mlp_substr_header_size(&plain, HDR, 4), Some(8));

        // A directory that runs past the AU has no answer: the parity window
        // would otherwise be placed over bytes that are not there.
        let truncated = &au[..HDR + 9];
        assert_eq!(mlp_substr_header_size(truncated, HDR, 4), None);
        assert_eq!(mlp_substr_header_size(&plain, HDR, 5), None);
    }

    fn make_truehd_unit(size_bytes: usize) -> Vec<u8> {
        let words = size_bytes / 2;
        let mut data = vec![0u8; size_bytes];
        data[0] = ((words >> 8) & 0x0F) as u8;
        data[1] = (words & 0xFF) as u8;
        data
    }

    // Make a synthetic major-sync AU pass the decodability gate (CRC +
    // parity). See docs/truehd.md#finalize_major_sync-test-helper.
    fn finalize_major_sync(au: &mut [u8]) {
        const MSHDR: usize = 28; // no extension (byte 25 clear)
        // num_substreams = 1 → major-sync byte 16 (AU[20]) top nibble.
        au[20] = (au[20] & 0x0F) | 0x10;
        // Substream directory entry at AU[4+MSHDR] = AU[32]: extraword flag clear.
        au[32] &= 0x7F;
        // Major-sync checksum, built EXACTLY as `mlp_major_sync_crc_ok` verifies it
        // (the MLP checksum16): swap_bytes(crc16_mlp(body)) ^ LE word before
        // the trailer, stored little-endian in the trailer.
        let body_end = 4 + MSHDR - 4; // AU[4..28]
        let crc = super::crc16_mlp(&au[4..body_end]).swap_bytes()
            ^ u16::from_le_bytes([au[body_end], au[body_end + 1]]);
        au[4 + MSHDR - 2] = (crc & 0xFF) as u8;
        au[4 + MSHDR - 1] = (crc >> 8) as u8;
        // Parity: choose the AU check nibble (AU[0] high bits) so the header +
        // directory fold to 0xF. The length low nibble (AU[0] low bits) is kept.
        let hi = au[0] & 0x0F;
        let p0 = (hi ^ au[1] ^ au[2] ^ au[3]) ^ (au[32] ^ au[33]);
        let c = ((p0 >> 4) ^ (p0 & 0x0F) ^ 0x0F) & 0x0F;
        au[0] = (c << 4) | hi;
    }

    /// Give a synthetic NON-major-sync AU a valid header parity nibble (1
    /// substream, directory at AU[4..6]), so it passes the gate once a preceding
    /// major sync has established `num_substreams`.
    fn finalize_normal_parity(au: &mut [u8]) {
        au[4] &= 0x7F; // no extraword
        let hi = au[0] & 0x0F;
        let p0 = (hi ^ au[1] ^ au[2] ^ au[3]) ^ (au[4] ^ au[5]);
        let c = ((p0 >> 4) ^ (p0 & 0x0F) ^ 0x0F) & 0x0F;
        au[0] = (c << 4) | hi;
    }

    fn valid_major_sync() -> Vec<u8> {
        let mut u = make_truehd_unit(200);
        u[4..8].copy_from_slice(&0xF872_6FBAu32.to_be_bytes());
        finalize_major_sync(&mut u);
        u
    }

    fn valid_normal_au() -> Vec<u8> {
        let mut u = make_truehd_unit(200);
        finalize_normal_parity(&mut u);
        u
    }

    #[test]
    fn corrupt_major_sync_drops_forward_to_next_valid() {
        // MLP state carries across AUs, so a corrupt AU is dropped FORWARD to the
        // next valid major sync. Sequence: valid MS, corrupt MS, normal AU, valid
        // MS — only the two valid syncs survive (the normal AU is poisoned collateral).
        let mut parser = TrueHdParser::new();
        let ms1 = valid_major_sync();
        let mut ms_bad = valid_major_sync();
        ms_bad[10] ^= 0xFF; // corrupt a CRC-covered header byte
        let normal = valid_normal_au(); // clean parity, but arrives mid-resync
        let ms2 = valid_major_sync();

        let mut data = ms1.clone();
        data.extend_from_slice(&ms_bad);
        data.extend_from_slice(&normal);
        data.extend_from_slice(&ms2);
        let mut frames = parser.parse(&make_pes(data, Some(90000)));
        frames.extend(parser.flush());

        assert_eq!(frames.len(), 2, "only the two valid major syncs survive");
        assert!(frames[0].keyframe && frames[1].keyframe);
        assert_eq!(
            parser.dropped_frames(),
            2,
            "corrupt MS + poisoned normal AU"
        );
    }

    #[test]
    fn discontinuity_resyncs_forward_to_next_major_sync() {
        // A discontinuity leaves MLP's cross-AU state stale, so it arms the same
        // drop-forward the corruption path uses: drop post-gap AUs until the next
        // CRC-valid major sync. Fixed 559 "restart header sync" errors on a real disc.
        let mut parser = TrueHdParser::new();

        // Establish a baseline so num_substreams is known and the gate is live.
        let mut pre = valid_major_sync();
        pre.extend_from_slice(&valid_normal_au());
        let pre_frames = parser.parse(&make_pes(pre, Some(90000)));
        assert_eq!(pre_frames.len(), 2, "baseline: major sync + one normal AU");

        // Post-gap PES: two normal AUs, then a valid major sync, then a normal AU.
        // `discontinuity: true` says packets were lost before this PES.
        let mut post = valid_normal_au();
        post.extend_from_slice(&valid_normal_au());
        post.extend_from_slice(&valid_major_sync());
        post.extend_from_slice(&valid_normal_au());
        let post_pes = PesPacket {
            source: None,
            pid: 0x1100,
            pts: Some(90000 + 4 * 900),
            dts: None,
            data: post,
            discontinuity: true,
        };
        let mut frames = parser.parse(&post_pes);
        frames.extend(parser.flush());

        // Only the major sync and the AU after it survive; the two leading
        // post-gap normal AUs are dropped for lack of a re-init point.
        assert_eq!(
            frames.len(),
            2,
            "resume only at the major sync + what follows it (got {})",
            frames.len()
        );
        assert!(
            frames[0].keyframe,
            "the first surviving post-gap frame MUST be a major sync (re-init \
             point), never a mid-stream AU"
        );
        assert!(
            !frames[1].keyframe,
            "the AU after the re-init point is a normal AU"
        );
        assert_eq!(
            parser.dropped_frames(),
            2,
            "the two post-gap AUs before the major sync are dropped as collateral"
        );
    }

    #[test]
    fn crc_failed_head_major_sync_is_kept_not_track_killed() {
        // REGRESSION (every TrueHD title after the checksum gate landed): a
        // checksum-failed major sync at stream head (no baseline yet) must NOT
        // arm drop-forward, or it silently drops the whole track (OOM decoder).
        let mut parser = TrueHdParser::new();
        let mut ms_bad = valid_major_sync();
        ms_bad[10] ^= 0xFF; // break a checksum-covered header byte → checksum fails
        let mut data = ms_bad;
        for _ in 0..6 {
            data.extend_from_slice(&valid_normal_au());
        }
        let mut frames = parser.parse(&make_pes(data, Some(90000)));
        frames.extend(parser.flush());
        assert_eq!(
            frames.len(),
            7,
            "no baseline yet: the CRC-failed head major sync + all following AUs are \
             kept, not dropped (got {})",
            frames.len()
        );
        assert_eq!(
            parser.dropped_frames(),
            0,
            "nothing dropped without a validated baseline to protect"
        );
        // And the invariant still holds ONCE a baseline exists: after a genuinely
        // valid major sync, a later corrupt one IS dropped (see
        // `corrupt_major_sync_drops_forward_to_next_valid`).
    }

    // Independent bitwise CRC-16 oracle (poly 0x002D), not tautological
    // with `crc16_mlp`. See docs/truehd.md#ref_crc16_2d-test-oracle.
    fn ref_crc16_2d(data: &[u8]) -> u16 {
        let mut crc: u16 = 0;
        for &b in data {
            crc ^= (b as u16) << 8;
            for _ in 0..8 {
                crc = if crc & 0x8000 != 0 {
                    (crc << 1) ^ 0x002D
                } else {
                    crc << 1
                };
            }
        }
        crc
    }

    #[test]
    fn extended_major_sync_crc_validates_and_rejects() {
        // COVERAGE GAP: the extended major-sync header path (ms[25]&1 set) had
        // zero test coverage — every other fixture builds only the basic header.
        // Build one with an independent oracle (ref_crc16_2d) to catch the regression.
        assert_eq!(
            ref_crc16_2d(b"123456789"),
            0x4FF7,
            "oracle anchored to catalogue"
        );

        // n = 3 extension words → mshdr = 28 + 2 + 2*3 = 36.
        let n = 3usize;
        let mshdr = 28 + 2 + 2 * n;
        assert_eq!(mshdr, 36);
        let mut ms = vec![0u8; 40]; // slack past the 36-byte header
        // Non-trivial, varied body so the CRC is a meaningful function of it.
        for (i, b) in ms.iter_mut().enumerate().take(mshdr - 4) {
            *b = (0x37u8).wrapping_add((i as u8).wrapping_mul(0x53));
        }
        ms[25] |= 1; // extension flag → selects the extended header size
        ms[26] = (ms[26] & 0x0F) | ((n as u8) << 4); // extension word count in high nibble

        // The 2-byte "penultimate" word (between the CRC-covered body and the
        // trailer). Chosen non-zero and non-palindromic so the LE/BE distinction
        // is observable.
        ms[mshdr - 4] = 0x12;
        ms[mshdr - 3] = 0x34;

        // Oracle: checksum16 = crc16_2D(body).swap_bytes() ^ le16(penultimate),
        // computed with the INDEPENDENT ref CRC, then stored LITTLE-ENDIAN.
        let le_word = u16::from_le_bytes([ms[mshdr - 4], ms[mshdr - 3]]);
        let trailer = ref_crc16_2d(&ms[..mshdr - 4]).swap_bytes() ^ le_word;
        ms[mshdr - 2] = (trailer & 0xFF) as u8;
        ms[mshdr - 1] = (trailer >> 8) as u8;
        assert_ne!(
            ms[mshdr - 2],
            ms[mshdr - 1],
            "trailer bytes must differ so the LE/BE swap below is a real distinction"
        );

        // The extended header size is computed from ms[25]/ms[26].
        assert_eq!(
            mlp_major_sync_header_size(&ms),
            Some(mshdr),
            "extended header size = 28 + 2 + 2*n"
        );
        // The validator accepts the independently-built extended major sync.
        assert!(
            mlp_major_sync_crc_ok(&ms, mshdr),
            "valid extended major-sync checksum must validate"
        );

        // A single corrupted body byte must be rejected.
        let mut corrupt = ms.clone();
        corrupt[10] ^= 0xFF;
        assert!(
            !mlp_major_sync_crc_ok(&corrupt, mshdr),
            "a corrupted extended major sync must be rejected"
        );

        // The endianness regression: the SAME checksum stored big-endian must be
        // rejected. A validator that reads the trailer big-endian (the shipped
        // bug) would instead accept this and reject the correct LE form above.
        let mut swapped = ms.clone();
        swapped.swap(mshdr - 2, mshdr - 1);
        assert!(
            !mlp_major_sync_crc_ok(&swapped, mshdr),
            "a big-endian-stored trailer must be rejected (little-endian is load-bearing)"
        );
    }

    #[test]
    fn parity_failure_is_dropped() {
        // A normal AU whose header parity is broken (after a major sync sets
        // num_substreams) is undecodable → dropped.
        let mut parser = TrueHdParser::new();
        let ms1 = valid_major_sync();
        let mut bad = valid_normal_au();
        // A single-nibble flip: MLP's nibble-fold parity is blind
        // to a full-byte flip, which changes both nibbles equally and cancels.
        bad[2] ^= 0x01;
        let ms2 = valid_major_sync();
        let mut data = ms1;
        data.extend_from_slice(&bad);
        data.extend_from_slice(&ms2);
        let mut frames = parser.parse(&make_pes(data, Some(90000)));
        frames.extend(parser.flush());
        assert_eq!(frames.len(), 2, "the parity-broken AU is dropped");
        assert_eq!(parser.dropped_frames(), 1);
        // The drop is a SILENCE GAP whose length is what the CLI reports as lost
        // audio: one AU = 40 samples at 48 kHz = 40/48000s = 833_333 ns. A count
        // without a rate-aware duration understates or invents the loss.
        assert_eq!(
            parser.dropped_duration_ns(),
            833_333,
            "one dropped AU = 40 samples at 48 kHz"
        );
    }

    #[test]
    fn drop_forward_preserves_av_sync_no_shift() {
        // THE INVARIANT: the resumed major sync keeps the exact PTS it would have
        // had with no drop — base + 3 AU durations (MS1, corrupt-MS, normal, MS2)
        // — so the drop is a silence gap, never a shift.
        let mut parser = TrueHdParser::new();
        let ms1 = valid_major_sync();
        let mut ms_bad = valid_major_sync();
        ms_bad[10] ^= 0xFF;
        let normal = valid_normal_au();
        let ms2 = valid_major_sync();
        let mut data = ms1;
        data.extend_from_slice(&ms_bad);
        data.extend_from_slice(&normal);
        data.extend_from_slice(&ms2);
        let mut frames = parser.parse(&make_pes(data, Some(90000)));
        frames.extend(parser.flush());
        assert_eq!(frames.len(), 2);
        assert_eq!(
            frames[1].pts_ns - frames[0].pts_ns,
            3 * AU_DURATION_NS,
            "resumed major sync keeps its true timeline (gap, not shift)"
        );
    }

    #[test]
    fn transient_corruptions_do_not_poison_whole_track() {
        // Regression (audit HIGH): drop-forward must not amplify a couple of
        // transient errors into a false whole-track poison — even resync runs
        // past 200 AUs must leave the track un-poisoned and keep good audio after.
        let mut parser = TrueHdParser::new();
        let mut data = valid_major_sync();
        // Corruption #1 then a long run of normal AUs (all collateral-dropped
        // while resyncing — no major sync to re-init on).
        let mut bad1 = valid_normal_au();
        bad1[2] ^= 0x01; // single-nibble parity break
        data.extend_from_slice(&bad1);
        for _ in 0..210 {
            data.extend_from_slice(&valid_normal_au());
        }
        // A valid major sync resumes; the good AUs after it MUST be kept.
        data.extend_from_slice(&valid_major_sync());
        for _ in 0..5 {
            data.extend_from_slice(&valid_normal_au());
        }
        let frames = parser.parse(&make_pes(data, Some(90000)));
        assert!(
            !parser.tally.is_poisoned(),
            "two transient errors must not poison the track"
        );
        // MS1 + resumed MS2 + the 5 good AUs after it survive.
        assert_eq!(
            frames.len(),
            7,
            "post-resync good audio is kept, not poisoned away"
        );
        assert!(
            parser.dropped_frames() > 200,
            "the resync run was still counted for reporting"
        );
    }

    #[test]
    fn corrupt_major_sync_rate_nibble_does_not_shift_pts() {
        // Regression (audit MED): a corrupt major sync's rate nibble must NOT
        // refine au_duration_ns — the rate is only trustworthy after the CRC
        // validates, else the resumed 48kHz audio is shifted (not gapped).
        let mut parser = TrueHdParser::new();
        let ms1 = valid_major_sync(); // 48 kHz
        let mut ms_bad = valid_major_sync();
        // Set the rate nibble (top nibble of format_info = au[8]) to 0x8 (44.1k).
        // au[8] is CRC-covered, so this also breaks the major-sync CRC → corrupt.
        ms_bad[8] = (ms_bad[8] & 0x0F) | 0x80;
        let ms2 = valid_major_sync(); // 48 kHz
        let mut data = ms1;
        data.extend_from_slice(&ms_bad);
        data.extend_from_slice(&ms2);
        let frames = parser.parse(&make_pes(data, Some(90000)));
        assert_eq!(frames.len(), 2, "corrupt MS dropped; MS1 and MS2 survive");
        assert_eq!(
            frames[1].pts_ns - frames[0].pts_ns,
            2 * AU_DURATION_NS,
            "resumed audio keeps the 48 kHz cadence — the corrupt MS's 44.1k rate was ignored"
        );
    }

    #[test]
    fn too_short_major_sync_does_not_clear_resync() {
        // Regression (audit LOW): while resyncing, a major sync too short to hold
        // (and CRC-validate) its header must NOT be treated as a clean resync
        // point — the runt is dropped and only a real validated major sync resumes.
        let mut parser = TrueHdParser::new();
        let ms1 = valid_major_sync();
        let mut bad = valid_normal_au();
        bad[2] ^= 0x01; // parity break → triggers resync
        // An 8-byte "major sync": length=4 words, sync at bytes 4..8, too short
        // to hold the 28-byte major-sync header.
        let runt = vec![0x00, 0x04, 0x00, 0x00, 0xF8, 0x72, 0x6F, 0xBA];
        let ms2 = valid_major_sync();
        let mut data = ms1;
        data.extend_from_slice(&bad);
        data.extend_from_slice(&runt);
        data.extend_from_slice(&ms2);
        let frames = parser.parse(&make_pes(data, Some(90000)));
        assert_eq!(frames.len(), 2, "the runt major sync did not resume decode");
        for f in &frames {
            assert_eq!(
                f.data.len(),
                200,
                "only the real 200-byte major syncs survive"
            );
        }
    }

    #[test]
    fn clean_truehd_stream_drops_nothing() {
        // A run of valid AUs passes untouched — zero false positives (the CRC and
        // parity are verified against real TrueHD output).
        let mut parser = TrueHdParser::new();
        let mut data = valid_major_sync();
        for _ in 0..5 {
            data.extend_from_slice(&valid_normal_au());
        }
        let frames = parser.parse(&make_pes(data, Some(90000)));
        assert_eq!(frames.len(), 6);
        assert_eq!(parser.dropped_frames(), 0);
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
        // dropped — splicing it corrupts AU framing; PTS re-seeds from the post-gap PES.
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
        // can carry a PTS that lags it slightly (muxer rounding jitter). An
        // unconditional reset snapped the next AU below the prior one — clamp forward-only.
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
        // Regression (multi-clip non-monotonic audio-DTS band): a non-seamless
        // clip boundary resets PES PTS near zero (large backward step, not
        // jitter) and must be ADOPTED raw, like DTS/AC-3, or audio strands at the prior tail.
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
        // An AU split across two PES packets: the first PES (pts 90000) begins
        // it, the second (pts 99999) merely continues it. The emitted AU must
        // keep the first PES's PTS, not the continuation's.
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
        // A zero-length AU header (4 bytes) must be skipped whole. Draining only
        // 2 would misread the timing bytes (0x01 0x90 = 400 words = 800 bytes) as
        // a bogus length, stalling the parser waiting for bytes that never come.
        let mut parser = TrueHdParser::new();
        let mut data = vec![0x00, 0x00, 0x01, 0x90]; // length=0, timing=0x0190
        data.extend_from_slice(&make_truehd_unit(200));
        let frames = parser.parse(&make_pes(data, Some(90000)));
        assert_eq!(frames.len(), 1, "real unit parses after zero-length header");
        assert_eq!(frames[0].data.len(), 200);
    }

    #[test]
    fn unmappable_ac3_header_resyncs_not_stalls() {
        // A permanently unmappable 0x0B77 header (reserved fscod==3) must NOT
        // stall the parser — it used to be treated as "incomplete, wait" and
        // break forever. Now it resyncs (drains 2 bytes) so a clean unit behind it is emitted.
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
        assert!(parser.acc.is_empty(), "buffer fully consumed, no stall");
    }

    #[test]
    fn truehd_au_with_0b77_head_not_stolen_by_ac3() {
        // A TrueHD AU whose first two bytes are 0x0B 0x77 must NOT be misrouted
        // to the AC-3 path — the AC-3 size closes the boundary wrong; secondary
        // corroboration rejects it since the computed end isn't followed by a real sync.
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

    // --- truehd_channels: per-bit mask channel counts (MLP channel table) ---

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
    fn channels_from_stream_rejects_mlp_sync_0xfb() {
        // 0xF8726FBB is the MLP stream type, NOT TrueHD (0xF8726FBA). Its next
        // word holds quantization/MLP rate fields, not TrueHD's rate nibble +
        // channel masks; decoding it as TrueHD reads channels from unrelated bits.
        let mut data = vec![0x00];
        data.extend_from_slice(&0xF872_6FBBu32.to_be_bytes());
        data.extend_from_slice(&0x0000_001Fu32.to_be_bytes());
        assert_eq!(
            truehd_channels_from_stream(&data),
            None,
            "an MLP (0xBB) major sync must not be decoded as TrueHD format_info"
        );
        // The same bytes under the TrueHD stream type DO decode.
        let mut data = vec![0x00];
        data.extend_from_slice(&0xF872_6FBAu32.to_be_bytes());
        data.extend_from_slice(&0x0000_001Fu32.to_be_bytes());
        assert_eq!(truehd_channels_from_stream(&data), Some(8));
    }

    #[test]
    fn mlp_sync_0xfb_yields_no_sample_rate_or_atmos() {
        // Same split for the shared scan: an MLP major sync must not produce a
        // TrueHD rate (bits 31..28 of an MLP header are a quantization code, not
        // the rate) nor an Atmos verdict.
        let mut data = vec![0x00];
        data.extend_from_slice(&0xF872_6FBBu32.to_be_bytes());
        // ratebits nibble 0x1 would decode as 96 kHz under the TrueHD layout.
        data.extend_from_slice(&0x1000_001Fu32.to_be_bytes());
        data.extend_from_slice(&[0x00; 12]);
        assert!(
            truehd_sync_info_from_stream(&data).is_none(),
            "no TrueHD sync info from an MLP major sync"
        );
        assert_eq!(truehd_sample_rate_from_stream(&data), None);
        // TrueHD stream type, identical trailing bytes → the rate IS decoded.
        let mut data = vec![0x00];
        data.extend_from_slice(&0xF872_6FBAu32.to_be_bytes());
        data.extend_from_slice(&0x1000_001Fu32.to_be_bytes());
        data.extend_from_slice(&[0x00; 12]);
        assert_eq!(truehd_sample_rate_from_stream(&data), Some(96000));
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
        finalize_major_sync(&mut unit);
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
        // The restart-point check masks the low sync bit, so 0xF8726FBB (MLP)
        // counts as a major sync too — both types re-init the decoder, so both
        // are keyframes (format_info decode alone is 0xBA-only; see the sibling test).
        let mut parser = TrueHdParser::new();
        let mut unit = make_truehd_unit(200);
        unit[4..8].copy_from_slice(&0xF872_6FBBu32.to_be_bytes());
        finalize_major_sync(&mut unit);
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
        assert_eq!(parser.acc.len(), 100, "partial AU retained");
    }

    /// Largest AU the 12-bit length field can declare: 0xFFF words × 2.
    const MAX_AU_BYTES: usize = 0xFFF * 2; // 8190

    #[test]
    fn buffer_stays_bounded_across_many_partial_pes() {
        // Malformed/never-completing input must keep the buffer bounded. The
        // bound that actually holds is MAX_AU_BYTES (8190), not MAX_TRUEHD_BUF —
        // asserting only `<= MAX_TRUEHD_BUF` is vacuous; assert the reachable ceiling instead.
        let mut parser = TrueHdParser::new();
        let mut worst = 0usize;
        for _ in 0..200 {
            let mut frag = vec![0u8; MAX_AU_BYTES - 1];
            frag[0] = 0xFF;
            frag[1] = 0xFF;
            let _ = parser.parse(&make_pes(frag, Some(0)));
            worst = worst.max(parser.acc.len());
            assert!(
                parser.acc.len() < MAX_AU_BYTES,
                "reassembly buffer exceeded the AU-length ceiling: {} >= {}",
                parser.acc.len(),
                MAX_AU_BYTES
            );
            assert!(
                parser.acc.len() <= MAX_TRUEHD_BUF,
                "reassembly buffer exceeded cap: {} > {}",
                parser.acc.len(),
                MAX_TRUEHD_BUF
            );
        }
        // The fixture must genuinely load the buffer, not self-drain: if this
        // trips, the test is measuring nothing.
        assert!(
            worst >= MAX_AU_BYTES - 8,
            "fixture must drive the buffer to the ceiling, peaked at {worst}"
        );
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
        parser.acc.seed(&[0x0B, 0x77, 0x00]);
        // Drive through parse: a short 0x0B77 head must wait, not emit.
        let f = parser.parse(&make_pes(vec![0x0B, 0x77, 0x00], Some(0)));
        assert!(f.is_empty());
    }

    // --- #2 sample rate from the major-sync rate nibble ---

    // `format_info` with given `ratebits` (top nibble) + a 7.1 8-channel
    // mask (ch8 = 0x1F) in the low 13 bits, co-located in one real word.
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
        // 0xF is the explicit invalid code; 0x3/0xB are formula-only, not
        // whitelisted; 0x7/0xE are reserved. None may produce a rate — the host
        // must fall back to its container value, never write a wrong SamplingFrequency.
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
        // Internal-consistency guard: with the 96kHz nibble AND a 7.1 mask in
        // the same word, rate reads 96000 and channels still read 8 — proving
        // the rate nibble (bits 31..28) and channel masks (bits 19..0) don't collide.
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
        finalize_major_sync(&mut a1);
        let mut a2 = make_truehd_unit(200);
        finalize_normal_parity(&mut a2);
        let mut data = a1;
        data.extend_from_slice(&a2);
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
        finalize_major_sync(&mut a1);
        let mut a2 = make_truehd_unit(200);
        finalize_normal_parity(&mut a2);
        let mut data = a1;
        data.extend_from_slice(&a2);
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

    /// Same rule for TrueHD: a unit spanning two packets belongs to the one
    /// that carried its first byte.
    #[test]
    fn an_access_unit_carries_the_source_of_the_packet_it_began_in() {
        let mut parser = TrueHdParser::new();
        let unit = make_truehd_unit(512);

        let mut p1 = make_pes(unit[..200].to_vec(), Some(90_000));
        p1.source = Some(crate::pes::SourcePos::at_byte(1_000));
        let first = parser.parse(&p1);
        assert!(first.is_empty(), "partial unit held");

        let mut p2 = make_pes(unit[200..].to_vec(), Some(180_000));
        p2.source = Some(crate::pes::SourcePos::at_byte(9_000));
        let frames = parser.parse(&p2);
        assert!(!frames.is_empty(), "the completed unit is emitted");
        assert_eq!(
            frames[0].source.map(|s| s.byte),
            Some(1_000),
            "the unit belongs to the packet its FIRST byte came from"
        );
    }
}
