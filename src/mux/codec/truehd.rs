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

// Is `w` specifically the TrueHD major sync (stream type 0xBA)? Must be exact, not the
// 0xBA/0xBB mask.
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

// Backstop on the reassembly buffer, parity with the AC-3/DTS/PGS caps. The parse loop
// already drains every complete AU, so the residue never exceeds one AU (8190 bytes) and
// this does not fire today; it bounds memory if that loop changes.
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
    /// Test-only: how many `PesBuf::drain` calls the zero-length-AU-header path
    /// made. Pins a WORK bound (one drain per contiguous zero-word run, not one
    /// per 4-byte word), which no frame-level assertion can observe.
    #[cfg(test)]
    zero_run_drains: u64,
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
            #[cfg(test)]
            zero_run_drains: 0,
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

    // Decide whether an AU is corrupt, updating `num_substreams` from a valid major sync.
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
                    return AuCheck::Corrupt("major-sync-crc"); // real corruption vs a proven baseline
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
            // Baseline proven by a CRC-validated major sync: an overrunning directory is corruption.
            return AuCheck::Corrupt("directory-overrun");
        };
        if !mlp_parity_ok(au, header_size, shs) {
            return AuCheck::Corrupt("parity");
        }
        if is_major_sync {
            AuCheck::ValidMajorSync { format_info }
        } else {
            AuCheck::Ok
        }
    }

    // Size (bytes) of the AC-3 frame at the buffer head: Unmappable/NeedMore/ Frame(n).
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

// Secondary validation: is the AC-3 frame's computed end a plausible boundary?
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
    /// whose substream-directory parity failed. Feeds the poison verdict. Carries the drop reason.
    Corrupt(&'static str),
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

// Major-sync header size: base 28 + `2 + extensions*2` when the extension flag is set.
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

// Validate the major-sync header checksum (CRC-16, poly 0x002D, byte- reversed vs standard).
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
                // Zero-length AU (malformed/padding). Draining 4 bytes per header
                // is O(run_len^2) (PesBuf::drain shifts the tail each call), so
                // scan the whole zero-header run first and drain it in one call.
                let mut skip = 4;
                while skip + 4 <= self.acc.len() {
                    let w = (((self.acc.as_slice()[skip] as usize) << 8)
                        | self.acc.as_slice()[skip + 1] as usize)
                        & 0xFFF;
                    if w != 0 {
                        break;
                    }
                    skip += 4;
                }
                self.acc.drain(skip);
                #[cfg(test)]
                {
                    self.zero_run_drains += 1;
                }
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
                    AuCheck::Corrupt(r) => {
                        if self.resync_pending {
                            // Part of the current drop-forward run — collateral.
                            drop_reason = Some(("resync", false));
                        } else {
                            // The trigger: one verified corruption that starts the
                            // drop-forward. Only this counts toward poison.
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
const THD_6CH: [u8; 5] = [2, 1, 1, 2, 2];

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

/// LFE channels (bit 2 LFE, 8ch bit 12 LFE2) within the presentation
/// `truehd_channels` counts, so the caller can split `N.M`.
pub fn truehd_lfe(format_info: u32) -> u8 {
    let ch8 = format_info & 0x1FFF;
    let ch6 = (format_info >> 15) & 0x1F;
    if ch8 != 0 {
        u8::from(ch8 & (1 << 2) != 0) + u8::from(ch8 & (1 << 12) != 0)
    } else {
        u8::from(ch6 & (1 << 2) != 0)
    }
}

/// Scan a demuxed TrueHD elementary-stream chunk for the first major sync and
/// decode its true channel count. The stream may interleave AC-3; we scan for
/// the major-sync word anywhere and read the following `format_info`.
pub fn truehd_channels_from_stream(data: &[u8]) -> Option<u8> {
    truehd_sync_info_from_stream(data).and_then(|s| truehd_channels(s.format_info))
}

/// Real sample rate (Hz) from a TrueHD major-sync `format_info` word.
///
/// The 4-bit `ratebits` nibble sits in `format_info` bits 31..28, the same word
/// `truehd_channels` reads for the channel masks. This is a **strict whitelist** of the six
/// rates that occur on real BD/UHD TrueHD; every other code returns `None` so a malformed field
/// can never produce a wrong `SamplingFrequency` — the caller falls back to its
/// container-derived rate.
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
#[path = "truehd_tests.rs"]
mod tests;
