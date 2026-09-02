//! DTS / DTS-HD elementary stream parser.
//!
//! DTS core syncword: 0x7FFE8001 (32 bits).
//! DTS-HD MA/HRA extension syncword: 0x64582025 (32 bits), appears after the core frame.
//! Buffers across PES boundaries so frames spanning two PES packets
//! are emitted complete.

use super::startcode::BitReader;
use super::{CodecParser, Frame, PesPacket};

const DTS_CORE_SYNC: [u8; 4] = [0x7F, 0xFE, 0x80, 0x01];
// DTS-HD extension syncword. See docs/dts.md — DTS_HD_EXT_SYNC: exact
// EXSS sizing keeps a false core sync inside its payload from splitting
// the access unit.
const DTS_HD_EXT_SYNC: [u8; 4] = [0x64, 0x58, 0x20, 0x25];

/// DTS / DTS-HD elementary-stream parser. Buffers DTS across PES boundaries so
/// a core frame plus all of its trailing DTS-HD extension substreams are
/// emitted together as one access unit, delimited by the next valid core sync.
/// This preserves the lossless extension data instead of downgrading to lossy
/// core (the lossy-core downgrade bug).
pub struct DtsParser {
    /// Bytes assembled across PES packets, each attributable to the packet
    /// that carried it. An emitted unit takes the facts of the packet covering
    /// its FIRST byte, so an AU whose core arrived in an earlier PES keeps that
    /// core's timestamp and source offset when its extensions arrive later.
    acc: super::pesbuf::PesBuf,
    /// PTS of the access unit currently being assembled in `buf` (the unit
    /// starting at the first buffered core sync). Captured when that core
    /// frame's PES first arrived; the trailing extension-substream PES
    /// packets carry their own (later) PTS which must NOT override it.
    pending_pts: i64,
    /// The `front_pts` of the PREVIOUS emitted access unit. When the current
    /// AU's `front_pts` differs, it began a new PES → re-base to it. When it is
    /// unchanged, this AU shares the previous AU's PES → advance one frame
    /// duration. This per-PES re-base (rather than a global running clock) is
    /// what keeps a feature-long DVD DTS track from drifting past its real
    /// length. `PTS_UNSET` = no AU emitted yet.
    last_front_pts: i64,
    /// The PTS for the NEXT AU *if it shares the current PES* (the within-PES
    /// running cursor: previous emit + its duration). Only consulted when
    /// `front_pts` is unchanged from `last_front_pts`. `PTS_UNSET` = no base yet.
    next_pts_ns: i64,
    /// Keep/drop bookkeeping for the decodability gate: counts, per-drop and
    /// aggregate logging, and the whole-track poison fallback. A dropped AU is
    /// NEVER emitted, but the PTS clock is still advanced across it (see
    /// [`Self::stamp_pts`] usage) so every SURVIVING AU keeps the exact timestamp it
    /// would have had — a drop becomes a silence gap, never a shift.
    tally: super::dropgate::DropTally,
}

impl Default for DtsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl DtsParser {
    pub fn new() -> Self {
        Self {
            acc: super::pesbuf::PesBuf::with_capacity(32768),
            pending_pts: 0,
            last_front_pts: PTS_UNSET,
            next_pts_ns: PTS_UNSET,
            tally: super::dropgate::DropTally::new("dts"),
        }
    }

    /// Number of access units dropped as undecodable so far. The mux/CLI reads
    /// this to surface the count ("dropped N damaged DTS frames").
    pub fn dropped_frames(&self) -> u64 {
        self.tally.dropped_frames()
    }

    /// Total decoded duration (ns) of all dropped access units — the length of
    /// audio silence introduced by dropping undecodable frames.
    pub fn dropped_duration_ns(&self) -> u64 {
        self.tally.dropped_duration_ns()
    }

    // See docs/dts.md — emit_or_drop: gates an AU through the decodability
    // check. The PTS clock is already advanced either way, so a drop is a
    // gap, never a shift; every drop is logged (fail-loud).
    fn emit_or_drop(
        &mut self,
        au: Vec<u8>,
        au_pts: i64,
        dur_ns: i64,
        src: Option<crate::pes::SourcePos>,
        out: &mut Vec<Frame>,
    ) {
        let verdict = if self.tally.is_poisoned() {
            Err(DropReason::TrackPoisoned)
        } else {
            core_header_drop_reason(&au).map_or(Ok(()), Err)
        };
        match verdict {
            Ok(()) => {
                self.tally.record_kept();
                out.push(Frame {
                    discontinuity: false,
                    coding: None,
                    // From the SAME packet as `au_pts` — both are the facts of
                    // the PES covering this unit's first byte.
                    source: src,
                    pts_ns: au_pts,
                    keyframe: true,
                    data: au,
                    duration_ns: Some(dur_ns as u64),
                });
            }
            Err(reason) => {
                self.tally
                    .record_drop(au_pts, dur_ns, au.len(), reason.as_str());
            }
        }
    }

    // See docs/dts.md — stamp_pts: re-base to each PES's own PTS, advancing
    // by one frame duration only within a run sharing that PES (fixes DVD's
    // several-cores-per-PES case without the drift a global clock caused).
    fn stamp_pts(&mut self, front: i64, dur_ns: i64) -> i64 {
        let base = if front != PTS_UNSET && front != self.last_front_pts {
            // New PES (or the first AU): trust its own timestamp — no drift.
            front
        } else if self.next_pts_ns != PTS_UNSET {
            // Same PES as the previous AU (front unchanged) → advance one frame.
            self.next_pts_ns
        } else if front != PTS_UNSET {
            front
        } else {
            0
        };
        self.last_front_pts = front;
        self.next_pts_ns = base + dur_ns;
        base
    }

    /// Drop `n` bytes from the front, rebasing attribution onto the new front.
    fn drain_front(&mut self, n: usize) {
        self.acc.drain(n);
    }

    /// PTS for the access unit at the front of the buffer: the facts of the
    /// packet covering offset 0, falling back to the unit's captured base.
    fn front_pts(&self) -> i64 {
        self.acc
            .front()
            .presentation_ns()
            .unwrap_or(self.pending_pts)
    }

    /// Source offset for that same unit — from the SAME packet as its PTS,
    /// which is the property the shared buffer exists to guarantee.
    fn front_source(&self) -> Option<crate::pes::SourcePos> {
        self.acc.front().source
    }
}

/// Hard cap on a buffered access unit (core + all its extension substreams).
/// A DTS-HD MA frame is at most a few tens of KB; if the buffer grows past
/// this without a clean boundary we resync rather than stall or balloon.
const MAX_AU_BYTES: usize = 65536;

// "Enough bytes to read the fsize field" — a HEADER-LAYOUT minimum, distinct
// from MIN_CORE_FRAME_BYTES (the decoded-size validity floor). See docs/dts.md
// — CORE_HEADER_MIN_BYTES.
const CORE_HEADER_MIN_BYTES: usize = 10;

// ETSI TS 102 114 on-wire FSIZE floor is 95, so a real core frame is at
// least 96 bytes; a smaller decoded size means a false/corrupt sync, so we
// resync rather than close an AU at a junk boundary. See docs/dts.md.
const MIN_CORE_FRAME_BYTES: usize = 96;

// Sentinel for "no valid PTS base captured yet": real PTS-in-ns values are
// non-negative, so this can never collide. Marks the base invalid after a
// forced flush so the next PES sets it regardless of buffer state.
const PTS_UNSET: i64 = -1;

impl CodecParser for DtsParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        // B1: a concealed/lost gap means the buffered DTS AU is TRUNCATED; splicing
        // post-gap bytes onto it corrupts the framing. Drop the partial AU and its
        // PTS marks (DTS frames independently resync, unlike TrueHD/MLP).
        if pes.discontinuity {
            self.acc.clear();
            self.pending_pts = PTS_UNSET;
            // A concealed gap is a timeline discontinuity: let the post-gap AU
            // re-base to its own PES PTS rather than the pre-gap cursor.
            self.next_pts_ns = PTS_UNSET;
            self.last_front_pts = PTS_UNSET;
        }
        if pes.data.is_empty() {
            return Vec::new();
        }
        // A PES with no PTS (rare for audio, but legal) must NOT reset the
        // timeline to 0 — continue from the most recent known base.
        let pts_ns = super::pesbuf::PesFacts::of(pes)
            .presentation_ns()
            .unwrap_or(if self.pending_pts >= 0 {
                self.pending_pts
            } else {
                0
            });

        // Blu-ray DTS-HD MA/HRA: core frame + extension substreams (lossless data)
        // arrive in SEPARATE, later PES packets, so assemble core-to-next-core
        // (else extensions get dropped = lossy core) and capture PTS base fresh.
        if self.acc.is_empty() || self.pending_pts == PTS_UNSET {
            self.pending_pts = pts_ns;
        }
        // Mark where THIS PES's bytes begin, with its PTS: the emitted AU takes
        // the PTS of the PES covering its first byte (see `front_pts`), so an AU
        // whose core arrived earlier keeps that timestamp over later extensions.
        self.acc.push_with(
            &pes.data,
            super::pesbuf::PesFacts::of(pes).with_pts_ns(pts_ns),
        );

        let mut frames = Vec::new();

        loop {
            // Resync to the first core sync; drop any leading junk.
            let Some(start) = find_sync(self.acc.as_slice(), &DTS_CORE_SYNC) else {
                // No core sync at all yet — keep at most a 3-byte tail so a
                // sync split across PES packets can still be found next time.
                if self.acc.len() > 3 {
                    let tail = self.acc.len() - 3;
                    self.drain_front(tail);
                }
                break;
            };
            if start > 0 {
                self.drain_front(start);
                // The sync `find_sync` located at offset `start` is now at
                // offset 0 by construction, so a re-scan would be a redundant
                // O(buf_len) walk per iteration; assert the invariant instead.
                debug_assert_eq!(
                    find_sync(self.acc.as_slice(), &DTS_CORE_SYNC),
                    Some(0),
                    "drain_front(start) must leave the core sync at offset 0"
                );
            }

            // Need the core header to size the core frame.
            if self.acc.len() < CORE_HEADER_MIN_BYTES {
                break;
            }
            let core_size = dts_core_frame_size(self.acc.as_slice());
            // A real core frame is at least MIN_CORE_FRAME_BYTES (96, ETSI floor);
            // a smaller size came from a false sync, so reject it (drain 4 bytes,
            // resync) rather than close the AU at a junk boundary.
            if !(MIN_CORE_FRAME_BYTES..=MAX_AU_BYTES).contains(&core_size) {
                // Bogus core sync — skip past it and resync.
                self.drain_front(4);
                continue;
            }
            if self.acc.len() < core_size {
                break; // core frame not fully buffered yet — wait
            }

            // The AU ends at the next *valid* core sync; extension bytes can contain
            // false syncword matches, so `next_core_boundary` validates decoded size.
            // `forced` marks a safety-valve flush whose PTS must NOT become the base.
            let mut forced = false;
            let (au_end, ext_clean) = match next_core_boundary(self.acc.as_slice(), core_size) {
                NextCore::Found { end, ext_clean } => (end, ext_clean),
                NextCore::NeedMore if self.acc.len() <= MAX_AU_BYTES => break,
                NextCore::NeedMore => {
                    // A candidate boundary isn't fully buffered; normally wait, but
                    // past the AU cap apply the same force-flush as `None` so a
                    // crafted stream can't grow `buf` without bound.
                    forced = true;
                    (self.acc.len(), true)
                }
                NextCore::None => {
                    // No next core sync yet: trailing extension PES packets may
                    // still arrive, so WAIT rather than emit a lossy core-only
                    // frame, unless the buffer has grown unreasonably large.
                    if self.acc.len() <= MAX_AU_BYTES {
                        break;
                    }
                    forced = true;
                    (self.acc.len(), true)
                }
            };

            // When the extension boundary is GARBAGE (`ext_clean == false`), emit
            // the clean core ALONE (lossy) and drain past it to the next core; an
            // unsizeable-but-recognized extension is kept in full (lossless).
            let emit_end = if ext_clean { au_end } else { core_size };
            let au: Vec<u8> = self.acc.as_slice()[..emit_end].to_vec();
            // The AU's own core PES PTS, stamped monotonically: honored when it
            // advances past the running clock (UHD, one AU per PES), but never
            // allowed to collide with the previous AU (DVD, several cores per PES).
            let dur_ns = dts_core_duration_ns(&au) as i64;
            // Advance the PTS clock BEFORE the decodability gate, so a dropped AU
            // still advances the timeline like an emitted one (gap, not shift).
            // `emit_or_drop` decides whether to actually push it.
            let au_pts = self.stamp_pts(self.front_pts(), dur_ns);
            // Read BEFORE draining: after the drain the front is the NEXT
            // unit's packet, not this one's.
            let au_src = self.front_source();
            self.emit_or_drop(au, au_pts, dur_ns, au_src, &mut frames);
            self.drain_front(au_end);
            // After draining, the marker covering the new front (if any) carries
            // the next AU's PTS; `pending_pts` is only the fallback when no
            // marker survives. Track it so the fallback stays sensible.
            self.pending_pts = self.front_pts();
            if forced {
                // Safety-valve flush: the next AU's real core PES hasn't arrived,
                // so invalidate the PTS rather than inherit this non-core PES's.
                self.pending_pts = PTS_UNSET;
            }
        }

        // An empty buffer holds no bytes for a mark to attribute, so drop the
        // marks with them. `drain` deliberately keeps the mark covering the new
        // front — correct while bytes remain, stale once none do.
        if self.acc.is_empty() {
            self.acc.clear();
        }

        frames
    }

    fn flush(&mut self) -> Vec<Frame> {
        let out = self.flush_tail();
        // Aggregate drop report at end-of-stream (warn-level, always visible).
        self.tally.log_summary();
        out
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

impl DtsParser {
    // Emits the final buffered AU (core + extensions, gated through the
    // decodability check) at end of stream. See docs/dts.md — flush_tail.
    fn flush_tail(&mut self) -> Vec<Frame> {
        if find_sync(self.acc.as_slice(), &DTS_CORE_SYNC) != Some(0)
            || self.acc.len() < CORE_HEADER_MIN_BYTES
        {
            self.acc.clear();
            return Vec::new();
        }
        let core_size = dts_core_frame_size(self.acc.as_slice());
        // `dts_core_frame_size` returns a 14-bit `fsize + 1` (never 0), so the
        // old `== 0` check was dead; reject a sub-minimum core like `parse()`.
        if core_size < MIN_CORE_FRAME_BYTES || self.acc.len() < core_size {
            self.acc.clear();
            return Vec::new();
        }
        // The final AU's PTS is the PES covering the buffer front (its core's
        // PES). Fall back to pending_pts, clamping the sentinel to 0.
        let au = self.acc.as_slice().to_vec();
        let dur_ns = dts_core_duration_ns(&au) as i64;
        let pts_ns = self.stamp_pts(self.front_pts(), dur_ns);
        let src = self.front_source();
        self.acc.clear();
        let mut out = Vec::new();
        self.emit_or_drop(au, pts_ns, dur_ns, src, &mut out);
        out
    }
}

fn find_sync(data: &[u8], pattern: &[u8; 4]) -> Option<usize> {
    if data.len() < 4 {
        return None;
    }
    (0..=data.len() - 4).find(|&i| data[i..i + 4] == *pattern)
}

/// Result of scanning for the next valid core sync that closes an access unit.
enum NextCore {
    /// A valid next core sync was found; the access unit ends at this offset.
    /// `ext_clean` is `false` only when the byte at the extension boundary was
    /// GARBAGE — neither a core sync nor a DTS-HD extension sync — meaning the
    /// extension region is corrupt (damaged source encoding). The caller then
    /// emits the clean DTS core alone and drops the garbage, instead of shipping
    /// a corrupt AU that makes the decoder cascade DSYNC / "Read past end of XLL".
    /// It stays `true` when the region is a real (if unsizeable) extension sync —
    /// that path is load-bearing for valid streams and must NOT be dropped.
    Found { end: usize, ext_clean: bool },
    /// A candidate core sync was found but its header isn't fully buffered yet,
    /// so its validity can't be decided — wait for more data.
    NeedMore,
    /// No (further) core sync found in the buffer.
    None,
}

// Byte length shared by both 32-bit DTS syncwords (core and EXSS). See
// docs/dts.md — SYNCWORD_BYTES / next_core_boundary for how candidate core
// syncs found in extension payload are validated by decoded size.
const SYNCWORD_BYTES: usize = DTS_CORE_SYNC.len();

// EXSS header field bit widths (ETSI TS 102 114). `bHeaderSizeType` selects
// short form (`nuExtSSHeaderSize` 8b, `nuExtSSFsize` 16b) or long (12/20b).
// See docs/dts.md.
const EXSS_USER_DEFINED_BITS: u32 = 8;
const EXSS_INDEX_BITS: u32 = 2;
const EXSS_HEADER_SIZE_TYPE_BITS: u32 = 1;
const EXSS_HDRSIZE_BITS_SHORT: u32 = 8;
const EXSS_FSIZE_BITS_SHORT: u32 = 16;
const EXSS_HDRSIZE_BITS_LONG: u32 = 12;
const EXSS_FSIZE_BITS_LONG: u32 = 20;
/// `bHeaderSizeType == 1` selects the long-form field widths.
const EXSS_HEADER_SIZE_TYPE_LONG: u32 = 1;
/// Bytes that must be buffered to read the EXSS size fields in the worst case
/// (long form): the 4-byte sync plus the bits up through `nuExtSSFsize`.
const EXSS_HEADER_MIN_BYTES: usize = SYNCWORD_BYTES
    + (EXSS_USER_DEFINED_BITS
        + EXSS_INDEX_BITS
        + EXSS_HEADER_SIZE_TYPE_BITS
        + EXSS_HDRSIZE_BITS_LONG
        + EXSS_FSIZE_BITS_LONG)
        .div_ceil(u8::BITS) as usize;

// EXSS total byte size (including the sync), read precisely from its
// header; `buf` must begin with DTS_HD_EXT_SYNC. Lets the AU framer skip
// the extension exactly rather than scanning its payload. See docs/dts.md.
fn exss_frame_size(buf: &[u8]) -> Option<usize> {
    if buf.len() < EXSS_HEADER_MIN_BYTES {
        return None;
    }
    let mut r = BitReader::new(&buf[SYNCWORD_BYTES..]);
    let _user = r.read_bits(EXSS_USER_DEFINED_BITS)?; // nUserDefinedBits
    let _idx = r.read_bits(EXSS_INDEX_BITS)?; // nExtSSIndex
    let large = r.read_bits(EXSS_HEADER_SIZE_TYPE_BITS)? == EXSS_HEADER_SIZE_TYPE_LONG;
    let (hbits, fbits) = if large {
        (EXSS_HDRSIZE_BITS_LONG, EXSS_FSIZE_BITS_LONG)
    } else {
        (EXSS_HDRSIZE_BITS_SHORT, EXSS_FSIZE_BITS_SHORT)
    };
    let _hdr = r.read_bits(hbits)?; // nuExtSSHeaderSize (not needed for framing)
    let fsize_minus_one = r.read_bits(fbits)?; // nuExtSSFsize = total bytes - 1
    Some(fsize_minus_one as usize + 1)
}

// Offset where the current AU ends (start of the next core frame): trailing
// extensions are skipped PRECISELY by declared size, so a false core sync
// in XLL payload can't be mistaken for the boundary. See docs/dts.md.
fn next_core_boundary(buf: &[u8], core_size: usize) -> NextCore {
    let mut pos = core_size;
    loop {
        if buf.len() < pos + SYNCWORD_BYTES {
            return NextCore::NeedMore; // need a syncword to identify the next chunk
        }
        if buf[pos..].starts_with(&DTS_HD_EXT_SYNC) {
            match exss_frame_size(&buf[pos..]) {
                Some(sz) if sz >= SYNCWORD_BYTES => {
                    if buf.len() < pos + sz {
                        return NextCore::NeedMore; // extension not fully buffered
                    }
                    pos += sz; // skip the whole extension substream precisely
                }
                // A real extension sync we couldn't size (truncated/unsupported):
                // heuristic fallback, but the region IS recognized, so keep it.
                _ => return scan_for_next_core(buf, pos, true),
            }
        } else if buf[pos..].starts_with(&DTS_CORE_SYNC) {
            // The bytes right after the precisely-skipped extensions are the next
            // core frame — the AU boundary.
            if buf.len() - pos < CORE_HEADER_MIN_BYTES {
                return NextCore::NeedMore;
            }
            let sz = dts_core_frame_size(&buf[pos..]);
            if (MIN_CORE_FRAME_BYTES..=MAX_AU_BYTES).contains(&sz) {
                return NextCore::Found {
                    end: pos,
                    ext_clean: true,
                };
            }
            return scan_for_next_core(buf, pos, true); // implausible core — recognized sync, keep
        } else {
            // GARBAGE at the extension boundary — neither a core nor extension
            // sync, so the extension region is corrupt: mark ext_clean = false so
            // the caller emits the clean core alone and drops the garbage.
            return scan_for_next_core(buf, pos, false);
        }
    }
}

// Heuristic fallback (pre-fix behaviour): scan for the next core syncword
// whose decoded size is plausible, used only when precise extension
// skipping can't proceed. See docs/dts.md.
fn scan_for_next_core(buf: &[u8], from: usize, ext_clean: bool) -> NextCore {
    let mut from = from;
    while let Some(rel) = find_sync(&buf[from..], &DTS_CORE_SYNC) {
        let pos = from + rel;
        if buf.len() - pos < CORE_HEADER_MIN_BYTES {
            return NextCore::NeedMore;
        }
        let sz = dts_core_frame_size(&buf[pos..]);
        if (MIN_CORE_FRAME_BYTES..=MAX_AU_BYTES).contains(&sz) {
            return NextCore::Found {
                end: pos,
                ext_clean,
            };
        }
        from = pos + SYNCWORD_BYTES;
    }
    NextCore::None
}

// `fsize` (14 bits, header bits 46-59) is length-minus-one on the wire;
// returns `fsize + 1`, the core length in bytes. `0` if `data` is short —
// every caller rejects that via the MIN floor. See docs/dts.md.
fn dts_core_frame_size(data: &[u8]) -> usize {
    if data.len() < CORE_HEADER_MIN_BYTES {
        return 0;
    }
    // fsize field: 14 bits starting at bit 46
    // byte 5 bits 1-0, byte 6 all 8, byte 7 bits 7-4
    let fsize =
        ((data[5] as usize & 0x03) << 12) | ((data[6] as usize) << 4) | ((data[7] as usize) >> 4);
    fsize + 1
}

/// DTS core `SFREQ` → sample rate (Hz). 4-bit index; reserved/invalid entries
/// fall back to 48 kHz (the DVD/UHD norm) so a bogus value never yields a zero
/// rate (division) or a wildly wrong frame duration.
const DTS_CORE_SAMPLE_RATES: [u32; 16] = [
    48_000, // 0: invalid → fallback
    8_000, 16_000, 32_000, 48_000, // 4: invalid → fallback
    48_000, // 5: invalid → fallback
    11_025, 22_050, 44_100, 48_000, // 9: invalid → fallback
    48_000, // 10: invalid → fallback
    12_000, 24_000, 48_000, 96_000, 192_000,
];

// Samples per DTS core frame: `(NBLKS + 1) * 32`. NBLKS (7 bits, ETSI TS
// 102 114) = byte4 bit0 + byte5 bits7-2, after FTYPE/SHORT/CPF. See
// docs/dts.md.
fn dts_core_samples(data: &[u8]) -> u32 {
    if data.len() < CORE_HEADER_MIN_BYTES {
        return 512; // typical; only reached on a truncated header
    }
    let nblks = ((data[4] as u32 & 0x01) << 6) | (data[5] as u32 >> 2);
    (nblks + 1) * 32
}

/// DTS core sample rate (Hz) from `SFREQ` (4 bits: byte8 bits5-2), with a 48 kHz
/// fallback for reserved indices.
fn dts_core_sample_rate(data: &[u8]) -> u32 {
    if data.len() < CORE_HEADER_MIN_BYTES {
        return 48_000;
    }
    let sfreq = (data[8] as usize >> 2) & 0x0F;
    DTS_CORE_SAMPLE_RATES[sfreq]
}

/// Duration of one DTS core access unit in nanoseconds: `samples / rate`,
/// rounded to nearest. This is what lets consecutive core frames packed in a
/// single DVD PES advance monotonically instead of colliding on one PES PTS.
fn dts_core_duration_ns(data: &[u8]) -> u64 {
    let samples = dts_core_samples(data) as u64;
    let rate = dts_core_sample_rate(data) as u64;
    (samples * 1_000_000_000 + rate / 2) / rate
}

// DTS core-header validity constants (ETSI TS 102 114): a NORMAL frame's
// `deficit_samples` must equal this; `npcmblocks` a multiple of
// DTS_SUBBAND_SAMPLES. See docs/dts.md.
const DTS_PCMBLOCK_SAMPLES: u32 = 32;
const DTS_SUBBAND_SAMPLES: u32 = 8;
// Number of LEGAL 6-bit AMODE codes (ETSI TS 102 114 §5.3.1): 0-15 are
// defined channel arrangements (10-15 are 6/7/8-ch), only 16-63 are
// reserved. See docs/dts.md.
const DTS_AMODE_COUNT: u32 = 16;
const DTS_LFE_FLAG_INVALID: u32 = 3;

// Sample rate (Hz) per core SFREQ code (ETSI TS 102 114 Table 6-4); `0`
// marks a reserved code (fails validation). Reserved: {0, 4, 5, 9, 10}.
// See docs/dts.md.
const DTS_CORE_SR_VALID: [u32; 16] = [
    0, 8_000, 16_000, 32_000, 0, 0, 11_025, 22_050, 44_100, 0, 0, 12_000, 24_000, 48_000, 96_000,
    192_000,
];

/// Bits per sample per core `PCMR` code (ETSI TS 102 114); a `0` entry marks a
/// reserved `PCMR` code that fails header validation as an invalid PCM
/// resolution; reserved codes are {4, 7}.
const DTS_CORE_PCMR_BITS: [u8; 8] = [16, 16, 20, 20, 0, 24, 24, 0];

/// Why an access unit was judged undecodable. Each core-header variant is a
/// condition under which the DTS core-frame header (ETSI TS 102 114) is invalid
/// and a decoder would reject the frame; `TrackPoisoned` is our whole-track drop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DropReason {
    DeficitSamples,
    PcmBlocks,
    FrameSize,
    Amode,
    SampleRate,
    LfeFlag,
    PcmRes,
    TrackPoisoned,
}

impl DropReason {
    /// Short static label for the drop log (the shared tally logs `&str`).
    fn as_str(&self) -> &'static str {
        match self {
            DropReason::DeficitSamples => "deficit-samples",
            DropReason::PcmBlocks => "pcm-blocks",
            DropReason::FrameSize => "frame-size",
            DropReason::Amode => "audio-mode",
            DropReason::SampleRate => "sample-rate",
            DropReason::LfeFlag => "lfe-flag",
            DropReason::PcmRes => "pcm-resolution",
            DropReason::TrackPoisoned => "track-poisoned",
        }
    }
}

// Decodability gate: ETSI TS 102 114 core-header validity checks. `Some`
// means genuinely undecodable; `None` also covers a truncated header
// (never false-drop our own buffer underrun). See docs/dts.md.
fn core_header_drop_reason(au: &[u8]) -> Option<DropReason> {
    let mut r = BitReader::new(au.get(SYNCWORD_BYTES..)?);

    // FTYPE: 1 = NORMAL frame, 0 = TERMINATION frame. Per ETSI TS 102 114 the
    // deficit-sample field must equal 32 ONLY for a normal frame; gating a
    // termination frame on it would silence the last frame of every stream.
    let normal_frame = r.read_bit()? == 1;
    let deficit_samples = r.read_bits(5)? + 1;
    if normal_frame && deficit_samples != DTS_PCMBLOCK_SAMPLES {
        return Some(DropReason::DeficitSamples);
    }
    let crc_present = r.read_bit()? == 1;
    let npcmblocks = r.read_bits(7)? + 1;
    if npcmblocks & (DTS_SUBBAND_SAMPLES - 1) != 0 {
        return Some(DropReason::PcmBlocks);
    }
    let frame_size = r.read_bits(14)? + 1;
    if frame_size < MIN_CORE_FRAME_BYTES as u32 {
        return Some(DropReason::FrameSize);
    }
    let audio_mode = r.read_bits(6)?;
    if audio_mode >= DTS_AMODE_COUNT {
        return Some(DropReason::Amode);
    }
    let sr_code = r.read_bits(4)? as usize;
    if DTS_CORE_SR_VALID[sr_code] == 0 {
        return Some(DropReason::SampleRate);
    }
    let _br_code = r.read_bits(5)?;
    // Reserved bit (ETSI TS 102 114): both reference decoders SKIP it rather
    // than reject on it, so read past without gating — a frame setting it is
    // still fully decodable.
    let _reserved = r.read_bit()?;
    // drc, ts, aux, hdcd (1 each) → ext_audio_type (3) → ext_present, aspf (1 each).
    r.skip_bits(4)?;
    r.skip_bits(3)?;
    r.skip_bits(2)?;
    let lfe_present = r.read_bits(2)?;
    if lfe_present == DTS_LFE_FLAG_INVALID {
        return Some(DropReason::LfeFlag);
    }
    let _predictor_history = r.read_bit()?;
    if crc_present {
        // Skip past the 16-bit header CRC here — it is not verified.
        r.skip_bits(16)?;
    }
    let _filter_perfect = r.read_bit()?;
    let _encoder_rev = r.read_bits(4)?;
    let _copy_hist = r.read_bits(2)?;
    let pcmr_code = r.read_bits(3)? as usize;
    if DTS_CORE_PCMR_BITS[pcmr_code] == 0 {
        return Some(DropReason::PcmRes);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::codec::pts_to_ns;
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

    fn make_dts_core(size: usize) -> Vec<u8> {
        let fsize = size - 1;
        let mut data = vec![0u8; size];
        data[0..4].copy_from_slice(&DTS_CORE_SYNC);
        // byte4: FTYPE(1) SHORT(5) CPF(0) NBLKS-high(0). FTYPE=1 (NORMAL frame);
        // SHORT=31 makes deficit_samples=32=DTS_PCMBLOCK_SAMPLES, required by the
        // decodability gate for a normal frame. (0x80 | (31 << 2) = 0xFC.)
        data[4] = 0x80 | (31u8 << 2);
        // NBLKS = 15 → (15+1)*32 = 512 samples/frame (the DVD/UHD DTS-core norm).
        // NBLKS is byte4 bit0 + byte5 bits7-2; here byte4 bit0 = 0, byte5 = 15<<2.
        data[5] = (15u8 << 2) | ((fsize >> 12) & 0x03) as u8;
        data[6] = ((fsize >> 4) & 0xFF) as u8;
        data[7] = (data[7] & 0x0F) | (((fsize & 0x0F) << 4) as u8);
        // SFREQ = 13 → 48 kHz (byte8 bits5-2). Only when the header byte exists.
        if size > 8 {
            data[8] = 13u8 << 2;
        }
        data
    }

    // 512 samples @ 48 kHz, rounded to nearest ns — the duration make_dts_core
    // frames advance by. (512 * 1e9 + 24000) / 48000 = 10_666_667 ns.
    const DTS_CORE_DUR_NS: i64 = (512 * 1_000_000_000 + 48_000 / 2) / 48_000;

    // --- stamp_pts: the "same PES, no fresh front timestamp" branch ---

    // Third arm: front == last_front_pts, no cursor yet — must stamp the
    // repeated front, not collapse to 0. See docs/dts.md.
    #[test]
    fn stamp_pts_reuses_front_when_no_running_cursor_yet() {
        let mut parser = DtsParser::new();
        parser.last_front_pts = 500;
        parser.next_pts_ns = PTS_UNSET;
        assert_eq!(
            parser.stamp_pts(500, DTS_CORE_DUR_NS),
            500,
            "front == last_front_pts and no cursor yet: stamp with front itself"
        );
    }

    // --- drain_front: collapsing duplicate offset-0 PTS markers must not leak ---

    // `drain_front` must collapse repeated offset-0 PTS markers each time, or
    // `pts_marks` grows unbounded across a multi-hour track. See docs/dts.md.
    #[test]
    fn drain_front_collapses_offset_zero_markers_instead_of_leaking() {
        let mut parser = DtsParser::new();
        for i in 0..200i64 {
            parser.acc.append_unattributed(&[0u8; 5]);
            parser
                .acc
                .mark_here(crate::mux::codec::pesbuf::PesFacts::default().with_pts_ns(i));
            parser
                .acc
                .mark_here(crate::mux::codec::pesbuf::PesFacts::default().with_pts_ns(i));
            parser.drain_front(5);
        }
        assert!(
            parser.acc.mark_count() <= 2,
            "pts_marks must stay bounded across repeated drains, got {}",
            parser.acc.mark_count()
        );
    }

    // A real EXSS substream (short header form) with an optional false core
    // sync embedded in its payload, to prove precise sizing bounds it. See
    // docs/dts.md.
    fn make_exss(total: usize, false_core_at: Option<usize>) -> Vec<u8> {
        let mut d = vec![0u8; total];
        d[0..4].copy_from_slice(&DTS_HD_EXT_SYNC);
        // Short form: all header fields 0 except nuExtSSFsize = total - 1, laid
        // out at bit 19 after the sync (byte 6 low 5 bits, byte 7, byte 8 top 3).
        let fsize = (total - 1) as u32;
        d[6] = ((fsize >> 11) & 0x1F) as u8;
        d[7] = ((fsize >> 3) & 0xFF) as u8;
        d[8] = ((fsize & 0x07) << 5) as u8;
        if let Some(at) = false_core_at {
            d[at..at + 4].copy_from_slice(&DTS_CORE_SYNC);
            let fcs = 512u32 - 1; // decode to a plausible core size — fools the heuristic
            d[at + 5] = (d[at + 5] & 0xFC) | ((fcs >> 12) & 0x03) as u8;
            d[at + 6] = ((fcs >> 4) & 0xFF) as u8;
            d[at + 7] = (d[at + 7] & 0x0F) | (((fcs & 0x0F) << 4) as u8);
        }
        d
    }

    #[test]
    fn plausible_false_core_sync_inside_real_exss_does_not_split_au() {
        // EXSS size parse round-trips.
        assert_eq!(exss_frame_size(&make_exss(600, None)), Some(600));

        // AU = core(512) + a REAL EXSS substream whose XLL payload embeds a false
        // DTS core syncword (plausible size 512). Precise EXSS sizing must span
        // the whole extension to the REAL next core, or the extension gets split.
        let core = make_dts_core(512);
        let exss = make_exss(600, Some(40));
        let next = make_dts_core(512);
        let mut buf = core.clone();
        buf.extend_from_slice(&exss);
        buf.extend_from_slice(&next);

        assert!(
            matches!(
                next_core_boundary(&buf, core.len()),
                NextCore::Found { end, .. } if end == core.len() + exss.len()
            ),
            "AU must end at the REAL next core (after the full EXSS), not the false sync inside it"
        );
    }

    #[test]
    fn garbage_extension_emits_core_only_but_valid_ext_is_kept() {
        // Damaged source: valid core, then GARBAGE (no core/extension sync) where
        // the extension belongs, then the next core. Must mark ext_clean=false and
        // emit the clean 512-byte CORE alone, draining past the garbage.
        let core = make_dts_core(512);
        let garbage = vec![0xE4, 0x3F, 0xE3, 0x90, 0xCC, 0x6C]; // real head bytes from a damaged stream
        let mut garbage = garbage;
        garbage.extend(std::iter::repeat_n(0xAB, 300));
        let next = make_dts_core(512);
        let mut buf = core.clone();
        buf.extend_from_slice(&garbage);
        buf.extend_from_slice(&next);
        assert!(
            matches!(
                next_core_boundary(&buf, core.len()),
                NextCore::Found { end, ext_clean: false } if end == core.len() + garbage.len()
            ),
            "garbage boundary must be flagged unclean"
        );

        let mut parser = DtsParser::new();
        let mut frames = parser.parse(&make_pes(buf, Some(90000)));
        frames.extend(parser.flush());
        assert!(!frames.is_empty());
        for f in &frames {
            assert_eq!(f.data.len(), 512, "garbage-extension AU emits core only");
            assert_eq!(&f.data[0..4], &DTS_CORE_SYNC);
        }

        // Contrast: a REAL extension sync (even if unsizeable) must be KEPT in
        // full — ext_clean stays true, never downgraded to core-only.
        let mut buf2 = make_dts_core(512);
        buf2.extend_from_slice(&make_dts_ext(256));
        buf2.extend_from_slice(&make_dts_core(512));
        assert!(
            matches!(
                next_core_boundary(&buf2, 512),
                NextCore::Found {
                    ext_clean: true,
                    ..
                }
            ),
            "a recognized extension sync is preserved, not dropped"
        );
    }

    #[test]
    fn parse_empty_pes() {
        let mut parser = DtsParser::new();
        let pes = make_pes(Vec::new(), Some(0));
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn parse_single_frame() {
        // A single core frame with no following core sync is the LAST access
        // unit — held during streaming (can't know an extension won't follow),
        // then drained on flush() at EOF.
        let mut parser = DtsParser::new();
        let frame = make_dts_core(512);
        let pes = make_pes(frame, Some(90000));
        assert!(parser.parse(&pes).is_empty());
        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].data.len(), 512);
    }

    #[test]
    fn parse_frame_spanning_two_pes() {
        let mut parser = DtsParser::new();
        let frame = make_dts_core(512);
        let mid = 256;

        let pes1 = make_pes(frame[..mid].to_vec(), Some(90000));
        assert!(parser.parse(&pes1).is_empty());

        let pes2 = make_pes(frame[mid..].to_vec(), Some(93000));
        assert!(parser.parse(&pes2).is_empty());
        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].data.len(), 512);
    }

    #[test]
    fn discontinuity_drops_truncated_partial() {
        // B1: a partial core is buffered, then a discontinuity-marked PES carries
        // a fresh one. The partial must be DROPPED, not spliced; the post-gap
        // core is the only AU emitted, with the post-gap PTS.
        let mut parser = DtsParser::new();

        // PES 1: first half of a 512-byte core (no boundary marker).
        let core = make_dts_core(512);
        let pes1 = make_pes(core[..256].to_vec(), Some(90000));
        assert!(parser.parse(&pes1).is_empty(), "partial core held");

        // Concealed gap: a fresh whole core, marked discontinuity.
        let fresh = make_dts_core(512);
        let pes2 = PesPacket {
            source: None,
            pid: 0x1100,
            pts: Some(99000),
            dts: None,
            data: fresh.clone(),
            discontinuity: true,
        };
        assert!(
            parser.parse(&pes2).is_empty(),
            "post-gap core held awaiting next core — NO corrupt partial emitted"
        );

        let tail = parser.flush();
        assert_eq!(tail.len(), 1, "exactly one clean AU across the gap");
        assert_eq!(
            tail[0].data, fresh,
            "AU is the fresh post-gap core, not a splice"
        );
        assert_eq!(
            tail[0].pts_ns,
            pts_to_ns(99000),
            "post-gap AU re-bases to the post-gap PTS, not the stranded pre-gap one"
        );
    }

    #[test]
    fn two_cores_back_to_back_advance_within_one_pes() {
        // Both cores arrive in ONE PES (DVD layout). AU1 keeps the PES PTS; AU2
        // must ADVANCE by one frame duration to stay monotonic — reusing the same
        // PES PTS for both was the "non monotonically increasing dts" bug (1.2.1).
        let mut parser = DtsParser::new();
        let mut stream = make_dts_core(512);
        stream.extend_from_slice(&make_dts_core(640));
        let f = parser.parse(&make_pes(stream, Some(90000)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].data.len(), 512);
        assert_eq!(f[0].pts_ns, pts_to_ns(90000), "AU1 keeps its PES PTS");
        assert_eq!(
            f[0].duration_ns,
            Some(DTS_CORE_DUR_NS as u64),
            "AU1 carries a real frame duration (was None)"
        );
        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].data.len(), 640);
        assert_eq!(
            tail[0].pts_ns,
            pts_to_ns(90000) + DTS_CORE_DUR_NS,
            "AU2 in the same PES advances one frame duration (monotonic)"
        );
    }

    #[test]
    fn two_aus_flushed_in_one_call_keep_their_own_pts() {
        // core1 in PES A (pts 100), then core2 + core3 in a LATER PES B (pts 200).
        // Processing PES B closes both AU1 (core1) and AU2 (core2, in PES B, so
        // 200) in one parse() call. Guard: AU1 must NOT be overwritten to 200.
        let mut parser = DtsParser::new();

        // PES A: just core1 (held — no following core yet).
        let f0 = parser.parse(&make_pes(make_dts_core(512), Some(90000)));
        assert!(f0.is_empty(), "core1 held awaiting next core");

        // PES B (realistically LATER — far past one frame): core2 + core3.
        // Closes AU1 (core1) and AU2 (core2).
        let mut pes_b = make_dts_core(600);
        pes_b.extend_from_slice(&make_dts_core(640));
        let f = parser.parse(&make_pes(pes_b, Some(190000)));
        assert_eq!(f.len(), 2, "AU1 and AU2 both close in this call");
        assert_eq!(f[0].data.len(), 512, "AU1 = core1");
        assert_eq!(
            f[0].pts_ns,
            pts_to_ns(90000),
            "AU1 keeps PES A's PTS, not the later PES B PTS"
        );
        assert_eq!(f[1].data.len(), 600, "AU2 = core2");
        assert_eq!(
            f[1].pts_ns,
            pts_to_ns(190000),
            "AU2's core is in PES B → attributes to PES B PTS (ahead of the clock, so it wins)"
        );

        // AU3 (core3) drains on flush — 2nd core in PES B, so it advances one
        // frame duration from AU2 to stay monotonic.
        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].pts_ns, pts_to_ns(190000) + DTS_CORE_DUR_NS);
    }

    #[test]
    fn second_au_with_core_in_earlier_pes_keeps_that_pts() {
        // core1+core2 arrive in PES A (pts 100); core3 in PES B (pts 200). When PES
        // B closes AU2 (core2, whose core was in PES A), AU2 must keep PES A's
        // 100 — the bug was AU2 inheriting the closing PES's 200.
        let mut parser = DtsParser::new();

        // PES A: core1 + core2. AU1 (core1) emits immediately (core2 boundary);
        // AU2 (core2) held awaiting a third core.
        let mut pes_a = make_dts_core(512);
        pes_a.extend_from_slice(&make_dts_core(600));
        let f = parser.parse(&make_pes(pes_a, Some(90000)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, pts_to_ns(90000), "AU1 PES A PTS");

        // PES B: core3 closes AU2 (core2). AU2's core was in PES A, so it's PES
        // A's 2nd frame — advances one duration from AU1, and does NOT inherit
        // the closing PES B PTS.
        let f2 = parser.parse(&make_pes(make_dts_core(640), Some(190000)));
        assert_eq!(f2.len(), 1);
        assert_eq!(f2[0].data.len(), 600, "AU2 = core2");
        assert_eq!(
            f2[0].pts_ns,
            pts_to_ns(90000) + DTS_CORE_DUR_NS,
            "AU2 = 2nd frame of PES A → PES A base + one frame, not the closing PES B PTS"
        );

        // AU3 = core3, whose own core is in PES B → jumps to PES B's PTS.
        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(
            tail[0].pts_ns,
            pts_to_ns(190000),
            "AU3 = core3 in PES B → PES B PTS"
        );
    }

    #[test]
    fn dvd_many_cores_one_pes_are_strictly_monotonic() {
        // Punisher-DVD repro: a PES with SEVERAL DTS cores must emit STRICTLY
        // increasing PTSs; the old code stamped every AU with the one PES PTS,
        // rejected by muxers as "non monotonically increasing dts: X >= X".
        let mut parser = DtsParser::new();
        let mut stream = Vec::new();
        for _ in 0..6 {
            stream.extend_from_slice(&make_dts_core(512));
        }
        let mut frames = parser.parse(&make_pes(stream, Some(90000)));
        frames.extend(parser.flush());
        assert_eq!(frames.len(), 6, "all six cores emitted");
        for w in frames.windows(2) {
            assert!(
                w[1].pts_ns > w[0].pts_ns,
                "consecutive DTS AUs must STRICTLY increase: {} !> {}",
                w[1].pts_ns,
                w[0].pts_ns
            );
        }
        // Each advances by exactly one frame duration, and carries that duration.
        assert_eq!(frames[0].pts_ns, pts_to_ns(90000));
        assert_eq!(frames[1].pts_ns, pts_to_ns(90000) + DTS_CORE_DUR_NS);
        assert_eq!(frames[5].pts_ns, pts_to_ns(90000) + 5 * DTS_CORE_DUR_NS);
        for f in &frames {
            assert_eq!(f.duration_ns, Some(DTS_CORE_DUR_NS as u64));
        }
    }

    #[test]
    fn dts_core_duration_512_samples_48khz() {
        // NBLKS=15 → (15+1)*32 = 512 samples; SFREQ=13 → 48 kHz.
        let core = make_dts_core(512);
        assert_eq!(dts_core_samples(&core), 512);
        assert_eq!(dts_core_sample_rate(&core), 48_000);
        assert_eq!(dts_core_duration_ns(&core), DTS_CORE_DUR_NS as u64);
    }

    #[test]
    fn dts_core_sfreq_reserved_falls_back_to_48k() {
        // A bogus SFREQ index must never yield a zero rate (division) — fall
        // back to 48 kHz.
        let mut core = make_dts_core(512);
        core[8] = 0; // SFREQ = 0 (reserved)
        assert_eq!(dts_core_sample_rate(&core), 48_000);
    }

    #[test]
    fn dts_core_sfreq_table_matches_the_dca_spec() {
        // Lock the SFREQ → sample-rate table to ETSI TS 102 114 Table 6-4. The
        // high-rate triad (48/96/192k at indices 13/14/15) must not shift; a wrong
        // entry computes an N× frame duration, reintroducing PTS drift.
        let mut core = make_dts_core(512);
        let set_sfreq = |c: &mut [u8], idx: u8| c[8] = (c[8] & !0x3C) | ((idx & 0x0F) << 2);
        for (idx, want) in [
            (1u8, 8_000u32),
            (2, 16_000),
            (3, 32_000),
            (6, 11_025),
            (7, 22_050),
            (8, 44_100),
            (11, 12_000),
            (12, 24_000),
            (13, 48_000),
            (14, 96_000),
            (15, 192_000),
        ] {
            set_sfreq(&mut core, idx);
            assert_eq!(
                dts_core_sample_rate(&core),
                want,
                "SFREQ {idx} must be {want} Hz"
            );
        }
    }

    #[test]
    fn new_pes_rebases_to_its_own_pts_no_drift() {
        // Drift-bug regression: a global running clock overshot a track by minutes.
        // A NEW PES BEHIND the running clock must re-base to its OWN timestamp;
        // the mkv muxer nudges AUDIO monotonic at emit — clamping here caused it.
        let mut parser = DtsParser::new();
        // PES A: core1 + core2 (2 frames), pts 90000.
        let mut pes_a = make_dts_core(512);
        pes_a.extend_from_slice(&make_dts_core(600));
        let f = parser.parse(&make_pes(pes_a, Some(90000)));
        assert_eq!(f.len(), 1, "AU1 (core1) emits on the core2 boundary");
        assert_eq!(f[0].pts_ns, pts_to_ns(90000), "AU1 = PES A base");
        // PES B: core3, pts only 500 ticks after PES A — LESS than one frame
        // (960 ticks @ 48 kHz). AU2 (core2, still PES A) advances within PES A;
        // AU3 (core3, PES B) must RE-BASE to PES B's own PTS.
        let f2 = parser.parse(&make_pes(make_dts_core(640), Some(90500)));
        assert_eq!(f2.len(), 1, "AU2 (core2) closes on core3");
        assert_eq!(
            f2[0].pts_ns,
            pts_to_ns(90000) + DTS_CORE_DUR_NS,
            "AU2 = 2nd frame of PES A → advances one frame within PES A"
        );
        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(
            tail[0].pts_ns,
            pts_to_ns(90500),
            "AU3 = core3 in PES B → re-bases to PES B's PTS"
        );
        assert_ne!(
            tail[0].pts_ns,
            pts_to_ns(90000) + 2 * DTS_CORE_DUR_NS,
            "must NOT carry the accumulated running clock across a PES (drift)"
        );
    }

    // Minimal EXSS substream (sync + zero-padding); the parser delimits by
    // the next CORE sync, not this extension's own size header. See
    // docs/dts.md.
    fn make_dts_ext(size: usize) -> Vec<u8> {
        let mut e = vec![0u8; size];
        e[0..4].copy_from_slice(&DTS_HD_EXT_SYNC);
        e
    }

    #[test]
    fn keeps_dts_hd_extension_in_separate_pes_packets() {
        // Real Blu-ray layout: the DTS core arrives in one PES, then its DTS-HD MA
        // extensions arrive in SEPARATE following PES packets. Must stitch core +
        // all trailing extensions into one AU, not emit lossy core-only frames.
        let mut parser = DtsParser::new();

        // Frame 1: core (512) + two extension substreams (256 + 200).
        assert!(
            parser
                .parse(&make_pes(make_dts_core(512), Some(90000)))
                .is_empty(),
            "core alone: must wait for any following extension"
        );
        assert!(
            parser
                .parse(&make_pes(make_dts_ext(256), Some(91000)))
                .is_empty(),
            "first extension PES: still waiting for the unit to close"
        );
        assert!(
            parser
                .parse(&make_pes(make_dts_ext(200), Some(91500)))
                .is_empty(),
            "second extension PES: unit still not closed (no next core yet)"
        );

        // Frame 2's core PES arrives — that closes frame 1. The emitted unit
        // must be core + BOTH extensions (lossless preserved), and keep the
        // core's PTS, not the extension PES timestamps.
        let f = parser.parse(&make_pes(make_dts_core(512), Some(93000)));
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].data.len(),
            512 + 256 + 200,
            "frame must include core + every extension substream"
        );

        // EOF drains frame 2.
        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].data.len(), 512);
    }

    #[test]
    fn extension_split_across_pes_is_preserved() {
        // An extension substream straddling a PES boundary must still be fully
        // attached to its core.
        let mut parser = DtsParser::new();
        let ext = make_dts_ext(300);
        assert!(
            parser
                .parse(&make_pes(make_dts_core(512), Some(90000)))
                .is_empty()
        );
        assert!(
            parser
                .parse(&make_pes(ext[..150].to_vec(), Some(91000)))
                .is_empty()
        );
        assert!(
            parser
                .parse(&make_pes(ext[150..].to_vec(), Some(91000)))
                .is_empty()
        );
        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].data.len(), 512 + 300);
    }

    // A core sync whose `fsize` decodes tiny (< MIN_CORE_FRAME_BYTES): must be
    // drained and resynced past, not close an AU at a junk boundary. See
    // docs/dts.md.
    fn bogus_tiny_core_sync() -> Vec<u8> {
        // Core sync + zero header bytes. fsize = 0 → core_size = 1 (< 10).
        let mut v = vec![0u8; 10];
        v[0..4].copy_from_slice(&DTS_CORE_SYNC);
        // bytes 5,6,7 left zero → fsize = 0 → dts_core_frame_size = 1.
        assert_eq!(dts_core_frame_size(&v), 1);
        v
    }

    #[test]
    fn bogus_tiny_core_sync_does_not_split_or_drop_real_au() {
        // A real core followed by an extension containing a false core sync whose
        // fsize decodes tiny. The bogus sync must NOT close the AU early or emit
        // a junk frame — skip it and preserve core + extension as one AU.
        let mut parser = DtsParser::new();

        // Frame 1: core(512) + an extension whose body embeds a bogus tiny
        // core sync midway through.
        let mut ext = make_dts_ext(256);
        // Embed the bogus core sync inside the extension body (offset 64).
        let bogus = bogus_tiny_core_sync();
        ext[64..64 + bogus.len()].copy_from_slice(&bogus);

        let mut frame1 = make_dts_core(512);
        frame1.extend_from_slice(&ext);

        // No next REAL core yet → frame 1 held.
        assert!(
            parser.parse(&make_pes(frame1, Some(90000))).is_empty(),
            "bogus tiny core sync must not close the AU; wait for a real core"
        );

        // Frame 2's real core arrives — closes frame 1.
        let f = parser.parse(&make_pes(make_dts_core(640), Some(93000)));
        assert_eq!(f.len(), 1, "exactly one real access unit emitted");
        assert_eq!(
            f[0].data.len(),
            512 + 256,
            "AU must be the full core + extension, not split at the bogus sync"
        );
        assert_eq!(f[0].pts_ns, pts_to_ns(90000), "AU keeps the core's PTS");

        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].data.len(), 640);
    }

    #[test]
    fn sub_spec_core_size_is_rejected_as_false_sync() {
        // A core sync whose decoded fsize+1 lands in [CORE_HEADER_MIN_BYTES,
        // MIN_CORE_FRAME_BYTES) — smaller than the 96-byte ETSI spec minimum — is
        // a false sync and must NOT close an AU. 64 sits in the raised reject window.
        let false_size = 64usize;
        assert!(
            (CORE_HEADER_MIN_BYTES..MIN_CORE_FRAME_BYTES).contains(&false_size),
            "test fixture must sit in the widened reject window"
        );
        let mut parser = DtsParser::new();

        // Frame 1: real core(512) + extension that embeds a sub-spec "core sync"
        // whose fsize decodes to 64 bytes.
        let mut ext = make_dts_ext(256);
        let bogus = make_dts_core(false_size); // valid-looking sync, size 64
        ext[64..64 + bogus.len()].copy_from_slice(&bogus);
        let mut frame1 = make_dts_core(512);
        frame1.extend_from_slice(&ext);

        assert!(
            parser.parse(&make_pes(frame1, Some(90000))).is_empty(),
            "sub-spec core size must not close the AU"
        );

        // Real next core closes frame 1 as core + full extension.
        let f = parser.parse(&make_pes(make_dts_core(640), Some(93000)));
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].data.len(),
            512 + 256,
            "AU must not be split at the sub-spec false sync"
        );
    }

    #[test]
    fn forced_emit_does_not_corrupt_next_au_pts() {
        // When the buffer exceeds MAX_AU_BYTES with no next core sync, the parser
        // force-emits for forward progress; the current PES is an extension PES
        // (later PTS). The forced path must NOT base the NEXT AU on that PTS.
        let mut parser = DtsParser::new();

        // Core PES at the real PTS, then a giant extension (no next core) that
        // pushes the buffer past MAX_AU_BYTES, forcing an emit.
        let core_pts = 90000i64;
        assert!(
            parser
                .parse(&make_pes(make_dts_core(512), Some(core_pts)))
                .is_empty()
        );
        let ext_pts = 120000i64; // later extension-PES timestamp
        let big_ext = make_dts_ext(MAX_AU_BYTES + 1024);
        let f = parser.parse(&make_pes(big_ext, Some(ext_pts)));
        assert_eq!(f.len(), 1, "oversized buffer force-emits one AU");
        assert_eq!(
            f[0].pts_ns,
            pts_to_ns(core_pts),
            "forced AU keeps the core PTS"
        );

        // The next REAL core PES arrives with its own PTS. Its AU must inherit
        // THIS core's PTS, not the prior extension PES timestamp.
        let next_core_pts = 150000i64;
        assert!(
            parser
                .parse(&make_pes(make_dts_core(512), Some(next_core_pts)))
                .is_empty()
        );
        let next_next_pts = 180000i64;
        let f2 = parser.parse(&make_pes(make_dts_core(512), Some(next_next_pts)));
        assert_eq!(f2.len(), 1);
        assert_eq!(
            f2[0].pts_ns,
            pts_to_ns(next_core_pts),
            "AU after a forced emit must use the next core's PTS, not the \
             stale extension PTS"
        );
    }

    #[test]
    fn needmore_past_cap_force_flushes_to_bound_buffer() {
        // A crafted stream whose extension declares a size larger than ever
        // buffered keeps `next_core_boundary` in sustained NeedMore; past
        // MAX_AU_BYTES the force-flush valve must fire, or `buf` grows forever.
        let mut parser = DtsParser::new();

        let core = make_dts_core(512);
        // Short-form EXSS header declaring the maximum 16-bit size (65536 bytes);
        // we buffer only a truncated prefix of it, so the extension is never
        // "fully buffered" and the candidate boundary stays NeedMore.
        let full_ext = make_exss(65536, None);
        assert_eq!(exss_frame_size(&full_ext), Some(65536));

        // Land the total buffer in (MAX_AU_BYTES, core_size + declared_ext_size):
        // 65600 > 65536 fires the cap; 65600 < 512 + 65536 = 66048 keeps NeedMore.
        let total = 65600usize;
        let mut data = core.clone();
        data.extend_from_slice(&full_ext[..total - core.len()]);
        assert!(data.len() > MAX_AU_BYTES, "buffer must exceed the AU cap");
        assert!(
            data.len() < core.len() + 65536,
            "extension must not be fully buffered (sustained NeedMore)"
        );
        assert!(
            matches!(next_core_boundary(&data, core.len()), NextCore::NeedMore),
            "the framing decision at this buffer size is NeedMore past the cap"
        );

        let frames = parser.parse(&make_pes(data, Some(90000)));
        assert_eq!(
            frames.len(),
            1,
            "NeedMore past the AU cap must force-emit, not stall and balloon the buffer"
        );
        assert!(
            parser.acc.is_empty(),
            "the forced flush drains the buffer instead of growing it unbounded"
        );
    }

    #[test]
    fn codec_private_none() {
        let parser = DtsParser::new();
        assert!(parser.codec_private().is_none());
    }

    // --- dts_core_frame_size: 14-bit fsize extraction (ETSI TS 102 114) ---

    #[test]
    fn core_frame_size_bit_layout() {
        // fsize is 14 bits at bits 46-59: byte5[1:0] (high2), byte6 (mid8),
        // byte7[7:4] (low4); returned value is fsize + 1 (on-wire length-1).
        // (1<<12)|(0xFF<<4)|0xF = 0x1FFF → size 8192.
        let mut d = vec![0u8; CORE_HEADER_MIN_BYTES];
        d[5] = 0x01;
        d[6] = 0xFF;
        d[7] = 0xF0;
        assert_eq!(dts_core_frame_size(&d), 0x1FFF + 1);
    }

    #[test]
    fn core_frame_size_ignores_unrelated_bits() {
        // Only byte5[1:0] feed fsize; the upper 6 bits of byte5 and the low 4 of
        // byte7 are unrelated. Set those to 1 and confirm they don't leak in.
        // byte5 = 0xFC (low2 = 0), byte6 = 0x01, byte7 = 0x0F (high4 = 0).
        let mut d = vec![0u8; CORE_HEADER_MIN_BYTES];
        d[5] = 0xFC; // low 2 bits zero
        d[6] = 0x01;
        d[7] = 0x0F; // high 4 bits zero
        // fsize = (0<<12) | (1<<4) | 0 = 16 → size 17.
        assert_eq!(dts_core_frame_size(&d), 17);
    }

    #[test]
    fn core_frame_size_short_input_zero() {
        // Below CORE_HEADER_MIN_BYTES → 0 (caller rejects via MIN floor).
        assert_eq!(dts_core_frame_size(&[0x7F, 0xFE, 0x80, 0x01]), 0);
        assert_eq!(dts_core_frame_size(&[]), 0);
    }

    #[test]
    fn core_frame_size_max_14bit() {
        // Max fsize 0x3FFF (all 14 bits set) → 16384, the documented upper
        // range bound. byte5 low2 = 0x03, byte6 = 0xFF, byte7 high4 = 0xF0.
        let mut d = vec![0u8; CORE_HEADER_MIN_BYTES];
        d[5] = 0x03;
        d[6] = 0xFF;
        d[7] = 0xF0;
        // wait — 0x03<<12 | 0xFF<<4 | 0x0F = 0x3FFF. byte7 high4 0xF0 >> 4 = 0xF.
        assert_eq!(dts_core_frame_size(&d), 0x3FFF + 1);
    }

    // --- dts_core_samples / dts_core_sample_rate: header-length boundary ---

    #[test]
    fn dts_core_samples_reads_nblks_high_bit_from_byte4_bit0() {
        // NBLKS is 7 bits: byte4 bit0 is the HIGH bit (<<6), byte5>>2 the low 6.
        // With byte4 bit0 set and byte5 = 0, nblks = 64 (not 0); a `<<` -> `>>`
        // typo on byte4 (always 0) would collapse samples from 2080 to 32.
        let mut d = vec![0u8; CORE_HEADER_MIN_BYTES];
        d[4] = 0x01;
        d[5] = 0x00;
        assert_eq!(dts_core_samples(&d), (64 + 1) * 32);
    }

    #[test]
    fn dts_core_samples_and_sample_rate_decode_at_exact_header_min_length() {
        // The guard is `data.len() < CORE_HEADER_MIN_BYTES`; exactly 10 bytes must
        // still read as a real header (index 8, SFREQ, is in bounds). Use non-
        // fallback NBLKS/SFREQ so a `<` -> `<=` typo is visible, not masked.
        let mut d = vec![0u8; CORE_HEADER_MIN_BYTES];
        d[4] = 0x00;
        d[5] = 0x00; // nblks = 0 -> 32 samples, not the 512 fallback
        d[8] = 6u8 << 2; // SFREQ = 6 -> 11_025 Hz, not the 48_000 fallback
        assert_eq!(d.len(), CORE_HEADER_MIN_BYTES);
        assert_eq!(dts_core_samples(&d), 32);
        assert_eq!(dts_core_sample_rate(&d), 11_025);
    }

    #[test]
    fn dts_core_samples_and_sample_rate_decode_past_header_min_length_too() {
        // A `<` -> `>` typo on the same guard takes the FALLBACK path once len
        // exceeds CORE_HEADER_MIN_BYTES — every real core frame would silently
        // report the 512-sample/48kHz fallback. Use a buffer twice the minimum.
        let mut d = vec![0u8; CORE_HEADER_MIN_BYTES * 2];
        d[4] = 0x00;
        d[5] = 0x00; // nblks = 0 -> 32 samples
        d[8] = 6u8 << 2; // SFREQ = 6 -> 11_025 Hz
        assert!(d.len() > CORE_HEADER_MIN_BYTES);
        assert_eq!(dts_core_samples(&d), 32);
        assert_eq!(dts_core_sample_rate(&d), 11_025);
    }

    // --- next_core_boundary: SYNCWORD_BYTES length guard ---

    // The `buf.len() < pos + SYNCWORD_BYTES` guard must be strict `<`: at
    // exactly that length the sync IS fully present. See docs/dts.md.
    #[test]
    fn next_core_boundary_exact_syncword_length_is_not_need_more() {
        let core = make_dts_core(MIN_CORE_FRAME_BYTES);
        let mut buf = core.clone();
        buf.extend_from_slice(&DTS_HD_EXT_SYNC); // exactly 4 bytes, nothing more
        let result = next_core_boundary(&buf, core.len());
        assert!(
            matches!(result, NextCore::None),
            "expected None (recognized-but-unsizeable extension sync with \
             nothing after it), got a different variant"
        );
    }

    // --- find_sync ---

    #[test]
    fn find_sync_locates_core() {
        let mut d = vec![0xAA, 0xBB];
        d.extend_from_slice(&DTS_CORE_SYNC);
        assert_eq!(find_sync(&d, &DTS_CORE_SYNC), Some(2));
    }

    #[test]
    fn find_sync_short_input_none() {
        // < 4 bytes can't hold a 4-byte sync.
        assert_eq!(find_sync(&[0x7F, 0xFE, 0x80], &DTS_CORE_SYNC), None);
        assert_eq!(find_sync(&[], &DTS_CORE_SYNC), None);
    }

    #[test]
    fn find_sync_partial_match_not_false_positive() {
        // First 3 sync bytes then a wrong 4th must not match.
        assert_eq!(find_sync(&[0x7F, 0xFE, 0x80, 0x00], &DTS_CORE_SYNC), None);
    }

    // --- next_core_boundary: candidate validation ---

    #[test]
    fn next_core_needs_more_when_candidate_header_truncated() {
        // A second core sync appears but fewer than CORE_HEADER_MIN_BYTES follow
        // it, so its size can't be judged (NeedMore → parse() breaks and waits).
        // Build core(512) + a bare 2nd sync with only the 4 syncword bytes.
        let mut parser = DtsParser::new();
        let mut data = make_dts_core(512);
        data.extend_from_slice(&DTS_CORE_SYNC); // 2nd sync, header truncated
        let f = parser.parse(&make_pes(data, Some(90000)));
        assert!(
            f.is_empty(),
            "candidate sync with truncated header must NOT close the AU yet"
        );
        // The first core's bytes are still buffered awaiting the verdict — not
        // dropped, not emitted.
        assert!(
            parser.acc.len() >= 512,
            "core1 retained while candidate boundary is undecided"
        );
    }

    #[test]
    fn multiple_false_syncs_in_extension_all_skipped() {
        // An extension body with SEVERAL byte sequences matching the core syncword
        // but decoding to sub-spec sizes must ALL be skipped; guards the loop in
        // next_core_boundary that advances `from = pos + 4` past each false sync.
        let mut parser = DtsParser::new();
        let mut ext = make_dts_ext(400);
        // Embed three bogus tiny core syncs at offsets 50, 150, 250.
        for &off in &[50usize, 150, 250] {
            ext[off..off + 4].copy_from_slice(&DTS_CORE_SYNC);
            // leave header bytes zero → fsize decodes to 1 → bogus.
        }
        let mut frame1 = make_dts_core(512);
        frame1.extend_from_slice(&ext);
        assert!(
            parser.parse(&make_pes(frame1, Some(90000))).is_empty(),
            "no real next core yet → AU held despite 3 false syncs"
        );
        let f = parser.parse(&make_pes(make_dts_core(640), Some(93000)));
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].data.len(),
            512 + 400,
            "AU spans the full extension, not split at any false sync"
        );
    }

    #[test]
    fn leading_junk_before_core_is_dropped() {
        // Bytes before the first core sync are not part of any AU and must be
        // dropped (drain_front(start)). Prepend junk, then core1 + core2.
        let mut parser = DtsParser::new();
        let mut data = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x12];
        data.extend_from_slice(&make_dts_core(512));
        data.extend_from_slice(&make_dts_core(640));
        let f = parser.parse(&make_pes(data, Some(90000)));
        assert_eq!(f.len(), 1, "AU1 closes at core2");
        assert_eq!(
            f[0].data.len(),
            512,
            "leading junk dropped — AU is exactly the core, no prefix bytes"
        );
    }

    #[test]
    fn no_core_sync_keeps_only_three_byte_tail() {
        // With no core sync at all, the parser retains at most a 3-byte tail so a
        // sync split across PES packets can complete. Feed 4 junk bytes; tail
        // must shrink to 3 (drain_front(len-3)).
        let mut parser = DtsParser::new();
        let f = parser.parse(&make_pes(vec![0x11, 0x22, 0x33, 0x44], Some(90000)));
        assert!(f.is_empty());
        assert_eq!(parser.acc.len(), 3, "only a 3-byte resync tail retained");
        assert_eq!(parser.acc.as_slice(), &[0x22, 0x33, 0x44]);
    }

    #[test]
    fn core_sync_split_across_pes_reassembles() {
        // The 4-byte core sync straddling a PES boundary must still be found:
        // 3 sync bytes retained as tail, the 4th + body arrive next PES.
        let mut parser = DtsParser::new();
        let core = make_dts_core(512);
        // PES 1: just the first 3 bytes of the sync.
        assert!(
            parser
                .parse(&make_pes(core[..3].to_vec(), Some(90000)))
                .is_empty()
        );
        assert_eq!(parser.acc.len(), 3, "3-byte sync prefix retained");
        // PES 2: the 4th sync byte + the rest of core1, then a 2nd core to close.
        let mut rest = core[3..].to_vec();
        rest.extend_from_slice(&make_dts_core(640));
        let f = parser.parse(&make_pes(rest, None));
        assert_eq!(f.len(), 1, "split-sync core recovered and closed");
        assert_eq!(f[0].data.len(), 512);
        assert_eq!(
            f[0].pts_ns,
            pts_to_ns(90000),
            "AU keeps the PTS of the PES that began the sync"
        );
    }

    #[test]
    fn core_header_incomplete_waits() {
        // A core sync with fewer than CORE_HEADER_MIN_BYTES buffered can't be
        // sized → parse() breaks and waits, emitting nothing.
        let mut parser = DtsParser::new();
        let mut data = DTS_CORE_SYNC.to_vec();
        data.extend_from_slice(&[0x00, 0x00, 0x00]); // only 7 bytes total < 10
        assert!(parser.parse(&make_pes(data, Some(90000))).is_empty());
        assert!(!parser.acc.is_empty(), "partial core header retained");
    }

    #[test]
    fn flush_rejects_sub_spec_core() {
        // flush must reject a buffered "core" whose decoded size is below the
        // 96-byte ETSI spec floor (a false sync), never emitting it.
        let mut parser = DtsParser::new();
        // A sync sized to 17 bytes (< MIN_CORE_FRAME_BYTES) with 17 bytes buffered.
        let mut d = vec![0u8; 17];
        d[0..4].copy_from_slice(&DTS_CORE_SYNC);
        d[6] = 0x01; // fsize → 16 → size 17
        parser.acc.seed(&d);
        assert!(parser.flush().is_empty(), "sub-spec core rejected at flush");
    }

    #[test]
    fn flush_rejects_core_extending_past_buffer() {
        // A valid-sized core header but with fewer bytes buffered than the
        // declared size must be dropped (never emit fewer bytes than declared).
        let mut parser = DtsParser::new();
        let core = make_dts_core(512);
        parser.acc.seed(&core[..300]); // header says 512, only 300 present
        assert!(
            parser.flush().is_empty(),
            "incomplete core not emitted truncated"
        );
    }

    #[test]
    fn flush_empty_buffer_is_empty() {
        let mut parser = DtsParser::new();
        assert!(parser.flush().is_empty());
    }

    #[test]
    fn flush_partial_sync_tail_dropped() {
        // A bare partial-sync tail (not at offset 0 / not a full core) is dropped.
        let mut parser = DtsParser::new();
        parser.acc.seed(&[0x7F, 0xFE, 0x80]); // 3 of 4 sync bytes
        assert!(parser.flush().is_empty());
        assert!(parser.acc.is_empty(), "buffer cleared on flush");
    }

    #[test]
    fn min_core_frame_bytes_boundary_accepts_96() {
        // A core sized to exactly MIN_CORE_FRAME_BYTES (96) is the smallest
        // valid core and must be accepted. core(96) + core(640) closes AU1=96.
        let mut parser = DtsParser::new();
        let mut data = make_dts_core(MIN_CORE_FRAME_BYTES);
        data.extend_from_slice(&make_dts_core(640));
        let f = parser.parse(&make_pes(data, Some(90000)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].data.len(), MIN_CORE_FRAME_BYTES);
    }

    #[test]
    fn core_one_below_min_is_rejected() {
        // A core decoding to 95 bytes (one below the 96-byte floor) is a false
        // sync: skip its 4 syncword bytes and resync to the next real core.
        let mut parser = DtsParser::new();
        let mut data = make_dts_core(MIN_CORE_FRAME_BYTES - 1); // size 95, false
        // Real core right after (so resync finds it).
        data.extend_from_slice(&make_dts_core(512));
        data.extend_from_slice(&make_dts_core(640)); // closes the real AU
        let f = parser.parse(&make_pes(data, Some(90000)));
        // The 95-byte false core is skipped; AU1 is the real 512 core.
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].data.len(),
            512,
            "sub-floor sync skipped, real 512 core is AU1"
        );
    }

    // A structurally-valid but UNDECODABLE core (LFE flag = reserved 3):
    // sizes/syncs normally, fails the header validity check. See
    // docs/dts.md.
    fn make_bad_dts_core(size: usize) -> Vec<u8> {
        let mut d = make_dts_core(size);
        assert!(
            core_header_drop_reason(&d).is_none(),
            "base core is decodable"
        );
        d[10] |= 0x06; // LFE flag = 3 (invalid)
        assert_eq!(
            core_header_drop_reason(&d),
            Some(DropReason::LfeFlag),
            "invalid-LFE core must be judged undecodable"
        );
        d
    }

    #[test]
    fn valid_stream_drops_nothing() {
        // A clean stream of decodable cores must pass the gate untouched — the
        // detector follows the spec's validity rules exactly, so zero false
        // positives.
        let mut parser = DtsParser::new();
        let mut stream = Vec::new();
        for _ in 0..5 {
            stream.extend_from_slice(&make_dts_core(512));
        }
        let mut frames = parser.parse(&make_pes(stream, Some(90000)));
        frames.extend(parser.flush());
        assert_eq!(frames.len(), 5, "all five cores emitted");
        assert_eq!(parser.dropped_frames(), 0, "nothing dropped");
        assert_eq!(parser.dropped_duration_ns(), 0);
    }

    #[test]
    fn undecodable_core_is_dropped_and_counted() {
        // A single undecodable core between good ones is dropped; the survivors
        // are emitted and the drop is counted.
        let mut parser = DtsParser::new();
        let mut stream = make_dts_core(512);
        stream.extend_from_slice(&make_bad_dts_core(512));
        stream.extend_from_slice(&make_dts_core(640));
        let mut frames = parser.parse(&make_pes(stream, Some(90000)));
        frames.extend(parser.flush());
        assert_eq!(frames.len(), 2, "the bad core is dropped, two survive");
        assert_eq!(frames[0].data.len(), 512);
        assert_eq!(frames[1].data.len(), 640);
        assert_eq!(parser.dropped_frames(), 1);
        assert_eq!(
            parser.dropped_duration_ns(),
            DTS_CORE_DUR_NS as u64,
            "one frame's worth of audio silence introduced"
        );
    }

    #[test]
    fn drop_preserves_av_sync_no_shift() {
        // THE INVARIANT: dropping an undecodable AU must never shift the audio
        // that follows. good/bad/good in ONE PES: the trailing good core keeps
        // the EXACT PTS it would have had with no drop — a gap, not a shift.
        let mut parser = DtsParser::new();
        let mut stream = make_dts_core(512); // c1: good
        stream.extend_from_slice(&make_bad_dts_core(512)); // c2: undecodable
        stream.extend_from_slice(&make_dts_core(640)); // c3: good
        let mut frames = parser.parse(&make_pes(stream, Some(90000)));
        frames.extend(parser.flush());

        assert_eq!(frames.len(), 2, "c2 dropped; c1 and c3 survive");
        let base = pts_to_ns(90000);
        assert_eq!(frames[0].pts_ns, base, "c1 keeps the PES base PTS");
        assert_eq!(
            frames[1].pts_ns,
            base + 2 * DTS_CORE_DUR_NS,
            "c3 keeps its TRUE timeline (base + 2 frames) — the drop is a gap, not a shift"
        );
        // The gap between the survivors is exactly the dropped frame's duration
        // beyond the normal one-frame spacing.
        assert_eq!(
            frames[1].pts_ns - frames[0].pts_ns,
            2 * DTS_CORE_DUR_NS,
            "surviving AUs are spaced by the real timeline including the dropped frame's slot"
        );
        assert_eq!(parser.dropped_frames(), 1);
    }

    #[test]
    fn whole_track_poison_drops_remainder() {
        // A track dominated by undecodable frames is judged too damaged to mux:
        // once the >50% verdict fires (after the minimum sample count), the
        // whole track — including any later good frames — is dropped.
        let mut parser = DtsParser::new();
        let mut stream = Vec::new();
        // 300 AUs, ~2/3 undecodable → well over the 50% threshold and the
        // 200-AU minimum.
        for i in 0..300 {
            if i % 3 == 0 {
                stream.extend_from_slice(&make_dts_core(512));
            } else {
                stream.extend_from_slice(&make_bad_dts_core(512));
            }
        }
        // A trailing burst of GOOD cores that must be dropped once poisoned.
        for _ in 0..20 {
            stream.extend_from_slice(&make_dts_core(512));
        }
        let mut frames = parser.parse(&make_pes(stream, Some(90000)));
        frames.extend(parser.flush());
        assert!(
            parser.tally.is_poisoned(),
            "track poisoned by >50% drop rate"
        );
        // Once poisoned, later good cores are dropped too, so the kept count
        // (kept = emitted survivors) is far below the ~120 good cores present.
        let kept = frames.len() as u64;
        assert!(
            kept < 120,
            "post-poison good frames also dropped (kept={kept})"
        );
        assert!(parser.dropped_frames() > 150, "majority dropped");
    }

    #[test]
    // The loop variable is the DOMAIN VALUE checked (a DTS SFREQ code), not a
    // collection cursor: `.iter().enumerate()` would rename it to `i` and read worse.
    #[allow(clippy::needless_range_loop)]
    fn sr_validity_table_marks_reserved_codes() {
        // The core-header sample-rate validity table must have ZERO (reject) at
        // exactly the reserved SFREQ codes {0,4,5,9,10} and a real rate
        // elsewhere — this is what drives the invalid-sample-rate rejection.
        for code in 0..16usize {
            let reserved = matches!(code, 0 | 4 | 5 | 9 | 10);
            assert_eq!(
                DTS_CORE_SR_VALID[code] == 0,
                reserved,
                "SFREQ code {code} reserved={reserved}"
            );
        }
    }

    #[test]
    fn every_core_header_error_class_is_detected() {
        // Exercise each header-validity rejection so the gate stays faithful to
        // the spec. Start from a decodable core and corrupt one field at a time.
        let good = make_dts_core(512);
        assert_eq!(core_header_drop_reason(&good), None);

        // deficit_samples != 32: clear SHORT (byte4 bits6-2) → deficit = 1.
        let mut d = good.clone();
        d[4] &= !0x7C;
        assert_eq!(
            core_header_drop_reason(&d),
            Some(DropReason::DeficitSamples)
        );

        // npcmblocks not a multiple of 8: NBLKS low bits (byte5 bits7-2) → 14
        // (npcmblocks=15, 15 & 7 = 7 ≠ 0).
        let mut d = good.clone();
        d[5] = (d[5] & 0x03) | (14u8 << 2);
        assert_eq!(core_header_drop_reason(&d), Some(DropReason::PcmBlocks));

        // audio_mode reserved (>= 16): AMODE = byte7 bits3-0 + byte8 bits7-6. Set
        // high nibble to 0xF → audio_mode = 60, a genuinely RESERVED code a
        // decoder rejects (10-15 are LEGAL, see legal_multichannel_amode_is_not_dropped).
        let mut d = good.clone();
        d[7] |= 0x0F;
        assert_eq!(core_header_drop_reason(&d), Some(DropReason::Amode));

        // sample_rate reserved: SFREQ (byte8 bits5-2) = 0.
        let mut d = good.clone();
        d[8] &= !0x3C;
        assert_eq!(core_header_drop_reason(&d), Some(DropReason::SampleRate));

        // lfe_present == 3: LFE is byte10 bits2-1.
        let mut d = good.clone();
        d[10] |= 0x06;
        assert_eq!(core_header_drop_reason(&d), Some(DropReason::LfeFlag));

        // pcmr_code reserved (7): pcmr is byte11 bit0 + byte12 bits7-6 → set all
        // three to 1 (code 7 → DTS_CORE_PCMR_BITS[7] = 0, a reserved PCMR code).
        let mut d = good.clone();
        d[11] |= 0x01;
        d[12] |= 0xC0;
        assert_eq!(core_header_drop_reason(&d), Some(DropReason::PcmRes));
    }

    #[test]
    fn legal_multichannel_amode_is_not_dropped() {
        // ETSI TS 102 114 §5.3.1: AMODE is 6 bits with 16 LEGAL codes (0-15), only
        // 16-63 reserved; codes 10-15 are decodable 6/7/8-channel layouts. The
        // gate must KEEP them — dropping a legal multichannel core silences audio.
        fn set_amode(core: &mut [u8], amode: u32) {
            // audio_mode = (byte7 & 0x0F) << 2 | (byte8 >> 6).
            core[7] = (core[7] & 0xF0) | ((amode >> 2) & 0x0F) as u8;
            core[8] = (core[8] & 0x3F) | (((amode & 0x03) << 6) as u8);
        }

        // Every legal code 0-15 is kept — the range is a literal (NOT
        // DTS_AMODE_COUNT) so reverting the bound to 10 makes 10-15 fail here.
        for amode in 0u32..16 {
            let mut core = make_dts_core(512);
            set_amode(&mut core, amode);
            assert_eq!(
                core_header_drop_reason(&core),
                None,
                "legal AMODE {amode} must not be dropped"
            );
        }
        // The first reserved code (16) and above are still rejected.
        for amode in [16u32, 40, 63] {
            let mut core = make_dts_core(512);
            set_amode(&mut core, amode);
            assert_eq!(
                core_header_drop_reason(&core),
                Some(DropReason::Amode),
                "reserved AMODE {amode} must be dropped"
            );
        }
    }

    /// Set FTYPE (byte4 bit7: 1 = normal, 0 = termination) and the 5-bit SHORT
    /// field (byte4 bits6-2), leaving CPF and the NBLKS high bit (bits1-0) intact.
    /// `deficit_samples = short_field + 1`.
    fn set_ftype_short(core: &mut [u8], normal: bool, short_field: u8) {
        core[4] = (core[4] & 0x03) | ((normal as u8) << 7) | ((short_field & 0x1F) << 2);
    }

    #[test]
    fn termination_frame_with_small_deficit_is_kept() {
        // ETSI TS 102 114: a normal frame carries a full 32-sample PCM block, but
        // a TERMINATION frame (FTYPE=0) may legally carry fewer and is decodable.
        // It must NOT be dropped — that would silence the last frame of a stream.
        let mut core = make_dts_core(512);
        set_ftype_short(&mut core, false, 10); // termination, deficit = 11 (< 32)
        assert_eq!(
            core_header_drop_reason(&core),
            None,
            "a termination frame with a small deficit is decodable and must be kept"
        );
        // End-to-end: a termination frame closed by a following core survives.
        let mut term = make_dts_core(512);
        set_ftype_short(&mut term, false, 5); // deficit = 6
        let mut stream = term;
        stream.extend_from_slice(&make_dts_core(640));
        let mut parser = DtsParser::new();
        let mut frames = parser.parse(&make_pes(stream, Some(90000)));
        frames.extend(parser.flush());
        assert_eq!(frames.len(), 2, "termination frame is emitted, not dropped");
        assert_eq!(frames[0].data.len(), 512);
        assert_eq!(parser.dropped_frames(), 0);
    }

    #[test]
    fn normal_frame_with_wrong_deficit_is_dropped() {
        // The other side of the FTYPE gate: a NORMAL frame (FTYPE=1) whose
        // deficit-sample field is not 32 is genuinely undecodable and must be
        // dropped. Guards against the fix over-relaxing into "never check deficit".
        let mut core = make_dts_core(512);
        set_ftype_short(&mut core, true, 10); // normal, deficit = 11 (!= 32)
        assert_eq!(
            core_header_drop_reason(&core),
            Some(DropReason::DeficitSamples),
            "a normal frame with deficit != 32 is undecodable and must be dropped"
        );
    }

    #[test]
    fn reserved_bit_set_is_not_dropped() {
        // The bit after RATE is RESERVED (ETSI TS 102 114); both reference
        // decoders SKIP it and never reject on it. Setting it (byte9 bit4) on an
        // otherwise-valid core must leave it KEPT.
        let mut core = make_dts_core(512);
        assert_eq!(core_header_drop_reason(&core), None, "baseline decodable");
        core[9] |= 0x10; // set the reserved bit
        assert_eq!(
            core_header_drop_reason(&core),
            None,
            "a set reserved bit must NOT drop a decodable frame"
        );
    }

    // Real-data fixture (ignored): re-parses a raw .dts stream and writes the
    // emitted AUs back out for validation against an external decoder. See
    // docs/dts.md. Env: DTS_IN, DTS_OUT.
    #[test]
    #[ignore]
    fn reparse_real_dts_file() {
        use std::io::Write;
        let inp = std::env::var("DTS_IN").expect("DTS_IN");
        let outp = std::env::var("DTS_OUT").expect("DTS_OUT");
        let bytes = std::fs::read(&inp).expect("read DTS_IN");
        let mut parser = DtsParser::new();
        let mut out =
            std::io::BufWriter::new(std::fs::File::create(&outp).expect("create DTS_OUT"));
        let mut au_count = 0usize;
        let mut out_bytes = 0usize;
        // 90 kHz PTS advancing per chunk; arbitrary chunking is faithful because
        // the framer resyncs on core sync and buffers across PES boundaries.
        let mut pts: i64 = 90_000;
        const CHUNK: usize = 64 * 1024;
        for chunk in bytes.chunks(CHUNK) {
            let pes = PesPacket {
                source: None,
                pid: 0x1100,
                pts: Some(pts),
                dts: None,
                data: chunk.to_vec(),
                discontinuity: false,
            };
            pts += 2_100; // ~one AU worth; value irrelevant to AU framing/bytes
            for f in parser.parse(&pes) {
                au_count += 1;
                out_bytes += f.data.len();
                out.write_all(&f.data).expect("write AU");
            }
        }
        for f in parser.flush() {
            au_count += 1;
            out_bytes += f.data.len();
            out.write_all(&f.data).expect("write AU");
        }
        out.flush().expect("flush");
        eprintln!(
            "REPARSE in={} bytes -> out={} bytes across {} AUs ({} bytes dropped)",
            bytes.len(),
            out_bytes,
            au_count,
            bytes.len().saturating_sub(out_bytes)
        );
    }

    // ── Exact-boundary behaviour of the AU framer ────────────────────────────

    // `find_sync` scans `0..=len-4`; the `len == 4` boundary (where a split
    // sync lands on its last byte) isn't covered elsewhere. See docs/dts.md.
    #[test]
    fn find_sync_matches_a_buffer_that_is_exactly_the_syncword() {
        assert_eq!(
            find_sync(&DTS_CORE_SYNC, &DTS_CORE_SYNC),
            Some(0),
            "a four-byte buffer that IS the syncword matches at 0"
        );
        let mut five = vec![0x00u8];
        five.extend_from_slice(&DTS_CORE_SYNC);
        assert_eq!(
            find_sync(&five, &DTS_CORE_SYNC),
            Some(1),
            "and the last possible start offset is len - 4"
        );
    }

    // Exactly CORE_HEADER_MIN_BYTES must be decoded, not deferred — else a
    // false sync sits at the front, blocking every later real core. See
    // docs/dts.md.
    #[test]
    fn a_core_header_of_exactly_the_minimum_length_is_decoded_not_deferred() {
        let mut parser = DtsParser::new();
        let mut d = vec![0u8; CORE_HEADER_MIN_BYTES];
        d[0..4].copy_from_slice(&DTS_CORE_SYNC);
        d[6] = 0x01; // fsize = 16 → core_size 17, below the 96-byte ETSI floor
        assert_eq!(d.len(), 10, "the fixture is exactly at the boundary");
        let out = parser.parse(&make_pes(d, Some(90_000)));
        assert!(out.is_empty(), "a false sync emits nothing");
        assert_eq!(
            parser.acc.len(),
            3,
            "the false sync was decoded, drained and resynced past — leaving only \
             the 3-byte split-sync carry-over"
        );
        assert_ne!(
            find_sync(parser.acc.as_slice(), &DTS_CORE_SYNC),
            Some(0),
            "and the bogus sync is no longer at the front of the buffer"
        );
    }

    // End-of-stream flush must emit a final AU whose core exactly fills the
    // buffer — the ordinary case; rejecting it drops every track's last frame.
    #[test]
    fn flush_emits_a_core_that_exactly_fills_the_buffer() {
        let mut parser = DtsParser::new();
        let core = make_dts_core(512);
        parser.acc.seed(&core.clone());
        parser.pending_pts = 90_000;
        let out = parser.flush();
        assert_eq!(out.len(), 1, "the final AU is emitted, not dropped");
        assert_eq!(out[0].data, core, "and it is the whole core frame");
        // One byte short is still refused — the bound is not simply absent.
        let mut parser = DtsParser::new();
        parser.acc.seed(&core[..511]);
        parser.pending_pts = 90_000;
        assert!(
            parser.flush().is_empty(),
            "a core one byte short of its declared size is not emitted truncated"
        );
    }

    // The flush guard is a DISJUNCTION: a buffer not BEGINNING with a core
    // sync is discarded regardless of length, or junk could size-decode into
    // a fake AU. See docs/dts.md.
    #[test]
    fn flush_discards_a_long_buffer_that_does_not_begin_with_a_core_sync() {
        // A well-formed core frame with ONE byte of its syncword corrupted: every
        // other field still decodes, and the decodability gate would pass it, so
        // only the leading-sync test stands between it and the output.
        let mut parser = DtsParser::new();
        let mut broken = make_dts_core(300);
        broken[0] ^= 0xFF; // no longer 0x7FFE8001 at offset 0
        assert_ne!(
            find_sync(&broken, &DTS_CORE_SYNC),
            Some(0),
            "the fixture really has no core sync at the front"
        );
        parser.acc.seed(&broken);
        parser.pending_pts = 90_000;
        assert!(
            parser.flush().is_empty(),
            "a buffer whose front is not a core sync is discarded, not size-decoded"
        );
        assert!(parser.acc.is_empty(), "and the junk is dropped");

        // The other half of the disjunction: a buffer too short to size, whose
        // front IS a core sync, is discarded too.
        let mut parser = DtsParser::new();
        parser.acc.seed(DTS_CORE_SYNC.as_ref());
        parser.pending_pts = 90_000;
        assert!(parser.flush().is_empty(), "a bare sync tail is not an AU");
    }

    // EXSS_HEADER_MIN_BYTES is the WORST-CASE header length, pinned at both
    // sides of the boundary rather than by value. See docs/dts.md.
    #[test]
    fn exss_frame_size_needs_the_worst_case_header_and_no_more() {
        assert_eq!(
            EXSS_HEADER_MIN_BYTES, 10,
            "4 sync bytes + ceil((8 + 2 + 1 + 12 + 20) / 8)"
        );
        let ext = make_exss(10, None);
        assert_eq!(
            exss_frame_size(&ext),
            Some(10),
            "exactly the minimum is enough to size a substream"
        );
        assert_eq!(
            exss_frame_size(&ext[..9]),
            None,
            "one byte short cannot be sized — the long form's fields are not all in"
        );
        assert_eq!(
            exss_frame_size(&ext[..4]),
            None,
            "the bare sync sizes nothing"
        );
    }

    // The point of the shared buffer: an AU whose core arrived EARLIER keeps
    // that packet's source offset, not the completing packet's. See
    // docs/dts.md.
    #[test]
    fn an_access_unit_carries_the_source_of_the_packet_its_core_arrived_in() {
        let mut parser = DtsParser::new();
        let core = make_dts_core(512);

        // The core starts here, at byte 1000 of the feed.
        let mut p1 = make_pes(core[..256].to_vec(), Some(90_000));
        p1.source = Some(crate::pes::SourcePos::at_byte(1_000));
        assert!(parser.parse(&p1).is_empty(), "partial core held");

        // The rest arrives later, at byte 9_000, together with the next core
        // that closes the unit.
        let mut rest = core[256..].to_vec();
        rest.extend_from_slice(&make_dts_core(512));
        let mut p2 = make_pes(rest, Some(180_000));
        p2.source = Some(crate::pes::SourcePos::at_byte(9_000));
        let frames = parser.parse(&p2);

        assert!(!frames.is_empty(), "the completed unit is emitted");
        assert_eq!(
            frames[0].source.map(|s| s.byte),
            Some(1_000),
            "the unit belongs to the packet its FIRST byte came from"
        );
        assert_eq!(
            frames[0].pts_ns,
            pts_to_ns(90_000),
            "and its timestamp comes from that same packet"
        );
    }

    /// A unit that begins and ends in one packet takes that packet's offset —
    /// the ordinary case, which must not regress while fixing the spanning one.
    #[test]
    fn a_self_contained_access_unit_carries_its_own_packets_source() {
        let mut parser = DtsParser::new();
        let mut data = make_dts_core(512);
        data.extend_from_slice(&make_dts_core(512));
        let mut p = make_pes(data, Some(90_000));
        p.source = Some(crate::pes::SourcePos::at_byte(4_242));
        let frames = parser.parse(&p);
        assert!(!frames.is_empty());
        assert_eq!(frames[0].source.map(|s| s.byte), Some(4_242));
    }
}
