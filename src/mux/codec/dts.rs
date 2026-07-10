//! DTS / DTS-HD elementary stream parser.
//!
//! DTS core syncword: 0x7FFE8001 (32 bits).
//! DTS-HD MA/HRA extension syncword: 0x64582025 (32 bits), appears after the core frame.
//! Buffers across PES boundaries so frames spanning two PES packets
//! are emitted complete.

use super::startcode::BitReader;
use super::{CodecParser, Frame, PesPacket, pts_to_ns};

const DTS_CORE_SYNC: [u8; 4] = [0x7F, 0xFE, 0x80, 0x01];
/// DTS-HD extension substream syncword. An access unit is delimited by the next
/// CORE sync; the parser locates and exactly sizes each extension substream (via
/// `exss_frame_size`) so a false core sync inside the EXSS payload can't split
/// the AU and truncate the lossless extension.
const DTS_HD_EXT_SYNC: [u8; 4] = [0x64, 0x58, 0x20, 0x25];

/// DTS / DTS-HD elementary-stream parser. Buffers DTS across PES boundaries so
/// a core frame plus all of its trailing DTS-HD extension substreams are
/// emitted together as one access unit, delimited by the next valid core sync.
/// This preserves the lossless extension data instead of downgrading to lossy
/// core (the lossy-core downgrade bug).
pub struct DtsParser {
    buf: Vec<u8>,
    /// PTS of the access unit currently being assembled in `buf` (the unit
    /// starting at the first buffered core sync). Captured when that core
    /// frame's PES first arrived; the trailing extension-substream PES
    /// packets carry their own (later) PTS which must NOT override it.
    pending_pts: i64,
    /// PTS markers attributing buffer regions to their source PES. Each entry
    /// is `(buffer_offset, pts_ns)` for the PES whose bytes begin at that
    /// offset. When an access unit is emitted from the front of `buf`, its PTS
    /// is the marker covering offset 0 — NOT the most recent PES's PTS. This is
    /// what fixes multi-AU-per-call PTS attribution: if a PES carries the
    /// extension substreams (and possibly the next core) for an AU whose own
    /// core arrived in an earlier PES, the emitted AU keeps its own core PES's
    /// timestamp instead of the later PES's. Offsets are kept relative to the
    /// current `buf` start and rebased whenever bytes are drained from the
    /// front.
    pts_marks: std::collections::VecDeque<(usize, i64)>,
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
}

impl Default for DtsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl DtsParser {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(32768),
            pending_pts: 0,
            pts_marks: std::collections::VecDeque::new(),
            last_front_pts: PTS_UNSET,
            next_pts_ns: PTS_UNSET,
        }
    }

    /// Stamp an access unit's PTS. `front` is the AU's own core-PES PTS (from
    /// [`front_pts`]); `dur_ns` its decoded duration.
    ///
    /// The model matches the (correct) AC-3 path: **re-base to each PES's own
    /// container timestamp, and advance by one frame duration ONLY within a run
    /// of AUs that share the same PES.** A new PES (its `front` differs from the
    /// previous AU's) trusts its own timestamp — so the emitted timeline tracks
    /// the container and never drifts. Advancing within a PES is what fixes the
    /// DVD case (several DTS core frames packed in one PES, which otherwise all
    /// collided on that single PES timestamp → "non monotonically increasing
    /// dts"). A global running clock was WRONG here: once accumulated frame
    /// durations exceeded the PES spacing it never re-based, drifting the track
    /// minutes past the real length over a feature-long title.
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

    /// Drop `n` bytes from the front of `buf` and rebase the PTS markers so
    /// their offsets stay relative to the new buffer start. A marker that now
    /// sits at or before offset 0 is clamped to 0 (it still covers the front).
    /// Redundant markers all at offset 0 collapse to the last one.
    fn drain_front(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        self.buf.drain(..n);
        for m in &mut self.pts_marks {
            m.0 = m.0.saturating_sub(n);
        }
        // Collapse all leading markers that now sit at offset 0 to the last
        // such marker — that is the PES whose data currently begins the buffer.
        let last_zero = self
            .pts_marks
            .iter()
            .rposition(|&(off, _)| off == 0)
            .filter(|&i| i > 0);
        if let Some(i) = last_zero {
            self.pts_marks.drain(..i);
        }
    }

    /// PTS that should be stamped on an access unit currently at the front of
    /// `buf` (offset 0): the most recent marker at offset 0, falling back to
    /// `pending_pts`.
    fn front_pts(&self) -> i64 {
        self.pts_marks
            .iter()
            .rev()
            .find(|&&(off, _)| off == 0)
            .map(|&(_, pts)| pts)
            .unwrap_or(self.pending_pts)
    }
}

/// Hard cap on a buffered access unit (core + all its extension substreams).
/// A DTS-HD MA frame is at most a few tens of KB; if the buffer grows past
/// this without a clean boundary we resync rather than stall or balloon.
const MAX_AU_BYTES: usize = 65536;

/// Number of leading bytes that must be buffered before the core `fsize` field
/// (bytes 5-7) can be decoded. This is a HEADER-LAYOUT minimum — "enough bytes
/// to read the size field" — and is deliberately distinct from
/// `MIN_CORE_FRAME_BYTES` (the decoded-frame-size validity floor). They must not
/// be conflated: this one gates buffer reads of the header, the other rejects
/// implausible decoded sizes.
const CORE_HEADER_MIN_BYTES: usize = 10;

/// Minimum plausible decoded DTS core frame size, per ETSI TS 102 114: the
/// on-wire FSIZE floor is 95, so a real core frame is at least 96 bytes. A
/// decoded `core_size` below this means we matched a false/corrupt core sync
/// (a lucky 0x7FFE8001 in extension-substream payload whose 14-bit `fsize`
/// decoded to a tiny value) rather than a real frame, so we resync instead of
/// closing an access unit at a junk boundary and dropping the DTS-HD extension
/// tail.
const MIN_CORE_FRAME_BYTES: usize = 96;

/// Sentinel for "no valid PTS base captured yet". Real PTS-in-ns values are
/// non-negative (derived from the unsigned 90 kHz PES timestamp), so a negative
/// value can never collide with a genuine timestamp. Used to mark the PTS base
/// invalid after a forced flush so the next PES sets it regardless of buffer
/// state.
const PTS_UNSET: i64 = -1;

impl CodecParser for DtsParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        // B1: a concealed/lost gap means the buffered DTS access unit is
        // TRUNCATED. Splicing post-gap bytes onto it corrupts the core/extension
        // framing (→ "Failed to decode block code(s)" / "Invalid data found").
        // Drop the partial AU and its PTS marks; the next PES re-bases a fresh
        // unit. (Audio has no inter-frame refs — dropping the spliced partial is
        // the whole fix; the video ResyncGate handles video.)
        //
        // Handle the discontinuity BEFORE the empty-data guard so the signal can
        // never be stranded by an empty post-gap PES (defensive; the demuxer only
        // emits non-empty PES today).
        if pes.discontinuity {
            self.buf.clear();
            self.pts_marks.clear();
            self.pending_pts = PTS_UNSET;
            // A concealed gap is a timeline discontinuity: let the post-gap AU
            // re-base to its own PES PTS rather than the pre-gap cursor.
            self.next_pts_ns = PTS_UNSET;
            self.last_front_pts = PTS_UNSET;
        }
        if pes.data.is_empty() {
            return Vec::new();
        }
        // A PES with no PTS (rare for audio, but legal — the case OSS demuxers
        // guard at a post-gap continuation) must NOT reset the timeline to 0;
        // continue from the most recent known base. Defense-in-depth: the
        // discontinuity-carrying PES is a PUSI with a PTS in practice.
        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or_else(|| {
            self.pts_marks
                .back()
                .map(|&(_, p)| p)
                .filter(|&p| p >= 0)
                .unwrap_or(if self.pending_pts >= 0 {
                    self.pending_pts
                } else {
                    0
                })
        });

        // On Blu-ray, a DTS-HD MA/HRA access unit is a DTS core frame
        // (sync 0x7FFE8001) followed by one or more DTS extension substreams
        // (sync 0x64582025). The m2ts demuxer hands those out as SEPARATE PES
        // packets on the same PID — the core in one PES, then the extension
        // substreams in following PES packets (with their own, later PTS). The
        // lossless audio lives entirely in the extension substreams, so an
        // access unit is only complete once all of its trailing extensions
        // have been buffered. We assemble across PES boundaries here: an access
        // unit runs from its core sync up to (but not including) the NEXT core
        // sync. Emitting on the core boundary keeps the core + every following
        // extension substream together (the lossless data), instead of the
        // old per-PES emit that dropped the extension PES packets and
        // downgraded the track to lossy DTS core (the lossy-core
        // downgrade bug). The PTS is the core frame's PTS, captured when the unit began.
        // Capture the access unit's PTS base on a fresh buffer, or whenever a
        // prior forced (safety-valve) flush left it invalidated — in the
        // forced case the bytes still in `buf` are not a real core frame, so
        // the first PES to arrive after the flush carries the correct base.
        if self.buf.is_empty() || self.pending_pts == PTS_UNSET {
            self.pending_pts = pts_ns;
        }
        // Mark where THIS PES's bytes begin in the buffer, with its PTS. The
        // emitted access unit takes the PTS of the PES covering its first byte
        // (see `front_pts`), so an AU whose core arrived in an earlier PES keeps
        // that core's timestamp even when its extensions / the following core
        // arrive (with a later PTS) in this same parse() call.
        // (pts_marks is bounded implicitly: an empty PES returns above without
        // pushing a mark, and a non-empty run grows `buf`, which is cleared —
        // along with pts_marks — once it exceeds MAX_AU_BYTES.)
        self.pts_marks.push_back((self.buf.len(), pts_ns));
        self.buf.extend_from_slice(&pes.data);

        let mut frames = Vec::new();

        loop {
            // Resync to the first core sync; drop any leading junk.
            let Some(start) = find_sync(&self.buf, &DTS_CORE_SYNC) else {
                // No core sync at all yet — keep at most a 3-byte tail so a
                // sync split across PES packets can still be found next time.
                if self.buf.len() > 3 {
                    let tail = self.buf.len() - 3;
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
                    find_sync(&self.buf, &DTS_CORE_SYNC),
                    Some(0),
                    "drain_front(start) must leave the core sync at offset 0"
                );
            }

            // Need the core header to size the core frame.
            if self.buf.len() < CORE_HEADER_MIN_BYTES {
                break;
            }
            let core_size = dts_core_frame_size(&self.buf);
            // `dts_core_frame_size` returns a 14-bit `fsize + 1`, so it is
            // always in [1, 16384]; the bare `== 0` / `> MAX_AU_BYTES` checks
            // can never fire. A real DTS core frame is at least
            // MIN_CORE_FRAME_BYTES (96, the ETSI spec floor), so any decoded
            // size below that came from a false/corrupt sync. Reject it (drain
            // the 4 syncword bytes and resync) instead of letting a tiny bogus
            // size close the current access unit at a junk boundary and drop the
            // trailing extension substreams. The `> MAX_AU_BYTES` upper bound is
            // kept as a harmless guard.
            if !(MIN_CORE_FRAME_BYTES..=MAX_AU_BYTES).contains(&core_size) {
                // Bogus core sync — skip past it and resync.
                self.drain_front(4);
                continue;
            }
            if self.buf.len() < core_size {
                break; // core frame not fully buffered yet — wait
            }

            // The access unit ends at the next *valid* core sync. The search
            // begins after this core's syncword so we don't re-match it.
            // Anything between the core and that next sync is this unit's
            // extension substream(s) — which can themselves contain byte
            // sequences matching the core syncword, so a raw `find_sync` match
            // is not enough: a candidate is only a real boundary if its decoded
            // core size is plausible. `next_core_boundary` skips bogus matches.
            //
            // `forced` distinguishes a real next-core boundary from a forced
            // safety-valve flush. On a forced flush the access unit was NOT
            // closed by a new core sync, so the bytes following it are not a
            // fresh core frame and the current PES's PTS (which on a forced
            // flush is an extension-substream PES, carrying its own later
            // timestamp) must NOT become the next unit's PTS base.
            let mut forced = false;
            let au_end = match next_core_boundary(&self.buf, core_size) {
                NextCore::Found(end) => end,
                NextCore::NeedMore => break, // candidate sync needs more header
                NextCore::None => {
                    // No next core sync buffered yet. The trailing extension
                    // substream PES packets may still be arriving, so WAIT for
                    // them rather than emit a core-only (lossy) frame — unless
                    // the buffer has grown unreasonably large, in which case
                    // emit what we have to guarantee forward progress.
                    if self.buf.len() <= MAX_AU_BYTES {
                        break;
                    }
                    forced = true;
                    self.buf.len()
                }
            };

            let au: Vec<u8> = self.buf[..au_end].to_vec();
            // The AU's own core PES PTS (the PES covering its first byte, even if
            // that PES preceded the one(s) carrying its extensions or the next
            // core), stamped monotonically: honored when it advances past the
            // running clock (UHD one-AU-per-PES), but never allowed to collide
            // with the previous AU when several cores share ONE PES (DVD).
            let dur_ns = dts_core_duration_ns(&au) as i64;
            let au_pts = self.stamp_pts(self.front_pts(), dur_ns);
            frames.push(Frame {
                discontinuity: false,
                coding: None,
                source: None,
                pts_ns: au_pts,
                keyframe: true,
                data: au,
                duration_ns: Some(dur_ns as u64),
            });
            self.drain_front(au_end);
            // After draining, the marker covering the new front (if any) carries
            // the next AU's PTS; `pending_pts` is only the fallback when no
            // marker survives. Track it so the fallback stays sensible.
            self.pending_pts = self.front_pts();
            if forced {
                // Safety-valve flush: the next access unit's real core PES has
                // not arrived. Invalidate the PTS so the next PES sets it
                // regardless of buffer state, rather than inheriting this
                // (non-core) PES's timestamp.
                self.pending_pts = PTS_UNSET;
                self.pts_marks.clear();
            }
        }

        // Discard markers that no longer reference live buffer bytes (everything
        // past the buffer end can't happen, but collapse duplicates at offset 0
        // and drop a stale empty-buffer marker set).
        if self.buf.is_empty() {
            self.pts_marks.clear();
        }

        frames
    }

    fn flush(&mut self) -> Vec<Frame> {
        // End of stream: emit the final access unit still buffered (the last
        // core + its extension substreams, which had no following core sync to
        // close it during streaming). Require a complete core frame; drop a
        // bare partial sync tail.
        if find_sync(&self.buf, &DTS_CORE_SYNC) != Some(0) || self.buf.len() < CORE_HEADER_MIN_BYTES
        {
            self.buf.clear();
            return Vec::new();
        }
        let core_size = dts_core_frame_size(&self.buf);
        // `dts_core_frame_size` returns a 14-bit `fsize + 1` (never 0), so the
        // old `== 0` check was dead; reject a sub-minimum core like `parse()`.
        if core_size < MIN_CORE_FRAME_BYTES || self.buf.len() < core_size {
            self.buf.clear();
            return Vec::new();
        }
        // The final AU's PTS is the PES covering the buffer front (its core's
        // PES). Fall back to pending_pts, clamping the sentinel to 0.
        let au = std::mem::take(&mut self.buf);
        let dur_ns = dts_core_duration_ns(&au) as i64;
        let pts_ns = self.stamp_pts(self.front_pts(), dur_ns);
        self.pts_marks.clear();
        vec![Frame {
            discontinuity: false,
            coding: None,
            source: None,
            pts_ns,
            keyframe: true,
            data: au,
            duration_ns: Some(dur_ns as u64),
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
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
    Found(usize),
    /// A candidate core sync was found but its header isn't fully buffered yet,
    /// so its validity can't be decided — wait for more data.
    NeedMore,
    /// No (further) core sync found in the buffer.
    None,
}

/// Find the next *valid* core sync after the current core frame, to delimit the
/// access unit. Extension-substream payload can contain byte sequences that
/// match the core syncword, so each candidate is validated by decoding its
/// core size: a match whose decoded size is implausible (< MIN_CORE_FRAME_BYTES
/// or > MAX_AU_BYTES) is a false sync and is skipped, continuing the search.
/// Both DTS syncwords (core `0x7FFE8001`, EXSS `0x64582025`) are 32-bit words.
const SYNCWORD_BYTES: usize = DTS_CORE_SYNC.len();

/// DTS-HD extension-substream (EXSS) header field bit widths (ETSI TS 102 114,
/// ExtSS header). `bHeaderSizeType` selects the short form (`nuExtSSHeaderSize`
/// 8 bits, `nuExtSSFsize` 16 bits) or, for larger substreams, the long form
/// (12 / 20 bits).
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

/// DTS-HD extension substream (EXSS) total byte size — INCLUDING the
/// `0x64582025` syncword — read precisely from its header. `buf` must begin with
/// `DTS_HD_EXT_SYNC`. `None` when the size fields aren't fully buffered.
///
/// `nuExtSSFsize` is the total frame size in bytes minus one. Parsing it lets the
/// AU framer skip the extension by its exact length instead of scanning its
/// (arbitrary) payload for a core sync.
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

/// Offset where the current access unit ends (the start of the next core
/// frame). The AU is the core frame plus its trailing DTS-HD extension
/// substreams, which are skipped PRECISELY by their declared size — so a chance
/// core syncword inside the XLL lossless payload can never be mistaken for the
/// next AU boundary (the bug that truncated the extension and produced the
/// "Failed to decode block code(s)" class). Falls back to the heuristic core-sync
/// scan only when an extension can't be sized (malformed / truncated input).
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
                // Couldn't size it (truncated/garbage header) — heuristic fallback.
                _ => return scan_for_next_core(buf, pos),
            }
        } else if buf[pos..].starts_with(&DTS_CORE_SYNC) {
            // The bytes right after the precisely-skipped extensions are the next
            // core frame — the AU boundary.
            if buf.len() - pos < CORE_HEADER_MIN_BYTES {
                return NextCore::NeedMore;
            }
            let sz = dts_core_frame_size(&buf[pos..]);
            if (MIN_CORE_FRAME_BYTES..=MAX_AU_BYTES).contains(&sz) {
                return NextCore::Found(pos);
            }
            return scan_for_next_core(buf, pos); // implausible core here — fall back
        } else {
            // Neither a known extension nor a core sync at the precise boundary
            // (padding / junk) — fall back to the heuristic scan.
            return scan_for_next_core(buf, pos);
        }
    }
}

/// Heuristic fallback (the pre-fix behaviour): scan forward for the next core
/// syncword whose decoded size is plausible. Used only when precise extension
/// skipping can't proceed; a chance core syncword in extension payload usually
/// decodes to an implausible size and is skipped.
fn scan_for_next_core(buf: &[u8], from: usize) -> NextCore {
    let mut from = from;
    while let Some(rel) = find_sync(&buf[from..], &DTS_CORE_SYNC) {
        let pos = from + rel;
        if buf.len() - pos < CORE_HEADER_MIN_BYTES {
            return NextCore::NeedMore;
        }
        let sz = dts_core_frame_size(&buf[pos..]);
        if (MIN_CORE_FRAME_BYTES..=MAX_AU_BYTES).contains(&sz) {
            return NextCore::Found(pos);
        }
        from = pos + SYNCWORD_BYTES;
    }
    NextCore::None
}

/// DTS core frame size from header bits. `fsize` is the 14-bit field at bits
/// 46-59 of the header (bytes 5-7). On the wire `fsize` is the frame length
/// minus one, so this returns `fsize + 1`, i.e. the core frame length in bytes
/// (range 1..=16384). Callers treat the result as the actual byte length and
/// the MIN..=MAX range checks assume so.
///
/// Returns `0` when `data` is shorter than `CORE_HEADER_MIN_BYTES` — every call
/// site rejects that via the minimum-frame lower bound, so a `0` is never
/// mistaken for a valid tiny frame.
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

/// Samples in one DTS core frame: `(NBLKS + 1) * 32`. `NBLKS` (7 bits) is the
/// core-header PCM-sample-block count — the same field ffmpeg's `dca` decoder
/// uses to timestamp frames. Bit layout after the 32-bit sync: FTYPE(1) SHORT(5)
/// CPF(1) **NBLKS(7)** FSIZE(14) …, so NBLKS = byte4 bit0 + byte5 bits7-2.
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

    fn make_dts_core(size: usize) -> Vec<u8> {
        let fsize = size - 1;
        let mut data = vec![0u8; size];
        data[0..4].copy_from_slice(&DTS_CORE_SYNC);
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

    /// A real DTS-HD EXSS substream of `total` bytes (short header form), with an
    /// optional false DTS core syncword embedded in its payload (decoding to a
    /// plausible core size) — to prove precise sizing, not a payload scan, bounds
    /// the extension.
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

        // AU = core(512) + a REAL EXSS substream whose XLL payload embeds a DTS
        // core syncword decoding to a plausible size (512). The heuristic-only
        // framer would split here and truncate the lossless extension (the
        // Dunkirk `dca` "Failed to decode block code(s)" class). Precise EXSS
        // sizing spans the whole extension to the REAL next core.
        let core = make_dts_core(512);
        let exss = make_exss(600, Some(40));
        let next = make_dts_core(512);
        let mut buf = core.clone();
        buf.extend_from_slice(&exss);
        buf.extend_from_slice(&next);

        assert!(
            matches!(
                next_core_boundary(&buf, core.len()),
                NextCore::Found(end) if end == core.len() + exss.len()
            ),
            "AU must end at the REAL next core (after the full EXSS), not the false sync inside it"
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
        // B1: a partial DTS core is buffered, then a concealed gap (PES marked
        // discontinuity) carries a fresh core. The truncated partial must be
        // DROPPED — splicing it makes the framer emit a corrupt sub-core-length
        // AU (the Dunkirk `dca` "Failed to decode block code(s)" class) and
        // strands the rest. With the fix the post-gap core is the only AU, and it
        // carries the post-gap PTS (not the stale pre-gap one).
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
        // Both cores arrive in ONE PES — the DVD layout. AU1 keeps the PES PTS;
        // AU2 must ADVANCE by one frame duration to stay monotonic. Reusing the
        // single PES PTS for both was the "non monotonically increasing dts to
        // muxer" bug (fixed for 1.2.1).
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
        // The real-stream trigger: core1 in PES A (pts 100), then core2 + core3
        // arrive in a LATER PES B (pts 200). Processing PES B closes both AU1
        // (core1) and AU2 (core2) in a single parse() call. AU2's core arrived
        // in PES A region? No — here AU2 (core2) is in PES B, so it should be
        // 200. The defect to guard is AU1 NOT being overwritten to 200, and AU2
        // not inheriting an unrelated timestamp.
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
        // core1 + core2 both arrive in PES A (pts 100); core3 arrives in PES B
        // (pts 200). When PES B closes AU2 (core2, whose core was in PES A), AU2
        // must keep PES A's 100 — the bug was AU2 inheriting the closing PES's
        // 200.
        let mut parser = DtsParser::new();

        // PES A: core1 + core2. AU1 (core1) emits immediately (core2 boundary);
        // AU2 (core2) held awaiting a third core.
        let mut pes_a = make_dts_core(512);
        pes_a.extend_from_slice(&make_dts_core(600));
        let f = parser.parse(&make_pes(pes_a, Some(90000)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, pts_to_ns(90000), "AU1 PES A PTS");

        // PES B (realistically later): core3 — closes AU2 (core2). AU2's core
        // was in PES A, so it is PES A's 2nd frame: it advances one frame
        // duration from AU1 (still on PES A's timeline, and monotonic) — it does
        // NOT inherit the closing PES B PTS.
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
        // Punisher-DVD reproduction: a single PES carrying SEVERAL DTS core
        // frames (the DVD packing) must emit STRICTLY-increasing PTSs. The old
        // code stamped every AU with the one PES PTS, which ffmpeg rejected as
        // "non monotonically increasing dts to muxer: X >= X".
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
    fn new_pes_rebases_to_its_own_pts_no_drift() {
        // Regression for the drift bug: a global running clock overshot a
        // feature-long DTS track by minutes (2h44 for a 2h03 film). When a NEW
        // PES arrives whose PTS is BEHIND where accumulated frame durations
        // would put a running clock, the AU must re-base to that PES's OWN
        // timestamp — tracking the container, not drifting ahead of it.
        //
        // The re-base can make one emitted PTS sit just below the previous AU's
        // (a fresh PES whose PTS lands under the within-PES cursor). That is
        // CORRECT here and is NOT a muxer defect: the parser reports the true
        // container timestamps, and the mkv muxer applies the strictly-monotonic
        // per-track nudge to AUDIO at emit time (`mkv::block_ts` / `monotonic_ts`,
        // tested in `mkv.rs`), so the written block DTS is always monotonic. The
        // alternative — clamping in the parser — is what reintroduced the drift.
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

    /// Build a minimal DTS-HD extension substream of `size` bytes (just the
    /// sync + zero-padding). The parser delimits extensions by the next CORE
    /// sync, not by the extension's own size header, so a valid header isn't
    /// required — only that the bytes carry no spurious core sync.
    fn make_dts_ext(size: usize) -> Vec<u8> {
        let mut e = vec![0u8; size];
        e[0..4].copy_from_slice(&DTS_HD_EXT_SYNC);
        e
    }

    #[test]
    fn keeps_dts_hd_extension_in_separate_pes_packets() {
        // The real Blu-ray layout (ground-truthed on real UHD discs): the DTS core
        // arrives in one PES, then its DTS-HD MA extension substreams arrive
        // in SEPARATE following PES packets on the same PID. The parser must
        // stitch core + all trailing extensions into one access unit — not
        // emit a core-only (lossy) frame and drop the extension PES packets
        // (the lossy-core downgrade bug).
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

    /// Build 4 bytes that look like a DTS core sync but whose `fsize` field
    /// decodes to a tiny `core_size` (< MIN_CORE_FRAME_BYTES). With the
    /// dead-code guards this passed validation and could close an access unit
    /// at a junk boundary; with the fix it must be drained and resynced past.
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
        // A real core frame followed by an extension substream that happens to
        // contain a false core sync whose fsize decodes tiny. The bogus sync
        // must NOT close the real access unit early (dropping the rest of the
        // extension) nor emit a junk few-byte frame — it must be skipped, and
        // the whole core + extension preserved as one access unit.
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
        // MIN_CORE_FRAME_BYTES) — i.e. a "frame" smaller than the 96-byte ETSI
        // spec minimum — is a false sync inside extension payload and must NOT
        // close an access unit. Pick a decoded size of 64 (well inside the old
        // 10..96 false-positive window the raised floor now rejects).
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
        // When the buffer exceeds MAX_AU_BYTES with no next core sync, the
        // parser force-emits for forward progress. The current PES at that
        // point is an extension-substream PES (later PTS). The forced path must
        // NOT make that extension PTS the base of the NEXT access unit.
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
    fn codec_private_none() {
        let parser = DtsParser::new();
        assert!(parser.codec_private().is_none());
    }

    // --- dts_core_frame_size: 14-bit fsize extraction (ETSI TS 102 114) ---

    #[test]
    fn core_frame_size_bit_layout() {
        // fsize is 14 bits at bits 46-59: byte5[1:0] (high 2), byte6 (mid 8),
        // byte7[7:4] (low 4). Returned value is fsize + 1 (on-wire length-1).
        // Set fsize = 0x1FFF (= 8191): byte5 low2 = 0b01, byte6 = 0xFF,
        // byte7 high4 = 0xF (0xF0). (1<<12)|(0xFF<<4)|0xF = 0x1FFF → size 8192.
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
        // it, so its size can't be judged → the access unit can't be closed yet
        // (NeedMore → parse() breaks and waits). Build core(512) + a bare 2nd
        // sync with only the 4 syncword bytes buffered (< CORE_HEADER_MIN_BYTES
        // after it), so the candidate can't be validated.
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
            parser.buf.len() >= 512,
            "core1 retained while candidate boundary is undecided"
        );
    }

    #[test]
    fn multiple_false_syncs_in_extension_all_skipped() {
        // An extension body containing SEVERAL byte sequences that match the core
        // syncword but decode to sub-spec sizes must ALL be skipped; the AU is
        // closed only at the next real core. Guards the loop in
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
        assert_eq!(parser.buf.len(), 3, "only a 3-byte resync tail retained");
        assert_eq!(parser.buf, vec![0x22, 0x33, 0x44]);
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
        assert_eq!(parser.buf.len(), 3, "3-byte sync prefix retained");
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
        assert!(!parser.buf.is_empty(), "partial core header retained");
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
        parser.buf = d;
        assert!(parser.flush().is_empty(), "sub-spec core rejected at flush");
    }

    #[test]
    fn flush_rejects_core_extending_past_buffer() {
        // A valid-sized core header but with fewer bytes buffered than the
        // declared size must be dropped (never emit fewer bytes than declared).
        let mut parser = DtsParser::new();
        let core = make_dts_core(512);
        parser.buf = core[..300].to_vec(); // header says 512, only 300 present
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
        parser.buf = vec![0x7F, 0xFE, 0x80]; // 3 of 4 sync bytes
        assert!(parser.flush().is_empty());
        assert!(parser.buf.is_empty(), "buffer cleared on flush");
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
}
