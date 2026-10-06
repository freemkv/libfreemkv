//! DTS / DTS-HD elementary stream parser.
//!
//! DTS core syncword: 0x7FFE8001 (32 bits).
//! DTS-HD MA/HRA extension syncword: 0x64582025 (32 bits), appears after the core frame.
//! Buffers across PES boundaries so frames spanning two PES packets
//! are emitted complete.

use super::startcode::BitReader;
use super::{CodecParser, Frame, PesPacket};

const DTS_CORE_SYNC: [u8; 4] = [0x7F, 0xFE, 0x80, 0x01];
// DTS-HD extension syncword.
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
    /// PTS of the access unit currently being assembled in `acc` (the unit
    /// starting at the first buffered core sync). Captured when that core
    /// frame's PES first arrived; the trailing extension-substream PES
    /// packets carry their own (later) PTS which must NOT override it.
    /// `PTS_UNSET` after a gap or a forced flush; `front_pts` falls back to it
    /// only when the front packet carries no timestamp.
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
    /// A core sync has been framed, so extension-only PES are DTS-HD extensions.
    core_seen: bool,
    /// Core-less DTS Express (EXSS only): frames are chained EXSS substreams.
    exss_only: bool,
    /// Last `(samples, rate)` read from EXSS static fields, for frames that omit them.
    exss_timing: Option<(u64, u64)>,
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
            core_seen: false,
            exss_only: false,
            exss_timing: None,
        }
    }

    /// Number of access units dropped as undecodable so far. Only the tally's
    /// end-of-stream summary logs it; no mux or CLI consumer reads this accessor.
    pub fn dropped_frames(&self) -> u64 {
        self.tally.dropped_frames()
    }

    /// Total decoded duration (ns) of all dropped access units — the length of
    /// audio silence introduced by dropping undecodable frames.
    pub fn dropped_duration_ns(&self) -> u64 {
        self.tally.dropped_duration_ns()
    }

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

    /// Best "most recent known base" (ns) for a PES that carries no PTS. Never
    /// resets the timeline to 0 while any prior base survives: prefer the
    /// current AU's captured base, then the projected next-AU start, then the
    /// last PES front. `pending_pts` going `PTS_UNSET` (e.g. after a forced
    /// flush) used to fall straight to 0, jumping the timeline backwards.
    fn continuation_base_ns(&self) -> i64 {
        if self.pending_pts >= 0 {
            self.pending_pts
        } else if self.next_pts_ns != PTS_UNSET {
            self.next_pts_ns
        } else if self.last_front_pts != PTS_UNSET {
            self.last_front_pts
        } else {
            0
        }
    }

    /// No core in the buffer: latch EXSS-only mode once `EXSS_ONLY_MIN_CHAIN` EXSS
    /// frames chain at their declared sizes with no core; a shorter run (a DTS-HD
    /// stream starting mid-AU or missing its first cores) is held, then dropped.
    fn probe_exss_only(&mut self) -> Step {
        let Some(p) = find_sync(self.acc.as_slice(), &DTS_HD_EXT_SYNC) else {
            return Step::NotExss;
        };
        if p > 0 {
            self.drain_front(p);
        }
        let buf = self.acc.as_slice();
        let (mut pos, mut chained) = (0, 0);
        while chained < EXSS_ONLY_MIN_CHAIN {
            if buf.len() < pos + SYNCWORD_BYTES {
                return Step::Break; // can't see what follows yet
            }
            if !buf[pos..].starts_with(&DTS_HD_EXT_SYNC) {
                break;
            }
            let Some(sz) = exss_frame_size(&buf[pos..]) else {
                return Step::Break; // header not fully buffered
            };
            if !(SYNCWORD_BYTES..=MAX_AU_BYTES).contains(&sz) {
                break;
            }
            if buf.len() < pos + sz {
                return Step::Break;
            }
            pos += sz;
            chained += 1;
        }
        if chained == EXSS_ONLY_MIN_CHAIN {
            self.exss_only = true;
        } else {
            // Chain broken: drop the leading extension (or bogus sync) and rescan.
            let first = exss_frame_size(buf)
                .filter(|sz| (SYNCWORD_BYTES..=MAX_AU_BYTES).contains(sz))
                .unwrap_or(SYNCWORD_BYTES);
            self.drain_front(first);
        }
        Step::Continue
    }

    /// EXSS-only mode: drop `n` resync bytes, counted as a (collateral) drop.
    fn drop_exss_junk(&mut self, n: usize) {
        if n > 0 {
            let pts = self.front_pts();
            self.tally.record_collateral_drop(pts, 0, n, "exss-resync");
            self.drain_front(n);
        }
    }

    /// EXSS frame duration; frames without static fields reuse the last parsed timing.
    fn exss_duration_ns(&mut self, au: &[u8]) -> u64 {
        if let Some(t) = exss_timing(au) {
            self.exss_timing = Some(t);
        }
        let (samples, rate) = self.exss_timing.unwrap_or((512, 48_000));
        (samples * 1_000_000_000 + rate / 2) / rate
    }

    /// Emit one frame per EXSS substream (core-less DTS Express).
    /// A valid core sync at or before the next EXSS leaves EXSS-only mode for good.
    fn frame_exss(&mut self, out: &mut Vec<Frame>) -> Step {
        let exss = find_sync(self.acc.as_slice(), &DTS_HD_EXT_SYNC);
        if let Some(q) =
            first_core_candidate(self.acc.as_slice(), 0).filter(|&q| exss.is_none_or(|p| q <= p))
        {
            if self.acc.len() - q < CORE_HEADER_MIN_BYTES {
                return Step::Break; // decide once the core header is buffered
            }
            self.exss_only = false;
            return Step::Continue;
        }
        let Some(p) = exss else {
            let tail = self.acc.len().saturating_sub(SYNCWORD_BYTES - 1);
            self.drop_exss_junk(tail);
            return Step::Break;
        };
        self.drop_exss_junk(p);
        let Some(sz) = exss_frame_size(self.acc.as_slice()) else {
            return Step::Break;
        };
        if !(SYNCWORD_BYTES..=MAX_AU_BYTES).contains(&sz) {
            self.drop_exss_junk(SYNCWORD_BYTES);
            return Step::Continue;
        }
        if self.acc.len() < sz {
            return Step::Break;
        }
        let au = self.acc.as_slice()[..sz].to_vec();
        let dur_ns = self.exss_duration_ns(&au) as i64;
        let pts = self.stamp_pts(self.front_pts(), dur_ns);
        let src = self.front_source();
        self.tally.record_kept();
        out.push(Frame {
            discontinuity: false,
            coding: None,
            source: src,
            pts_ns: pts,
            keyframe: true,
            data: au,
            duration_ns: Some(dur_ns as u64),
        });
        self.drain_front(sz);
        self.pending_pts = if self.acc.is_empty() {
            PTS_UNSET
        } else {
            self.front_pts()
        };
        Step::Continue
    }

    /// Drop `n` bytes from the front, rebasing attribution onto the new front.
    fn drain_front(&mut self, n: usize) {
        #[cfg(test)]
        DRAIN_CALLS.with(|c| c.set(c.get() + 1));
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

/// Loop control for the EXSS-only helpers; `NotExss` = no EXSS present.
enum Step {
    Continue,
    Break,
    NotExss,
}

// Test-only count of `drain_front` calls, proving a bogus-sync resync is one drain.
#[cfg(test)]
thread_local! {
    static DRAIN_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Hard cap on a buffered access unit (core + all its extension substreams).
/// A DTS-HD MA frame is at most a few tens of KB; if the buffer grows past
/// this without a clean boundary we resync rather than stall or balloon.
const MAX_AU_BYTES: usize = 65536;

// Chained core-less EXSS frames needed to enter EXSS-only mode, so the orphan
// extensions of one or two lost leading cores in a DTS-HD MA/HRA stream can't.
const EXSS_ONLY_MIN_CHAIN: usize = 3;

// "Enough bytes to read the fsize field" — a HEADER-LAYOUT minimum, distinct from
// MIN_CORE_FRAME_BYTES (the decoded-size validity floor).
const CORE_HEADER_MIN_BYTES: usize = 10;

// ETSI TS 102 114 on-wire FSIZE floor is 95, so a real core frame is at least 96 bytes; a
// smaller decoded size means a false/corrupt sync, so we resync rather than close an AU at a
// junk boundary.
const MIN_CORE_FRAME_BYTES: usize = 96;

// Sentinel for "no valid PTS base captured yet": real PTS-in-ns values are
// non-negative, so this can never collide. Marks the base invalid after a
// forced flush so the next PES sets it regardless of buffer state.
const PTS_UNSET: i64 = -1;

impl CodecParser for DtsParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        // A PTS-less PES after a gap continues from the pre-gap projection, not 0. Best
        // effort: the lost span is unknown, so such a PES lands early by the gap's length.
        let gap_carry = if self.next_pts_ns != PTS_UNSET {
            self.next_pts_ns
        } else {
            self.continuation_base_ns()
        };
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
            .unwrap_or_else(|| {
                if pes.discontinuity {
                    gap_carry
                } else {
                    self.continuation_base_ns()
                }
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
            if self.exss_only {
                match self.frame_exss(&mut frames) {
                    Step::Continue => continue,
                    _ => break,
                }
            }
            // Resync to the first candidate core sync; drop leading junk and any run of
            // bogus (implausibly sized) syncs in ONE drain rather than 4 bytes at a time.
            let Some(start) = first_core_candidate(self.acc.as_slice(), 0) else {
                if !self.core_seen {
                    match self.probe_exss_only() {
                        Step::Continue => continue,
                        Step::Break => break,
                        Step::NotExss => {}
                    }
                }
                // No candidate core sync yet — keep at most a 3-byte tail so a
                // sync split across PES packets can still be found next time.
                if self.acc.len() > 3 {
                    let tail = self.acc.len() - 3;
                    self.drain_front(tail);
                }
                break;
            };
            self.core_seen = true;
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
            // Plausible by construction: `first_core_candidate` skipped bogus sizes.
            let core_size = dts_core_frame_size(self.acc.as_slice());
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
            // After draining, the mark over the new front carries the next AU's PTS;
            // `pending_pts` is the fallback when none survives. A fully drained buffer
            // has no live front, so its offset-0 mark is the just-emitted AU's — invalidate rather than keep that stale PTS as a fallback for a later PTS-less PES.
            self.pending_pts = if self.acc.is_empty() {
                PTS_UNSET
            } else {
                self.front_pts()
            };
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
    // Emits the final buffered AU (core + extensions, gated through the decodability check) at
    // end of stream.
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
        // A trailing core sync at EOS opens a NEW access unit that never completed
        // (a complete next core would have closed this AU during parse()), so it is
        // truncated — drop it rather than splice onto the final AU; real trailing extensions are kept.
        let emit_end = final_au_end(self.acc.as_slice(), core_size);
        // The final AU's PTS is the PES covering the buffer front (its core's
        // PES). Fall back to pending_pts, clamping the sentinel to 0.
        let au = self.acc.as_slice()[..emit_end].to_vec();
        let dur_ns = dts_core_duration_ns(&au) as i64;
        let pts_ns = self.stamp_pts(self.front_pts(), dur_ns);
        let src = self.front_source();
        self.acc.clear();
        let mut out = Vec::new();
        self.emit_or_drop(au, pts_ns, dur_ns, src, &mut out);
        out
    }
}

/// Offset where the FINAL access unit ends at end-of-stream, mirroring `next_core_boundary`.
/// Complete extension substreams are kept; a plausible or truncated trailing core begins a
/// NEW AU that never closed, so the AU ends there; garbage or a truncated extension leaves
/// the core alone.
fn final_au_end(buf: &[u8], core_size: usize) -> usize {
    let mut pos = exss_start(buf, core_size);
    loop {
        let tail = &buf[pos..];
        if tail.is_empty() {
            return buf.len();
        }
        if tail.len() < SYNCWORD_BYTES {
            // A core-sync prefix is a truncated new AU (dropped); anything else is a
            // truncated extension or garbage, which leaves the core alone.
            return if DTS_CORE_SYNC.starts_with(tail) {
                pos
            } else {
                core_size
            };
        }
        if buf[pos..].starts_with(&DTS_HD_EXT_SYNC) {
            match exss_frame_size(&buf[pos..]) {
                // A fully-buffered extension substream belongs to this AU.
                Some(sz) if sz >= SYNCWORD_BYTES && buf.len() >= pos + sz => pos += sz,
                // Sized but truncated at EOS: a decoder would read past it; core only.
                Some(sz) if sz >= SYNCWORD_BYTES => return core_size,
                // Unsizeable: scan for the next core, as parse() does.
                _ => return first_core_candidate(buf, pos).unwrap_or(buf.len()),
            }
        } else if buf[pos..].starts_with(&DTS_CORE_SYNC) {
            // A trailing core (truncated — a complete one would have closed the AU during
            // parse) is a new AU that never completed: drop it. Plausibility as in
            // `next_core_boundary`: a false sync stays with this AU.
            return first_core_candidate(buf, pos).unwrap_or(buf.len());
        } else {
            // Garbage at the extension boundary: parse() emits the core alone.
            return core_size;
        }
    }
}

// Where extensions after a core begin: an EXSS is 4-byte aligned after a core whose size is
// not a multiple of 4, so skip up to 3 padding bytes to it.
fn exss_start(buf: &[u8], core_size: usize) -> usize {
    let aligned = core_size.next_multiple_of(4);
    let padded_exss = buf
        .get(aligned..)
        .is_some_and(|t| t.starts_with(&DTS_HD_EXT_SYNC));
    let direct = buf
        .get(core_size..)
        .is_some_and(|t| t.starts_with(&DTS_CORE_SYNC) || t.starts_with(&DTS_HD_EXT_SYNC));
    if padded_exss && !direct {
        aligned
    } else {
        core_size
    }
}

// First core sync at or after `from` whose header is truncated (undecidable) or whose size is
// plausible; bogus syncs before it are skipped in one pass.
fn first_core_candidate(buf: &[u8], from: usize) -> Option<usize> {
    let mut from = from;
    while let Some(rel) = find_sync(&buf[from..], &DTS_CORE_SYNC) {
        let pos = from + rel;
        if buf.len() - pos < CORE_HEADER_MIN_BYTES
            || (MIN_CORE_FRAME_BYTES..=MAX_AU_BYTES).contains(&dts_core_frame_size(&buf[pos..]))
        {
            return Some(pos);
        }
        from = pos + SYNCWORD_BYTES;
    }
    None
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

// Byte length shared by both 32-bit DTS syncwords (core and EXSS).
const SYNCWORD_BYTES: usize = DTS_CORE_SYNC.len();

// EXSS header field bit widths (ETSI TS 102 114). `bHeaderSizeType` selects short form
// (`nuExtSSHeaderSize` 8b, `nuExtSSFsize` 16b) or long (12/20b).
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

// EXSS total byte size (including the sync), read precisely from its header; `buf` must begin
// with DTS_HD_EXT_SYNC. Lets the AU framer skip the extension exactly rather than scanning its
// payload.
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

// `(samples, rate)` of one EXSS frame: `512 * (nuExSSFrameDurationCode + 1)` samples at the
// `nuRefClockCode` rate (32/44.1/48 kHz). `None` without static fields.
fn exss_timing(buf: &[u8]) -> Option<(u64, u64)> {
    let mut r = BitReader::new(buf.get(SYNCWORD_BYTES..)?);
    r.skip_bits(EXSS_USER_DEFINED_BITS + EXSS_INDEX_BITS)?;
    let (hbits, fbits) = if r.read_bits(EXSS_HEADER_SIZE_TYPE_BITS)? == EXSS_HEADER_SIZE_TYPE_LONG {
        (EXSS_HDRSIZE_BITS_LONG, EXSS_FSIZE_BITS_LONG)
    } else {
        (EXSS_HDRSIZE_BITS_SHORT, EXSS_FSIZE_BITS_SHORT)
    };
    r.skip_bits(hbits + fbits)?;
    if r.read_bit()? == 0 {
        return None;
    }
    let rate = match r.read_bits(2)? {
        0 => 32_000u64,
        1 => 44_100,
        _ => 48_000,
    };
    Some((512 * (r.read_bits(3)? as u64 + 1), rate))
}

// Offset where the current AU ends (start of the next core frame): trailing extensions are
// skipped PRECISELY by declared size, so a false core sync in XLL payload can't be mistaken for
// the boundary.
fn next_core_boundary(buf: &[u8], core_size: usize) -> NextCore {
    // Up to 3 alignment bytes may precede the EXSS; decide only once they are buffered.
    if buf.len() < core_size.next_multiple_of(4) + SYNCWORD_BYTES {
        return NextCore::NeedMore;
    }
    let mut pos = exss_start(buf, core_size);
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

// Heuristic fallback (pre-fix behaviour): scan for the next core syncword whose decoded size is
// plausible, used only when precise extension skipping can't proceed.
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

// `fsize` (14 bits, header bits 46-59) is length-minus-one on the wire; returns `fsize + 1`,
// the core length in bytes. `0` if `data` is short — every caller rejects that via the MIN
// floor.
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

// Samples per DTS core frame: `(NBLKS + 1) * 32`. NBLKS (7 bits, ETSI TS 102 114) = byte4 bit0
// + byte5 bits7-2, after FTYPE/SHORT/CPF.
fn dts_core_samples(data: &[u8]) -> u32 {
    if data.len() < CORE_HEADER_MIN_BYTES {
        return 512; // typical; only reached on a truncated header
    }
    let nblks = ((data[4] as u32 & 0x01) << 6) | (data[5] as u32 >> 2);
    (nblks + 1) * 32
}

/// DTS core sample rate (Hz) from `SFREQ` (4 bits: byte8 bits5-2); reserved
/// indices fall back to 48 kHz so the rate is never zero.
fn dts_core_sample_rate(data: &[u8]) -> u32 {
    if data.len() < CORE_HEADER_MIN_BYTES {
        return 48_000;
    }
    let sfreq = (data[8] as usize >> 2) & 0x0F;
    match DTS_CORE_SR_VALID[sfreq] {
        0 => 48_000,
        r => r,
    }
}

/// Duration of one DTS core access unit in nanoseconds: `samples / rate`,
/// rounded to nearest. This is what lets consecutive core frames packed in a
/// single DVD PES advance monotonically instead of colliding on one PES PTS.
fn dts_core_duration_ns(data: &[u8]) -> u64 {
    let samples = dts_core_samples(data) as u64;
    let rate = dts_core_sample_rate(data) as u64;
    (samples * 1_000_000_000 + rate / 2) / rate
}

// DTS core-header validity constants (ETSI TS 102 114): a NORMAL frame's `deficit_samples` must
// equal this; `npcmblocks` a multiple of DTS_SUBBAND_SAMPLES.
const DTS_PCMBLOCK_SAMPLES: u32 = 32;
const DTS_SUBBAND_SAMPLES: u32 = 8;
// Number of LEGAL 6-bit AMODE codes (ETSI TS 102 114 §5.3.1): 0-15 are defined channel
// arrangements (10-15 are 6/7/8-ch), only 16-63 are reserved.
const DTS_AMODE_COUNT: u32 = 16;
const DTS_LFE_FLAG_INVALID: u32 = 3;

// Sample rate (Hz) per core SFREQ code (ETSI TS 102 114 Table 6-4); `0` marks a reserved code
// (fails validation). Reserved: {0, 4, 5, 9, 10}.
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

// Decodability gate: ETSI TS 102 114 core-header validity checks. `Some` means genuinely
// undecodable; `None` also covers a truncated header (never false-drop our own buffer
// underrun).
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
#[path = "dts_tests.rs"]
mod tests;
