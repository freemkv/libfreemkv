//! HDMV PGS (Presentation Graphics Stream) subtitle parser.
//!
//! PGS segments: PCS, WDS, PDS, ODS, END. Each PES packet starts with
//! one of those (segment_type byte at offset 0).
//!
//! Subtitle display lifecycle (BD spec): a "display" PCS
//! (number_of_composition_objects > 0) starts a visible subtitle, and a
//! later "empty" PCS (== 0) clears it. Preserve BOTH display sets: some
//! decoders rely on the empty PCS + END even when Matroska carries a duration.
//! Also set the visible block's `BlockDuration` to (clear_pts - display_pts).

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

const SEGMENT_PCS: u8 = 0x16;
const SEGMENT_END: u8 = 0x80;
// Upper bound on a pending display set's bytes (real sets are well under 1 MB).
// Caps a malformed stream that appends non-PCS segments forever without a PCS,
// dropping further appends until the next PCS resyncs — mirrors DTS/AC-3 caps.
const MAX_PGS_PENDING_BYTES: usize = 4 * 1024 * 1024;
// Fallback on-screen dwell (5 s) for a display set whose real end is unknown (EOS
// trailing set, no-PTS/truncated arms): a None duration on a DefaultDuration-less
// subtitle track leaves ffmpeg "Timestamps are unset in a packet" (issue #52).
const DEFAULT_PGS_DURATION_NS: u64 = 5_000_000_000;
// Cap on a COMPUTED span (clear_pts - display_pts): a missing intermediate PCS can
// inflate one set to minutes. Past this the value is untrusted and the fallback
// dwell is used instead; set well above any real dwell so long cues aren't clipped.
const MAX_PGS_DURATION_NS: u64 = 30_000_000_000;
// Offset of number_of_composition_objects in a PCS: 3-byte segment header +
// 10 bytes of PCS fields (video_w/h, frame_rate, comp_num, comp_state,
// palette_update, palette_id_ref) = 13.
const PCS_NUM_OBJECTS_OFFSET: usize = 13;
// Offset of the first composition_object's flags byte within a PCS PES payload:
// PCS header(13) + number_of_composition_objects(1) + object_id_ref(2) +
// window_id_ref(1) = 17. `forced_on_flag` is bit 0x40 of that byte (HDMV PCS).
const PCS_FIRST_OBJECT_FLAGS_OFFSET: usize = 17;
const PCS_FORCED_ON_FLAG: u8 = 0x40;

/// Whether an emitted PGS display-set frame is a FORCED subtitle — the
/// `forced_on_flag` (0x40) on its first composition object. The frame data
/// begins with the display PCS (segment type 0x16), so the flag is read
/// directly from it. Returns `None` when the block is not a display PCS with
/// a composition object (clear PCS, non-PCS segment, or truncated header).
pub fn display_set_is_forced(frame_data: &[u8]) -> Option<bool> {
    if frame_data.first() != Some(&SEGMENT_PCS) {
        return None;
    }
    if *frame_data.get(PCS_NUM_OBJECTS_OFFSET)? == 0 {
        return None; // clear PCS — no composition to classify
    }
    let flags = *frame_data.get(PCS_FIRST_OBJECT_FLAGS_OFFSET)?;
    Some(flags & PCS_FORCED_ON_FLAG != 0)
}

/// Accumulates the "is this PGS subtitle track a forced-narrative track?" verdict
/// from its display sets. A track is forced iff it displayed at least one subtitle
/// and EVERY display set carried the forced_on_flag — a dedicated forced track,
/// as opposed to a full track that merely has occasional forced signs.
///
/// This is the SINGLE classification used by both the MKV muxer (accumulating a
/// track's frames during a rip) and the `info`-time forced probe (feeding the
/// demuxed display sets), so both reach the identical verdict.
#[derive(Debug, Clone)]
pub struct ForcedTracker {
    has_display: bool,
    all_forced: bool,
    displays: u32,
    forced_displays: u32,
}

impl Default for ForcedTracker {
    fn default() -> Self {
        Self {
            has_display: false,
            all_forced: true,
            displays: 0,
            forced_displays: 0,
        }
    }
}

/// The disc-shaped facts about ONE subtitle track that a demotion decision
/// rests on — how many display sets were seen, and how many of them carried the
/// HDMV `forced_on_flag`.
///
/// Split out from [`ForcedTracker`] so the two places that can contradict a
/// vendor label (the scan-time probe, which accumulates per-extent evidence,
/// and the muxer, which holds a live tracker per track) feed the SAME rule.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct ForcedFacts {
    /// Display sets observed on this track.
    pub displays: u32,
    /// How many of them carried `forced_on_flag`.
    pub forced_displays: u32,
}

/// A track must have shown at least this many display sets before "none of them
/// was forced" is allowed to contradict a vendor forced label.
///
/// Absence is weak evidence on a handful of sets: a genuine forced-narrative
/// track is SMALL (measured shape: tens of display sets for a whole feature), so
/// a couple of unflagged sets is exactly what one looks like on a disc whose
/// authoring never sets the flag.
pub const DEMOTE_MIN_DISPLAY_SETS: u32 = 8;

/// ...and it must carry at least this fraction (1/N) of the display sets of the
/// BUSIEST subtitle track on the disc.
///
/// This is the shape test that separates the two populations. Measured: a
/// dedicated forced track carries a low-tens count of display sets for a whole
/// feature, a full dialogue track carries one to two thousand — two orders of
/// magnitude apart. A track sitting within a quarter of the busiest track's
/// count is a full track, whatever its label says; a track at one percent of it
/// is the forced-narrative track its label claims and must keep that label.
pub const DEMOTE_MIN_DISPLAY_SHARE_DIVISOR: u32 = 4;

/// Whether content evidence is strong enough to CONTRADICT a vendor label that says a track is
/// forced — i.e. to demote 1 → 0. Promotion needs no such gate; demotion requires, in order:
/// something observed at all; the flag IN USE (`disc_uses_forced_flag`, or on this very track);
/// and the track's SHAPE ([`DEMOTE_MIN_DISPLAY_SETS`], [`DEMOTE_MIN_DISPLAY_SHARE_DIVISOR`])
/// matching a full dialogue track rather than a forced-narrative one. `busiest_displays` is the
/// largest `displays` over every subtitle track judged together.
pub fn demotable(facts: ForcedFacts, disc_uses_forced_flag: bool, busiest_displays: u32) -> bool {
    if facts.displays == 0 {
        return false;
    }
    let flag_in_use = disc_uses_forced_flag || facts.forced_displays > 0;
    if !flag_in_use || facts.displays < DEMOTE_MIN_DISPLAY_SETS {
        return false;
    }
    // `displays >= busiest / DIVISOR`, multiplied out (u64: `displays` is a
    // disc-derived count, so the product must not be able to wrap).
    u64::from(facts.displays) * u64::from(DEMOTE_MIN_DISPLAY_SHARE_DIVISOR)
        >= u64::from(busiest_displays)
}

impl ForcedTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one emitted PGS block into the verdict. Non-display blocks (clear
    /// PCS, other segments) are ignored.
    pub fn observe(&mut self, frame_data: &[u8]) {
        if let Some(forced) = display_set_is_forced(frame_data) {
            self.has_display = true;
            self.all_forced &= forced;
            // Saturating: the counts drive a shape comparison between tracks, so
            // a pathological stream must pin them, never wrap (and never panic
            // on an overflow in a debug build).
            self.displays = self.displays.saturating_add(1);
            if forced {
                self.forced_displays = self.forced_displays.saturating_add(1);
            }
        }
    }

    /// The counts behind the verdict: how many display sets were seen and how
    /// many carried `forced_on_flag`. Feeds [`demotable`].
    pub fn facts(&self) -> ForcedFacts {
        ForcedFacts {
            displays: self.displays,
            forced_displays: self.forced_displays,
        }
    }

    /// Whether the track has already shown a NON-forced subtitle — i.e. its
    /// verdict is settled at "not forced" and further observation can be skipped
    /// (the early-exit the probe uses to avoid reading the whole clip).
    pub fn settled_not_forced(&self) -> bool {
        self.has_display && !self.all_forced
    }

    /// Whether ANY display set was observed. When false the track's forced state
    /// is unknown (no PGS content seen — e.g. an undecrypted/unread stream), so a
    /// probe should leave any existing (vendor-derived) flag untouched rather
    /// than assert "not forced".
    pub fn observed(&self) -> bool {
        self.has_display
    }

    /// Final verdict: forced iff it displayed subtitles and every one was forced.
    pub fn is_forced(&self) -> bool {
        self.has_display && self.all_forced
    }
}

/// Stateful parser that preserves PGS display and clear sets, adding durations
/// to visible subtitles. Implements [`CodecParser`].
pub struct PgsParser {
    /// The display set being accumulated, with the facts of the PES that
    /// STARTED it. A set spans PES packets — it opens on a display PCS and
    /// closes on the next one (or END for a clear) — so its timestamp and source are the
    /// opening packet's, never the closing packet's. Same rule the other
    /// buffering parsers get from `PesBuf::front`.
    pending: Option<(super::pesbuf::PesFacts, Vec<u8>)>,
    /// How many bytes of `pending`'s data [`Self::complete_clear_pts`] has already
    /// walked and confirmed are complete segments (no END found among them).
    /// Lets a pending clear set that accumulates many small appended PES
    /// resume the segment walk where it left off instead of re-walking from
    /// byte 0 on every PES — that rescan is O(n^2) in the number of appends
    /// (L026). Always 0 when `pending` doesn't hold a clear set; reset
    /// whenever `pending` is assigned a fresh set.
    clear_scan_offset: usize,
    /// A gap was seen: discard segments until the next PCS starts a display set.
    skip_to_pcs: bool,
    /// Test-only: total iterations of the `complete_clear_pts` walk loop,
    /// proving the walk stays O(n) in the number of appends rather than
    /// O(n^2) (L026).
    #[cfg(test)]
    scan_steps: u64,
}

impl Default for PgsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl PgsParser {
    /// Create a fresh PGS parser with no pending display set.
    pub fn new() -> Self {
        Self {
            pending: None,
            clear_scan_offset: 0,
            skip_to_pcs: false,
            #[cfg(test)]
            scan_steps: 0,
        }
    }

    /// True when `data` is whole segments ending in END.
    fn ends_with_end(data: &[u8]) -> bool {
        let (mut off, mut last) = (0, 0);
        while data.len() - off >= 3 {
            let size = 3 + usize::from(u16::from_be_bytes([data[off + 1], data[off + 2]]));
            if off + size > data.len() {
                return false;
            }
            last = data[off];
            off += size;
        }
        off == data.len() && last == SEGMENT_END
    }

    fn is_clear(data: &[u8]) -> bool {
        data.first() == Some(&SEGMENT_PCS) && data.get(PCS_NUM_OBJECTS_OFFSET) == Some(&0)
    }

    // Walk segment lengths for END (never search bitmap bytes for 0x80).
    // Resumes from `clear_scan_offset` rather than rescanning `data` from 0
    // every call, which is O(n^2) over many small appends (L026).
    fn complete_clear_pts(&mut self) -> Option<i64> {
        let (facts, data) = self.pending.as_ref()?;
        if !Self::is_clear(data) {
            return None;
        }
        let presentation_ns = facts.presentation_ns();
        let mut offset = self.clear_scan_offset.min(data.len());
        let mut found_end = false;
        #[cfg(test)]
        let mut steps = 0u64;
        while data.len() - offset >= 3 {
            #[cfg(test)]
            {
                steps += 1;
            }
            let rest = &data[offset..];
            let size = 3 + usize::from(u16::from_be_bytes([rest[1], rest[2]]));
            if size > rest.len() {
                break; // incomplete trailing segment; wait for more bytes
            }
            if rest[0] == SEGMENT_END && size == 3 && rest.len() == 3 {
                offset += size;
                found_end = true;
                break;
            }
            offset += size;
        }
        self.clear_scan_offset = offset;
        #[cfg(test)]
        {
            self.scan_steps += steps;
        }
        if found_end { presentation_ns } else { None }
    }

    // One emission path for clear, replacement, malformed input and EOF.
    // Missing end times use the existing fallback only for visible sets.
    fn emit_pending(&mut self, end_pts_ns: Option<i64>) -> Option<Frame> {
        let (facts, data) = self.pending.take()?;
        self.clear_scan_offset = 0;
        let start_pts = facts.presentation_ns().unwrap_or(0);
        let computed = end_pts_ns
            .map(|end| end.saturating_sub(start_pts).max(0) as u64)
            .unwrap_or(DEFAULT_PGS_DURATION_NS);
        // Clamp a pathologically large computed span to the cap itself, not
        // the much shorter 5s DEFAULT dwell (L053): the fallback made a long
        // sign or caption vanish early in players that honour BlockDuration.
        let duration = if Self::is_clear(&data) {
            // A clear is an instantaneous state change, not a visible cue.
            // The MKV writer rounds this up to its minimum duration tick.
            0
        } else if computed > MAX_PGS_DURATION_NS {
            MAX_PGS_DURATION_NS
        } else {
            computed
        };
        Some(Frame {
            discontinuity: false,
            coding: None,
            source: facts.source,
            pts_ns: start_pts,
            keyframe: true,
            data,
            duration_ns: Some(duration),
        })
    }
}

impl CodecParser for PgsParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        // Keep PTS as Option: collapsing a missing PTS to 0 gives a wrong start
        // and an absurd duration (full disc runtime). Well-formed BD PCS packets
        // always carry a PTS, so a missing one is a malformed-stream path we skip.
        let pts = pes.pts.map(pts_to_ns);

        let is_pcs = pes.data[0] == SEGMENT_PCS;

        // A PCS too short for number_of_composition_objects is malformed. Don't
        // let it fall to the non-PCS arm (would pollute the pending set): close
        // any pending set with a fallback and drop the header to resync on next PCS.
        if is_pcs && pes.data.len() <= PCS_NUM_OBJECTS_OFFSET {
            self.skip_to_pcs = true;
            return self.emit_pending(None).into_iter().collect();
        }

        let pcs_num_objects = if is_pcs {
            Some(pes.data[PCS_NUM_OBJECTS_OFFSET])
        } else {
            None
        };

        let mut out = Vec::new();
        match pcs_num_objects {
            // Every PCS starts a new set, including empty compositions. Keep
            // the clear's following WDS/END with it, rather than emitting orphan
            // packets that lose timing when display sets are merged.
            Some(_) => match pts {
                Some(start) => {
                    self.skip_to_pcs = false;
                    // A gap before this PCS cut the held set off from its END;
                    // closing it would emit a malformed display set.
                    if pes.discontinuity
                        && !self
                            .pending
                            .as_ref()
                            .is_some_and(|(_, d)| Self::ends_with_end(d))
                    {
                        self.pending = None;
                    }
                    self.clear_scan_offset = 0;
                    out.extend(self.emit_pending(Some(start)));
                    // The set's facts are THIS packet's — the one that opened
                    // it. `start` is that packet's PTS by construction.
                    self.pending = Some((super::pesbuf::PesFacts::of(pes), pes.data.clone()));
                }
                // A PCS with no PTS has an unknown start time. Don't
                // store it with a 0 sentinel (wrong start, absurd duration).
                // Flush any prior pending with a fallback and skip this one.
                None => {
                    self.skip_to_pcs = true;
                    out.extend(self.emit_pending(None));
                }
            },
            // Non-PCS first segment — either a continuation of the
            // current display set, or non-standard layout. If we have
            // a pending display, append; otherwise emit as-is.
            None => {
                // A gap: emit a complete held set (the lost PCS would have closed
                // it), drop a truncated one, and skip orphans up to the next PCS.
                if pes.discontinuity {
                    if self
                        .pending
                        .as_ref()
                        .is_some_and(|(_, d)| Self::ends_with_end(d))
                    {
                        out.extend(self.emit_pending(None));
                    }
                    self.pending = None;
                    self.clear_scan_offset = 0;
                    self.skip_to_pcs = true;
                }
                if self.skip_to_pcs {
                    return out;
                }
                if let Some((_, ref mut buf)) = self.pending {
                    // Bound accumulation: a well-formed display set is small.
                    // Past the cap, drop further appends (malformed stream);
                    // the next PCS will take/replace `pending` and resync.
                    if buf.len() + pes.data.len() <= MAX_PGS_PENDING_BYTES {
                        buf.extend_from_slice(&pes.data);
                    }
                } else if let Some(pts_ns) = pts {
                    // A lone non-PCS segment with a real PTS — pass it through.
                    // (A missing PTS falls through to the drop path below: a
                    // bitmap with no timing reference would land at 00:00:00.)
                    out.push(Frame {
                        discontinuity: false,
                        coding: None,
                        // Emitted straight from THIS packet, so its facts are
                        // this packet's -- the same rule as a pending set,
                        // which takes the facts of the packet that opened it.
                        source: super::pesbuf::PesFacts::of(pes).source,
                        pts_ns,
                        keyframe: true,
                        data: pes.data.clone(),
                        duration_ns: Some(DEFAULT_PGS_DURATION_NS),
                    });
                }
                // No pending set AND no PTS: drop it. Emitting at pts_ns=0 would
                // place a stray bitmap at 00:00:00.000 with no timing reference
                // — same reason the no-PTS PCS arms above avoid the 0 sentinel.
            }
        }

        // No need to wait for the next (possibly hour-distant) subtitle to
        // release a completed clear. Its timestamp is the opening PCS's PTS.
        if let Some(pts) = self.complete_clear_pts() {
            out.extend(self.emit_pending(Some(pts)));
        }
        out
    }

    fn flush(&mut self) -> Vec<Frame> {
        // A display set is only emitted when the next PCS arrives; at EOS there
        // is no follower, so without this the last subtitle would be silently
        // dropped. Keep the fallback duration for a display with no known end.
        self.emit_pending(None).into_iter().collect()
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

#[cfg(test)]
#[path = "pgs_tests_inline.rs"]
mod tests;

#[cfg(test)]
#[path = "pgs_tests.rs"]
mod lifecycle_tests;
