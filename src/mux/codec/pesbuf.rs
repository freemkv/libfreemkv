//! One accumulation buffer for parsers that assemble access units across PES
//! packets.
//!
//! A buffering parser has to answer the same question for every unit it emits:
//! *which PES contributed this unit's FIRST byte?* Its timestamp comes from
//! that PES, and so does the source byte offset that identifies which clip of a
//! multi-clip title the unit belongs to. The trailing PES packets that complete
//! the unit carry their own, later, values which must not override it.
//!
//! That question was answered three different ways. DTS kept a deque of
//! `(offset, pts)` markers and took the one covering offset 0 — correct. AC-3
//! kept a single carry-over timestamp. TrueHD kept its own. None of them
//! carried the source offset at all, so provenance existed only for video, and
//! a title whose clip marks could not be read from timestamps alone had nine
//! audio and subtitle tracks with nothing to place them by.
//!
//! Three spellings of one rule is how they drifted, so this is the one place it
//! lives. The buffer owns the bytes AND the marks, and returns a PES's facts
//! together — a parser cannot take the timestamp from one PES and the source
//! from another, because it does not assemble them itself.

use super::PesPacket;
use super::pts_to_ns;
use crate::pes::SourcePos;

/// What a PES contributes to the bytes it carried.
///
/// Returned as a unit so a caller cannot mix fields from different packets.
/// The timestamps are carried RAW, as the packet had them. This type answers
/// *which packet* a unit's facts come from — the question that was being
/// answered three different ways. How a given codec derives a timestamp from
/// that packet stays the codec's business: DVD subtitles read `pts` only and
/// fall back to 0, most audio takes `pts.or(dts)`. Deriving it here would have
/// changed those semantics silently while fixing provenance.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct PesFacts {
    /// Presentation timestamp in 90kHz ticks, as carried.
    pub pts: Option<i64>,
    /// Decode timestamp in 90kHz ticks, as carried.
    pub dts: Option<i64>,
    /// Byte offset of this PES's first ES byte within the title's feed — what
    /// identifies the clip a frame came from. `None` when the demuxer was fed
    /// without a base offset.
    pub source: Option<SourcePos>,
    /// Packets for this stream were lost before this PES.
    pub discontinuity: bool,
}

impl PesFacts {
    /// The facts a PES packet carries, read straight off it.
    ///
    /// Every parser reads its frame's timestamp, source and discontinuity from
    /// a `PesFacts` — never off a `PesPacket` field directly — so the three can
    /// never be taken from different packets. Parsers that assemble units
    /// across packets get theirs from [`PesBuf::front`]; this is the same value
    /// for a parser whose unit begins in the packet it is handed.
    pub(crate) fn of(pes: &PesPacket) -> Self {
        Self {
            pts: pes.pts,
            dts: pes.dts,
            source: pes.source,
            discontinuity: pes.discontinuity,
        }
    }

    /// This unit's presentation time in nanoseconds — the ONE derivation.
    ///
    /// PTS and DTS are not two spellings of one value: PTS is when to display,
    /// DTS is when to decode, and for a stream that REORDERS (video carrying
    /// B-frames) they differ, so reading DTS as a presentation time would be
    /// wrong. Reordering is handled by the video path, which reconstructs
    /// display order rather than calling this.
    ///
    /// For everything that reaches here — audio and subtitles — there is no
    /// reordering, so DTS *is* the presentation time, and falling back to it is
    /// reading the same value from whichever field the packet used. A fallback
    /// for a missing field, not a second rule: dvdsub read `pts` alone and
    /// returned 0 for a packet that carried only DTS.
    pub(crate) fn presentation_ns(&self) -> Option<i64> {
        self.pts.or(self.dts).map(pts_to_ns)
    }
}

/// Bytes accumulated across PES packets, each byte attributable to the packet
/// that carried it.
pub(crate) struct PesBuf {
    buf: Vec<u8>,
    /// `(offset of this PES's first byte within `buf`, its facts)`, ascending.
    /// Offsets are relative to the current front and are rebased on `drain`.
    marks: std::collections::VecDeque<(usize, PesFacts)>,
}

impl PesBuf {
    pub(crate) fn with_capacity(n: usize) -> Self {
        Self {
            buf: Vec::with_capacity(n),
            marks: std::collections::VecDeque::new(),
        }
    }

    /// Append a PES's payload, recording where its bytes begin.
    ///
    /// A PES carrying no payload records nothing: it contributed no byte, so it
    /// can never be the answer to "which packet carried the byte at offset N",
    /// and admitting a zero-length mark would let it shadow the packet that
    /// actually did.
    pub(crate) fn push(&mut self, pes: &PesPacket) {
        if pes.data.is_empty() {
            return;
        }
        self.marks.push_back((self.buf.len(), PesFacts::of(pes)));
        self.buf.extend_from_slice(&pes.data);
    }

    /// Append raw bytes attributed to the SAME PES as the bytes already at the
    /// end of the buffer. For a parser that rewrites or re-frames payload
    /// in-place rather than appending a packet verbatim.
    pub(crate) fn push_bytes(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// The facts of the PES that carried the byte at `off`.
    ///
    /// The last mark at or before `off`: bytes belong to the most recent packet
    /// that started at or before them. Defaults when the buffer holds bytes
    /// that predate any mark (a parser that seeded it directly).
    pub(crate) fn facts_at(&self, off: usize) -> PesFacts {
        let mut found = PesFacts::default();
        for &(at, facts) in &self.marks {
            if at > off {
                break;
            }
            found = facts;
        }
        found
    }

    /// The facts of the PES that carried the byte at the FRONT of the buffer —
    /// the first byte of the access unit a parser is about to emit, which is
    /// the whole point of this type.
    pub(crate) fn front(&self) -> PesFacts {
        self.facts_at(0)
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub(crate) fn len(&self) -> usize {
        self.buf.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Consume `n` bytes from the front, rebasing the marks onto the new front.
    ///
    /// The mark covering the new front byte is RETAINED at offset 0 even though
    /// its packet started earlier: those bytes are still that packet's. Dropping
    /// it would attribute the remainder of a straddling unit to whichever packet
    /// happened to start next — precisely the misattribution this type exists to
    /// prevent, and it would land at a clip boundary, the one place it matters.
    pub(crate) fn drain(&mut self, n: usize) {
        let n = n.min(self.buf.len());
        if n == 0 {
            return;
        }
        self.buf.drain(..n);
        let covering = self.facts_at(n);
        self.marks.retain(|&(at, _)| at > n);
        for m in &mut self.marks {
            m.0 -= n;
        }
        if self.marks.front().map(|&(at, _)| at) != Some(0) {
            self.marks.push_front((0, covering));
        }
    }

    pub(crate) fn clear(&mut self) {
        self.buf.clear();
        self.marks.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pes(data: &[u8], pts: Option<i64>, byte: Option<u64>) -> PesPacket {
        PesPacket {
            pid: 0x1100,
            pts,
            dts: None,
            data: data.to_vec(),
            source: byte.map(SourcePos::at_byte),
            discontinuity: false,
        }
    }

    /// The point of the type: a unit assembled from two packets is attributed
    /// to the one that carried its FIRST byte, not the one that completed it.
    #[test]
    fn a_unit_spanning_two_packets_belongs_to_the_packet_it_started_in() {
        let mut b = PesBuf::with_capacity(64);
        b.push(&pes(&[1, 2, 3], Some(90_000), Some(1_000)));
        b.push(&pes(&[4, 5, 6], Some(180_000), Some(2_000)));
        let f = b.front();
        assert_eq!(f.presentation_ns(), Some(pts_to_ns(90_000)));
        assert_eq!(f.source.unwrap().byte, 1_000, "the packet it STARTED in");
    }

    /// And the timestamp and the source come from the SAME packet — the
    /// property that cannot hold when each is derived separately.
    #[test]
    fn timestamp_and_source_always_come_from_one_packet() {
        let mut b = PesBuf::with_capacity(64);
        b.push(&pes(&[1, 2], Some(90_000), Some(10)));
        b.push(&pes(&[3, 4], Some(180_000), Some(20)));
        b.push(&pes(&[5, 6], Some(270_000), Some(30)));
        for off in 0..6usize {
            let f = b.facts_at(off);
            let want = match off {
                0 | 1 => (Some(pts_to_ns(90_000)), 10),
                2 | 3 => (Some(pts_to_ns(180_000)), 20),
                _ => (Some(pts_to_ns(270_000)), 30),
            };
            assert_eq!(
                (f.presentation_ns(), f.source.unwrap().byte),
                want,
                "offset {off}"
            );
        }
    }

    /// Draining a completed unit must leave the REMAINDER attributed to the
    /// packet that carried it, not to whichever packet starts next.
    #[test]
    fn draining_keeps_the_remainder_attributed_to_its_own_packet() {
        let mut b = PesBuf::with_capacity(64);
        b.push(&pes(&[1, 2, 3, 4], Some(90_000), Some(1_000)));
        b.push(&pes(&[5, 6], Some(180_000), Some(2_000)));
        // Emit a 2-byte unit: bytes 3 and 4 are still the FIRST packet's.
        b.drain(2);
        assert_eq!(b.front().source.unwrap().byte, 1_000);
        // Emit those two: now the front is genuinely the second packet's.
        b.drain(2);
        assert_eq!(b.front().source.unwrap().byte, 2_000);
        assert_eq!(b.front().presentation_ns(), Some(pts_to_ns(180_000)));
    }

    /// A payload-less PES contributed no byte, so it must not shadow the packet
    /// that did — otherwise the next unit takes a timestamp from a packet whose
    /// bytes are not in it.
    #[test]
    fn an_empty_packet_does_not_claim_the_next_units_bytes() {
        let mut b = PesBuf::with_capacity(64);
        b.push(&pes(&[1, 2], Some(90_000), Some(1_000)));
        b.push(&pes(&[], Some(180_000), Some(2_000)));
        assert_eq!(b.front().source.unwrap().byte, 1_000);
        assert_eq!(b.len(), 2, "an empty packet adds no bytes");
    }

    /// Draining everything and refilling must not resurrect a stale mark.
    #[test]
    fn a_fully_drained_buffer_takes_its_next_packets_facts() {
        let mut b = PesBuf::with_capacity(64);
        b.push(&pes(&[1, 2], Some(90_000), Some(1_000)));
        b.drain(2);
        assert!(b.is_empty());
        b.push(&pes(&[9], Some(450_000), Some(9_000)));
        assert_eq!(b.front().source.unwrap().byte, 9_000);
        assert_eq!(b.front().presentation_ns(), Some(pts_to_ns(450_000)));
    }

    /// The two ways a parser can obtain facts must agree when the unit begins
    /// in the packet just handed over — otherwise "one pattern" is two.
    #[test]
    fn a_unit_starting_in_this_packet_reads_the_same_either_way() {
        let p = pes(&[1, 2, 3], Some(90_000), Some(4_242));
        let mut b = PesBuf::with_capacity(16);
        b.push(&p);
        assert_eq!(b.front(), PesFacts::of(&p));
    }

    /// Over-draining is clamped rather than panicking: a parser that
    /// mis-sizes a unit must not take the process down.
    #[test]
    fn draining_past_the_end_is_clamped() {
        let mut b = PesBuf::with_capacity(16);
        b.push(&pes(&[1, 2], Some(90_000), Some(1)));
        b.drain(99);
        assert!(b.is_empty());
    }
}
