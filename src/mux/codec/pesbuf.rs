//! One accumulation buffer for parsers that assemble access units across PES
//! packets.
//!
//! The buffer owns the bytes and the marks together, so a unit's timestamp
//! and source offset always come from the PES that carried its first byte,
//! never taken from two different packets. See docs/pesbuf.md for the full
//! rationale.

use super::PesPacket;
use super::pts_to_ns;
use crate::pes::SourcePos;

// What a PES contributes to the bytes it carried, returned as a unit so a
// caller cannot mix fields from different packets. Timestamps are raw, as
// the packet had them; see docs/pesbuf.md for how codecs derive from them.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct PesFacts {
    /// Presentation time in NANOSECONDS, already derived (see `of`). Stored
    /// derived rather than raw so there is one derivation and no caller can
    /// pick a different one.
    pub pts_ns: Option<i64>,
    /// Byte offset of this PES's first ES byte within the title's feed — what
    /// identifies the clip a frame came from. `None` when the demuxer was fed
    /// without a base offset.
    pub source: Option<SourcePos>,
    /// Packets for this stream were lost before this PES.
    pub discontinuity: bool,
}

impl PesFacts {
    // The facts a PES packet carries, read straight off it. Every parser reads
    // timestamp/source/discontinuity from PesFacts, never PesPacket directly,
    // so they can't be mixed. See docs/pesbuf.md; PesBuf::front gives the same.
    pub(crate) fn of(pes: &PesPacket) -> Self {
        Self {
            pts_ns: pes.pts.or(pes.dts).map(pts_to_ns),
            source: pes.source,
            discontinuity: pes.discontinuity,
        }
    }

    // Same facts with the presentation time replaced by one the parser resolved
    // itself, for a packet with no timestamp continuing an earlier base.
    // Attribution is unchanged: still this packet's bytes and source offset.
    pub(crate) fn with_pts_ns(self, pts_ns: i64) -> Self {
        Self {
            pts_ns: Some(pts_ns),
            ..self
        }
    }

    // This unit's presentation time in nanoseconds — the ONE derivation. PTS and
    // DTS differ for reordering (B-frame) streams, so video derives display
    // order itself; see docs/pesbuf.md for audio/subtitle fallback rationale.
    pub(crate) fn presentation_ns(&self) -> Option<i64> {
        self.pts_ns
    }
}

// The facts of the packet covering `off` within a PesBuf::marks_snapshot.
// Same at-or-before rule as PesBuf::facts_at, for a scanner holding a
// snapshot rather than the buffer.
pub(crate) fn facts_for(marks: &[(usize, PesFacts)], off: usize) -> PesFacts {
    let mut found = PesFacts::default();
    for &(at, facts) in marks {
        if at > off {
            break;
        }
        found = facts;
    }
    found
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

    // Append a PES's payload, recording where its bytes begin. A PES with no
    // payload records nothing, so a zero-length mark can never shadow the
    // packet that actually carried the byte at a given offset.
    pub(crate) fn push(&mut self, pes: &PesPacket) {
        if pes.data.is_empty() {
            return;
        }
        self.marks.push_back((self.buf.len(), PesFacts::of(pes)));
        self.buf.extend_from_slice(&pes.data);
    }

    /// Append a payload under facts the caller resolved — for a parser that
    /// carries a timestamp forward across a packet that omitted one. Same
    /// attribution rule; only the timestamp differs from what the packet said.
    pub(crate) fn push_with(&mut self, data: &[u8], facts: PesFacts) {
        if data.is_empty() {
            return;
        }
        self.marks.push_back((self.buf.len(), facts));
        self.buf.extend_from_slice(data);
    }

    // The facts of the PES that carried the byte at `off`: the last mark at or
    // before it. Defaults when the buffer holds bytes that predate any mark
    // (a parser that seeded it directly).
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

    // Consume `n` bytes from the front, rebasing marks onto the new front. The
    // mark covering the new byte at offset 0 is retained even though its packet
    // started earlier; see docs/pesbuf.md for why dropping it misattributes.
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

    // Seed the buffer directly with bytes carrying no packet attribution, for
    // tests driving a parser's scanner without a demuxer. Facts for these bytes
    // default to absent, which is what an unattributed byte honestly is.
    #[cfg(test)]
    pub(crate) fn seed(&mut self, data: &[u8]) {
        self.buf.clear();
        self.marks.clear();
        self.buf.extend_from_slice(data);
    }

    /// Append unattributed bytes, keeping existing content and marks.
    #[cfg(test)]
    pub(crate) fn append_unattributed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// How many packet marks are held — a test hook for the bound on marks.
    #[cfg(test)]
    pub(crate) fn mark_count(&self) -> usize {
        self.marks.len()
    }

    /// Record a mark at the current end without appending bytes — a test hook
    /// for exercising mark bookkeeping directly.
    #[cfg(test)]
    pub(crate) fn mark_here(&mut self, facts: PesFacts) {
        self.marks.push_back((self.buf.len(), facts));
    }

    // The marks, for a scanner resolving facts at several offsets while the
    // buffer's bytes are borrowed elsewhere. Use with facts_for, which applies
    // the same at-or-before rule as PesBuf::facts_at.
    pub(crate) fn marks_snapshot(&self) -> Vec<(usize, PesFacts)> {
        self.marks.iter().copied().collect()
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
