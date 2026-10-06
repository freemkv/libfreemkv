//! One accumulation buffer for parsers that assemble access units across PES
//! packets.
//!
//! The buffer owns the bytes and the marks together, so a unit's timestamp and source offset
//! always come from the PES that carried its first byte, never taken from two different
//! packets.

use super::PesPacket;
use super::pts_to_ns;
use crate::pes::SourcePos;

// What a PES contributes to the bytes it carried, returned as a unit so a caller cannot mix
// fields from different packets. Timestamps are raw, as the packet had them.
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
    // timestamp/source/discontinuity from PesFacts, never PesPacket directly, so they can't be
    // mixed.
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

    // This unit's presentation time in nanoseconds — the ONE derivation. PTS and DTS differ for
    // reordering (B-frame) streams, so video derives display order itself.
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

/// Internal hard cap on buffered bytes — defense-in-depth, mirroring
/// reorder.rs's `MAX_GOP_BYTES`/`MAX_GOP_FRAMES`. Even a caller that never
/// drains (a misbehaving or crafted-input codec) cannot grow this buffer
/// without bound: a push past the cap drops the OLDEST bytes to make room,
/// rebasing marks through `drain` so attribution of the surviving bytes stays
/// correct. Real callers cap their own access units far below this (DTS at 64
/// KiB), so this only ever fires on pathological input. Bounding the bytes also
/// bounds the mark count, since every mark is anchored to at least one byte.
const MAX_BUFFERED_BYTES: usize = 16 * 1024 * 1024;

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
        self.enforce_cap();
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
        self.enforce_cap();
    }

    /// Enforce [`MAX_BUFFERED_BYTES`] by dropping the oldest bytes when a push
    /// overflows it. `drain` rebases the marks, so the surviving bytes keep
    /// correct attribution and the mark count stays bounded with the bytes.
    fn enforce_cap(&mut self) {
        if self.buf.len() > MAX_BUFFERED_BYTES {
            let overflow = self.buf.len() - MAX_BUFFERED_BYTES;
            self.drain(overflow);
        }
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

    // Consume `n` bytes from the front, rebasing marks onto the new front. The mark covering
    // the new byte at offset 0 is retained even though its packet started earlier.
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
#[path = "pesbuf_tests.rs"]
mod tests;
