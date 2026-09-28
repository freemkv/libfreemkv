//! Bounded framing and timestamp accounting shared by ADTS and MPEG audio.

use super::dropgate::DropTally;
use super::pesbuf::{PesBuf, PesFacts};
use super::{Frame, PesPacket};

pub(super) struct Header {
    pub bytes: usize,
    pub skip: usize,
    pub samples: u32,
    pub rate: u32,
}

/// How a codec's syncframes are recognised without parser side effects.
pub(super) struct Sync {
    /// Mask over the byte after 0xFF: 0xF0 for ADTS's 12-bit sync, 0xE0 for MPEG's 11-bit.
    pub mask: u8,
    /// Pure size of a valid frame at the slice head (None if none), to find framing mid-stream.
    pub frame_len: fn(&[u8]) -> Option<usize>,
}

// A resync run after a verified drop: bytes skipped since, and whether they cost clock slots
// (not after a gap, where the leading bytes are a fragment, not a lost AU).
struct Resync {
    skipped: usize,
    advance: bool,
}

pub(super) struct AudioFrames {
    buf: PesBuf,
    sync: Sync,
    framed: bool,
    anchor: Option<i64>,
    next_pts: i64,
    // Size and duration of the last emitted frame: the yardstick for frames lost in a resync.
    last_frame: Option<(usize, u64)>,
    resync: Option<Resync>,
    // A verified drop was counted in this PES; later false syncs in it are the same fault.
    drop_counted: bool,
    tally: DropTally,
}

impl AudioFrames {
    pub fn new(codec: &'static str, sync: Sync) -> Self {
        Self {
            buf: PesBuf::with_capacity(8192),
            sync,
            framed: false,
            anchor: None,
            next_pts: 0,
            last_frame: None,
            resync: None,
            drop_counted: false,
            tally: DropTally::new(codec),
        }
    }

    pub fn dropped_frames(&self) -> u64 {
        self.tally.dropped_frames()
    }
    pub fn dropped_duration_ns(&self) -> u64 {
        self.tally.dropped_duration_ns()
    }

    pub fn parse(
        &mut self,
        pes: &PesPacket,
        min_header: usize,
        header: impl FnMut(&[u8]) -> Option<Header>,
    ) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        let facts = PesFacts::of(pes);
        if pes.discontinuity {
            self.buf.clear();
            self.anchor = None;
            self.resync = None;
        }
        if self.tally.is_poisoned() {
            self.tally.record_collateral_drop(
                facts.presentation_ns().unwrap_or(self.next_pts),
                0,
                pes.data.len(),
                "track-poisoned",
            );
            return Vec::new();
        }
        // Preserve the existing raw-AAC/nonframed passthrough contract. Once
        // sync has been seen, later nonsync bytes are continuations, not units.
        let mut data = &pes.data[..];
        if !self.framed && self.buf.is_empty() && data[0] != 0xff {
            // A stream joined mid-frame: drop the fragment before a confirmed header.
            if let Some(at) = self.first_confirmed_header(data) {
                tracing::debug!(target: "mux", bytes = at, "skipped a leading partial audio frame");
                data = &data[at..];
            }
        }
        if !self.framed && self.buf.is_empty() && data[0] != 0xff {
            self.tally.record_kept();
            let pts_ns = facts.presentation_ns().unwrap_or(self.next_pts);
            self.next_pts = pts_ns;
            return vec![Frame {
                pts_ns,
                keyframe: true,
                data: pes.data.clone(),
                source: facts.source,
                discontinuity: facts.discontinuity,
                ..Frame::default()
            }];
        }
        self.framed = true;
        // Audio PES packets are small. Refuse pathological accumulation before
        // copying; a corrupt size must not grow memory without bound.
        if self.buf.len().saturating_add(pes.data.len()) > 1024 * 1024 {
            self.buf.clear();
            self.tally.record_drop(
                facts.presentation_ns().unwrap_or(self.next_pts),
                0,
                pes.data.len(),
                "buffer-limit",
            );
            return Vec::new();
        }
        self.buf.push_with(data, facts);
        self.drop_counted = false;
        self.frame_buffered(min_header, false, header)
    }

    // Offset of the first sync whose frame is chained to the next one or ends the packet.
    fn first_confirmed_header(&self, data: &[u8]) -> Option<usize> {
        let sized_at = |i: usize| data.get(i..).and_then(self.sync.frame_len);
        (1..data.len()).find(|&i| {
            sized_at(i).is_some_and(|n| i + n == data.len() || sized_at(i + n).is_some())
        })
    }

    // Stamp the unit at `consumed`: a new PES timestamp re-anchors the running clock.
    fn anchor_at(&mut self, consumed: usize) -> PesFacts {
        let facts = self.buf.facts_at(consumed);
        if let Some(pts) = facts.presentation_ns()
            && self.anchor != Some(pts)
        {
            self.anchor = Some(pts);
            self.next_pts = pts;
        }
        facts
    }

    // Frames every complete unit at the buffer front; a header callback asks to wait by
    // returning `bytes` beyond what is buffered. While resyncing, a header must chain.
    fn frame_buffered(
        &mut self,
        min_header: usize,
        eos: bool,
        mut header: impl FnMut(&[u8]) -> Option<Header>,
    ) -> Vec<Frame> {
        let mut frames = Vec::new();
        let mut consumed = 0;
        while self.buf.len() - consumed >= min_header {
            let data = &self.buf.as_slice()[consumed..];
            let sync = data[0] == 0xff && data[1] & self.sync.mask == self.sync.mask;
            let Some(chained) = self.chains(data, min_header, eos) else {
                break;
            };
            let Some(h) = header(data).filter(|_| chained) else {
                self.skip_byte(consumed, sync, min_header);
                consumed += 1;
                continue;
            };
            if data.len() < h.bytes {
                break;
            }
            let data = data[h.skip..h.bytes].to_vec();
            self.end_resync();
            let facts = self.anchor_at(consumed);
            let duration = u64::from(h.samples) * 1_000_000_000 / u64::from(h.rate);
            frames.push(Frame {
                pts_ns: self.next_pts,
                keyframe: true,
                data,
                duration_ns: Some(duration),
                source: facts.source,
                discontinuity: facts.discontinuity,
                coding: None,
            });
            self.next_pts = self.next_pts.saturating_add(duration as i64);
            self.last_frame = Some((h.bytes, duration));
            self.tally.record_kept();
            consumed += h.bytes;
        }
        self.buf.drain(consumed);
        frames
    }

    // Outside a resync every header stands. Inside one, a frame counts only if another valid
    // header follows it (or, at EOS, it ends the data); `None` waits for more bytes. Checked
    // with the pure `frame_len` so a rejected candidate never reaches the parser's header.
    fn chains(&self, data: &[u8], min_header: usize, eos: bool) -> Option<bool> {
        if self.resync.is_none() {
            return Some(true);
        }
        let Some(n) = (self.sync.frame_len)(data) else {
            return Some(false);
        };
        match data.get(n..) {
            None if eos => Some(true),
            Some([]) if eos => Some(true),
            Some(next) if next.len() < min_header && !eos => None,
            Some(next) => Some((self.sync.frame_len)(next).is_some()),
            None => None,
        }
    }

    // A byte no frame starts at. The first sync-looking one in a PES is a verified drop that
    // opens (or, under a new timestamp, restarts) the resync run the skipped bytes count toward.
    fn skip_byte(&mut self, consumed: usize, sync: bool, min_header: usize) {
        if sync && !self.drop_counted {
            self.drop_counted = true;
            let before = self.anchor;
            self.anchor_at(consumed);
            self.tally
                .record_drop(self.next_pts, 0, min_header, "header");
            if self.resync.is_none() || self.anchor != before {
                let advance = before.is_some();
                self.resync = Some(Resync {
                    skipped: 0,
                    advance,
                });
            }
        }
        if let Some(r) = &mut self.resync {
            r.skipped += 1;
        }
    }

    // A frame was found: the lost AUs (skipped bytes over the last frame's size, at least one)
    // keep their slots on the clock; their reported duration stays unmeasured.
    fn end_resync(&mut self) {
        self.drop_counted = false;
        if let (Some(r), Some((bytes, duration))) = (self.resync.take(), self.last_frame)
            && r.advance
        {
            let lost = ((r.skipped + bytes / 2) / bytes).max(1) as u64;
            self.next_pts = self
                .next_pts
                .saturating_add(lost.saturating_mul(duration) as i64);
        }
    }

    pub fn flush(&mut self) -> Vec<Frame> {
        self.flush_with(0, |_| None)
    }

    // Before a discontinuity clears the buffer: frame what `header` can size with no more
    // data coming (as at EOS), then drop the rest.
    pub fn drain_before_gap(
        &mut self,
        min_header: usize,
        header: impl FnMut(&[u8]) -> Option<Header>,
    ) -> Vec<Frame> {
        let frames = self.frame_buffered(min_header, true, header);
        self.buf.clear();
        frames
    }

    // EOS: frame what `header` can size now that no more data is coming, then drop the
    // rest. A trailing partial syncframe is not decodable; never manufacture one.
    pub fn flush_with(
        &mut self,
        min_header: usize,
        header: impl FnMut(&[u8]) -> Option<Header>,
    ) -> Vec<Frame> {
        let frames = if min_header > 0 {
            self.frame_buffered(min_header, true, header)
        } else {
            Vec::new()
        };
        self.buf.clear();
        self.tally.log_summary();
        frames
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // L044: once poisoned, later drops are collateral; they must not add to the verified count.
    #[test]
    fn drops_after_poison_are_collateral() {
        let mut af = AudioFrames::new(
            "test",
            Sync {
                mask: 0xf0,
                frame_len: |_| None,
            },
        );
        while !af.tally.is_poisoned() {
            af.tally.record_drop(0, 0, 1, "bad");
        }
        let verified = af.tally.verified_dropped();
        let pes = PesPacket {
            source: None,
            pid: 0x1100,
            pts: Some(0),
            dts: None,
            data: vec![0xFF; 16],
            discontinuity: false,
        };
        assert!(af.parse(&pes, 7, |_| None).is_empty());
        assert_eq!(
            af.dropped_frames(),
            verified + 1,
            "the poisoned PES is counted"
        );
        assert_eq!(af.tally.verified_dropped(), verified);
    }
}
