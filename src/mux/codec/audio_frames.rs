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
    /// Mask over the byte after 0xFF. Both syncwords are "The bit string '1111 1111 1111'."
    /// ([13818-7 §8.1.1.2], [11172-3 §2.4.2.3]): 0xF0 for ADTS; 0xE0 for MPEG audio, whose
    /// twelfth bit MPEG 2.5 (not ISO) reuses.
    pub mask: u8,
    /// Pure size of a valid frame at the slice head (None if none), to find framing mid-stream.
    pub frame_len: fn(&[u8]) -> Option<usize>,
    /// Key of the header fields that stay the same for a stream (first four bytes given).
    pub fixed: fn(&[u8]) -> u32,
}

// A resync run after a verified drop: bytes skipped since, the clock slot of its first lost
// AU (if a clock is known), and whether a byte estimate may advance the clock (not after a gap).
struct Resync {
    skipped: usize,
    start: Option<i64>,
    advance: bool,
}

pub(super) struct AudioFrames {
    buf: PesBuf,
    sync: Sync,
    framed: bool,
    anchor: Option<i64>,
    next_pts: i64,
    // Size and duration of the last kept frame; duration is the slot length for a run's losses.
    last_frame: Option<(usize, u64)>,
    // Stream key and (sum, count) of kept frame sizes under it: the VBR yardstick for byte runs.
    sizes: (u32, u64, u64),
    last_head: [u8; 4],
    // A frame was just emitted, so a header is due next: a failure here is a lost AU.
    header_due: bool,
    resync: Option<Resync>,
    // A verified drop was counted in this PES; later false syncs in it are the same fault.
    drop_counted: bool,
    // Buffer offset of the newest PES's first byte, while it is being framed.
    pes_start: Option<usize>,
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
            sizes: (0, 0, 0),
            last_head: [0; 4],
            header_due: false,
            resync: None,
            drop_counted: false,
            pes_start: None,
            tally: DropTally::new(codec),
        }
    }

    pub fn dropped_frames(&self) -> u64 {
        self.tally.dropped_frames()
    }
    pub fn dropped_duration_ns(&self) -> u64 {
        self.tally.dropped_duration_ns()
    }
    #[cfg(test)]
    pub fn verified_dropped(&self) -> u64 {
        self.tally.verified_dropped()
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
            // Bytes cleared at a gap are a fragment of an AU cut by the loss, not counted as one;
            // an open run is settled from its skipped bytes.
            self.buf.clear();
            self.settle_run(None);
            self.anchor = None;
            self.header_due = false;
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
        self.pes_start = Some(self.buf.len());
        self.buf.push_with(data, facts);
        self.drop_counted = false;
        let frames = self.frame_buffered(min_header, false, header);
        self.pes_start = None;
        frames
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
            let head = [data[0], data[1], data[2], data[3]];
            let data = data[h.skip..h.bytes].to_vec();
            self.end_resync(consumed);
            self.last_head = head;
            let facts = self.anchor_at(consumed);
            let duration = u64::from(h.samples) * 1_000_000_000 / u64::from(h.rate);
            if self.tally.is_poisoned() {
                self.tally
                    .record_collateral_drop(self.next_pts, 0, h.bytes, "track-poisoned");
            } else {
                frames.push(Frame {
                    pts_ns: self.next_pts,
                    keyframe: true,
                    data,
                    duration_ns: Some(duration),
                    source: facts.source,
                    discontinuity: facts.discontinuity,
                    coding: None,
                });
                self.tally.record_kept();
                self.count_size(head, h.bytes);
            }
            self.next_pts = self.next_pts.saturating_add(duration as i64);
            self.last_frame = Some((h.bytes, duration));
            self.header_due = true;
            consumed += h.bytes;
        }
        self.buf.drain(consumed);
        frames
    }

    // Outside a resync every header stands. Inside one, a frame counts if a valid header
    // follows it, or if it matches the last good frame's fixed header and ends at a sync-shaped
    // (corrupt) header or at EOS. `None` waits for more bytes. Pure: no parser state is touched.
    fn chains(&self, data: &[u8], min_header: usize, eos: bool) -> Option<bool> {
        if self.resync.is_none() {
            return Some(true);
        }
        let Some(n) = (self.sync.frame_len)(data) else {
            return Some(false);
        };
        let Some(next) = data.get(n..) else {
            return eos.then_some(true);
        };
        if next.len() < min_header {
            return eos.then(|| next.is_empty() || self.matches_last(data));
        }
        let sync_next = next[0] == 0xff && next[1] & self.sync.mask == self.sync.mask;
        Some((self.sync.frame_len)(next).is_some() || (sync_next && self.matches_last(data)))
    }

    fn matches_last(&self, data: &[u8]) -> bool {
        self.last_frame.is_some()
            && data.len() >= 4
            && (self.sync.fixed)(data) == (self.sync.fixed)(&self.last_head)
    }

    // A byte no frame starts at. Where a header was due, or at the first sync-shaped byte of a
    // PES, a verified drop opens a run. [13818-1 §2.4.3.7] a PES's PTS belongs to the first AU
    // starting in it, so a new PTS there restarts an open run; the old one is settled first.
    fn skip_byte(&mut self, consumed: usize, sync: bool, min_header: usize) {
        let due = std::mem::take(&mut self.header_due);
        let running = self.resync.is_some();
        // An open run continues through bytes of a PES already scanned.
        let new_pes = self.pes_start.is_none_or(|s| consumed >= s);
        if (sync || due) && !self.drop_counted && (!running || new_pes) {
            self.drop_counted = true;
            let before = self.anchor;
            self.anchor_at(consumed);
            if !running || self.anchor != before {
                self.settle_run(self.anchor.filter(|_| running));
                if self.tally.is_poisoned() {
                    self.tally.record_collateral_drop(
                        self.next_pts,
                        0,
                        min_header,
                        "track-poisoned",
                    );
                } else {
                    self.tally
                        .record_drop(self.next_pts, 0, min_header, "header");
                }
                // Mirror limit: after a gap there is no clock (advance false), so a PES that
                // starts with a fragment and then a corrupt AU stamps its next good frame early.
                self.resync = Some(Resync {
                    skipped: 0,
                    start: self.anchor.map(|_| self.next_pts),
                    advance: before.is_some(),
                });
            }
        }
        if let Some(r) = &mut self.resync {
            r.skipped += 1;
        }
    }

    // A frame locks at `consumed`. A fresh PTS there ends the run exactly; otherwise the byte
    // estimate advances the clock, never past a timestamp already buffered after the lock.
    fn end_resync(&mut self, consumed: usize) {
        self.drop_counted = false;
        let fresh = self.buf.facts_at(consumed).presentation_ns();
        let lost_ns = self.settle_run(fresh.filter(|&p| self.anchor != Some(p)));
        if lost_ns == 0 {
            return;
        }
        self.next_pts = self.next_pts.saturating_add(lost_ns);
        let next = self
            .buf
            .marks_snapshot()
            .into_iter()
            .find_map(|(at, f)| (at > consumed).then_some(f.presentation_ns()).flatten());
        if let (Some(q), Some((_, d))) = (next, self.last_frame) {
            self.next_pts = self.next_pts.min(q.saturating_sub(d as i64));
        }
    }

    // Close any open run and count its lost AUs (the first was the verified drop, the rest are
    // collateral): exactly from timestamps when it ends at `end` with a known start, else skipped
    // bytes over the mean kept frame size (at least one). Returns the clock advance to apply.
    fn settle_run(&mut self, end: Option<i64>) -> i64 {
        let (Some(r), Some((_, duration))) = (self.resync.take(), self.last_frame) else {
            return 0;
        };
        let d = duration.max(1) as i64;
        let lost = match (end, r.start) {
            (Some(end), Some(start)) => (end.saturating_sub(start) + d / 2).div_euclid(d).max(0),
            _ => {
                let mean = (self.sizes.1 / self.sizes.2.max(1)).max(1) as usize;
                ((r.skipped + mean / 2) / mean).max(1) as i64
            }
        };
        for _ in 1..lost {
            self.tally
                .record_collateral_drop(self.next_pts, 0, 0, "resync-lost");
        }
        // A byte estimate needs a running clock (a fresh PTS re-anchors the clock anyway).
        if r.advance { lost.saturating_mul(d) } else { 0 }
    }

    // Running mean of kept frame sizes, restarted when the stream key changes.
    fn count_size(&mut self, head: [u8; 4], bytes: usize) {
        let key = (self.sync.fixed)(&head);
        if self.sizes.2 == 0 || self.sizes.0 != key {
            self.sizes = (key, 0, 0);
        }
        self.sizes.1 += bytes as u64;
        self.sizes.2 += 1;
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
        self.settle_run(None);
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
                fixed: |_| 0,
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
