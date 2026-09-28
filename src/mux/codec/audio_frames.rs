//! Bounded framing and timestamp accounting shared by ADTS and MPEG audio.
//!
//! A resync run is the bytes from a failed header to the next locked frame, EOS or a gap.
//! Every run rule keeps these invariants:
//! - I1: one run (corruption event) is exactly one verified fault; its other lost AUs are
//!   collateral, so they never feed the poison gate.
//! - I2: a run never counts more lost AUs than its bytes can hold (bytes / smallest legal
//!   frame + 1); a timestamp jump beyond that is an unflagged discontinuity.
//! - I3: emitted PTS never repeat or go backwards: after a run, output waits for the next
//!   timestamp; only EOS or a gap (nothing later to contradict it) uses a byte estimate.
//! - I4: consistent timestamps are authoritative: they place the lock and count the run.
//! - I5: where bytes must decide, the rule with the smaller worst-case error wins, and its
//!   limit is documented where the rule lives.

use super::dropgate::DropTally;
use super::pesbuf::{PesBuf, PesFacts};
use super::{Frame, PesPacket};

// Most bytes held after a lock while waiting for the next timestamp (see place_lock).
const MAX_HOLD: usize = 64 * 1024;

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
    /// Pure duration in ns of a valid frame at the slice head.
    pub frame_ns: fn(&[u8]) -> Option<u64>,
    /// Smallest legal frame in bytes: the I2 bound on the AUs a run's bytes can hold.
    pub min_frame: usize,
}

// A run: bytes skipped, the slot of its first lost AU (None: no clock), whether a clock ran
// before it (not after a gap), and the latest PES timestamp met inside it with `skipped` there.
struct Resync {
    skipped: usize,
    start: Option<i64>,
    advance: bool,
    mid: Option<(i64, usize)>,
}

// Where a locked frame goes after a run, and how many AUs the run lost (exact: timestamps).
struct Placed {
    stamp: i64,
    lost: u64,
    exact: bool,
}

/// Frames ADTS or MPEG audio. One verified fault per run (I1) keeps the poison verdict out of
/// reach while any frame decodes: a damaged track keeps its genuine frames, and every lost AU
/// is still counted. Per project principle; do not change without a user decision.
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
    jump_warned: bool,
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
            jump_warned: false,
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
            self.settle_run();
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
        self.buf.push_with(data, facts);
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
    // returning `bytes` beyond what is buffered. While resyncing, a header must chain, and the
    // lock waits until it can be placed (I3).
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
            let mut placed = None;
            if chained && self.resync.is_some() && (self.sync.frame_len)(data).is_some() {
                let Some(p) = self.place_lock(consumed, eos) else {
                    break;
                };
                placed = Some(p);
            }
            let Some(h) = header(data).filter(|_| chained) else {
                self.skip_byte(consumed, sync);
                consumed += 1;
                continue;
            };
            if data.len() < h.bytes {
                break;
            }
            let head = [data[0], data[1], data[2], data[3]];
            let data = data[h.skip..h.bytes].to_vec();
            let facts = match placed {
                Some(p) => self.end_run(consumed, p),
                None => self.anchor_at(consumed),
            };
            self.last_head = head;
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

    // A byte no frame starts at. Where a header was due, or at a sync-shaped byte outside a
    // run, a verified drop opens a run (I1). Inside one, a new PES timestamp is recorded if the
    // bytes so far can hold the AUs it implies, else the run ends as a discontinuity (I2).
    fn skip_byte(&mut self, consumed: usize, sync: bool) {
        let due = std::mem::take(&mut self.header_due);
        if let Some(r) = &self.resync
            && let Some(p) = self.buf.facts_at(consumed).presentation_ns()
            && self.anchor != Some(p)
            && r.mid.map(|m| m.0) != Some(p)
        {
            let skipped = r.skipped;
            if self.holds(r, p) {
                self.resync.as_mut().unwrap().mid = Some((p, skipped));
            } else {
                if self.last_frame.is_some() {
                    self.warn_jump(p);
                }
                self.settle_run();
                self.anchor = None;
            }
        }
        if self.resync.is_none() && (sync || due) {
            let before = self.anchor;
            self.anchor_at(consumed);
            if self.tally.is_poisoned() {
                self.tally
                    .record_collateral_drop(self.next_pts, 0, 1, "track-poisoned");
            } else {
                self.tally.record_drop(self.next_pts, 0, 1, "header");
            }
            self.resync = Some(Resync {
                skipped: 0,
                start: self.anchor.map(|_| self.next_pts),
                advance: before.is_some(),
                mid: None,
            });
        }
        if let Some(r) = &mut self.resync {
            r.skipped += 1;
        }
    }

    // I2: can the run's bytes hold the AUs from its start to timestamp `p`? Without a measured
    // frame (none kept yet) nothing can be checked, so a new timestamp ends the run instead.
    fn holds(&self, r: &Resync, p: i64) -> bool {
        let (Some(start), Some((_, d))) = (r.start, self.last_frame) else {
            return false;
        };
        let n = (p.saturating_sub(start) + d as i64 / 2).div_euclid(d.max(1) as i64);
        p >= start && n <= self.cap(r.skipped)
    }

    fn cap(&self, skipped: usize) -> i64 {
        (skipped / self.sync.min_frame.max(1) + 1) as i64
    }

    fn warn_jump(&mut self, p: i64) {
        if !std::mem::replace(&mut self.jump_warned, true) {
            tracing::warn!(target: "mux", pts_ns = p, "audio timestamp jump beyond the lost bytes; treated as a discontinuity");
        }
    }

    // Mean kept frame size for the stream key (the minimum legal size before any frame).
    fn yardstick(&self) -> usize {
        ((self.sizes.1 / self.sizes.2.max(1)) as usize).max(self.sync.min_frame.max(1))
    }

    fn by_bytes(&self, bytes: usize) -> i64 {
        Self::div_round(bytes, self.yardstick())
    }

    fn div_round(bytes: usize, y: usize) -> i64 {
        let y = y.max(1);
        ((bytes + y / 2) / y) as i64
    }

    // AUs between a recorded mid-run PTS and `skipped`, sized by the run's own AUs where the
    // PTS measured them (its bytes before `p` over the AUs `p` implies), else by the mean.
    fn after_mid(&self, r: &Resync, p: i64, s0: usize, start: i64, d: i64) -> (i64, i64) {
        let n = (p - start + d / 2).div_euclid(d);
        // VBR spans about 10x, so an implied size below a tenth of the mean is not believed.
        let floor = self.yardstick() / 10;
        let y = if n > 0 {
            (s0 / n as usize).max(floor)
        } else {
            self.yardstick()
        };
        (n, Self::div_round(r.skipped - s0, y))
    }

    // AUs the run lost by its bytes: exact up to a recorded mid-run PTS, estimated after it.
    // At least one (the verified fault) and at most what the bytes hold (I2).
    fn lost_by_bytes(&self, r: &Resync) -> i64 {
        if self.sizes.2 == 0 {
            return 1; // no kept frame to measure by: only the verified fault is known
        }
        let n = match (r.mid, r.start, self.last_frame) {
            (Some((p, s0)), Some(start), Some((_, d))) => {
                let (before, after) = self.after_mid(r, p, s0, start, d.max(1) as i64);
                before + after
            }
            _ => self.by_bytes(r.skipped),
        };
        n.clamp(1, self.cap(r.skipped))
    }

    // Place the frame locking at `consumed` after a run, or `None` to wait. A PES starting
    // here names it by its PTS; otherwise the next buffered PTS `q` fixes it at q - k frames,
    // k counted by walking headers (I4). With neither, only EOS or a gap may estimate (I3).
    fn place_lock(&self, consumed: usize, eos: bool) -> Option<Placed> {
        let r = self.resync.as_ref()?;
        let data = &self.buf.as_slice()[consumed..];
        let d_lock = (self.sync.frame_ns)(data).unwrap_or(0) as i64;
        let fresh = self
            .buf
            .facts_at(consumed)
            .presentation_ns()
            .filter(|&p| self.anchor != Some(p) && r.mid.map(|m| m.0) != Some(p));
        let next = self.buf.marks_snapshot().into_iter().find_map(|(at, f)| {
            (at > consumed)
                .then(|| f.presentation_ns().map(|q| (q, at - consumed)))
                .flatten()
        });
        let d = self.last_frame.map_or(d_lock, |l| l.1 as i64).max(1);
        // Only an estimate that adds slots (a prior clock or mid-run PTS) waits. 13818-1 §2.7.4
        // (recollection, not quoted) puts a PTS at least every 0.7 s: inside MAX_HOLD below ~750
        // kbit/s; past MAX_HOLD the lock takes the fewest slots, which cannot overshoot (I3).
        let adds_slots = r.advance || r.mid.is_some();
        let exact = match (fresh, next) {
            (Some(p), _) => Some(p),
            (None, Some((q, end))) => match self.frames_before(data, end) {
                Ok(k) => Some(q - k * d_lock),
                // The walk left the chain (another corruption before q): q measures nothing
                // about this lock. Estimate it, below q by the walked frames, a lost AU and the
                // next chain that reaches q, whose lock is then placed exactly (I3, I4).
                Err((k, at)) => {
                    let room = k + 1 + self.next_chain(data, at, end);
                    let stamp = self.estimate(r, d).min(q - room * d_lock);
                    return Some(Placed {
                        stamp: r.start.map_or(stamp, |s| stamp.max(s)),
                        lost: self.lost_by_bytes(r) as u64,
                        exact: false,
                    });
                }
            },
            (None, None) if eos || !adds_slots => None,
            (None, None) if data.len() > MAX_HOLD => {
                let stamp = r.mid.map_or(r.start.unwrap_or(self.next_pts) + d, |m| m.0);
                return Some(Placed {
                    stamp,
                    lost: self.lost_by_bytes(r) as u64,
                    exact: false,
                });
            }
            (None, None) => return None,
        };
        if let Some(stamp) = exact {
            return Some(match r.start {
                Some(start) if stamp >= start => {
                    let n = (stamp - start + d / 2).div_euclid(d);
                    if n <= self.cap(r.skipped) {
                        Placed {
                            stamp,
                            lost: n as u64,
                            exact: true,
                        }
                    } else {
                        Placed {
                            stamp,
                            lost: self.lost_by_bytes(r) as u64,
                            exact: false,
                        }
                    }
                }
                // No clock before the run, or a timestamp behind it: I3 keeps the clock.
                Some(start) => Placed {
                    stamp: start,
                    lost: self.lost_by_bytes(r) as u64,
                    exact: false,
                },
                None => Placed {
                    stamp,
                    lost: self.lost_by_bytes(r) as u64,
                    exact: false,
                },
            });
        }
        Some(Placed {
            stamp: self.estimate(r, d),
            lost: self.lost_by_bytes(r) as u64,
            exact: false,
        })
    }

    // Byte-estimated stamp for a lock no timestamp places. Limit (I5): with a mid-run PTS, the
    // bytes after it (sized by the run's own AUs) decide between a corrupt AU and a carried
    // fragment; off by one slot only when that stretch strays over half the run's AU size.
    fn estimate(&self, r: &Resync, d: i64) -> i64 {
        match (r.mid, r.start) {
            (Some((p, s0)), Some(start)) => p + self.after_mid(r, p, s0, start, d).1 * d,
            (Some((p, s0)), None) => p + self.by_bytes(r.skipped - s0) * d,
            (None, Some(start)) if r.advance => start + self.by_bytes(r.skipped).max(1) * d,
            (None, start) => start.unwrap_or(self.next_pts),
        }
    }

    // Frames starting in `data[..end]`, walked by header; Err((walked, offset)) if the chain
    // breaks first.
    fn frames_before(&self, data: &[u8], end: usize) -> Result<i64, (i64, usize)> {
        let (mut pos, mut k) = (0, 0);
        while pos < end {
            let Some(n) = data.get(pos..).and_then(self.sync.frame_len) else {
                return Err((k, pos));
            };
            pos += n;
            k += 1;
        }
        Ok(k)
    }

    // Frames of the first header chain after `from` that walks unbroken to `end` (0 if none).
    fn next_chain(&self, data: &[u8], from: usize, end: usize) -> i64 {
        (from + 1..end)
            .filter(|&i| data[i] == 0xff)
            .find_map(|i| self.frames_before(&data[i..], end - i).ok())
            .unwrap_or(0)
    }

    // The lock is placed: count the run (the first lost AU was the verified fault), and stamp.
    fn end_run(&mut self, consumed: usize, p: Placed) -> PesFacts {
        self.resync = None;
        let d = self.last_frame.map_or(0, |l| l.1);
        let dur = if p.exact { d } else { 0 };
        if p.exact && p.lost > 0 {
            self.tally.add_dropped_duration(d);
        }
        for _ in 1..p.lost {
            self.tally
                .record_collateral_drop(p.stamp, dur as i64, 0, "resync-lost");
        }
        let facts = self.buf.facts_at(consumed);
        if let Some(pts) = facts.presentation_ns() {
            self.anchor = Some(pts);
        }
        self.next_pts = p.stamp;
        facts
    }

    // Close a run no frame ended (EOS, a gap, a timestamp jump): count it by its bytes.
    fn settle_run(&mut self) {
        if let Some(r) = self.resync.take() {
            for _ in 1..self.lost_by_bytes(&r) {
                self.tally
                    .record_collateral_drop(self.next_pts, 0, 0, "resync-lost");
            }
        }
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
        self.settle_run();
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
                frame_ns: |_| None,
                min_frame: 7,
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
