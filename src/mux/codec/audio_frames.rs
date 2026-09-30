//! Bounded framing and timestamp accounting shared by ADTS and MPEG audio.
//!
//! A resync run (a failed header to the next locked frame, EOS or a gap) keeps invariants:
//! - I1: one run (corruption event) is exactly one verified fault; its other lost AUs are
//!   collateral, so they never feed the poison gate.
//! - I2: lost AUs are the clock skip taken, capped by the bytes (/ smallest frame + 1).
//! - I3: emitted PTS never repeat or go backwards within a stream key: a lock waits for the
//!   next timestamp, and an emission backstop continues from the last frame if needed.
//! - I4: consistent timestamps are authoritative: they place the lock and count the run.
//! - I5: without a timestamp, the lock takes the fewest slots the run allows (early, never
//!   late). PTS error is at most the AUs lost between the frame's bracketing timestamps.
//!
//! A lock after a run or a gap must chain to a valid header (in a new key, twice); two
//! candidates ending at one byte are both refused; at EOS a frame that cannot complete is none.
//! Limit, even on a clean stream: a flagged gap (a BD clip's discontinuity_indicator too), then
//! a key change with one frame before EOS or the next gap, loses that frame (it cannot chain
//! twice): at worst 1-2 AUs at the end of a title that changes format.

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
pub(super) struct SyncSpec {
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
// before it (not after a gap), and the latest PES timestamp met inside it.
struct Resync {
    skipped: usize,
    start: Option<i64>,
    advance: bool,
    mid: Option<i64>,
}

// Where a locked frame goes after a run, and how many AUs the run lost (exact: timestamps).
struct Placed {
    stamp: i64,
    lost: u64,
    exact: bool,
    // The timestamp jumped beyond what the run's bytes hold (an unflagged discontinuity).
    jumped: bool,
    bytes: usize,
}

/// Frames ADTS or MPEG audio. One verified fault per run (I1) keeps the poison verdict out of
/// reach while any frame decodes: a damaged track keeps its genuine frames, and its lost AUs
/// are counted (a lower bound where no later timestamp measured a run, see I5). Per project
/// principle; do not change without a user decision.
pub(super) struct AudioFrames {
    buf: PesBuf,
    sync: SyncSpec,
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
    // After a gap no frame is locked yet, so the first one must chain like a resync lock.
    unlocked: bool,
    // With no clock since a gap, the latest PES timestamp among bytes skipped so far.
    gap_pts: Option<i64>,
    // Bytes and runs placed by estimate since the clock was last pinned by a timestamp.
    unpinned: (usize, i64),
    // Buffer offset where two candidate AUs ended together (see coterminal).
    veto_end: Option<usize>,
    resync: Option<Resync>,
    jump_warned: bool,
    // I3 backstop: the last emitted frame's stream key, PTS and duration.
    emitted: Option<(u32, i64, u64)>,
    backstop_logged: bool,
    tally: DropTally,
}

impl AudioFrames {
    pub fn new(codec: &'static str, sync: SyncSpec) -> Self {
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
            unlocked: false,
            gap_pts: None,
            unpinned: (0, 0),
            veto_end: None,
            resync: None,
            jump_warned: false,
            emitted: None,
            backstop_logged: false,
            tally: DropTally::new(codec),
        }
    }

    /// Access units dropped as undecodable. A lower bound where no later timestamp measured
    /// a run (EOS, a gap): such a run counts the fewest AUs it allows (I5).
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
            // The PES after a gap may start inside an AU whose payload holds a false header.
            self.unlocked = true;
            self.gap_pts = None;
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

    // With no clock since a gap, the latest PES timestamp before the frame names an AU at or
    // before it (its PES's first): take it, early by the AUs lost between, never late (I5).
    fn anchor_after_gap(&mut self, consumed: usize) -> PesFacts {
        if self.anchor.is_none()
            && let Some(p) = self
                .buf
                .marks_snapshot()
                .into_iter()
                .rev()
                .find_map(|(at, f)| (at <= consumed).then(|| f.presentation_ns()).flatten())
                .or(self.gap_pts)
        {
            self.anchor = Some(p);
            self.next_pts = p;
        }
        self.anchor_at(consumed)
    }

    // Stamp the unit at `consumed`: a new PES timestamp re-anchors the running clock.
    fn anchor_at(&mut self, consumed: usize) -> PesFacts {
        let facts = self.buf.facts_at(consumed);
        if let Some(pts) = facts.presentation_ns()
            && self.anchor != Some(pts)
        {
            self.anchor = Some(pts);
            self.next_pts = pts;
            self.unpinned = (0, 0);
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
            let (twin, veto) = self.coterminal(data, consumed);
            if veto.is_some() {
                self.veto_end = veto;
            }
            let data = &self.buf.as_slice()[consumed..];
            let chained = chained && !twin;
            let mut placed = None;
            if chained && self.resync.is_some() && (self.sync.frame_len)(data).is_some() {
                let Some(p) = self.place_lock(consumed, eos) else {
                    break;
                };
                placed = Some(p);
            }
            let Some(h) = chained.then(|| header(data)).flatten() else {
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
                None => self.anchor_after_gap(consumed),
            };
            self.last_head = head;
            let duration = u64::from(h.samples) * 1_000_000_000 / u64::from(h.rate);
            self.backstop(head);
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
                self.emitted = Some(((self.sync.fixed)(&head), self.next_pts, duration));
            }
            self.next_pts = self.next_pts.saturating_add(duration as i64);
            self.last_frame = Some((h.bytes, duration));
            self.header_due = true;
            self.unlocked = false;
            consumed += h.bytes;
        }
        self.buf.drain(consumed);
        self.veto_end = self.veto_end.and_then(|e| e.checked_sub(consumed));
        frames
    }

    // Two AUs cannot end at one byte. While resyncing, a candidate with another header inside it
    // ending where it ends is ambiguous (either may be a false header in corrupt payload), so
    // both are refused: one good frame lost rather than a junk AU emitted.
    fn coterminal(&self, data: &[u8], at: usize) -> (bool, Option<usize>) {
        if self.resync.is_none() && !self.unlocked {
            return (false, None);
        }
        let Some(n) = (self.sync.frame_len)(data) else {
            return (false, None);
        };
        if self.veto_end == Some(at + n) {
            return (true, None);
        }
        let span = data.len().min(n);
        let inner =
            (1..span).any(|i| data[i] == 0xff && (self.sync.frame_len)(&data[i..]) == Some(n - i));
        (inner, inner.then_some(at + n))
    }

    // Outside a resync every header stands. Inside one, a frame counts if a valid header
    // follows it, or if it matches the last good frame's fixed header and ends at a sync-shaped
    // (corrupt) header or at EOS. `None` waits for more bytes. Pure: no parser state is touched.
    fn chains(&self, data: &[u8], min_header: usize, eos: bool) -> Option<bool> {
        // After a gap every header may lie in the carried fragment's payload, even at the PES's
        // first byte: the first frame chains like a resync lock.
        if self.resync.is_none() && !self.unlocked {
            return Some(true);
        }
        let Some(n) = (self.sync.frame_len)(data) else {
            return Some(false);
        };
        // A frame that cannot complete is no lock at EOS: scanning goes on past it (a false
        // header in corrupt payload would otherwise swallow the good frames after it).
        let Some(next) = data.get(n..) else {
            return eos.then_some(false);
        };
        // At EOS, in the stream's key, ending the data or before a truncated sync (random payload
        // holds false headers that may end anywhere).
        if next.len() < min_header {
            let sync_start = next.first().is_none_or(|&b| b == 0xff)
                && next
                    .get(1)
                    .is_none_or(|&b| b & self.sync.mask == self.sync.mask);
            let key_ok = self.last_frame.is_none() || self.matches_last(data);
            return eos.then_some(sync_start && key_ok);
        }
        let sync_next = next[0] == 0xff && next[1] & self.sync.mask == self.sync.mask;
        let Some(n2) = (self.sync.frame_len)(next) else {
            return Some(sync_next && self.matches_last(data));
        };
        if self.last_frame.is_none() || self.matches_last(data) {
            return Some(true);
        }
        // A key other than the stream's (a change during corruption, or a false header whose
        // length lands on a real frame) must chain twice within its own key.
        let key = (self.sync.fixed)(data);
        let same = |d: &[u8]| d.len() >= 4 && (self.sync.fixed)(d) == key;
        match next.get(n2..) {
            Some(nn) if nn.len() >= min_header => {
                Some(same(next) && same(nn) && (self.sync.frame_len)(nn).is_some())
            }
            _ if eos => Some(false),
            _ => None,
        }
    }

    fn matches_last(&self, data: &[u8]) -> bool {
        self.last_frame.is_some()
            && data.len() >= 4
            && (self.sync.fixed)(data) == (self.sync.fixed)(&self.last_head)
    }

    // A byte no frame starts at. Where a header was due, or at a sync-shaped byte outside a
    // run, a verified drop opens a run (I1). Inside one, a new PES timestamp is recorded (see
    // `holds`).
    fn skip_byte(&mut self, consumed: usize, sync: bool) {
        let due = std::mem::take(&mut self.header_due);
        if let Some(r) = &self.resync
            && let Some(p) = self.buf.facts_at(consumed).presentation_ns()
            && self.anchor != Some(p)
            && r.mid != Some(p)
        {
            if self.holds(r, p) {
                if let Some(r) = self.resync.as_mut() {
                    r.mid = Some(p);
                }
            } else {
                self.settle_run();
                self.anchor = None;
            }
        }
        if self.anchor.is_none() {
            self.gap_pts = self
                .buf
                .facts_at(consumed)
                .presentation_ns()
                .or(self.gap_pts);
        }
        if self.resync.is_none() && (sync || due) {
            let before = self.anchor;
            self.anchor_after_gap(consumed);
            if self.tally.is_poisoned() {
                self.tally
                    .record_collateral_drop(self.next_pts, 0, 1, "track-poisoned");
            } else {
                self.tally.record_drop(self.next_pts, 0, 1, "header");
            }
            self.resync = Some(Resync {
                skipped: 0,
                start: self.anchor.map(|_| self.next_pts),
                // Only a clock that ran before the run: after a gap its first sync-shaped byte may be
                // a false one in the carried fragment, even at the PES's first byte.
                advance: before.is_some(),
                mid: None,
            });
        }
        if let Some(r) = &mut self.resync {
            r.skipped += 1;
        }
    }

    // Can a new PES timestamp met mid-run belong to this run? Not before the run's start (time
    // does not go back), nor with no frame measured (nothing to check it by): either ends it.
    // A forward jump stays in the run: lost counts are capped by the bytes anyway (I2).
    fn holds(&self, r: &Resync, p: i64) -> bool {
        r.start.is_none_or(|start| p >= start) && self.last_frame.is_some()
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

    // AUs a run no frame ended (EOS, a gap, a timestamp jump) lost, by its bytes over the mean
    // kept frame: at least one (the verified fault), at most what the bytes hold (I2).
    fn lost_by_bytes(&self, r: &Resync) -> i64 {
        if self.sizes.2 == 0 {
            return 1; // no kept frame to measure by: only the verified fault is known
        }
        let n = self.by_bytes(r.skipped);
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
            .filter(|&p| self.anchor != Some(p) && r.mid != Some(p));
        let next = self.buf.marks_snapshot().into_iter().find_map(|(at, f)| {
            (at > consumed)
                .then(|| f.presentation_ns().map(|q| (q, at - consumed)))
                .flatten()
        });
        let d = self.last_frame.map_or(d_lock, |l| l.1 as i64).max(1);
        // With a clock to place it by, the lock waits for the next PTS. 13818-1 §2.7.4 (from
        // memory, not quoted) puts one at least every 0.7 s: inside MAX_HOLD below ~750 kbit/s;
        // past MAX_HOLD it takes the fewest slots, which cannot overshoot (I3).
        let adds_slots =
            r.advance || r.mid.is_some() || (r.start.is_some() && self.last_frame.is_some());
        let (stamp, exact) = match (fresh, next) {
            (Some(p), _) => (p, true),
            // Walk every chain from the lock to q. Unbroken, q places the lock exactly (I4);
            // otherwise it is estimated, leaving a slot for every walked frame and one lost AU
            // per break, and each later chain's lock is placed the same way (I3).
            (None, Some((q, end))) => match self.walk_to(data, end) {
                (walked, 0) => (q - walked, true),
                (walked, breaks) => (self.split(r, q, walked, breaks, d), false),
            },
            (None, None) if eos || !adds_slots => (self.estimate(r, d), false),
            (None, None) if data.len() > MAX_HOLD => (self.estimate(r, d), false),
            (None, None) => return None,
        };
        Some(self.count(r, stamp, exact, d))
    }

    // Lost AUs are the clock skip the placement takes (I4), at least the verified fault. A
    // skip the bytes cannot hold is an unflagged discontinuity, counted by bytes (I2).
    fn count(&self, r: &Resync, stamp: i64, exact: bool, d: i64) -> Placed {
        let Some(start) = r.start else {
            let lost = self.lost_by_bytes(r) as u64;
            return Placed {
                stamp,
                lost,
                exact: false,
                jumped: false,
                bytes: r.skipped,
            };
        };
        let skip = (stamp - start + d / 2).div_euclid(d);
        // Estimated runs since the clock was last pinned may have left their AUs to this skip.
        let (open_bytes, open_runs) = self.unpinned;
        if skip > self.cap(r.skipped + open_bytes) + open_runs {
            let lost = self.lost_by_bytes(r).min(skip) as u64;
            return Placed {
                stamp,
                lost,
                exact: false,
                jumped: true,
                bytes: r.skipped,
            };
        }
        Placed {
            stamp,
            lost: skip.max(1) as u64,
            exact,
            jumped: false,
            bytes: r.skipped,
        }
    }

    /// Stamp for a lock no timestamp places (EOS, a gap, no clock, MAX_HOLD, a walk break): the
    /// fewest slots the run allows, at a mid-run PTS or one past its start. Limit (I5): early by
    /// the AUs lost after that point, never late. A byte yardstick cannot tell a big AU's tail
    /// from several small AUs and can overshoot without bound, which the I3 backstop would carry.
    fn estimate(&self, r: &Resync, d: i64) -> i64 {
        match (r.mid, r.start) {
            (Some(p), _) => p,
            (None, Some(start)) => start + d * i64::from(r.advance),
            (None, None) => self.next_pts,
        }
    }

    // Time of the frames starting in `data[..end]` and the breaks between them: each chain is walked by header; after a break, the next chain starts at the first
    // header whose frame chains to another (or reaches `end`).
    fn walk_to(&self, data: &[u8], end: usize) -> (i64, i64) {
        let at = |i: usize| data.get(i..).and_then(self.sync.frame_len);
        let key = |i: usize| data.get(i..i + 4).map(self.sync.fixed);
        let chains = |i: usize| {
            key(i) == key(0) && at(i).is_some_and(|n| i + n >= end || at(i + n).is_some())
        };
        let (mut pos, mut walked, mut breaks) = (0, 0, 0);
        while pos < end {
            if let Some(n) = at(pos) {
                // Each frame's own duration: a key or rate change may lie on the way.
                walked += data.get(pos..).and_then(self.sync.frame_ns).unwrap_or(0) as i64;
                pos += n;
                continue;
            }
            breaks += 1;
            let next = (pos + 1..end)
                .find(|&i| data[i] == 0xff && chains(i))
                .unwrap_or(end);
            pos = next;
        }
        (walked, breaks)
    }

    // Lock stamp when breaks lie between it and q: the fewest slots the run allows (at a mid-run
    // PTS, or one past its start), below q by every later frame and break (I3, I5). A byte split
    // between run and breaks can err late, and a late lock would push later frames on.
    fn split(&self, r: &Resync, q: i64, walked: i64, breaks: i64, d: i64) -> i64 {
        self.estimate(r, d).min(q - walked - breaks * d)
    }

    // The lock is placed: count the run (the first lost AU was the verified fault), and stamp.
    fn end_run(&mut self, consumed: usize, p: Placed) -> PesFacts {
        self.resync = None;
        if p.jumped {
            self.warn_jump(p.stamp);
        }
        self.unpinned = if p.exact {
            (0, 0)
        } else {
            (self.unpinned.0 + p.bytes, self.unpinned.1 + 1)
        };
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

    // I3 guarantee, not the primary mechanism: within a stream key, never emit at or before the
    // last emitted PTS (a timestamp behind the clock, or a misplaced lock); continue from it.
    fn backstop(&mut self, head: [u8; 4]) {
        let Some((key, last, d)) = self.emitted else {
            return;
        };
        if key == (self.sync.fixed)(&head) && self.next_pts <= last {
            if !std::mem::replace(&mut self.backstop_logged, true) {
                tracing::debug!(target: "mux", pts_ns = self.next_pts, last_ns = last, "audio PTS would not advance; continuing from the last frame");
            }
            self.next_pts = last.saturating_add(d as i64);
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
mod fuzz;

#[cfg(test)]
mod tests {
    use super::*;

    // L044: once poisoned, later drops are collateral; they must not add to the verified count.
    #[test]
    fn drops_after_poison_are_collateral() {
        let mut af = AudioFrames::new(
            "test",
            SyncSpec {
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
