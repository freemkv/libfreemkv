//! Which pictures of a one-picture-per-PES video stream (H.264, HEVC, VC-1) a decoder can
//! reconstruct from the pictures this output holds. MPEG-2 decides per GOP in its own parser.
//!
//! Pictures are decided in decode order against the anchors the output holds: none at the
//! stream start, at a join (the decode clock or PTS moves at a random-access picture), at a
//! broken link, or after a gap (from the keyframe the `ResyncGate` resumes on; until then
//! the gate drops and counts). A picture that needs a missing anchor is dropped and its
//! discontinuity flag carries to the next emitted frame. Dropped pictures keep their display
//! slots, so every kept frame keeps its PTS.

use super::Frame;

/// What a picture needs from the pictures before it in decode order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Need {
    /// Not a picture, or one decodable from its own random-access picture: kept.
    Nothing,
    /// The frame's keyframe. `closed`: its leading pictures need no earlier picture;
    /// `broken`: the anchor before it is not the one its leading pictures were coded against.
    Rap { closed: bool, broken: bool },
    /// Intra picture that is not a random-access point: kept, and an anchor.
    Intra,
    /// Predicted anchor: needs the last anchor, then is one.
    Anchor,
    /// Leading picture: needs the anchor before its random-access picture unless that is closed.
    Leading,
    /// Leading when displayed before its random-access picture, else trailing.
    ByPts,
    /// Trailing non-anchor picture: needs one anchor.
    Trailing,
}

// Most leading pictures assumed where only PTS tells a join (no DTS): no stream bounds how
// many pictures display before a random-access picture and decode after it.
const MAX_LEADING: i64 = 16;

// Frame period assumed before one is measured (24 fps).
const FALLBACK_PERIOD_NS: i64 = 1_000_000_000 / 24;

// PTS steps below this are not a frame period (none is under 1/120 s).
const MIN_PERIOD_NS: i64 = 5_000_000;

/// Per-stream decodability state; see the module docs.
pub(crate) struct Decodable {
    /// Anchors the output holds, saturating at 2.
    held: u8,
    /// Whether the current random-access picture is closed.
    rap_closed: bool,
    /// PTS (unwrapped ns) of the current random-access picture.
    rap_pts: Option<i64>,
    /// A discontinuity was seen; the held anchors reset at the next keyframe.
    gap: bool,
    /// A dropped picture's discontinuity flag, carried to the next emitted frame.
    carry_discontinuity: bool,
    /// Highest PTS seen (unwrapped ns): the display frontier a random-access picture continues.
    high: Option<i64>,
    /// Last PTS seen (unwrapped ns), for the frame period.
    last: Option<i64>,
    /// Smallest PTS step seen between consecutive pictures (ns).
    period: Option<i64>,
    /// Decode time (unwrapped ns: DTS, else PTS) of the last picture.
    last_decode: Option<i64>,
    /// Accumulated 33-bit wrap offset (ns).
    wrap: i64,
    /// Whether a random-access picture always displays after every earlier picture (HEVC
    /// IRAP, VC-1 entry point), so any backward step at one is a join.
    strict: bool,
}

// 2^33 90 kHz ticks in ns: the PTS wrap.
const WRAP_NS: i64 = (1i64 << 33) * 100_000 / 9;

impl Decodable {
    pub(crate) fn new(strict: bool) -> Self {
        Self {
            held: 0,
            rap_closed: false,
            rap_pts: None,
            gap: false,
            carry_discontinuity: false,
            high: None,
            last: None,
            period: None,
            last_decode: None,
            wrap: 0,
            strict,
        }
    }

    fn unwrap(&self, pts: i64) -> i64 {
        let u = pts + self.wrap;
        match self.high {
            Some(h) if h - u > WRAP_NS / 2 => u + WRAP_NS,
            _ => u,
        }
    }

    /// Whether a random-access picture at `pts` / `dts` (ns) starts a join. With a DTS (it
    /// has leading pictures only then), the decode clock moved by other than ~one frame from
    /// the last picture; else its PTS does not continue the display frontier within a run of
    /// leading pictures, or (strict) steps back.
    pub(crate) fn joined(&self, pts: Option<i64>, dts: Option<i64>) -> bool {
        if let (Some(d), Some(last)) = (dts, self.last_decode) {
            let p = self.period.unwrap_or(FALLBACK_PERIOD_NS);
            // A 2:3 pulldown frame decodes 1.5 periods after the one before it.
            let step = self.unwrap(d) - last;
            return step < p / 2 || step > p * 7 / 4;
        }
        let (Some(pts), Some(high)) = (pts, self.high) else {
            return false;
        };
        // A forward move is judged only once a frame period is measured.
        let window = |p: i64| (MAX_LEADING + 1) * p + p / 2;
        let step = self.unwrap(pts) - high;
        let forward = self.period.is_some_and(|p| step > window(p));
        let back = if self.strict {
            step < 0
        } else {
            step < -window(self.period.unwrap_or(FALLBACK_PERIOD_NS))
        };
        forward || back
    }

    // Move the frontier, decode clock and measured period to `pts` / `dts`; a join re-bases
    // the frontier.
    fn observe(&mut self, pts: i64, dts: Option<i64>, joined: bool) {
        let u = self.unwrap(pts);
        self.wrap = u - pts;
        if let Some(last) = self.last {
            let d = (u - last).abs();
            if !joined && d >= MIN_PERIOD_NS && self.period.is_none_or(|p| d < p) {
                self.period = Some(d);
            }
        }
        self.last = Some(u);
        self.last_decode = Some(dts.map_or(u, |d| self.unwrap(d)));
        self.high = Some(match self.high {
            Some(h) if !joined => h.max(u),
            _ => u,
        });
    }

    /// Decide `frame` (decode order); `None` drops it. `pts` / `dts` are its PES timestamps,
    /// as carried.
    pub(crate) fn admit(
        &mut self,
        mut frame: Frame,
        need: Need,
        pts: Option<i64>,
        dts: Option<i64>,
    ) -> Option<Frame> {
        let rap = matches!(need, Need::Rap { .. });
        let joined = rap && self.joined(pts, dts);
        if let Some(p) = pts {
            self.observe(p, dts, joined);
        }
        if frame.discontinuity {
            self.gap = true;
        }
        if self.gap && frame.keyframe {
            self.gap = false;
            self.held = 0;
        }
        let ok = match need {
            Need::Nothing => true,
            Need::Rap { closed, broken } => {
                if joined || broken {
                    self.held = 0;
                }
                self.rap_closed = closed;
                self.rap_pts = pts.map(|p| self.unwrap(p));
                true
            }
            Need::Intra => true,
            Need::Anchor | Need::Trailing => self.held >= 1,
            Need::Leading => self.held >= self.leading_need(),
            Need::ByPts => {
                let leading =
                    matches!((pts, self.rap_pts), (Some(p), Some(r)) if self.unwrap(p) < r);
                self.held >= if leading { self.leading_need() } else { 1 }
            }
        };
        if ok && matches!(need, Need::Rap { .. } | Need::Intra | Need::Anchor) {
            self.held = (self.held + 1).min(2);
        }
        if ok {
            frame.discontinuity |= std::mem::take(&mut self.carry_discontinuity);
            Some(frame)
        } else {
            self.carry_discontinuity |= frame.discontinuity;
            None
        }
    }

    fn leading_need(&self) -> u8 {
        if self.rap_closed { 1 } else { 2 }
    }
}
