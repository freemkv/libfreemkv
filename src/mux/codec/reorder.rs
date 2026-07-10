//! Display-order PTS reconstruction for sparse-PTS program-stream video.
//!
//! MPEG program streams (DVD VOB, HD-DVD EVO) timestamp video at GOP
//! granularity: only one access unit per GOP carries a PES PTS, and the rest
//! arrive with none. The H.264 / HEVC / VC-1 parsers collapse a missing PTS to
//! `0` (`pes.pts.or(dts).unwrap_or(0)`), so on such a source every non-anchor
//! frame lands on the same block timestamp. A decoder then cannot order them and
//! reports "non monotonically increasing dts". (The MPEG-2 parser already avoids
//! this by reconstructing per-picture PTS from `temporal_reference`; these three
//! codecs carry no such field.)
//!
//! [`SparsePtsReorder`] reconstructs a display-order PTS for every frame from two
//! signals the parsers already provide — the coded picture type (I/P/B) and the
//! sparse anchor PTS — plus a per-frame duration self-calibrated from the spacing
//! between consecutive GOP anchors (no external frame-rate needed). It mirrors
//! the MPEG-2 parser's GOP-buffered origin-locking, but derives display order via
//! the classic single-anchor-delay rule instead of `temporal_reference`:
//!
//! - In DECODE order an anchor (I/P) is stored before the B-frames that
//!   reference it forward, so decode `I P B P B` displays as `I B P B P`.
//! - The rule that produces that mapping: an anchor is displayed only after the
//!   previously-held anchor; a B-frame displays immediately. This is exact for
//!   the classic (non-hierarchical) GOP structures HD-DVD H.264/VC-1 use.
//!
//! This reconstruction is applied ONLY on the program-stream path
//! (`ContentFormat::MpegPs`). BD/UHD transport streams carry a per-frame PTS and
//! are never routed through it, so the primary decode path is untouched.

use super::Frame;
use super::coding::CodingType;

/// Fallback per-frame duration (ns) when the anchor spacing cannot calibrate one
/// (a stream with a single GOP, or no anchor PTS at all): 24000/1001 fps film,
/// the dominant HD-DVD cadence. Only affects intra-GOP spacing — each GOP's
/// origin is re-locked to its own anchor PTS, so a wrong fallback cannot drift
/// the timeline across GOPs.
const FALLBACK_FRAME_DUR_NS: i64 = 1_001_000_000 / 24;

/// Force-complete the current GOP once it reaches this many buffered pictures
/// even without a keyframe. A GOP is normally a few dozen frames; a stream that
/// never signals a keyframe (open-GOP recovery-point coding, or crafted/corrupt
/// disc bytes) would otherwise buffer every access unit — the whole title — in
/// RAM. Mirrors the MPEG-2 parser's `MAX_PENDING_FRAMES` backstop so no
/// reassembly buffer grows unbounded on disc-controlled input.
const MAX_GOP_FRAMES: usize = 600;

/// One buffered coded picture awaiting its GOP's completion.
struct Pending {
    /// Explicit PES PTS (ns) for this AU, or `None` when the source omitted it.
    explicit: Option<i64>,
    /// Coded picture type; `P` (anchor) when the parser could not determine it,
    /// so an unknown frame is never mis-placed as a bi-predicted B.
    ctype: CodingType,
    frame: Frame,
}

/// A completed GOP, buffered until the NEXT GOP's anchor is known so a per-frame
/// duration can be calibrated from the two anchors before its frames are emitted.
struct Gop {
    pend: Vec<Pending>,
    /// Display index (0-based) of each `pend` entry, in `pend` (decode) order.
    dispidx: Vec<i64>,
    /// Display-frame count (== `pend.len()`).
    count: i64,
    /// The anchor: `(explicit_pts, dispidx)` of the first buffered frame that
    /// carried an explicit PTS, used to lock the display origin. `None` when the
    /// GOP carried no PTS at all (origin then continues from the running base).
    anchor: Option<(i64, i64)>,
}

/// Reconstructs display-order PTS for a sparse-PTS video elementary stream.
pub(crate) struct SparsePtsReorder {
    /// Frames of the GOP currently accumulating, in decode order.
    cur: Vec<Pending>,
    /// The previously-completed GOP, held one step so its duration can be
    /// calibrated from the next GOP's anchor before it is emitted.
    held: Option<Gop>,
    /// Self-calibrated per-frame display duration (ns); 0 until two anchors seen.
    dur_ns: i64,
    /// Display time (ns) at which the next emitted GOP should begin, when its own
    /// anchor is absent. Advanced by each emitted GOP.
    next_start_ns: i64,
}

impl SparsePtsReorder {
    pub(crate) fn new() -> Self {
        Self {
            cur: Vec::new(),
            held: None,
            dur_ns: 0,
            next_start_ns: 0,
        }
    }

    /// Feed one parsed frame with its explicit PES PTS (or `None`). Returns any
    /// frames whose display PTS is now finalized (emitted in decode order).
    pub(crate) fn push(&mut self, explicit: Option<i64>, frame: Frame) -> Vec<Frame> {
        let ctype = frame
            .coding
            .map(|c| c.coding_type())
            .unwrap_or(CodingType::P);
        // A keyframe opens a new GOP: the picture already accumulated in `cur` is
        // a complete GOP. Complete it (this frame belongs to the NEW GOP). Also
        // force-complete a pathologically long run that never signalled a
        // keyframe, so a crafted/corrupt stream cannot buffer without bound.
        let mut out = Vec::new();
        if (frame.keyframe || self.cur.len() >= MAX_GOP_FRAMES) && !self.cur.is_empty() {
            out = self.complete_current_gop();
        }
        self.cur.push(Pending {
            explicit,
            ctype,
            frame,
        });
        out
    }

    /// Flush all buffered frames at end of stream.
    pub(crate) fn flush(&mut self) -> Vec<Frame> {
        let mut out = self.complete_current_gop();
        if let Some(gop) = self.held.take() {
            out.extend(self.emit_gop(gop));
        }
        out
    }

    /// Move `cur` into a completed [`Gop`]; if a GOP was already held, calibrate
    /// the duration from the two anchors and emit the held one.
    fn complete_current_gop(&mut self) -> Vec<Frame> {
        if self.cur.is_empty() {
            return Vec::new();
        }
        let pend = std::mem::take(&mut self.cur);
        let dispidx = display_indices(pend.iter().map(|p| p.ctype));
        let count = pend.len() as i64;
        let anchor = pend
            .iter()
            .zip(&dispidx)
            .find_map(|(p, &d)| p.explicit.map(|pts| (pts, d)));
        let gop = Gop {
            pend,
            dispidx,
            count,
            anchor,
        };

        let mut out = Vec::new();
        match self.held.take() {
            Some(held) => {
                // Calibrate a per-frame duration from the two anchors' spacing,
                // spread across the held GOP's display-frame count. Approximate
                // (assumes both anchors sit at a similar relative display slot),
                // but each GOP re-locks its own origin, so the estimate only sets
                // intra-GOP spacing.
                if self.dur_ns == 0 {
                    if let (Some((p_held, _)), Some((p_next, _))) = (held.anchor, gop.anchor) {
                        let span = p_next - p_held;
                        if span > 0 && held.count > 0 {
                            self.dur_ns = (span / held.count).max(1);
                        }
                    }
                }
                out = self.emit_gop(held);
                self.held = Some(gop);
            }
            None => self.held = Some(gop),
        }
        out
    }

    /// Assign each frame in `gop` its display PTS and return them in decode order.
    fn emit_gop(&mut self, gop: Gop) -> Vec<Frame> {
        let dur = if self.dur_ns > 0 {
            self.dur_ns
        } else {
            FALLBACK_FRAME_DUR_NS
        };
        // Lock the display origin: prefer the GOP's own anchor PTS (back out its
        // display offset); otherwise continue from the running base.
        let origin = match gop.anchor {
            Some((pts, didx)) => pts - didx * dur,
            None => self.next_start_ns,
        };
        let Gop {
            pend,
            dispidx,
            count,
            ..
        } = gop;
        let mut out = Vec::with_capacity(pend.len());
        for (mut p, didx) in pend.into_iter().zip(dispidx) {
            p.frame.pts_ns = origin + didx * dur;
            // Carry the calibrated per-frame duration so the muxer emits a
            // BlockDuration and the back-patched Segment Duration covers the
            // final frame (the source gives no duration on this path).
            p.frame.duration_ns = Some(dur as u64);
            out.push(p.frame);
        }
        // Next GOP with no anchor continues after this one's last display slot.
        self.next_start_ns = origin + count * dur;
        out
    }
}

/// Display index (0-based, decode order in → decode order out) for a GOP's coded
/// picture types via the classic single-anchor-delay reorder: an anchor (I/P) is
/// displayed only after the previously-held anchor; a B displays immediately.
/// Decode `I P B P B` → display indices `[0, 2, 1, 4, 3]` (display `I B P B P`).
fn display_indices(types: impl Iterator<Item = CodingType>) -> Vec<i64> {
    let types: Vec<CodingType> = types.collect();
    let mut disp = vec![0i64; types.len()];
    let mut held: Option<usize> = None;
    let mut cursor = 0i64;
    for (i, &c) in types.iter().enumerate() {
        match c {
            CodingType::I | CodingType::P => {
                if let Some(h) = held {
                    disp[h] = cursor;
                    cursor += 1;
                }
                held = Some(i);
            }
            CodingType::B => {
                disp[i] = cursor;
                cursor += 1;
            }
        }
    }
    if let Some(h) = held {
        disp[h] = cursor;
    }
    disp
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::codec::coding::PictureInfo;

    fn frame(ctype: CodingType, keyframe: bool) -> Frame {
        Frame {
            keyframe,
            coding: Some(PictureInfo::coding_type_only(ctype)),
            ..Default::default()
        }
    }

    #[test]
    fn display_indices_map_classic_gop() {
        use CodingType::*;
        // decode I P B P B  ->  display I B P B P
        let d = display_indices([I, P, B, P, B].into_iter());
        assert_eq!(d, vec![0, 2, 1, 4, 3]);
    }

    #[test]
    fn display_indices_all_anchors_are_identity() {
        use CodingType::*;
        let d = display_indices([I, P, P, P].into_iter());
        assert_eq!(d, vec![0, 1, 2, 3]);
    }

    #[test]
    fn reconstructs_monotonic_display_pts_from_one_anchor_per_gop() {
        use CodingType::*;
        // Two GOPs of 5 frames, decode order I P B P B, anchor PTS only on the
        // GOP's I (0 ns, then ~5-frames-later). Frame duration should calibrate
        // to the spacing/5 and every frame get a distinct increasing display PTS.
        let dur = 41_708_333i64;
        let mut r = SparsePtsReorder::new();
        let mut got: Vec<i64> = Vec::new();
        // GOP 1: anchor on the I at t=0.
        for (k, (ct, pts)) in [(I, Some(0i64)), (P, None), (B, None), (P, None), (B, None)]
            .into_iter()
            .enumerate()
        {
            let out = r.push(pts, frame(ct, k == 0));
            got.extend(out.iter().map(|f| f.pts_ns));
        }
        // GOP 2: anchor on the I at t = 5*dur (its true display time).
        for (k, (ct, pts)) in [
            (I, Some(5 * dur)),
            (P, None),
            (B, None),
            (P, None),
            (B, None),
        ]
        .into_iter()
        .enumerate()
        {
            let out = r.push(pts, frame(ct, k == 0));
            got.extend(out.iter().map(|f| f.pts_ns));
        }
        got.extend(r.flush().iter().map(|f| f.pts_ns));

        // Ten frames out, none dropped.
        assert_eq!(got.len(), 10, "all frames emitted");
        // The calibrated duration is (5*dur)/5 = dur.
        // GOP 1 decode order I P B P B -> display indices 0 2 1 4 3 -> PTS:
        assert_eq!(
            &got[0..5],
            &[0, 2 * dur, dur, 4 * dur, 3 * dur],
            "GOP1 display PTS in decode order"
        );
        // GOP 2 re-locks origin to 5*dur.
        assert_eq!(
            &got[5..10],
            &[5 * dur, 7 * dur, 6 * dur, 9 * dur, 8 * dur],
            "GOP2 display PTS continue monotonically per display order"
        );
    }

    #[test]
    fn force_flushes_a_gop_that_never_signals_a_keyframe() {
        use CodingType::*;
        // A stream that never flags a keyframe (open-GOP recovery points, or a
        // crafted/corrupt disc) must not buffer the whole title: the cap
        // force-completes GOPs so frames are emitted well before flush().
        let mut r = SparsePtsReorder::new();
        let mut emitted = 0usize;
        for i in 0..(MAX_GOP_FRAMES * 3) {
            let pts = (i == 0).then_some(0);
            emitted += r.push(pts, frame(P, false)).len();
        }
        assert!(
            emitted >= MAX_GOP_FRAMES,
            "cap force-flushed GOPs before EOF (emitted {emitted})"
        );
    }

    #[test]
    fn no_pts_collisions_within_a_gop() {
        use CodingType::*;
        // Every frame distinct in DISPLAY order — the property the mkv muxer
        // needs so a decoder can derive monotonic DTS.
        let mut r = SparsePtsReorder::new();
        let mut all: Vec<i64> = Vec::new();
        for gop in 0..3 {
            for (k, ct) in [I, P, B, P, B].into_iter().enumerate() {
                let pts = (k == 0).then_some(gop as i64 * 5 * 41_708_333);
                all.extend(r.push(pts, frame(ct, k == 0)).iter().map(|f| f.pts_ns));
            }
        }
        all.extend(r.flush().iter().map(|f| f.pts_ns));
        let mut sorted = all.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), all.len(), "no two frames share a display PTS");
    }
}
