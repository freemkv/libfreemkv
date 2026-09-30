//! Sink-side decode timestamps for reordered video (design `mpg-output-design.md` §2.3, S0a).
//!
//! H.222.0 §2.7.5 (h222:6425-6429): "A decoding_timestamp (DTS) shall appear in a PES packet
//! header if and only if the following two conditions are met: • a PTS is present in the PES
//! packet header; • the decoding time differs from the presentation time."
//!
//! The IR carries PTS only, so a sink derives DTS online: [`DtsDeriver::for_codec`], then
//! [`DtsDeriver::push`] per IR frame in decode order, with the exact tick value its PES header
//! will carry. Units, R and field pairs come from the ES bytes and codec_private, never from
//! `PesFrame::coding`. With R the parsed reorder depth and S the sorted unit PTS so far,
//! `DTS_k = min(S[k−R], PTS_k)`; units 0..R−1 resolve together once unit R arrives (the sink
//! holds its output meanwhile), and one order guard keeps written DTS strictly increasing.
//! Every anomaly is counted in [`DtsCounters`].

use crate::disc::Codec;
use crate::mux::codec::h264::{self, SpsDtsInfo};
use crate::mux::codec::hevc;
use crate::mux::codec::startcode::{find_start_code, skip_start_code};
use std::collections::VecDeque;

/// Largest reorder depth honoured: H.264 MaxDpbFrames is at most 16. A larger parsed
/// value (corrupt SPS) clamps here.
pub(crate) const MAX_REORDER: usize = 16;

// Unit PTS values kept: the order statistic S[k−R] is the (R+1)-th largest, so the
// largest MAX_REORDER + 1 answer it exactly for every R the deriver can hold.
const KEEP: usize = MAX_REORDER + 1;

// Start-up T when the stream states no frame rate and the window has no positive gap:
// "else 1/24 s" (design §2.3), in 90 kHz ticks.
const FALLBACK_FRAME_TICKS: i64 = 3_750;

// Field period when the stream states no frame rate: 1/(2·23.976 Hz), rounded up, the
// longest broadcast/disc field period, so pairing never rejects a real second field.
const FALLBACK_FIELD_TICKS: i64 = 1_877;

/// Anomalies the deriver resolved rather than writing a bad DTS (design §2.3).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DtsCounters {
    /// AUs where the ≤ PTS cap bound, the order guard fired, or a DTS below 0 was
    /// clamped (`dts_order_violations`). Counted once per AU.
    pub order_violations: u64,
    /// Start-up holds the sink's cap released before unit R arrived (`dts_hold_overflow`).
    pub hold_overflow: u64,
}

impl std::ops::AddAssign for DtsCounters {
    fn add_assign(&mut self, o: Self) {
        self.order_violations += o.order_violations;
        self.hold_overflow += o.hold_overflow;
    }
}

/// Online DTS derivation for one video track (design §2.3 `DtsDeriver`).
pub(crate) struct DtsDeriver {
    scanner: Scanner,
    core: Core,
    /// Frame period adopted from another track (an MVC dependent view follows its base).
    adopted_frame_ticks: Option<i64>,
}

impl DtsDeriver {
    /// Deriver for a track of `codec`. A parameter set in `codec_private` (avcC, hvcC,
    /// an MPEG-2 sequence header, a VC-1 BITMAPINFOHEADER) counts as the first parse.
    pub(crate) fn for_codec(codec: Codec, codec_private: Option<&[u8]>) -> Self {
        let nal_len = crate::mux::hevc::nal_length_size(codec, codec_private);
        let scanner = match codec {
            Codec::Mpeg2 | Codec::Mpeg1 => Scanner::Mpeg2 { frame_ticks: None },
            Codec::H264 => Scanner::H264 { nal_len, sps: None },
            Codec::Hevc => Scanner::Hevc {
                nal_len,
                frame_ticks: None,
            },
            Codec::Vc1 => Scanner::Vc1,
            _ => Scanner::Never,
        };
        let mut d = Self {
            scanner,
            core: Core::default(),
            adopted_frame_ticks: None,
        };
        if let Some(r) = codec_private.and_then(|cp| d.scanner.scan_codec_private(cp)) {
            d.core.on_params(r);
        }
        d
    }

    /// A deriver that parses nothing and only [`adopt`](Self::adopt)s parameters.
    pub(crate) fn follower() -> Self {
        Self::for_codec(Codec::Unknown(0), None)
    }

    /// Reorder depth R and stated frame period (90 kHz ticks) so far.
    pub(crate) fn params(&self) -> (usize, Option<i64>) {
        (self.core.r, self.frame_ticks())
    }

    /// Follow another track's parameters: an MVC dependent view, whose own NAL units
    /// carry no SPS, takes its base view's R and frame period; its PTS sequence is the
    /// base's, so the same rule yields the same DTS.
    pub(crate) fn adopt(&mut self, (r, frame_ticks): (usize, Option<i64>)) {
        self.adopted_frame_ticks = frame_ticks.or(self.adopted_frame_ticks);
        if r > self.core.r || (r > 0 && !self.core.have_params) {
            self.core.on_params(r);
        }
    }

    fn frame_ticks(&self) -> Option<i64> {
        self.scanner
            .frame_ticks()
            .or(self.adopted_frame_ticks)
            .filter(|&t| t > 0)
    }

    /// Feed one IR frame in decode order. `pts_ticks` is exactly the PTS the sink's
    /// PES header will carry (design §2.3 tick-domain rule 3).
    pub(crate) fn push(&mut self, pts_ticks: i64, es: &[u8]) {
        let scan = self.scanner.scan(es);
        if let Some(r) = scan.reorder {
            self.core.on_params(r);
        }
        self.core.frame_ticks = self.frame_ticks();
        self.core.push(pts_ticks, scan.shape);
    }

    /// True while a start-up (or R-upgrade re-start) window waits for its unit R: the
    /// sink must hold every track's output until this clears.
    pub(crate) fn pending(&self) -> bool {
        self.core.window.is_some()
    }

    /// DTS for the oldest pushed frame not yet taken, once resolved: `Some(None)` writes
    /// PTS only, `Some(Some(dts))` writes both. `None` while it is still in a window.
    pub(crate) fn pop(&mut self) -> Option<Option<i64>> {
        let out = self.core.queue.front()?.out?;
        self.core.queue.pop_front();
        Some(out)
    }

    /// The sink's hold cap was reached: resolve the open window over the units held
    /// (the EOF formula), counted in `hold_overflow`.
    pub(crate) fn release_cap(&mut self) {
        if self.core.window.is_some() {
            self.core.counters.hold_overflow += 1;
            self.core.resolve_window(false);
        }
    }

    /// End of input: resolve an open window over the units available.
    pub(crate) fn finish(&mut self) {
        self.core.resolve_window(false);
    }

    /// Anomalies so far.
    pub(crate) fn counters(&self) -> DtsCounters {
        self.core.counters
    }
}

/// How one IR frame codes its picture(s).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// A frame picture, a complementary field pair inside one IR frame, or undetermined.
    Frame,
    /// A single field picture, a candidate first or second field of a pair.
    Field {
        bottom: bool,
        /// H.264 `frame_num`; `None` for MPEG-2.
        frame_num: Option<u32>,
        /// H.264 IDR: a second field is never IDR.
        idr: bool,
    },
}

/// What one IR frame told the deriver.
struct Scan {
    /// Reorder depth from a parameter set in this frame.
    reorder: Option<usize>,
    shape: Shape,
}

/// Per-codec ES reader: parameter sets → R, pictures → [`Shape`].
enum Scanner {
    /// MPEG-2 (and MPEG-1, the same syntax without extensions).
    Mpeg2 {
        frame_ticks: Option<i64>,
    },
    H264 {
        nal_len: usize,
        sps: Option<SpsDtsInfo>,
    },
    Hevc {
        nal_len: usize,
        frame_ticks: Option<i64>,
    },
    Vc1,
    /// Codecs S0 never reorders (no DTS, no parsing).
    Never,
}

impl Scanner {
    fn scan_codec_private(&mut self, cp: &[u8]) -> Option<usize> {
        match self {
            Scanner::Mpeg2 { frame_ticks } => scan_mpeg2(cp, frame_ticks).reorder,
            Scanner::H264 { sps, .. } => {
                let annex_b = crate::mux::hevc::avcc_to_annex_b(cp)?;
                let mut r = None;
                for nal in AnnexB::new(&annex_b) {
                    r = h264_param(nal, sps).or(r);
                }
                r
            }
            Scanner::Hevc { frame_ticks, .. } => {
                let annex_b = crate::mux::hevc::hvcc_to_annex_b(cp)?;
                let (r, t) = AnnexB::new(&annex_b).filter_map(hevc_param).last()?;
                *frame_ticks = t.or(*frame_ticks);
                Some(r)
            }
            Scanner::Vc1 => AnnexB::new(cp).find_map(vc1_param),
            Scanner::Never => None,
        }
    }

    fn scan(&mut self, es: &[u8]) -> Scan {
        let frame = |reorder| Scan {
            reorder,
            shape: Shape::Frame,
        };
        match self {
            Scanner::Mpeg2 { frame_ticks } => scan_mpeg2(es, frame_ticks),
            Scanner::H264 { nal_len, sps } => scan_h264(es, *nal_len, sps),
            Scanner::Hevc {
                nal_len,
                frame_ticks,
            } => {
                let param = nals(es, *nal_len).filter_map(hevc_param).last();
                if let Some((_, Some(t))) = param {
                    *frame_ticks = Some(t);
                }
                frame(param.map(|(r, _)| r))
            }
            Scanner::Vc1 => frame(AnnexB::new(es).find_map(vc1_param)),
            Scanner::Never => frame(None),
        }
    }

    // The frame period the stream states (h222:2012-2013: the reorder delay "is a
    // multiple of the nominal picture period"); HEVC states a picture period.
    fn frame_ticks(&self) -> Option<i64> {
        match self {
            Scanner::Mpeg2 { frame_ticks } | Scanner::Hevc { frame_ticks, .. } => *frame_ticks,
            Scanner::H264 { sps, .. } => sps.and_then(|s| s.frame_period_ticks),
            _ => None,
        }
    }
}

// ISO/IEC 13818-2 frame_rate_code → (num, den) frames per second.
const MPEG2_FRAME_RATES: [(i64, i64); 9] = [
    (0, 1),
    (24_000, 1001),
    (24, 1),
    (25, 1),
    (30_000, 1001),
    (30, 1),
    (50, 1),
    (60_000, 1001),
    (60, 1),
];

// frame_rate = frame_rate_value × (frame_rate_extension_n + 1) ÷ (frame_rate_extension_d
// + 1) (ISO/IEC 13818-2 §6.3.5), as one frame period in 90 kHz ticks, rounded.
fn mpeg2_frame_ticks(num: i64, den: i64, n: i64, d: i64) -> i64 {
    let (top, bottom) = (90_000 * den * (d + 1), num * (n + 1));
    (top + bottom / 2) / bottom
}

// MPEG-2 ES: a sequence header sets R = 1, or R = 0 when its sequence_extension has
// low_delay = 1 (h222:8835-8837: "I- and P-picture VAUs within low-delay video sequences,
// the DTS is not coded"). Pictures are counted by start code; picture_structure 1/2 = field.
fn scan_mpeg2(es: &[u8], frame_ticks: &mut Option<i64>) -> Scan {
    let mut reorder = None;
    // frame_rate_code's (num, den) from this frame's sequence header.
    let mut rate = None;
    // picture_structure of each picture in this IR frame (None until its coding extension).
    let mut pics: Vec<Option<u8>> = Vec::new();
    let mut pos = 0;
    while let Some(sc) = find_start_code(es, pos) {
        pos = sc + 3;
        let Some(&code) = es.get(sc + 3) else { break };
        match code {
            // sequence_header: frame_rate_code is the low nibble of its 4th byte.
            0xB3 => {
                reorder = Some(1);
                if let Some(&b) = es.get(sc + 7) {
                    let (num, den) = MPEG2_FRAME_RATES
                        .get((b & 0x0F) as usize)
                        .copied()
                        .unwrap_or((0, 1));
                    if num > 0 {
                        rate = Some((num, den));
                        *frame_ticks = Some(mpeg2_frame_ticks(num, den, 0, 0));
                    }
                }
            }
            0xB5 => match es.get(sc + 4).map(|b| b >> 4) {
                // sequence_extension: low_delay is the MSB of its 6th byte.
                // frame_rate_extension_n (2) and _d (5) follow it in the same byte.
                Some(0b0001) if reorder.is_some() => {
                    let b = es.get(sc + 9).copied().unwrap_or(0);
                    reorder = Some(if b & 0x80 != 0 { 0 } else { 1 });
                    if let Some((num, den)) = rate {
                        let (n, d) = ((b >> 5) & 0x03, b & 0x1F);
                        *frame_ticks = Some(mpeg2_frame_ticks(num, den, n as i64, d as i64));
                    }
                }
                // picture_coding_extension: picture_structure is the low 2 bits of byte 3.
                Some(0b1000) => {
                    if let Some(last @ None) = pics.last_mut() {
                        *last = es.get(sc + 6).map(|b| b & 0x03);
                    }
                }
                _ => {}
            },
            0x00 => pics.push(None),
            _ => {}
        }
    }
    let shape = match pics.as_slice() {
        [Some(ps @ (1 | 2))] => Shape::Field {
            bottom: *ps == 2,
            frame_num: None,
            idr: false,
        },
        _ => Shape::Frame,
    };
    Scan { reorder, shape }
}

// H.264: an SPS (type 7) sets R; the first slice (type 1/5) of each picture gives its
// field_pic_flag/bottom_field_flag/frame_num. Two pictures in one IR frame are one unit.
fn scan_h264(es: &[u8], nal_len: usize, sps: &mut Option<SpsDtsInfo>) -> Scan {
    const NAL_SLICE: u8 = 1;
    const NAL_IDR: u8 = 5;
    let mut reorder = None;
    let mut first: Option<(h264::SliceFieldInfo, bool)> = None;
    let mut second_picture = false;
    for nal in nals(es, nal_len) {
        let t = nal[0] & 0x1F;
        if let Some(r) = h264_param(nal, sps) {
            reorder = Some(r);
        } else if (t == NAL_SLICE || t == NAL_IDR) && !second_picture {
            let Some(ctx) = sps.as_ref() else { continue };
            let Some(sl) = h264::parse_slice_field_info(nal, ctx) else {
                continue;
            };
            match first {
                None => first = Some((sl, t == NAL_IDR)),
                Some(_) if sl.first_mb == 0 => second_picture = true,
                Some(_) => {}
            }
        }
    }
    let shape = match first {
        Some((sl, idr)) if !second_picture => match sl.field {
            Some(bottom) => Shape::Field {
                bottom,
                frame_num: Some(sl.frame_num),
                idr,
            },
            None => Shape::Frame,
        },
        _ => Shape::Frame,
    };
    Scan { reorder, shape }
}

// An H.264 SPS NAL: records its slice-header layout and returns its R.
fn h264_param(nal: &[u8], sps: &mut Option<SpsDtsInfo>) -> Option<usize> {
    const NAL_SPS: u8 = 7;
    if nal.first()? & 0x1F != NAL_SPS {
        return None;
    }
    let info = h264::parse_sps_dts_info(nal)?;
    *sps = Some(info);
    Some(info.reorder as usize)
}

// An HEVC SPS NAL (type 33): R = sps_max_num_reorder_pics[sps_max_sub_layers_minus1],
// and the VUI picture period when stated.
fn hevc_param(nal: &[u8]) -> Option<(usize, Option<i64>)> {
    const NAL_SPS: u8 = 33;
    if (nal.first()? >> 1) & 0x3F != NAL_SPS {
        return None;
    }
    hevc::parse_sps_dts(nal).map(|(r, t)| (r as usize, t))
}

// A VC-1 sequence header BDU (start code suffix 0x0F): R = 1 (design §2.3).
fn vc1_param(bdu: &[u8]) -> Option<usize> {
    (*bdu.first()? == 0x0F).then_some(1)
}

// NAL units of an IR frame: length-prefixed (`nal_len` octets) as the writer reads it; Annex B
// only when it starts with a start code and the prefixes do not exactly tile the frame.
fn nals(es: &[u8], nal_len: usize) -> Nals<'_> {
    let prefixed = LengthPrefixed {
        es,
        nal_len,
        pos: 0,
    };
    let start_code = es.starts_with(&[0, 0, 1]) || es.starts_with(&[0, 0, 0, 1]);
    if start_code && !prefixed.tiles() {
        Nals::AnnexB(AnnexB::new(es))
    } else {
        Nals::Prefixed(prefixed)
    }
}

enum Nals<'a> {
    AnnexB(AnnexB<'a>),
    Prefixed(LengthPrefixed<'a>),
}

impl<'a> Iterator for Nals<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        match self {
            Nals::AnnexB(it) => it.next(),
            Nals::Prefixed(it) => it.next(),
        }
    }
}

struct LengthPrefixed<'a> {
    es: &'a [u8],
    nal_len: usize,
    pos: usize,
}

impl LengthPrefixed<'_> {
    fn size(&self) -> usize {
        if (1..=4).contains(&self.nal_len) {
            self.nal_len
        } else {
            4
        }
    }

    // Whether the length prefixes land exactly on the end of the frame.
    fn tiles(&self) -> bool {
        let size = self.size();
        let mut pos = 0usize;
        while pos < self.es.len() {
            let Some(head) = self.es.get(pos..pos + size) else {
                return false;
            };
            let len = head.iter().fold(0usize, |a, &b| (a << 8) | b as usize);
            pos = pos.saturating_add(size).saturating_add(len);
        }
        pos == self.es.len()
    }
}

impl<'a> Iterator for LengthPrefixed<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        let size = self.size();
        loop {
            let head = self.es.get(self.pos..self.pos + size)?;
            let len = head.iter().fold(0usize, |a, &b| (a << 8) | b as usize);
            let start = self.pos + size;
            let body = self.es.get(start..start.checked_add(len)?)?;
            self.pos = start + len;
            if !body.is_empty() {
                return Some(body);
            }
        }
    }
}

// Units between Annex B start codes (the byte after `00 00 01` first).
struct AnnexB<'a> {
    es: &'a [u8],
    pos: Option<usize>,
}

impl<'a> AnnexB<'a> {
    fn new(es: &'a [u8]) -> Self {
        Self {
            es,
            pos: find_start_code(es, 0),
        }
    }
}

impl<'a> Iterator for AnnexB<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        loop {
            let sc = self.pos?;
            let start = skip_start_code(self.es, sc)?;
            let next = find_start_code(self.es, start);
            self.pos = next;
            let end = next.unwrap_or(self.es.len());
            if end > start {
                return Some(&self.es[start..end]);
            }
        }
    }
}

/// One pushed IR frame awaiting `pop`.
#[derive(Debug)]
struct Entry {
    pts: i64,
    kind: Kind,
    /// Resolved DTS: `Some(None)` = PTS only, `Some(Some(d))` = write `d`.
    out: Option<Option<i64>>,
}

#[derive(Debug, Clone, Copy)]
enum Kind {
    /// The first (or only) IR frame of a unit.
    Unit,
    /// The second field of the previous unit, `delta` ticks after its first field.
    Second { delta: i64 },
}

/// A first field still open for pairing.
struct OpenField {
    pts: i64,
    bottom: bool,
    frame_num: Option<u32>,
}

/// A start-up (or R-upgrade re-start) window: its entries are the last `entries` queued.
struct Window {
    restart: bool,
    /// Resolve once `target + 1` units have arrived.
    target: usize,
    entries: usize,
    unit_pts: Vec<i64>,
    /// DTS_last when the window opened (the re-start interval's lower bound).
    dts_before: Option<i64>,
}

/// The codec-agnostic rule engine, all in the sink's tick domain.
#[derive(Default)]
struct Core {
    /// Reorder depth R; only ever increases (h222:2013-2016).
    r: usize,
    /// A parameter set has parsed; until then R = 0 and nothing is counted.
    have_params: bool,
    /// Open a window at the next unit: `Some(restart)`.
    start_pending: Option<bool>,
    /// The largest [`KEEP`] unit PTS values, ascending.
    top: Vec<i64>,
    units: u64,
    /// Latest DTS in decode order, including second fields and PTS-only AUs.
    dts_last: Option<i64>,
    /// Effective DTS of the latest resolved unit (its PTS when PTS-only).
    last_unit_dts: i64,
    /// Largest ΔPTS between a second field and its first field.
    field_delta_max: i64,
    open_field: Option<OpenField>,
    /// The stated frame period (90 kHz ticks) at the latest push, if any.
    frame_ticks: Option<i64>,
    window: Option<Window>,
    queue: VecDeque<Entry>,
    counters: DtsCounters,
}

impl Core {
    // A parameter set parsed with reorder depth `r_new`. R only increases: a stream "should
    // include re-ordering delay starting at the beginning of the stream" (h222:2015-2016).
    // A larger R opens a start-up at the next unit (before any unit) or the re-start.
    fn on_params(&mut self, r_new: usize) {
        let r_new = r_new.min(MAX_REORDER);
        self.have_params = true;
        if r_new <= self.r {
            return;
        }
        self.r = r_new;
        match &mut self.window {
            Some(w) => w.target = r_new,
            None => self.start_pending = Some(self.units > 0),
        }
    }

    fn push(&mut self, pts: i64, shape: Shape) {
        let field_ticks = self
            .frame_ticks
            .map_or(FALLBACK_FIELD_TICKS, |f| (f + 1) / 2);
        let open = self.open_field.take();
        let second = match (shape, open) {
            (
                Shape::Field {
                    bottom,
                    frame_num,
                    idr,
                },
                Some(o),
            ) if bottom != o.bottom
                && !idr
                && frame_num == o.frame_num
                && pts > o.pts
                && pts - o.pts <= field_ticks + 1 =>
            {
                Some(pts - o.pts)
            }
            _ => None,
        };
        let kind = match second {
            Some(delta) => {
                self.field_delta_max = self.field_delta_max.max(delta);
                Kind::Second { delta }
            }
            None => {
                if let Shape::Field {
                    bottom, frame_num, ..
                } = shape
                {
                    self.open_field = Some(OpenField {
                        pts,
                        bottom,
                        frame_num,
                    });
                }
                self.add_unit_pts(pts);
                if let Some(restart) = self.start_pending.take() {
                    self.window = Some(Window {
                        restart,
                        target: self.r,
                        entries: 0,
                        unit_pts: Vec::new(),
                        dts_before: self.dts_last,
                    });
                }
                if let Some(w) = &mut self.window {
                    w.unit_pts.push(pts);
                }
                Kind::Unit
            }
        };
        self.queue.push_back(Entry {
            pts,
            kind,
            out: None,
        });
        match &mut self.window {
            Some(w) => {
                w.entries += 1;
                if w.unit_pts.len() > w.target {
                    self.resolve_window(true);
                }
            }
            None => {
                let i = self.queue.len() - 1;
                self.resolve_steady(i);
            }
        }
    }

    fn add_unit_pts(&mut self, pts: i64) {
        self.units += 1;
        let at = self.top.partition_point(|&v| v <= pts);
        self.top.insert(at, pts);
        if self.top.len() > KEEP {
            self.top.remove(0);
        }
    }

    // S_{0..k}[k−R], the (R+1)-th largest unit PTS so far; the smallest while k < R
    // (only after a cap release).
    fn order_stat(&self) -> i64 {
        let n = self.top.len();
        self.top[n - 1 - self.r.min(n - 1)]
    }

    // Steady state: a unit takes `min(S[k−R], PTS_k)`; a second field `DTS_1st + ΔPTS`.
    fn resolve_steady(&mut self, i: usize) {
        let raw = match self.queue[i].kind {
            Kind::Unit => self.order_stat(),
            Kind::Second { delta } => self.last_unit_dts + delta,
        };
        self.set(i, raw);
    }

    // Resolve entry `i` from its raw DTS through the cap and the guard.
    fn set(&mut self, i: usize, raw: i64) {
        let (pts, kind) = (self.queue[i].pts, self.queue[i].kind);
        let out = self.finalize(pts, raw);
        if let Kind::Unit = kind {
            self.last_unit_dts = out.unwrap_or(pts);
        }
        self.queue[i].out = Some(out);
    }

    // The ≤ PTS cap, the ≥ 0 clamp and the order guard (design §2.3, MPG4-1): a DTS not
    // above DTS_last becomes DTS_last + 1, and if that passes PTS the AU is PTS only.
    // Returns the DTS to write; `None` = PTS only (flags '10', h222:3236-3238).
    fn finalize(&mut self, pts: i64, raw: i64) -> Option<i64> {
        let mut bad = false;
        let mut d = raw;
        if d > pts {
            d = pts;
            bad = true;
        }
        if d < 0 {
            d = 0;
            bad = true;
        }
        if let Some(last) = self.dts_last
            && d <= last
        {
            d = last + 1;
            bad = true;
        }
        if bad && self.have_params {
            self.counters.order_violations += 1;
        }
        if d > pts {
            // PTS only; DTS_last keeps its value so written DTS never step back.
            return None;
        }
        self.dts_last = Some(d);
        // Flags compare ticks after conversion (MPG3-5): equal ticks → PTS only.
        (d != pts).then_some(d)
    }

    // Resolve the open window. `complete`: unit `target` has arrived and takes the steady
    // rule. Otherwise (cap release, EOF) every held unit is placed.
    fn resolve_window(&mut self, complete: bool) {
        let Some(w) = self.window.take() else { return };
        let start = self.queue.len() - w.entries;
        let placed = if complete { w.entries - 1 } else { w.entries };
        let mut s = w.unit_pts.clone();
        s.sort_unstable();
        let s0 = s[0];
        match w.dts_before {
            Some(before) if w.restart => {
                // Re-start: one slot per field entry, evenly in the open interval
                // (DTS_last, S_new[0]); the guard covers an empty interval.
                let step = (s0 - before) / (placed as i64 + 1);
                for (slot, i) in (start..start + placed).enumerate() {
                    self.set(i, before + (slot as i64 + 1) * step);
                }
            }
            _ => {
                // Start-up: DTS_k = S[0] − (L − k)·T over the L + 1 window units. The
                // reorder delay "is a multiple of the nominal picture period" (h222:2012-2013),
                // so T is the stated frame period, floored at 2·ΔPTS_field (MPG3-3).
                let last = s.len() - 1;
                let t = self
                    .frame_ticks
                    .unwrap_or_else(|| min_gap(&s))
                    .max(2 * self.field_delta_max);
                let mut k = 0usize;
                for i in start..start + placed {
                    match self.queue[i].kind {
                        Kind::Unit => {
                            self.set(i, s0 - (last - k.min(last)) as i64 * t);
                            k += 1;
                        }
                        Kind::Second { .. } => self.resolve_steady(i),
                    }
                }
            }
        }
        for i in start + placed..self.queue.len() {
            self.resolve_steady(i);
        }
    }
}

// T when no frame period is stated: the smallest positive gap between the sorted window
// PTS, else 1/24 s.
fn min_gap(sorted: &[i64]) -> i64 {
    sorted
        .windows(2)
        .map(|w| w[1] - w[0])
        .filter(|&g| g > 0)
        .min()
        .unwrap_or(FALLBACK_FRAME_TICKS)
}

#[cfg(test)]
pub(crate) mod test_es;

#[cfg(test)]
mod tests;
