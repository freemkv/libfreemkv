//! Access-unit assembly — a codec-parser helper.
//!
//! The contract a codec parser converts is `PES → access units (Frames)`. A
//! *transport* stream hands the parser one AU per PES for free (BD aligns one
//! access unit per PES; the TS demuxer reassembles to the
//! `payload_unit_start_indicator`). A *program* stream does not — the PS muxer
//! chops the elementary stream into fixed-size PES fragments with no AU
//! alignment, and only the first fragment of an AU carries a PTS. So a parser
//! that assumes one-AU-per-PES (h264/hevc/vc1, written against TS) mis-frames a
//! program stream, while `mpeg2` — the DVD/PS codec — must reassemble across PES.
//!
//! [`AuAssembler`] is that reassembly, factored out so EVERY program-stream video
//! parser shares one implementation instead of hand-rolling the buffer. The
//! h264/hevc/vc1 parsers ([`Mode::StartCode`] / [`Mode::Vc1`]) and the MPEG-2
//! parser ([`Mode::Mpeg2`], via [`AuAssembler::mpeg2`]) all drive it. It buffers
//! PES-fragment bytes and emits one AU per codec AU boundary, carrying the
//! AU-start timing/source forward. Since the boundary is a codec start code, it
//! lives with the codec parser (which picks the marker); only the generic
//! buffering + timing-carry is shared here.
//!
//! This is *inside* the parser, not a pipeline stage: the pipeline stays
//! `Demuxer → PES → Parser → Frames`, and the demuxer stays codec-agnostic. Every
//! stream a parser sees runs through one of these — self-framing codecs (MPEG-2,
//! audio) use [`Mode::Passthrough`] so the parser code path is uniform.

use crate::disc::Codec;
use crate::pes::SourcePos;
use std::collections::VecDeque;

/// Safety cap on a single in-progress access unit. A real coded picture is far
/// below this; a stream that never yields a second AU boundary is force-flushed
/// at the cap rather than buffering without bound on hostile/corrupt input.
const MAX_AU_BUFFER: usize = 8 * 1024 * 1024;

/// Cap on buffered timing/discontinuity marks. A real access unit spans a few
/// hundred PES fragments at most; this bounds the mark deques so a run of
/// zero-length (or start-code-free) timed fragments — which grow no buffer bytes
/// and so never trip the `MAX_AU_BUFFER` mark-prune — cannot accumulate marks
/// without bound on hostile/corrupt disc input.
const MAX_MARKS: usize = 64 * 1024;

/// One AU-complete unit drained from the buffer: its elementary-stream bytes plus
/// the timing/source/discontinuity of the fragment that opened the AU.
pub(crate) struct AssembledAu {
    pub data: Vec<u8>,
    pub pts: Option<i64>,
    pub dts: Option<i64>,
    pub source: Option<SourcePos>,
    pub discontinuity: bool,
}

/// VC-1 (SMPTE 421M Annex E) BDU start-code suffixes, `00 00 01 <type>`.
const VC1_FRAME: u8 = 0x0D; // coded picture
const VC1_ENTRY: u8 = 0x0E; // entry-point header
const VC1_SEQ: u8 = 0x0F; // sequence header

/// MPEG-2 (ISO/IEC 13818-2) start-code suffixes, `00 00 01 <type>`.
const MP2_PICTURE: u8 = 0x00; // picture_start_code
const MP2_SEQ: u8 = 0xB3; // sequence_header_code
const MP2_GOP: u8 = 0xB8; // group_start_code

/// How a stream's fragments become AU-complete units.
#[derive(Clone, Copy)]
enum Mode {
    /// Split the elementary stream on the codec's single AU-delimiter start code
    /// `00 00 01 <marker>` (H.264 AUD `0x09`, HEVC AUD `0x46`). Every AU opens with
    /// exactly that code, so a plain split is correct.
    StartCode(u8),
    /// VC-1 has no single AU delimiter: an access unit is a `[sequence header?]
    /// [entry point?][frame][slices…]` group. The sequence-header (`0x0F`) and
    /// entry-point (`0x0E`) BDUs precede the frame (`0x0D`) they belong to, so a
    /// plain `0x0D` split would glue them onto the *previous* AU and strip every
    /// I-frame of its headers. The boundary is instead the next `0x0F`/`0x0E`/`0x0D`
    /// start code that follows a frame already seen in the current AU.
    Vc1,
    /// MPEG-2 access unit: `[sequence header?][GOP header?][picture][slices…]`.
    /// Structurally identical to [`Mode::Vc1`] — the sequence (`0xB3`) and GOP
    /// (`0xB8`) headers precede the picture (`0x00`) they introduce, so the
    /// boundary is the next picture / sequence / GOP start code that follows a
    /// picture already seen. Slice (`0x01..=0xAF`), extension (`0xB5`),
    /// user-data (`0xB2`) and sequence-end (`0xB7`) codes are NOT boundaries.
    Mpeg2,
    /// The codec self-frames (MPEG-2 reassembles in its own parser; audio resyncs
    /// on syncwords), so each fragment passes straight through as one unit. Lets
    /// the caller run EVERY stream through an assembler with no per-codec branch.
    Passthrough,
}

/// A timing/source mark taken at the absolute stream offset of a fragment that
/// carried it, so it survives `buf.drain(..)` and can be attributed to the AU
/// whose byte range contains it.
struct Mark {
    off: u64,
    pts: Option<i64>,
    dts: Option<i64>,
    source: Option<SourcePos>,
}

/// Reassembles PES fragments into AU-complete units. One per stream; stateful
/// across `push` calls.
pub(crate) struct AuAssembler {
    mode: Mode,
    /// Buffered elementary-stream bytes not yet emitted as a complete AU.
    buf: Vec<u8>,
    /// Absolute stream offset of `buf[0]`, so marks (taken at absolute offsets)
    /// survive `buf.drain(..)`.
    base: u64,
    /// Timing/source marks, in fragment order.
    marks: VecDeque<Mark>,
    /// Absolute offsets of fragments flagged with an upstream discontinuity.
    disc_marks: VecDeque<u64>,
    /// A `MAX_AU_BUFFER` backstop discard happened and no AU has been emitted
    /// since. Sticky rather than an offset mark, because the bytes it refers to
    /// no longer exist: the discard is followed by a pre-sync trim that would
    /// retire any mark placed at the new base, and the gap must outlive that.
    /// Consumed by the next AU to emit. See `discard_gap_before`.
    pending_gap: bool,
    /// Incremental boundary-scan cursor: the offset into `buf` up to which the
    /// current AU has already been searched for its end without finding one. Each
    /// `push` resumes the boundary search from here instead of rescanning the
    /// whole buffer, so reassembling one AU split across N PES fragments costs
    /// O(AU bytes) total, not O(AU bytes²/fragment). Reset to 0 whenever `buf[0]`
    /// moves (an AU drained, or leading bytes dropped).
    scan_pos: usize,
    /// Whether the current AU has already contained a coded frame/picture — the
    /// state the VC-1/MPEG-2 boundary rule carries across a resumed scan (their
    /// boundary is "the next opener after a frame is already seen"). Meaningless
    /// for `Mode::StartCode`. Reset with `scan_pos`.
    seen_unit: bool,
    /// Pre-sync opener-search cursor: the offset up to which the buffer has been
    /// searched for the FIRST AU opener with none found. Resumes the opener scan
    /// so a long run of junk with no start code (hostile/corrupt input) costs
    /// O(bytes) total, not O(buffer) per push. Reset when `buf[0]` moves.
    opener_pos: usize,
    /// Test-only: how many times `take_front` fell back to the COPY path. The
    /// handover is the whole point of `take_front`, so "did it actually fire" is a
    /// property to MEASURE, not to reason about. See
    /// `handover_survives_a_large_au_instead_of_copying_every_later_one`.
    #[cfg(test)]
    copy_path_hits: usize,
}

impl AuAssembler {
    /// An assembler for `codec`. Video codecs whose parsers assume AU-complete PES
    /// (H.264 / HEVC / VC-1) get a [`Mode::StartCode`] assembler; MPEG-2 (self-
    /// reassembles) and audio/subtitle codecs (self-framing) get [`Mode::Passthrough`]
    /// so callers can run every stream through this uniformly.
    pub(crate) fn for_codec(codec: Codec) -> Self {
        let mode = match codec {
            Codec::H264 => Mode::StartCode(0x09), // access_unit_delimiter NAL (type 9)
            Codec::Hevc => Mode::StartCode(0x46), // AUD NAL (type 35 → (35 << 1) = 0x46)
            Codec::Vc1 => Mode::Vc1,              // frame + preceding seq/entry headers
            _ => Mode::Passthrough,
        };
        Self {
            mode,
            // Passthrough never writes `buf` (one fragment → one unit); only the
            // reassembling modes need reserve. Avoids ~256 KiB per audio/subtitle
            // stream (and every TS/BD stream, which never feeds the assembler).
            buf: match mode {
                Mode::Passthrough => Vec::new(),
                _ => Vec::with_capacity(256 * 1024),
            },
            base: 0,
            marks: VecDeque::new(),
            disc_marks: VecDeque::new(),
            pending_gap: false,
            scan_pos: 0,
            seen_unit: false,
            opener_pos: 0,
            #[cfg(test)]
            copy_path_hits: 0,
        }
    }

    /// An assembler that reassembles MPEG-2 access units. The MPEG-2 parser owns
    /// one of these directly (rather than hand-rolling the buffer): the demux
    /// layer runs MPEG-2 through [`Mode::Passthrough`] and hands each fragment to
    /// the parser, which feeds them here to be reframed on picture boundaries.
    pub(crate) fn mpeg2() -> Self {
        Self {
            mode: Mode::Mpeg2,
            buf: Vec::with_capacity(128 * 1024),
            base: 0,
            marks: VecDeque::new(),
            disc_marks: VecDeque::new(),
            pending_gap: false,
            scan_pos: 0,
            seen_unit: false,
            opener_pos: 0,
            #[cfg(test)]
            copy_path_hits: 0,
        }
    }

    /// Feed one PES fragment the caller OWNS; return every AU now complete. For
    /// a self-framing (`Passthrough`) stream the payload is MOVED straight into
    /// the emitted unit with no copy — the common DVD/HD-DVD case (MPEG-2 video,
    /// all audio). A buffering mode copies into `buf` exactly as [`Self::push`].
    pub(crate) fn push_owned(
        &mut self,
        data: Vec<u8>,
        pts: Option<i64>,
        dts: Option<i64>,
        source: Option<SourcePos>,
        discontinuity: bool,
    ) -> Vec<AssembledAu> {
        if matches!(self.mode, Mode::Passthrough) {
            return vec![AssembledAu {
                data,
                pts,
                dts,
                source,
                discontinuity,
            }];
        }
        self.push(&data, pts, dts, source, discontinuity)
    }

    /// Feed one PES fragment (borrowed); return every AU that is now complete.
    pub(crate) fn push(
        &mut self,
        data: &[u8],
        pts: Option<i64>,
        dts: Option<i64>,
        source: Option<SourcePos>,
        discontinuity: bool,
    ) -> Vec<AssembledAu> {
        // Self-framing codecs pass through unchanged — one fragment, one unit,
        // its own timing. (This is exactly today's behaviour for mpeg2/audio.)
        if matches!(self.mode, Mode::Passthrough) {
            return vec![AssembledAu {
                data: data.to_vec(),
                pts,
                dts,
                source,
                discontinuity,
            }];
        }
        let off = self.base + self.buf.len() as u64;
        if pts.is_some() || dts.is_some() || source.is_some() {
            self.marks.push_back(Mark {
                off,
                pts,
                dts,
                source,
            });
            // Backstop: the `buf`-size cap prunes marks only when bytes accumulate.
            // A run of zero-length (or start-code-free) timed fragments grows no
            // bytes, so bound the deque directly — drop the oldest (stalest) mark,
            // which belongs to an already-emitted or lost AU. A real AU spans far
            // fewer fragments than this cap.
            if self.marks.len() > MAX_MARKS {
                self.marks.pop_front();
            }
        }
        if discontinuity {
            self.disc_marks.push_back(off);
            if self.disc_marks.len() > MAX_MARKS {
                self.disc_marks.pop_front();
            }
        }
        self.buf.extend_from_slice(data);
        self.drain(false)
    }

    /// Emit the trailing in-progress AU at end of stream (no following boundary).
    pub(crate) fn flush(&mut self) -> Vec<AssembledAu> {
        if matches!(self.mode, Mode::Passthrough) {
            return Vec::new();
        }
        self.drain(true)
    }

    fn drain(&mut self, force: bool) -> Vec<AssembledAu> {
        if matches!(self.mode, Mode::Passthrough) {
            return Vec::new();
        }
        let mut out = Vec::new();
        loop {
            // Locate the AU start code that opens the buffered run (resumes from
            // opener_pos so an unsynced junk run is scanned once, not per push).
            let Some(a0) = self.au_opener_resumable() else {
                // No AU boundary buffered. Bound memory: drop all but a 3-byte
                // tail (enough to catch a start-code prefix straddling the cut)
                // once over the cap; otherwise wait for more data.
                if self.buf.len() > MAX_AU_BUFFER {
                    let drop = self.buf.len() - 3;
                    self.buf.drain(..drop);
                    self.base += drop as u64;
                    self.reset_scan();
                    self.discard_gap_before(self.base);
                }
                break;
            };
            if a0 > 0 {
                // Leading bytes before the first AU boundary are a partial AU from
                // before we synced (or junk) — discard them and any stale marks.
                self.buf.drain(..a0);
                self.base += a0 as u64;
                self.reset_scan();
                self.drop_marks_before(self.base);
                continue;
            }
            // The AU runs from here (buf[0]) to the NEXT AU boundary. The search
            // resumes from `scan_pos` (bytes already searched with no boundary),
            // so one AU spread across many fragments is scanned once, not per push.
            let end = match self.au_boundary_resumable() {
                Some(next) => next,
                // No next boundary yet: on EOF (or over-cap backstop) the rest of
                // the buffer is this AU; otherwise wait for more data.
                None if force => self.buf.len(),
                None if self.buf.len() > MAX_AU_BUFFER => self.buf.len(),
                None => break,
            };
            if end == 0 {
                break;
            }
            let end_abs = self.base + end as u64;

            // The AU's own timing/source: take the FIRST Some of each field
            // across every mark in this AU's range [base, end_abs), independently
            // — one PES fragment may carry the source while a later fragment of
            // the same AU carries the PTS (and vice versa), so reading only the
            // front mark would drop the other field. This restores the semantics
            // of the pre-consolidation separate pts/source mark deques.
            let (mut pts, mut dts, mut source) = (None, None, None);
            while self.marks.front().is_some_and(|m| m.off < end_abs) {
                let m = self.marks.pop_front().unwrap();
                pts = pts.or(m.pts);
                dts = dts.or(m.dts);
                source = source.or(m.source);
            }
            // A backstop discard is a gap in its own right, independent of any
            // upstream signal: bytes were thrown away, so this AU does not
            // continue the last one emitted.
            let mut discontinuity = std::mem::take(&mut self.pending_gap);
            if self.disc_marks.front().is_some_and(|&o| o < end_abs) {
                discontinuity = true;
            }
            while self.disc_marks.front().is_some_and(|&o| o < end_abs) {
                self.disc_marks.pop_front();
            }

            let data = self.take_front(end);
            self.base += end as u64;
            self.reset_scan();
            out.push(AssembledAu {
                data,
                pts,
                dts,
                source,
                discontinuity,
            });
        }
        out
    }

    /// Detach `buf[..end]` as the emitted AU's own `Vec` and leave `buf` holding
    /// the tail.
    ///
    /// The AU's bytes are HANDED OVER — `buf`'s allocation becomes the returned
    /// `Vec` and a fresh buffer (pre-sized to the same capacity, so the next AU
    /// accumulates without re-growing) takes its place holding only the short
    /// tail. `buf[..end].to_vec()` + `drain(..end)` instead copied every AU out
    /// in full: on a UHD HEVC title that is a whole-frame memcpy (hundreds of KB)
    /// per coded picture, ~200k times, for bytes that are about to be discarded
    /// from `buf` anyway.
    ///
    /// The allocation COUNT is unchanged (one per AU either way — the frame `Vec`
    /// before, the replacement buffer now), so the only difference is the copy
    /// that no longer happens. Nothing depends on `buf` keeping its identity: the
    /// only state tied to `buf[0]`'s position is `base`/`scan_pos`/`opener_pos`,
    /// which the caller updates immediately after.
    ///
    /// Falls back to a copy when the buffer's capacity is far larger than the AU
    /// (a small AU after a multi-MB one): handing over would otherwise attach an
    /// oversized idle allocation to a small frame for as long as the frame queues
    /// downstream, trading a copy for resident memory.
    ///
    /// That fallback must not become permanent. `buf`'s capacity used to be a
    /// one-way high-water mark — the replacement buffer was created with
    /// `cap.max(tail_len)`, and the copy path's `drain` also preserves `cap` — so
    /// once ONE large AU had been assembled, every later smaller AU satisfied
    /// `cap > 2*end` and took the copy path forever. On a UHD HEVC title the first
    /// IDR grows `buf` to ~4-8 MB, after which each ~200-400 KB P/B AU paid a
    /// whole-AU allocation plus a whole-AU memcpy plus a tail memmove for ~99% of
    /// the ~200,000 coded pictures — tens of GB of exactly the memcpy this handover
    /// exists to remove. So the copy path now also RELEASES the high-water
    /// capacity, which re-arms the handover for the next AU: one copy after a size
    /// step down, not one per frame forever.
    fn take_front(&mut self, end: usize) -> Vec<u8> {
        let cap = self.buf.capacity();
        let tail_len = self.buf.len() - end;
        if cap > end.saturating_mul(2) {
            #[cfg(test)]
            {
                self.copy_path_hits += 1;
            }
            let data = self.buf[..end].to_vec();
            self.buf.drain(..end);
            // Shrink toward what this AU actually needed (the tail plus room for
            // another AU of about this size). Only the short tail is copied, and it
            // brings `cap` back under the `2*end` threshold so the next AU of this
            // size hands over instead of copying.
            self.buf.shrink_to(end.max(tail_len));
            return data;
        }
        // Replacement buffer: enough for the tail plus room to accumulate the next
        // AU of about this size. NOT `cap`, which would re-pin the high-water mark.
        let mut tail = Vec::with_capacity(end.max(tail_len));
        tail.extend_from_slice(&self.buf[end..]);
        let mut data = std::mem::replace(&mut self.buf, tail);
        data.truncate(end);
        data
    }

    /// Reset the incremental boundary-scan cursor. Called whenever `buf[0]` moves
    /// (an AU drained, or leading bytes discarded) so the next scan starts fresh
    /// from the new AU opener.
    fn reset_scan(&mut self) {
        self.scan_pos = 0;
        self.seen_unit = false;
        self.opener_pos = 0;
    }

    /// Locate the first AU opener in `buf`, resuming the search from `opener_pos`
    /// (bytes already searched with no opener) so a long unsynced run costs
    /// O(bytes) total, not O(buffer) per push. Advances `opener_pos` on a miss.
    fn au_opener_resumable(&mut self) -> Option<usize> {
        match au_opener_from(self.mode, &self.buf, self.opener_pos) {
            Some(o) => Some(o),
            None => {
                // Nothing yet; next call resumes here (back up 3 for a straddling
                // start-code prefix). Never advance past what is searchable.
                self.opener_pos = self.buf.len().saturating_sub(3).max(self.opener_pos);
                None
            }
        }
    }

    /// Find the end of the AU that opens at `buf[0]`, resuming from `scan_pos`
    /// (and, for VC-1/MPEG-2, the carried `seen_unit`) instead of rescanning the
    /// whole buffer. On no boundary yet, advances `scan_pos`/`seen_unit` so the
    /// next call continues where this one stopped. Equivalent result to a
    /// from-scratch whole-buffer scan, but O(total AU bytes) across all pushes.
    fn au_boundary_resumable(&mut self) -> Option<usize> {
        match self.mode {
            Mode::StartCode(marker) => {
                // Stateless: the AU ends at the next delimiter after the opener at
                // buf[0]. Resume from the furthest searched offset (never before 4,
                // to skip the opening delimiter). find_start_code needs 4 bytes, so
                // back up 3 to catch a code straddling the previous buffer end.
                let from = self.scan_pos.max(4);
                match find_start_code(&self.buf, from, marker) {
                    Some(e) => Some(e),
                    None => {
                        self.scan_pos = self.buf.len().saturating_sub(3).max(from);
                        None
                    }
                }
            }
            Mode::Vc1 => self.scan_unit_boundary(VC1_FRAME, &[VC1_ENTRY, VC1_SEQ]),
            Mode::Mpeg2 => self.scan_unit_boundary(MP2_PICTURE, &[MP2_SEQ, MP2_GOP]),
            Mode::Passthrough => None,
        }
    }

    /// Resumable form of the VC-1/MPEG-2 boundary rule: scan from `scan_pos`,
    /// carrying `seen_unit`; the AU ends at the next `frame` / `header` start code
    /// once a frame is already seen. Advances `scan_pos`/`seen_unit` when no
    /// boundary is found so the next push continues, not restarts.
    fn scan_unit_boundary(&mut self, frame: u8, headers: &[u8]) -> Option<usize> {
        let buf = &self.buf;
        let mut i = self.scan_pos;
        let mut seen = self.seen_unit;
        while i + 4 <= buf.len() {
            if buf[i] == 0 && buf[i + 1] == 0 && buf[i + 2] == 1 {
                let c = buf[i + 3];
                let is_frame = c == frame;
                if (is_frame || headers.contains(&c)) && i > 0 && seen {
                    // The AU ends at the next frame/header once a frame is seen.
                    return Some(i);
                }
                if is_frame {
                    seen = true;
                }
                i += 4;
            } else {
                i += 1;
            }
        }
        // No boundary yet. Persist the scan state so the next append resumes here
        // rather than rescanning from 0 (the i+=4 stride is preserved exactly).
        self.scan_pos = i;
        self.seen_unit = seen;
        None
    }

    /// Retire every mark that falls before `off`, timing and discontinuity
    /// alike.
    ///
    /// This is the STREAM-START case: bytes ahead of the first AU boundary are
    /// the tail of an access unit that began before we had sync, and there is
    /// no prior AU for them to be discontinuous *from*. Carrying a mark forward
    /// here would arm the resync gate at the head of every title and drop its
    /// first GOP.
    fn drop_marks_before(&mut self, off: u64) {
        while self.marks.front().is_some_and(|m| m.off < off) {
            self.marks.pop_front();
        }
        while self.disc_marks.front().is_some_and(|&o| o < off) {
            self.disc_marks.pop_front();
        }
    }

    /// Retire stale timing marks before `off` and record that a GAP occurred
    /// there.
    ///
    /// This is the BACKSTOP case: `MAX_AU_BUFFER` bytes accumulated with no AU
    /// start code in them, so the run is unusable and gets thrown away. Unlike
    /// the stream-start trim above, there IS a prior AU here, and whatever
    /// follows definitively does not continue it — a decoder handed the next
    /// picture would resolve its references against frames separated from it by
    /// megabytes of discarded data.
    ///
    /// So the discard is itself a discontinuity, whether or not the source
    /// signalled one. It is recorded as a sticky flag rather than an offset
    /// mark because a mark placed at the new base would be retired moments
    /// later by the pre-sync trim that follows resync — the gap has to outlive
    /// the bytes that caused it. It arms the resync gate, which drops to the
    /// next keyframe instead of emitting a picture with dangling references.
    ///
    /// Timing marks before `off` are still retired — they describe bytes that
    /// no longer exist, and the AU that eventually emits takes its PTS from the
    /// fragment that actually opened it.
    fn discard_gap_before(&mut self, off: u64) {
        while self.marks.front().is_some_and(|m| m.off < off) {
            self.marks.pop_front();
        }
        while self.disc_marks.front().is_some_and(|&o| o < off) {
            self.disc_marks.pop_front();
        }
        self.pending_gap = true;
    }
}

/// Offset of the start code that opens the next AU in `buf` (at or after 0), or
/// `None` if no AU-opening start code is buffered yet.
fn au_opener_from(mode: Mode, buf: &[u8], from: usize) -> Option<usize> {
    match mode {
        Mode::StartCode(marker) => find_start_code(buf, from, marker),
        // Any of the three AU-opening BDU types opens a VC-1 access unit.
        Mode::Vc1 => find_vc1_start(buf, from),
        // A sequence header, GOP header, or picture opens an MPEG-2 access unit.
        Mode::Mpeg2 => find_mpeg2_start(buf, from),
        Mode::Passthrough => None,
    }
}

/// Find the next `00 00 01 <marker>` start code at or after `from`.
fn find_start_code(buf: &[u8], from: usize, marker: u8) -> Option<usize> {
    let mut i = from;
    while i + 4 <= buf.len() {
        if buf[i] == 0 && buf[i + 1] == 0 && buf[i + 2] == 1 && buf[i + 3] == marker {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Find the next VC-1 AU-opening BDU start code (`00 00 01` followed by a
/// sequence header, entry point, or frame) at or after `from`.
fn find_vc1_start(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 4 <= buf.len() {
        if buf[i] == 0
            && buf[i + 1] == 0
            && buf[i + 2] == 1
            && matches!(buf[i + 3], VC1_FRAME | VC1_ENTRY | VC1_SEQ)
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Find the next MPEG-2 AU-opening start code (`00 00 01` followed by a picture,
/// sequence header, or GOP header) at or after `from`.
fn find_mpeg2_start(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 4 <= buf.len() {
        if buf[i] == 0
            && buf[i + 1] == 0
            && buf[i + 2] == 1
            && matches!(buf[i + 3], MP2_PICTURE | MP2_SEQ | MP2_GOP)
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const AUD: &[u8] = &[0x00, 0x00, 0x01, 0x09]; // H.264 access-unit delimiter

    fn au(payload: u8, len: usize) -> Vec<u8> {
        let mut v = AUD.to_vec();
        v.extend(std::iter::repeat_n(payload, len));
        v
    }

    #[test]
    fn self_framing_codecs_pass_through_each_fragment_unchanged() {
        // MPEG-2 (self-reassembles in its parser) and audio (syncword resync) run
        // through a Passthrough assembler: every fragment emerges immediately as
        // one unit with its own timing — byte-identical to today's path.
        for codec in [Codec::Mpeg2, Codec::Ac3Plus, Codec::Dts, Codec::Lpcm] {
            let mut a = AuAssembler::for_codec(codec);
            let out = a.push(&[1, 2, 3, 4], Some(42), None, None, false);
            assert_eq!(
                out.len(),
                1,
                "{codec:?} passes each fragment straight through"
            );
            assert_eq!(out[0].data, vec![1, 2, 3, 4]);
            assert_eq!(out[0].pts, Some(42));
            assert!(a.flush().is_empty(), "passthrough buffers nothing");
        }
    }

    #[test]
    fn video_codecs_reassemble_across_fragments() {
        // H.264 buffers: one fragment is NOT a complete AU on its own.
        let mut a = AuAssembler::for_codec(Codec::H264);
        assert!(
            a.push(&[0, 0, 1, 0x09, 0xAB], Some(1), None, None, false)
                .is_empty(),
            "holds an AU until the next boundary"
        );
    }

    #[test]
    fn one_au_split_across_fragments_reassembles_with_start_pts() {
        // A single AU (AUD + 100 bytes) arrives as three fragments; only the
        // first carries a PTS. It must emit exactly ONE AU with that PTS.
        let mut a = AuAssembler::for_codec(Codec::H264);
        let full = au(0xAB, 100);
        assert!(
            a.push(&full[..40], Some(9000), None, None, false)
                .is_empty()
        );
        assert!(a.push(&full[40..80], None, None, None, false).is_empty());
        assert!(a.push(&full[80..], None, None, None, false).is_empty());
        let out = a.flush();
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].pts,
            Some(9000),
            "AU carries its START pts, not 0/None"
        );
        assert_eq!(out[0].data, full);
    }

    #[test]
    fn two_aus_emit_when_the_second_boundary_arrives() {
        let mut a = AuAssembler::for_codec(Codec::H264);
        let au1 = au(0x11, 50);
        let au2 = au(0x22, 60);
        let mut buf = au1.clone();
        buf.extend_from_slice(&au2);
        // AU1 + AU2's opening AUD → AU1 completes, tagged pts1.
        let out = a.push(&buf[..au1.len() + 4], Some(1000), None, None, false);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].data, au1);
        assert_eq!(out[0].pts, Some(1000));
        a.push(&buf[au1.len() + 4..], None, None, None, false);
        let out2 = a.flush();
        assert_eq!(out2.len(), 1);
        assert_eq!(out2[0].data, au2);
    }

    #[test]
    fn au_merges_pts_and_source_from_different_fragments() {
        // One fragment of an AU may carry the source stamp while a later fragment
        // of the SAME AU carries the PTS (each PES gets a source; only the anchor
        // gets a PTS). The AU must keep BOTH — reading only the front mark would
        // drop whichever field the first fragment lacked.
        let src = crate::pes::SourcePos::at_byte(4242);
        let mut a = AuAssembler::for_codec(Codec::H264);
        let full = au(0xAB, 80);
        // Fragment 1: source only, no PTS.
        assert!(a.push(&full[..30], None, None, Some(src), false).is_empty());
        // Fragment 2 (same AU): PTS only, no source.
        assert!(
            a.push(&full[30..], Some(9000), None, None, false)
                .is_empty()
        );
        let out = a.flush();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pts, Some(9000), "PTS from the 2nd fragment retained");
        assert_eq!(
            out[0].source.map(|s| s.byte),
            Some(4242),
            "source from the 1st fragment retained"
        );
    }

    #[test]
    fn discontinuity_flag_attaches_to_the_au_it_opens() {
        // A discontinuity-flagged fragment opens AU2; that flag must land on AU2,
        // not AU1 (the B1 resync gate keys off it).
        let mut a = AuAssembler::for_codec(Codec::H264);
        let au1 = au(0x11, 30);
        let au2 = au(0x22, 30);
        a.push(&au1, Some(1), None, None, false);
        // AU2 arrives flagged; its opening AUD completes AU1 first.
        let out = a.push(&au2, Some(2), None, None, true);
        assert_eq!(out.len(), 1, "AU1 completes when AU2's boundary arrives");
        assert!(!out[0].discontinuity, "AU1 is NOT the discontinuity");
        let out2 = a.flush();
        assert_eq!(out2.len(), 1);
        assert!(out2[0].discontinuity, "AU2 carries the discontinuity");
    }

    #[test]
    fn leading_bytes_before_first_au_are_discarded() {
        let mut a = AuAssembler::for_codec(Codec::H264);
        let mut buf = vec![0xFF, 0xFF, 0xFF, 0xFF];
        buf.extend_from_slice(&au(0x33, 20));
        a.push(&buf, Some(500), None, None, false);
        let out = a.flush();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].data, au(0x33, 20), "leading junk dropped, AU intact");
    }

    // ── VC-1 AU grouping ──────────────────────────────────────────────────

    fn bdu(ty: u8, payload: u8, len: usize) -> Vec<u8> {
        let mut v = vec![0x00, 0x00, 0x01, ty];
        v.extend(std::iter::repeat_n(payload, len));
        v
    }

    #[test]
    fn vc1_i_frame_keeps_its_preceding_seq_and_entry_headers() {
        // An I-frame AU is [seq 0x0F][entry 0x0E][frame 0x0D][slices]; a following
        // P-frame is just [frame 0x0D][slices]. A plain 0x0D split would strand the
        // seq/entry headers on the P-frame's AU — the decode bug. The VC-1 mode must
        // group them with the I-frame that follows them.
        let mut a = AuAssembler::for_codec(Codec::Vc1);
        let mut iframe = bdu(VC1_SEQ, 0xAA, 8);
        iframe.extend(bdu(VC1_ENTRY, 0xBB, 6));
        iframe.extend(bdu(VC1_FRAME, 0xCC, 20)); // frame + slice bytes
        let pframe = bdu(VC1_FRAME, 0xDD, 15);

        // Feed the I-frame; it stays open until the P-frame's boundary arrives.
        assert!(a.push(&iframe, Some(9000), None, None, false).is_empty());
        let out = a.push(&pframe, Some(9376), None, None, false);
        assert_eq!(out.len(), 1, "I-frame AU completes at the P-frame boundary");
        assert_eq!(out[0].data, iframe, "I-frame AU retains seq+entry+frame");
        assert_eq!(out[0].pts, Some(9000));

        let tail = a.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].data, pframe, "P-frame is its own AU");
        assert_eq!(tail[0].pts, Some(9376));
    }

    #[test]
    fn vc1_consecutive_frames_split_one_per_au() {
        // Back-to-back frames with no headers between them each form their own AU.
        let mut a = AuAssembler::for_codec(Codec::Vc1);
        let f1 = bdu(VC1_FRAME, 0x11, 30);
        let f2 = bdu(VC1_FRAME, 0x22, 40);
        let mut both = f1.clone();
        both.extend_from_slice(&f2);
        both.extend(bdu(VC1_FRAME, 0x33, 4)); // opening boundary of a 3rd frame
        let out = a.push(&both, Some(1), None, None, false);
        assert_eq!(out.len(), 2, "two complete frames emit");
        assert_eq!(out[0].data, f1);
        assert_eq!(out[1].data, f2);
    }

    #[test]
    fn vc1_entry_point_without_seq_header_still_groups_with_frame() {
        // Mid-GOP open points can carry an entry-point header with no sequence
        // header; it must still attach to the frame that follows it.
        let mut a = AuAssembler::for_codec(Codec::Vc1);
        let mut au = bdu(VC1_ENTRY, 0xEE, 5);
        au.extend(bdu(VC1_FRAME, 0xFF, 12));
        let mut done = a.push(&au, Some(500), None, None, false);
        // Next frame's opening boundary closes the entry+frame AU.
        done.extend(a.push(&bdu(VC1_FRAME, 0x00, 4), None, None, None, false));
        done.extend(a.flush());
        assert_eq!(done.len(), 2);
        assert_eq!(done[0].data, au, "entry+frame grouped");
        assert_eq!(done[0].pts, Some(500));
    }

    // ── MPEG-2 AU grouping ────────────────────────────────────────────────

    #[test]
    fn mpeg2_keeps_seq_and_gop_headers_with_their_picture() {
        // A GOP-opening AU is [seq 0xB3][gop 0xB8][picture 0x00][slices]; the next
        // picture (no headers) is its own AU. The seq/GOP headers must stay with
        // the picture they introduce, not glue onto the previous AU.
        let mut a = AuAssembler::mpeg2();
        let mut gop = bdu(MP2_SEQ, 0xAA, 10);
        gop.extend(bdu(MP2_GOP, 0xBB, 8));
        gop.extend(bdu(MP2_PICTURE, 0xCC, 20)); // picture + slice bytes
        let pic2 = bdu(MP2_PICTURE, 0xDD, 15);

        assert!(a.push(&gop, Some(9000), None, None, false).is_empty());
        let out = a.push(&pic2, Some(9376), None, None, false);
        assert_eq!(
            out.len(),
            1,
            "first AU completes at the next picture boundary"
        );
        assert_eq!(out[0].data, gop, "AU retains seq + GOP + picture");
        assert_eq!(out[0].pts, Some(9000));

        let tail = a.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].data, pic2, "second picture is its own AU");
        assert_eq!(tail[0].pts, Some(9376));
    }

    #[test]
    fn mpeg2_slice_codes_are_not_au_boundaries() {
        // Slice start codes (0x01..=0xAF) inside a picture must not split the AU.
        let mut a = AuAssembler::mpeg2();
        let mut pic = bdu(MP2_PICTURE, 0x11, 4);
        pic.extend(bdu(0x01, 0x22, 10)); // slice 1
        pic.extend(bdu(0xAF, 0x33, 10)); // slice 175 (max slice code)
        let next = bdu(MP2_PICTURE, 0x44, 4); // opening boundary of the next AU
        let out = a.push(&[pic.clone(), next].concat(), Some(1), None, None, false);
        assert_eq!(out.len(), 1, "slices stay inside the one picture AU");
        assert_eq!(out[0].data, pic, "AU spans the picture and all its slices");
    }

    #[test]
    fn mpeg2_reassembles_one_picture_split_across_fragments() {
        // A picture split across three PES fragments; only the first carries a PTS.
        let mut a = AuAssembler::mpeg2();
        let full = bdu(MP2_PICTURE, 0xEE, 100);
        assert!(a.push(&full[..40], Some(500), None, None, false).is_empty());
        assert!(a.push(&full[40..80], None, None, None, false).is_empty());
        assert!(a.push(&full[80..], None, None, None, false).is_empty());
        let out = a.flush();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pts, Some(500), "AU carries its START pts");
        assert_eq!(out[0].data, full);
    }

    /// Split `stream` into fragments of `frag` bytes, push them through the given
    /// assembler mode, and return the reassembled AU byte-payloads.
    fn reassemble_with(mut a: AuAssembler, stream: &[u8], frag: usize) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut i = 0;
        while i < stream.len() {
            let end = (i + frag).min(stream.len());
            for au in a.push(&stream[i..end], None, None, None, false) {
                out.push(au.data);
            }
            i = end;
        }
        for au in a.flush() {
            out.push(au.data);
        }
        out
    }

    #[test]
    fn resumable_boundary_matches_from_scratch_across_all_fragmentations() {
        // The incremental scan_pos cursor must produce byte-identical AUs to a
        // whole-buffer rescan, at EVERY fragment granularity (this is what makes
        // the O(n) resume equivalent to the old O(n^2) from-scratch scan). Build a
        // multi-AU stream per codec, reassemble it fed 1 byte at a time up to
        // whole, and require one canonical result.
        let h264 = {
            let mut s = au(0x11, 40); // AU1 (AUD + payload)
            s.extend(au(0x22, 70)); // AU2
            s.extend(au(0x33, 25)); // AU3
            s
        };
        let vc1 = {
            let mut s = bdu(VC1_SEQ, 0xAA, 8);
            s.extend(bdu(VC1_ENTRY, 0xBB, 6));
            s.extend(bdu(VC1_FRAME, 0xCC, 50)); // I-frame AU
            s.extend(bdu(VC1_FRAME, 0xDD, 30)); // P-frame AU
            s.extend(bdu(VC1_FRAME, 0xEE, 20)); // P-frame AU
            s
        };
        let mpeg2 = {
            let mut s = bdu(MP2_SEQ, 0xAA, 10);
            s.extend(bdu(MP2_GOP, 0xBB, 8));
            s.extend(bdu(MP2_PICTURE, 0xCC, 60)); // GOP-opening picture AU
            s.extend(bdu(MP2_PICTURE, 0xDD, 40)); // picture AU
            s
        };
        // (label, stream, assembler factory). MPEG-2 uses the dedicated mpeg2()
        // assembler (Mode::Mpeg2); the AUD/VC-1 codecs use for_codec().
        type MakeAsm = fn() -> AuAssembler;
        let cases: [(&str, &[u8], MakeAsm); 3] = [
            ("h264", &h264, || AuAssembler::for_codec(Codec::H264)),
            ("vc1", &vc1, || AuAssembler::for_codec(Codec::Vc1)),
            ("mpeg2", &mpeg2, AuAssembler::mpeg2),
        ];
        for (label, stream, make) in cases {
            let whole = reassemble_with(make(), stream, stream.len());
            assert!(!whole.is_empty(), "{label}: baseline produced AUs");
            for frag in 1..=stream.len() {
                let got = reassemble_with(make(), stream, frag);
                assert_eq!(
                    got, whole,
                    "{label}: fragmented at {frag} differs from whole-buffer reassembly"
                );
            }
        }
    }

    #[test]
    fn marks_deques_stay_bounded_on_zero_length_timed_fragments() {
        // A run of zero-length fragments that each carry a PTS (or a
        // discontinuity) grows no buffer bytes, so the buf-size cap never prunes
        // the mark deques. The MAX_MARKS backstop must bound them regardless.
        let mut a = AuAssembler::for_codec(Codec::H264);
        for i in 0..(MAX_MARKS * 2) {
            a.push(&[], Some(i as i64), None, None, true);
        }
        assert!(
            a.marks.len() <= MAX_MARKS,
            "marks bounded at MAX_MARKS, got {}",
            a.marks.len()
        );
        assert!(
            a.disc_marks.len() <= MAX_MARKS,
            "disc_marks bounded at MAX_MARKS, got {}",
            a.disc_marks.len()
        );
    }

    /// The 8 MiB backstop throws away a start-code-free run as unusable. The
    /// AU that eventually emits after that discard MUST be marked
    /// discontinuous, whether or not the source ever signalled a
    /// discontinuity: megabytes of the stream are simply gone, so the next
    /// picture cannot resolve its references against the last one that was
    /// emitted.
    ///
    /// `discontinuity` is what arms the resync gate downstream
    /// (`resync.rs`, driven from `mux/disc.rs`), which drops to the next
    /// keyframe rather than emitting a picture with dangling references. If the
    /// flag is retired with the discarded bytes, the gate never arms and the
    /// broken picture goes out — a silent corruption, which is the one class of
    /// loss this crate refuses to have.
    #[test]
    fn a_backstop_discard_marks_the_next_au_discontinuous() {
        let mut a = AuAssembler::for_codec(Codec::H264);

        // A clean AU first, so there IS a prior AU to be discontinuous from.
        let first = au(0x11, 64);
        let mut stream = first.clone();
        stream.extend_from_slice(AUD);
        let out = a.push(&stream, Some(1000), None, None, false);
        assert_eq!(out.len(), 1, "the first AU emits normally");
        assert!(
            !out[0].discontinuity,
            "an ordinary AU at the head of a clean run is continuous"
        );

        // Now start-code-free junk past the cap. The FIRST over-cap run still
        // has the next AU's delimiter at buf[0], so it is force-flushed as an
        // (over-long) access unit — nothing is discarded and nothing is lost.
        // Only once the buffer holds no opener at all does the backstop throw
        // bytes away, which is the case this test is about.
        let junk = vec![0xAB; MAX_AU_BUFFER + 4096];
        a.push(&junk, Some(2000), None, None, false);
        a.push(&junk, Some(2100), None, None, false);

        // Resync: a fresh AU, followed by the delimiter that closes it.
        let mut resumed = au(0x22, 64);
        resumed.extend_from_slice(AUD);
        let out = a.push(&resumed, Some(3000), None, None, false);

        let au2 = out
            .iter()
            .find(|x| x.data.contains(&0x22))
            .expect("the post-gap AU must emit");
        assert!(
            au2.discontinuity,
            "the AU following an 8 MiB backstop discard follows a gap and must \
             say so; without the flag the resync gate never arms and a picture \
             with dangling references is emitted as if it were sound"
        );
    }

    /// The opposite case, and the reason the two call sites are separate.
    ///
    /// Bytes ahead of the FIRST access-unit delimiter are the tail of an AU
    /// that began before we had sync. There is no prior AU for them to be
    /// discontinuous from, so retiring the marks there is right — and
    /// necessary: marking the first AU of every title discontinuous would arm
    /// the resync gate at the head of each one and drop its opening GOP.
    #[test]
    fn a_stream_start_trim_does_not_mark_the_first_au_discontinuous() {
        let mut a = AuAssembler::for_codec(Codec::H264);

        // Junk BEFORE the first delimiter — a partial AU from before sync.
        // Small enough that the backstop never fires; this is the a0 > 0 path.
        let mut stream = vec![0xCD; 512];
        stream.extend_from_slice(&au(0x33, 64));
        stream.extend_from_slice(AUD);

        let out = a.push(&stream, Some(1000), None, None, false);
        let first = out.first().expect("the first synced AU must emit");
        assert!(
            !first.discontinuity,
            "trimming pre-sync bytes at stream start is not a gap in the \
             stream; flagging it would drop the opening GOP of every title"
        );
    }

    /// A discontinuity the SOURCE signalled, on bytes the backstop later throws
    /// away, must not be lost either — the gap is real regardless of which
    /// mechanism noticed it first, and the two must not cancel out.
    #[test]
    fn a_signalled_discontinuity_survives_a_backstop_discard() {
        let mut a = AuAssembler::for_codec(Codec::H264);

        let mut stream = au(0x11, 64);
        stream.extend_from_slice(AUD);
        a.push(&stream, Some(1000), None, None, false);

        // The source says this fragment follows a gap, AND it is start-code-free
        // and long enough to trip the backstop. Two runs, so the second reaches
        // the discard rather than the force-flush (see the test above).
        let junk = vec![0xAB; MAX_AU_BUFFER + 4096];
        a.push(&junk, Some(2000), None, None, true);
        a.push(&junk, Some(2100), None, None, false);

        let mut resumed = au(0x22, 64);
        resumed.extend_from_slice(AUD);
        let out = a.push(&resumed, Some(3000), None, None, false);

        let au2 = out
            .iter()
            .find(|x| x.data.contains(&0x22))
            .expect("the post-gap AU must emit");
        assert!(
            au2.discontinuity,
            "a source-signalled discontinuity on discarded bytes must still \
             reach the AU that follows them"
        );
    }

    #[test]
    fn over_cap_without_boundary_force_flushes() {
        let mut a = AuAssembler::for_codec(Codec::H264);
        let big = au(0x44, MAX_AU_BUFFER + 16);
        let emitted = a.push(&big, Some(1), None, None, false);
        assert!(
            !emitted.is_empty(),
            "over-cap AU is force-flushed, not buffered forever"
        );
    }

    /// MEASURED: a drained AU must be HANDED the accumulation buffer's
    /// allocation, not copied out of it. The emitted `Vec`'s data pointer is the
    /// buffer's own pointer — which is only true if no full-frame copy happened.
    /// (`buf[..end].to_vec()` allocates fresh, so the pointers differ.) One
    /// whole-AU memcpy per coded picture is ~200k memcpys of a few hundred KB
    /// each on a UHD feature.
    #[test]
    fn drained_au_takes_over_the_buffer_allocation_without_copying() {
        let mut a = AuAssembler::for_codec(Codec::H264);
        // An AU large enough that the buffer's capacity is not >2x its size (the
        // small-AU copy path exists so a small frame cannot carry an oversized
        // idle allocation downstream).
        let au1 = au(0x11, 400 * 1024);
        let au2 = au(0x22, 400 * 1024);
        let mut stream = au1.clone();
        stream.extend_from_slice(&au2);

        // Push everything except the final byte of AU2's delimiter, so no AU has
        // been emitted yet but the buffer holds the whole of AU1.
        a.push(&stream[..au1.len() + 3], Some(1), None, None, false);
        let before = a.buf.as_ptr();
        let cap_before = a.buf.capacity();
        let out = a.push(
            &stream[au1.len() + 3..au1.len() + 4],
            None,
            None,
            None,
            false,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].data, au1,
            "handover must preserve the AU bytes exactly"
        );
        assert_eq!(
            out[0].data.as_ptr(),
            before,
            "the emitted AU must own the buffer's allocation (no whole-frame copy)"
        );
        // The replacement buffer keeps room for another AU of about this size, so
        // the next AU does not re-grow — but it is NOT pinned to the OLD capacity,
        // which would make `buf` a permanent high-water mark and send every later
        // smaller AU down the copy path (see
        // `handover_survives_a_large_au_instead_of_copying_every_later_one`).
        assert!(
            a.buf.capacity() >= au1.len(),
            "replacement buffer must fit another AU of this size: {} < {}",
            a.buf.capacity(),
            au1.len()
        );
        assert!(
            a.buf.capacity() <= cap_before,
            "replacement buffer must never EXCEED the old capacity"
        );
        assert_eq!(a.buf.len(), 4, "the buffer holds only AU2's delimiter tail");
    }

    /// MEASURED: `take_front`'s copy fallback must not become permanent.
    ///
    /// `buf`'s capacity used to be a one-way high-water mark, and the copy path's
    /// `drain` preserves it, so after ONE large AU every later smaller AU satisfied
    /// `cap > 2*end` and copied forever. On a UHD HEVC title the first IDR grows
    /// `buf` to multiple MB, after which ~99% of the ~200,000 coded pictures each
    /// paid a whole-AU allocation + whole-AU memcpy + tail memmove — tens of GB of
    /// exactly the copy the handover exists to remove. Counted at the copy path
    /// itself: one copy is expected right after the size step down; a per-frame
    /// copy is the bug.
    #[test]
    fn handover_survives_a_large_au_instead_of_copying_every_later_one() {
        let mut a = AuAssembler::for_codec(Codec::H264);
        // Production shape: BD-TS aligns one access unit per PES, so each `push`
        // carries about one AU and the buffer holds ~one AU at a time. One large AU
        // (the IDR) followed by a run of much smaller ones (P/B frames). Each AU is
        // pushed with the NEXT AU's opener so the previous one closes.
        const SMALL: usize = 64 * 1024;
        let mut pending = au(0x11, 2 * 1024 * 1024);
        for i in 0..20u8 {
            let next = au(0x30 + i, SMALL);
            // Append the next AU's 4-byte opener to close `pending`, push, and
            // carry the rest of `next` forward.
            pending.extend_from_slice(&next[..4]);
            a.push(&pending, Some(1), None, None, false);
            pending = next[4..].to_vec();
        }
        let hits = a.copy_path_hits;
        assert!(
            hits <= 2,
            "the copy fallback must re-arm the handover, not fire for every AU \
             after a large one: {hits} copies over 20 access units"
        );
    }

    // ── AU-opener detection: the per-mode start-code rule ─────────────────
    //
    // `au_opener_from` is the SECOND implementation of a rule each codec parser
    // also encodes (h264 `NAL_AUD`, hevc `NAL_AUD`, vc1 `SC_*`, mpeg2
    // `PICTURE_CODE`/`SEQ_HEADER_CODE`/`GOP_CODE`). Two independent copies of one
    // rule drift; these cases pin this copy to the normative byte values and to
    // the codes that are explicitly NOT openers, so a drift shows up here.

    /// The opener offset must be the position of the real start code, never a
    /// fixed 0. A constant `Some(0)` makes every pre-sync run of junk bytes look
    /// like the head of an access unit, so the first AU of every stream that does
    /// not begin exactly on a start code is emitted with junk glued to its front.
    #[test]
    fn au_opener_from_locates_the_real_start_code_per_codec() {
        // Junk that contains a start-code PREFIX but no opener suffix, so a
        // scanner that stopped at `00 00 01` alone would answer wrongly.
        let junk: &[u8] = &[0xFF, 0x00, 0x00, 0x01, 0x67, 0xAA];
        let cases: &[(Mode, u8, &str)] = &[
            // ISO/IEC 14496-10 §7.4.1: nal_unit_type 9 = access unit delimiter,
            // and nal_ref_idc shall be 0 for it, so the header byte is 0x09.
            (Mode::StartCode(0x09), 0x09, "H.264 AUD"),
            // ITU-T H.265 §7.4.2.2: nal_unit_type 35 = AUD_NUT. The first NAL
            // header byte is forbidden_zero_bit(1) | nal_unit_type(6) |
            // nuh_layer_id MSB(1) = (35 << 1) = 0x46 on the base layer.
            (Mode::StartCode(0x46), 0x46, "HEVC AUD"),
            // SMPTE 421M Annex E BDU types.
            (Mode::Vc1, VC1_SEQ, "VC-1 sequence header"),
            (Mode::Vc1, VC1_ENTRY, "VC-1 entry point"),
            (Mode::Vc1, VC1_FRAME, "VC-1 frame"),
            // ISO/IEC 13818-2 §6.2.1 Table 6-1 start code values.
            (Mode::Mpeg2, MP2_PICTURE, "MPEG-2 picture"),
            (Mode::Mpeg2, MP2_SEQ, "MPEG-2 sequence header"),
            (Mode::Mpeg2, MP2_GOP, "MPEG-2 GOP header"),
        ];
        for &(mode, code, what) in cases {
            let mut buf = junk.to_vec();
            buf.extend_from_slice(&[0x00, 0x00, 0x01, code, 0x5A]);
            assert_eq!(
                au_opener_from(mode, &buf, 0),
                Some(junk.len()),
                "{what}: opener must be found at the start code, not at 0"
            );
            // `from` must actually skip: searching past the only opener finds none.
            assert_eq!(
                au_opener_from(mode, &buf, junk.len() + 1),
                None,
                "{what}: the resume cursor must be honoured"
            );
        }
    }

    /// Start codes that are NOT access-unit openers must not be reported as one.
    /// Treating a slice or an extension header as an AU start splits one coded
    /// picture into several frames, each missing its picture header.
    #[test]
    fn non_opening_start_codes_are_not_au_openers() {
        // ISO/IEC 13818-2 Table 6-1: slice (0x01..=0xAF), user data (0xB2),
        // extension (0xB5), sequence end (0xB7) all appear INSIDE an access unit.
        for code in [0x01u8, 0xAF, 0xB2, 0xB5, 0xB7] {
            let buf = [0x00, 0x00, 0x01, code, 0x11, 0x22];
            assert_eq!(
                au_opener_from(Mode::Mpeg2, &buf, 0),
                None,
                "MPEG-2 start code {code:#04x} must not open an access unit"
            );
        }
        // SMPTE 421M: slice (0x0B) and field (0x0C) BDUs belong to the frame
        // already in progress; end-of-sequence (0x0A) opens nothing.
        for code in [0x0Au8, 0x0B, 0x0C] {
            let buf = [0x00, 0x00, 0x01, code, 0x11, 0x22];
            assert_eq!(
                au_opener_from(Mode::Vc1, &buf, 0),
                None,
                "VC-1 BDU {code:#04x} must not open an access unit"
            );
        }
        // H.264: an SPS (7) / PPS (8) / IDR slice (5) is not the AU DELIMITER the
        // StartCode mode splits on.
        for code in [0x05u8, 0x67, 0x68] {
            let buf = [0x00, 0x00, 0x01, code, 0x11, 0x22];
            assert_eq!(au_opener_from(Mode::StartCode(0x09), &buf, 0), None);
        }
        // Passthrough never frames — the codec self-frames.
        assert_eq!(
            au_opener_from(Mode::Passthrough, &[0, 0, 1, 0x09, 0xAA], 0),
            None
        );
    }

    /// `au_opener_resumable` must return the true offset AND advance
    /// `opener_pos` only over bytes that cannot hide a straddling start code.
    /// A constant `Some(0)` short-circuits both.
    #[test]
    fn au_opener_resumable_reports_the_real_offset_and_resumes_safely() {
        let mut a = AuAssembler::for_codec(Codec::H264);

        // A junk run with no opener: None, and the cursor parks 3 bytes back so a
        // start code split across the append boundary is still found.
        a.buf.extend_from_slice(&[0xFFu8; 32]);
        assert_eq!(a.au_opener_resumable(), None, "no opener in a junk run");
        assert_eq!(
            a.opener_pos, 29,
            "resume 3 bytes back for a straddling code"
        );

        // Now append a start code that STRADDLES the previous end: the first three
        // bytes of `00 00 01 09` land at offsets 29..32.
        a.buf.truncate(29);
        a.buf.extend_from_slice(&[0x00, 0x00, 0x01, 0x09, 0x77]);
        assert_eq!(
            a.au_opener_resumable(),
            Some(29),
            "a start code straddling the previous scan end must still be found"
        );
    }

    /// After the pre-sync bytes are discarded, the emitted AU must take the
    /// timing of the fragment that ACTUALLY opened it. `drop_marks_before` is
    /// what retires the discarded fragment's marks; a no-op there stamps the
    /// first real access unit with the PTS and source of bytes that were thrown
    /// away — a whole-title A/V sync offset, since every later frame is timed
    /// relative to it.
    #[test]
    fn discarded_pre_sync_marks_do_not_time_the_first_access_unit() {
        let src = |b: u64| SourcePos {
            byte: b,
            ..Default::default()
        };
        let mut a = AuAssembler::for_codec(Codec::H264);

        // Fragment 1: pre-sync junk, no start code. Carries its own PTS/source.
        assert!(
            a.push(&[0xFFu8; 24], Some(1_000), Some(900), Some(src(11)), false)
                .is_empty()
        );
        // Fragment 2: the first real AU opener, with the timing that belongs to it.
        assert!(
            a.push(
                &au(0x33, 40),
                Some(2_000),
                Some(1_900),
                Some(src(22)),
                false
            )
            .is_empty()
        );
        // Fragment 3: a second AU, closing the first.
        let out = a.push(
            &au(0x44, 40),
            Some(3_000),
            Some(2_900),
            Some(src(33)),
            false,
        );

        assert_eq!(out.len(), 1, "the first AU closes on the second opener");
        assert_eq!(out[0].data, au(0x33, 40), "junk discarded, AU intact");
        assert_eq!(
            out[0].pts,
            Some(2_000),
            "the AU must take the opening fragment's PTS, not the discarded junk's"
        );
        assert_eq!(out[0].dts, Some(1_900), "same for DTS");
        assert_eq!(
            out[0].source.map(|s| s.byte),
            Some(22),
            "same for the source position used by the recovery map"
        );

        let tail = a.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].pts, Some(3_000), "the second AU keeps its own PTS");
    }

    /// `for_codec` is the dispatch that decides whether a stream is REASSEMBLED
    /// across PES fragments or passed straight through. Getting it wrong is
    /// silent: an H.264/HEVC/VC-1 stream routed to `Passthrough` on a program
    /// source emits one "frame" per PES fragment — a few hundred bytes of a
    /// coded picture, framed as a whole access unit — and the output plays as
    /// corruption, not as an error.
    ///
    /// Each mode is identified BEHAVIOURALLY (feed a two-AU stream in two halves
    /// and see whether it reassembles), so the case cannot pass by matching a
    /// constant.
    #[test]
    fn for_codec_routes_each_video_codec_to_its_reassembly_mode() {
        // Buffering codecs: a stream split mid-AU must NOT emit until the second
        // AU's opener arrives, and must then emit the FIRST AU whole.
        let buffering: &[(Codec, u8)] = &[
            (Codec::H264, 0x09), // ISO/IEC 14496-10 §7.4.1 AUD
            (Codec::Hevc, 0x46), // ITU-T H.265 §7.4.2.2 AUD_NUT, (35 << 1)
        ];
        for &(codec, marker) in buffering {
            let mut a = AuAssembler::for_codec(codec);
            let mut unit = vec![0x00, 0x00, 0x01, marker];
            unit.extend(std::iter::repeat_n(0x5Au8, 30));
            // First half of AU 1: nothing complete yet.
            assert!(
                a.push(&unit[..20], Some(1), None, None, false).is_empty(),
                "{codec:?} must buffer a partial access unit, not emit it"
            );
            assert!(
                a.push(&unit[20..], None, None, None, false).is_empty(),
                "{codec:?} must hold AU 1 until the next opener"
            );
            // AU 2's opener closes AU 1.
            let out = a.push(&unit, Some(2), None, None, false);
            assert_eq!(out.len(), 1, "{codec:?} emits exactly one AU");
            assert_eq!(out[0].data, unit, "{codec:?} reassembles AU 1 whole");
            assert_eq!(out[0].pts, Some(1), "{codec:?} carries the AU-start PTS");
        }

        // VC-1 buffers too, on its own boundary rule (no single AU delimiter).
        let mut a = AuAssembler::for_codec(Codec::Vc1);
        let frame = bdu(VC1_FRAME, 0x77, 30);
        assert!(a.push(&frame, Some(1), None, None, false).is_empty());
        assert_eq!(
            a.push(&frame, Some(2), None, None, false).len(),
            1,
            "VC-1 emits AU 1 when the next frame BDU opens AU 2"
        );

        // Self-framing codecs pass each fragment through immediately — the same
        // half-AU input that the buffering modes held back comes straight out.
        for codec in [Codec::Mpeg2, Codec::Ac3, Codec::TrueHd, Codec::Pgs] {
            let mut a = AuAssembler::for_codec(codec);
            let out = a.push(&[0x00, 0x00, 0x01, 0x09, 0xAA], Some(7), None, None, false);
            assert_eq!(out.len(), 1, "{codec:?} must pass through, not buffer");
            assert_eq!(out[0].pts, Some(7));
            assert!(a.flush().is_empty(), "{codec:?} buffers nothing at EOF");
        }
    }
}
