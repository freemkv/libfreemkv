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
            buf: Vec::with_capacity(256 * 1024),
            base: 0,
            marks: VecDeque::new(),
            disc_marks: VecDeque::new(),
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
        }
    }

    /// Feed one PES fragment; return every AU that is now complete.
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
        }
        if discontinuity {
            self.disc_marks.push_back(off);
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
        let mode = self.mode;
        let mut out = Vec::new();
        loop {
            // Locate the AU start code that opens the buffered run.
            let Some(a0) = au_opener(mode, &self.buf) else {
                // No AU boundary buffered. Bound memory: drop all but a 3-byte
                // tail (enough to catch a start-code prefix straddling the cut)
                // once over the cap; otherwise wait for more data.
                if self.buf.len() > MAX_AU_BUFFER {
                    let drop = self.buf.len() - 3;
                    self.buf.drain(..drop);
                    self.base += drop as u64;
                    self.drop_marks_before(self.base);
                }
                break;
            };
            if a0 > 0 {
                // Leading bytes before the first AU boundary are a partial AU from
                // before we synced (or junk) — discard them and any stale marks.
                self.buf.drain(..a0);
                self.base += a0 as u64;
                self.drop_marks_before(self.base);
                continue;
            }
            // The AU runs from here (buf[0]) to the NEXT AU boundary.
            let end = match au_boundary(mode, &self.buf) {
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

            // The AU's own timing/source/discontinuity: by the mark-drain
            // invariant (stale marks below `base` were already dropped) the front
            // mark, if it sits before this AU's end, belongs to this AU.
            let (mut pts, mut dts, mut source) = (None, None, None);
            if let Some(m) = self.marks.front() {
                if m.off < end_abs {
                    pts = m.pts;
                    dts = m.dts;
                    source = m.source;
                }
            }
            while self.marks.front().is_some_and(|m| m.off < end_abs) {
                self.marks.pop_front();
            }
            let mut discontinuity = false;
            if self.disc_marks.front().is_some_and(|&o| o < end_abs) {
                discontinuity = true;
            }
            while self.disc_marks.front().is_some_and(|&o| o < end_abs) {
                self.disc_marks.pop_front();
            }

            let data = self.buf[..end].to_vec();
            self.buf.drain(..end);
            self.base += end as u64;
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

    fn drop_marks_before(&mut self, off: u64) {
        while self.marks.front().is_some_and(|m| m.off < off) {
            self.marks.pop_front();
        }
        while self.disc_marks.front().is_some_and(|&o| o < off) {
            self.disc_marks.pop_front();
        }
    }
}

/// Offset of the start code that opens the next AU in `buf` (at or after 0), or
/// `None` if no AU-opening start code is buffered yet.
fn au_opener(mode: Mode, buf: &[u8]) -> Option<usize> {
    match mode {
        Mode::StartCode(marker) => find_start_code(buf, 0, marker),
        // Any of the three AU-opening BDU types opens a VC-1 access unit.
        Mode::Vc1 => find_vc1_start(buf, 0),
        // A sequence header, GOP header, or picture opens an MPEG-2 access unit.
        Mode::Mpeg2 => find_mpeg2_start(buf, 0),
        Mode::Passthrough => None,
    }
}

/// Offset where the AU that opens at `buf[0]` ends (the start of the next AU), or
/// `None` if the next boundary is not yet buffered.
fn au_boundary(mode: Mode, buf: &[u8]) -> Option<usize> {
    match mode {
        // AU ends at the next delimiter; skip the opening one at buf[0].
        Mode::StartCode(marker) => find_start_code(buf, 4, marker),
        Mode::Vc1 => find_vc1_au_end(buf),
        Mode::Mpeg2 => find_mpeg2_au_end(buf),
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

/// End offset of the VC-1 access unit that opens at `buf[0]`: the next
/// sequence-header / entry-point / frame BDU that appears *after* this AU already
/// contains a frame (`0x0D`). Returns `None` while the AU is still open (no frame
/// yet, or no following BDU buffered). A leading `0x0F`/`0x0E` header group thus
/// stays attached to the frame it precedes rather than the previous AU.
fn find_vc1_au_end(buf: &[u8]) -> Option<usize> {
    let mut seen_frame = false;
    let mut i = 0usize;
    while i + 4 <= buf.len() {
        if buf[i] == 0 && buf[i + 1] == 0 && buf[i + 2] == 1 {
            match buf[i + 3] {
                VC1_FRAME => {
                    if i > 0 && seen_frame {
                        return Some(i);
                    }
                    seen_frame = true;
                }
                VC1_ENTRY | VC1_SEQ => {
                    if i > 0 && seen_frame {
                        return Some(i);
                    }
                }
                _ => {}
            }
            i += 4;
        } else {
            i += 1;
        }
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

/// End offset of the MPEG-2 access unit that opens at `buf[0]`: the next picture
/// / sequence / GOP start code that appears *after* this AU already contains a
/// picture (`0x00`). Returns `None` while the AU is still open (no picture yet,
/// or no following boundary buffered). A leading sequence/GOP header thus stays
/// attached to the picture it introduces. Slice / extension / user-data /
/// sequence-end codes are skipped — they belong to the current AU.
fn find_mpeg2_au_end(buf: &[u8]) -> Option<usize> {
    let mut seen_picture = false;
    let mut i = 0usize;
    while i + 4 <= buf.len() {
        if buf[i] == 0 && buf[i + 1] == 0 && buf[i + 2] == 1 {
            match buf[i + 3] {
                MP2_PICTURE => {
                    if i > 0 && seen_picture {
                        return Some(i);
                    }
                    seen_picture = true;
                }
                MP2_SEQ | MP2_GOP => {
                    if i > 0 && seen_picture {
                        return Some(i);
                    }
                }
                _ => {}
            }
            i += 4;
        } else {
            i += 1;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const AUD: &[u8] = &[0x00, 0x00, 0x01, 0x09]; // H.264 access-unit delimiter

    fn au(payload: u8, len: usize) -> Vec<u8> {
        let mut v = AUD.to_vec();
        v.extend(std::iter::repeat(payload).take(len));
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
        v.extend(std::iter::repeat(payload).take(len));
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
}
