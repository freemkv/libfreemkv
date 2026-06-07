//! DTS / DTS-HD elementary stream parser.
//!
//! DTS core syncword: 0x7FFE8001 (32 bits).
//! DTS-HD MA/HRA extension syncword: 0x64582025 (32 bits), appears after the core frame.
//! Buffers across PES boundaries so frames spanning two PES packets
//! are emitted complete.

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

const DTS_CORE_SYNC: [u8; 4] = [0x7F, 0xFE, 0x80, 0x01];
/// DTS-HD extension substream syncword. The parser delimits an access unit by
/// the next CORE sync (so every extension between two cores is captured), and
/// never needs to locate or size the extension itself — so this is referenced
/// only by the tests that synthesize extension substreams.
#[cfg(test)]
const DTS_HD_EXT_SYNC: [u8; 4] = [0x64, 0x58, 0x20, 0x25];

pub struct DtsParser {
    buf: Vec<u8>,
    /// PTS of the access unit currently being assembled in `buf` (the unit
    /// starting at the first buffered core sync). Captured when that core
    /// frame's PES first arrived; the trailing extension-substream PES
    /// packets carry their own (later) PTS which must NOT override it.
    pending_pts: i64,
    /// PTS markers attributing buffer regions to their source PES. Each entry
    /// is `(buffer_offset, pts_ns)` for the PES whose bytes begin at that
    /// offset. When an access unit is emitted from the front of `buf`, its PTS
    /// is the marker covering offset 0 — NOT the most recent PES's PTS. This is
    /// what fixes multi-AU-per-call PTS attribution: if a PES carries the
    /// extension substreams (and possibly the next core) for an AU whose own
    /// core arrived in an earlier PES, the emitted AU keeps its own core PES's
    /// timestamp instead of the later PES's. Offsets are kept relative to the
    /// current `buf` start and rebased whenever bytes are drained from the
    /// front.
    pts_marks: Vec<(usize, i64)>,
}

impl Default for DtsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl DtsParser {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(32768),
            pending_pts: 0,
            pts_marks: Vec::new(),
        }
    }

    /// Drop `n` bytes from the front of `buf` and rebase the PTS markers so
    /// their offsets stay relative to the new buffer start. A marker that now
    /// sits at or before offset 0 is clamped to 0 (it still covers the front).
    /// Redundant markers all at offset 0 collapse to the last one.
    fn drain_front(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        self.buf.drain(..n);
        for m in &mut self.pts_marks {
            m.0 = m.0.saturating_sub(n);
        }
        // Collapse all leading markers that now sit at offset 0 to the last
        // such marker — that is the PES whose data currently begins the buffer.
        let last_zero = self
            .pts_marks
            .iter()
            .rposition(|&(off, _)| off == 0)
            .filter(|&i| i > 0);
        if let Some(i) = last_zero {
            self.pts_marks.drain(..i);
        }
    }

    /// PTS that should be stamped on an access unit currently at the front of
    /// `buf` (offset 0): the most recent marker at offset 0, falling back to
    /// `pending_pts`.
    fn front_pts(&self) -> i64 {
        self.pts_marks
            .iter()
            .rev()
            .find(|&&(off, _)| off == 0)
            .map(|&(_, pts)| pts)
            .unwrap_or(self.pending_pts)
    }
}

/// Hard cap on a buffered access unit (core + all its extension substreams).
/// A DTS-HD MA frame is at most a few tens of KB; if the buffer grows past
/// this without a clean boundary we resync rather than stall or balloon.
const MAX_AU_BYTES: usize = 65536;

/// Minimum plausible DTS core frame size. The core header alone is ~10-14
/// bytes; a decoded `core_size` below this means we matched a false/corrupt
/// core sync (the 14-bit `fsize` field decoded to a tiny value) rather than a
/// real frame, so we resync instead of emitting a junk access unit.
const MIN_CORE_FRAME_BYTES: usize = 10;

/// Sentinel for "no valid PTS base captured yet". Real PTS-in-ns values are
/// non-negative (derived from the unsigned 90 kHz PES timestamp), so a negative
/// value can never collide with a genuine timestamp. Used to mark the PTS base
/// invalid after a forced flush so the next PES sets it regardless of buffer
/// state.
const PTS_UNSET: i64 = -1;

impl CodecParser for DtsParser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        if pes.data.is_empty() {
            return Vec::new();
        }
        let pts_ns = pes.pts.map(pts_to_ns).unwrap_or(0);

        // On Blu-ray, a DTS-HD MA/HRA access unit is a DTS core frame
        // (sync 0x7FFE8001) followed by one or more DTS extension substreams
        // (sync 0x64582025). The m2ts demuxer hands those out as SEPARATE PES
        // packets on the same PID — the core in one PES, then the extension
        // substreams in following PES packets (with their own, later PTS). The
        // lossless audio lives entirely in the extension substreams, so an
        // access unit is only complete once all of its trailing extensions
        // have been buffered. We assemble across PES boundaries here: an access
        // unit runs from its core sync up to (but not including) the NEXT core
        // sync. Emitting on the core boundary keeps the core + every following
        // extension substream together (the lossless data), instead of the
        // old per-PES emit that dropped the extension PES packets and
        // downgraded the track to lossy DTS core (the Dunkirk / Fight Club
        // bug). The PTS is the core frame's PTS, captured when the unit began.
        // Capture the access unit's PTS base on a fresh buffer, or whenever a
        // prior forced (safety-valve) flush left it invalidated — in the
        // forced case the bytes still in `buf` are not a real core frame, so
        // the first PES to arrive after the flush carries the correct base.
        if self.buf.is_empty() || self.pending_pts == PTS_UNSET {
            self.pending_pts = pts_ns;
        }
        // Mark where THIS PES's bytes begin in the buffer, with its PTS. The
        // emitted access unit takes the PTS of the PES covering its first byte
        // (see `front_pts`), so an AU whose core arrived in an earlier PES keeps
        // that core's timestamp even when its extensions / the following core
        // arrive (with a later PTS) in this same parse() call.
        self.pts_marks.push((self.buf.len(), pts_ns));
        self.buf.extend_from_slice(&pes.data);

        let mut frames = Vec::new();

        loop {
            // Resync to the first core sync; drop any leading junk.
            let Some(start) = find_sync(&self.buf, &DTS_CORE_SYNC) else {
                // No core sync at all yet — keep at most a 3-byte tail so a
                // sync split across PES packets can still be found next time.
                if self.buf.len() > 3 {
                    let tail = self.buf.len() - 3;
                    self.drain_front(tail);
                }
                break;
            };
            if start > 0 {
                self.drain_front(start);
                if find_sync(&self.buf, &DTS_CORE_SYNC) != Some(0) {
                    // Shouldn't happen, but never loop forever.
                    break;
                }
            }

            // Need the core header to size the core frame.
            if self.buf.len() < 10 {
                break;
            }
            let core_size = dts_core_frame_size(&self.buf);
            // `dts_core_frame_size` returns a 14-bit `fsize + 1`, so it is
            // always in [1, 16384]; the bare `== 0` / `> MAX_AU_BYTES` checks
            // can never fire. A real DTS core header is at least ~10-14 bytes,
            // so any decoded size below that came from a false/corrupt sync.
            // Reject it (drain the 4 syncword bytes and resync) instead of
            // letting a tiny bogus size close the current access unit at a junk
            // boundary and drop the trailing extension substreams. The
            // `> MAX_AU_BYTES` upper bound is kept as a harmless guard.
            if !(MIN_CORE_FRAME_BYTES..=MAX_AU_BYTES).contains(&core_size) {
                // Bogus core sync — skip past it and resync.
                self.drain_front(4);
                continue;
            }
            if self.buf.len() < core_size {
                break; // core frame not fully buffered yet — wait
            }

            // The access unit ends at the next *valid* core sync. The search
            // begins after this core's syncword so we don't re-match it.
            // Anything between the core and that next sync is this unit's
            // extension substream(s) — which can themselves contain byte
            // sequences matching the core syncword, so a raw `find_sync` match
            // is not enough: a candidate is only a real boundary if its decoded
            // core size is plausible. `next_core_boundary` skips bogus matches.
            //
            // `forced` distinguishes a real next-core boundary from a forced
            // safety-valve flush. On a forced flush the access unit was NOT
            // closed by a new core sync, so the bytes following it are not a
            // fresh core frame and the current PES's PTS (which on a forced
            // flush is an extension-substream PES, carrying its own later
            // timestamp) must NOT become the next unit's PTS base.
            let mut forced = false;
            let au_end = match next_core_boundary(&self.buf, core_size) {
                NextCore::Found(end) => end,
                NextCore::NeedMore => break, // candidate sync needs more header
                NextCore::None => {
                    // No next core sync buffered yet. The trailing extension
                    // substream PES packets may still be arriving, so WAIT for
                    // them rather than emit a core-only (lossy) frame — unless
                    // the buffer has grown unreasonably large, in which case
                    // emit what we have to guarantee forward progress.
                    if self.buf.len() <= MAX_AU_BYTES {
                        break;
                    }
                    forced = true;
                    self.buf.len()
                }
            };

            let au: Vec<u8> = self.buf[..au_end].to_vec();
            // The AU takes the PTS of the PES covering its first byte — its own
            // core's PES, even if that PES preceded the one(s) carrying its
            // extensions or the next core.
            let au_pts = self.front_pts();
            frames.push(Frame {
                pts_ns: au_pts,
                keyframe: true,
                data: au,
                duration_ns: None,
            });
            self.drain_front(au_end);
            // After draining, the marker covering the new front (if any) carries
            // the next AU's PTS; `pending_pts` is only the fallback when no
            // marker survives. Track it so the fallback stays sensible.
            self.pending_pts = self.front_pts();
            if forced {
                // Safety-valve flush: the next access unit's real core PES has
                // not arrived. Invalidate the PTS so the next PES sets it
                // regardless of buffer state, rather than inheriting this
                // (non-core) PES's timestamp.
                self.pending_pts = PTS_UNSET;
                self.pts_marks.clear();
            }
        }

        // Discard markers that no longer reference live buffer bytes (everything
        // past the buffer end can't happen, but collapse duplicates at offset 0
        // and drop a stale empty-buffer marker set).
        if self.buf.is_empty() {
            self.pts_marks.clear();
        }

        frames
    }

    fn flush(&mut self) -> Vec<Frame> {
        // End of stream: emit the final access unit still buffered (the last
        // core + its extension substreams, which had no following core sync to
        // close it during streaming). Require a complete core frame; drop a
        // bare partial sync tail.
        if find_sync(&self.buf, &DTS_CORE_SYNC) != Some(0) || self.buf.len() < 10 {
            self.buf.clear();
            return Vec::new();
        }
        let core_size = dts_core_frame_size(&self.buf);
        // `dts_core_frame_size` returns a 14-bit `fsize + 1` (never 0), so the
        // old `== 0` check was dead; reject a sub-minimum core like `parse()`.
        if core_size < MIN_CORE_FRAME_BYTES || self.buf.len() < core_size {
            self.buf.clear();
            return Vec::new();
        }
        // The final AU's PTS is the PES covering the buffer front (its core's
        // PES). Fall back to pending_pts, clamping the sentinel to 0.
        let front = self.front_pts();
        let pts_ns = if front == PTS_UNSET { 0 } else { front };
        let au = std::mem::take(&mut self.buf);
        self.pts_marks.clear();
        vec![Frame {
            pts_ns,
            keyframe: true,
            data: au,
            duration_ns: None,
        }]
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

fn find_sync(data: &[u8], pattern: &[u8; 4]) -> Option<usize> {
    if data.len() < 4 {
        return None;
    }
    (0..=data.len() - 4).find(|&i| data[i..i + 4] == *pattern)
}

/// Result of scanning for the next valid core sync that closes an access unit.
enum NextCore {
    /// A valid next core sync was found; the access unit ends at this offset.
    Found(usize),
    /// A candidate core sync was found but its header isn't fully buffered yet,
    /// so its validity can't be decided — wait for more data.
    NeedMore,
    /// No (further) core sync found in the buffer.
    None,
}

/// Find the next *valid* core sync after the current core frame, to delimit the
/// access unit. Extension-substream payload can contain byte sequences that
/// match the core syncword, so each candidate is validated by decoding its
/// core size: a match whose decoded size is implausible (< MIN_CORE_FRAME_BYTES
/// or > MAX_AU_BYTES) is a false sync and is skipped, continuing the search.
fn next_core_boundary(buf: &[u8], core_size: usize) -> NextCore {
    let mut from = core_size;
    while let Some(rel) = find_sync(&buf[from..], &DTS_CORE_SYNC) {
        let pos = from + rel;
        // Need the candidate's core header to judge it.
        if buf.len() - pos < 10 {
            return NextCore::NeedMore;
        }
        let sz = dts_core_frame_size(&buf[pos..]);
        if (MIN_CORE_FRAME_BYTES..=MAX_AU_BYTES).contains(&sz) {
            return NextCore::Found(pos);
        }
        // False sync inside extension payload — skip it and keep searching.
        from = pos + 4;
    }
    NextCore::None
}

/// DTS core frame size from header bits.
/// fsize is at bits 46-59 (14 bits) of the header: bytes 5-7.
fn dts_core_frame_size(data: &[u8]) -> usize {
    if data.len() < 10 {
        return 0;
    }
    // fsize field: 14 bits starting at bit 46
    // byte 5 bits 1-0, byte 6 all 8, byte 7 bits 7-4
    let fsize =
        ((data[5] as usize & 0x03) << 12) | ((data[6] as usize) << 4) | ((data[7] as usize) >> 4);
    fsize + 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::ts::PesPacket;

    fn make_pes(data: Vec<u8>, pts: Option<i64>) -> PesPacket {
        PesPacket {
            pid: 0x1100,
            pts,
            dts: None,
            data,
        }
    }

    fn make_dts_core(size: usize) -> Vec<u8> {
        let fsize = size - 1;
        let mut data = vec![0u8; size];
        data[0..4].copy_from_slice(&DTS_CORE_SYNC);
        data[5] = (data[5] & 0xFC) | ((fsize >> 12) & 0x03) as u8;
        data[6] = ((fsize >> 4) & 0xFF) as u8;
        data[7] = (data[7] & 0x0F) | (((fsize & 0x0F) << 4) as u8);
        data
    }

    #[test]
    fn parse_empty_pes() {
        let mut parser = DtsParser::new();
        let pes = make_pes(Vec::new(), Some(0));
        assert!(parser.parse(&pes).is_empty());
    }

    #[test]
    fn parse_single_frame() {
        // A single core frame with no following core sync is the LAST access
        // unit — held during streaming (can't know an extension won't follow),
        // then drained on flush() at EOF.
        let mut parser = DtsParser::new();
        let frame = make_dts_core(512);
        let pes = make_pes(frame, Some(90000));
        assert!(parser.parse(&pes).is_empty());
        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].data.len(), 512);
    }

    #[test]
    fn parse_frame_spanning_two_pes() {
        let mut parser = DtsParser::new();
        let frame = make_dts_core(512);
        let mid = 256;

        let pes1 = make_pes(frame[..mid].to_vec(), Some(90000));
        assert!(parser.parse(&pes1).is_empty());

        let pes2 = make_pes(frame[mid..].to_vec(), Some(93000));
        assert!(parser.parse(&pes2).is_empty());
        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].data.len(), 512);
    }

    #[test]
    fn two_cores_back_to_back_emit_first_on_boundary() {
        // The first complete unit is emitted as soon as the next core sync is
        // seen; the second is held until flush. Both cores arrived in ONE PES,
        // so both legitimately carry that PES's PTS.
        let mut parser = DtsParser::new();
        let mut stream = make_dts_core(512);
        stream.extend_from_slice(&make_dts_core(640));
        let f = parser.parse(&make_pes(stream, Some(90000)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].data.len(), 512);
        assert_eq!(f[0].pts_ns, pts_to_ns(90000), "AU1 keeps its PES PTS");
        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].data.len(), 640);
        assert_eq!(
            tail[0].pts_ns,
            pts_to_ns(90000),
            "AU2 came in the same PES → same PTS"
        );
    }

    #[test]
    fn two_aus_flushed_in_one_call_keep_their_own_pts() {
        // The real-stream trigger: core1 in PES A (pts 100), then core2 + core3
        // arrive in a LATER PES B (pts 200). Processing PES B closes both AU1
        // (core1) and AU2 (core2) in a single parse() call. AU2's core arrived
        // in PES A region? No — here AU2 (core2) is in PES B, so it should be
        // 200. The defect to guard is AU1 NOT being overwritten to 200, and AU2
        // not inheriting an unrelated timestamp.
        let mut parser = DtsParser::new();

        // PES A: just core1 (held — no following core yet).
        let f0 = parser.parse(&make_pes(make_dts_core(512), Some(100)));
        assert!(f0.is_empty(), "core1 held awaiting next core");

        // PES B: core2 + core3. Closes AU1 (core1) and AU2 (core2).
        let mut pes_b = make_dts_core(600);
        pes_b.extend_from_slice(&make_dts_core(640));
        let f = parser.parse(&make_pes(pes_b, Some(200)));
        assert_eq!(f.len(), 2, "AU1 and AU2 both close in this call");
        assert_eq!(f[0].data.len(), 512, "AU1 = core1");
        assert_eq!(
            f[0].pts_ns,
            pts_to_ns(100),
            "AU1 keeps PES A's PTS, not the later PES B PTS"
        );
        assert_eq!(f[1].data.len(), 600, "AU2 = core2");
        assert_eq!(
            f[1].pts_ns,
            pts_to_ns(200),
            "AU2's core arrived in PES B → PES B PTS"
        );

        // AU3 (core3) drains on flush, also PES B PTS.
        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].pts_ns, pts_to_ns(200));
    }

    #[test]
    fn second_au_with_core_in_earlier_pes_keeps_that_pts() {
        // core1 + core2 both arrive in PES A (pts 100); core3 arrives in PES B
        // (pts 200). When PES B closes AU2 (core2, whose core was in PES A), AU2
        // must keep PES A's 100 — the bug was AU2 inheriting the closing PES's
        // 200.
        let mut parser = DtsParser::new();

        // PES A: core1 + core2. AU1 (core1) emits immediately (core2 boundary);
        // AU2 (core2) held awaiting a third core.
        let mut pes_a = make_dts_core(512);
        pes_a.extend_from_slice(&make_dts_core(600));
        let f = parser.parse(&make_pes(pes_a, Some(100)));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].pts_ns, pts_to_ns(100), "AU1 PES A PTS");

        // PES B: core3 — closes AU2 (core2). AU2's core was in PES A.
        let f2 = parser.parse(&make_pes(make_dts_core(640), Some(200)));
        assert_eq!(f2.len(), 1);
        assert_eq!(f2[0].data.len(), 600, "AU2 = core2");
        assert_eq!(
            f2[0].pts_ns,
            pts_to_ns(100),
            "AU2's core arrived in PES A → must keep PES A's PTS, not PES B's"
        );

        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].pts_ns, pts_to_ns(200), "AU3 = core3, PES B PTS");
    }

    /// Build a minimal DTS-HD extension substream of `size` bytes (just the
    /// sync + zero-padding). The parser delimits extensions by the next CORE
    /// sync, not by the extension's own size header, so a valid header isn't
    /// required — only that the bytes carry no spurious core sync.
    fn make_dts_ext(size: usize) -> Vec<u8> {
        let mut e = vec![0u8; size];
        e[0..4].copy_from_slice(&DTS_HD_EXT_SYNC);
        e
    }

    #[test]
    fn keeps_dts_hd_extension_in_separate_pes_packets() {
        // The real Blu-ray layout (ground-truthed on Dunkirk): the DTS core
        // arrives in one PES, then its DTS-HD MA extension substreams arrive
        // in SEPARATE following PES packets on the same PID. The parser must
        // stitch core + all trailing extensions into one access unit — not
        // emit a core-only (lossy) frame and drop the extension PES packets
        // (the Dunkirk / Fight Club lossy-core bug).
        let mut parser = DtsParser::new();

        // Frame 1: core (512) + two extension substreams (256 + 200).
        assert!(
            parser
                .parse(&make_pes(make_dts_core(512), Some(90000)))
                .is_empty(),
            "core alone: must wait for any following extension"
        );
        assert!(
            parser
                .parse(&make_pes(make_dts_ext(256), Some(91000)))
                .is_empty(),
            "first extension PES: still waiting for the unit to close"
        );
        assert!(
            parser
                .parse(&make_pes(make_dts_ext(200), Some(91500)))
                .is_empty(),
            "second extension PES: unit still not closed (no next core yet)"
        );

        // Frame 2's core PES arrives — that closes frame 1. The emitted unit
        // must be core + BOTH extensions (lossless preserved), and keep the
        // core's PTS, not the extension PES timestamps.
        let f = parser.parse(&make_pes(make_dts_core(512), Some(93000)));
        assert_eq!(f.len(), 1);
        assert_eq!(
            f[0].data.len(),
            512 + 256 + 200,
            "frame must include core + every extension substream"
        );

        // EOF drains frame 2.
        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].data.len(), 512);
    }

    #[test]
    fn extension_split_across_pes_is_preserved() {
        // An extension substream straddling a PES boundary must still be fully
        // attached to its core.
        let mut parser = DtsParser::new();
        let ext = make_dts_ext(300);
        assert!(
            parser
                .parse(&make_pes(make_dts_core(512), Some(90000)))
                .is_empty()
        );
        assert!(
            parser
                .parse(&make_pes(ext[..150].to_vec(), Some(91000)))
                .is_empty()
        );
        assert!(
            parser
                .parse(&make_pes(ext[150..].to_vec(), Some(91000)))
                .is_empty()
        );
        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].data.len(), 512 + 300);
    }

    /// Build 4 bytes that look like a DTS core sync but whose `fsize` field
    /// decodes to a tiny `core_size` (< MIN_CORE_FRAME_BYTES). With the
    /// dead-code guards this passed validation and could close an access unit
    /// at a junk boundary; with the fix it must be drained and resynced past.
    fn bogus_tiny_core_sync() -> Vec<u8> {
        // Core sync + zero header bytes. fsize = 0 → core_size = 1 (< 10).
        let mut v = vec![0u8; 10];
        v[0..4].copy_from_slice(&DTS_CORE_SYNC);
        // bytes 5,6,7 left zero → fsize = 0 → dts_core_frame_size = 1.
        assert_eq!(dts_core_frame_size(&v), 1);
        v
    }

    #[test]
    fn bogus_tiny_core_sync_does_not_split_or_drop_real_au() {
        // A real core frame followed by an extension substream that happens to
        // contain a false core sync whose fsize decodes tiny. The bogus sync
        // must NOT close the real access unit early (dropping the rest of the
        // extension) nor emit a junk few-byte frame — it must be skipped, and
        // the whole core + extension preserved as one access unit.
        let mut parser = DtsParser::new();

        // Frame 1: core(512) + an extension whose body embeds a bogus tiny
        // core sync midway through.
        let mut ext = make_dts_ext(256);
        // Embed the bogus core sync inside the extension body (offset 64).
        let bogus = bogus_tiny_core_sync();
        ext[64..64 + bogus.len()].copy_from_slice(&bogus);

        let mut frame1 = make_dts_core(512);
        frame1.extend_from_slice(&ext);

        // No next REAL core yet → frame 1 held.
        assert!(
            parser.parse(&make_pes(frame1, Some(90000))).is_empty(),
            "bogus tiny core sync must not close the AU; wait for a real core"
        );

        // Frame 2's real core arrives — closes frame 1.
        let f = parser.parse(&make_pes(make_dts_core(640), Some(93000)));
        assert_eq!(f.len(), 1, "exactly one real access unit emitted");
        assert_eq!(
            f[0].data.len(),
            512 + 256,
            "AU must be the full core + extension, not split at the bogus sync"
        );
        assert_eq!(f[0].pts_ns, pts_to_ns(90000), "AU keeps the core's PTS");

        let tail = parser.flush();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].data.len(), 640);
    }

    #[test]
    fn forced_emit_does_not_corrupt_next_au_pts() {
        // When the buffer exceeds MAX_AU_BYTES with no next core sync, the
        // parser force-emits for forward progress. The current PES at that
        // point is an extension-substream PES (later PTS). The forced path must
        // NOT make that extension PTS the base of the NEXT access unit.
        let mut parser = DtsParser::new();

        // Core PES at the real PTS, then a giant extension (no next core) that
        // pushes the buffer past MAX_AU_BYTES, forcing an emit.
        let core_pts = 90000i64;
        assert!(
            parser
                .parse(&make_pes(make_dts_core(512), Some(core_pts)))
                .is_empty()
        );
        let ext_pts = 120000i64; // later extension-PES timestamp
        let big_ext = make_dts_ext(MAX_AU_BYTES + 1024);
        let f = parser.parse(&make_pes(big_ext, Some(ext_pts)));
        assert_eq!(f.len(), 1, "oversized buffer force-emits one AU");
        assert_eq!(
            f[0].pts_ns,
            pts_to_ns(core_pts),
            "forced AU keeps the core PTS"
        );

        // The next REAL core PES arrives with its own PTS. Its AU must inherit
        // THIS core's PTS, not the prior extension PES timestamp.
        let next_core_pts = 150000i64;
        assert!(
            parser
                .parse(&make_pes(make_dts_core(512), Some(next_core_pts)))
                .is_empty()
        );
        let next_next_pts = 180000i64;
        let f2 = parser.parse(&make_pes(make_dts_core(512), Some(next_next_pts)));
        assert_eq!(f2.len(), 1);
        assert_eq!(
            f2[0].pts_ns,
            pts_to_ns(next_core_pts),
            "AU after a forced emit must use the next core's PTS, not the \
             stale extension PTS"
        );
    }

    #[test]
    fn codec_private_none() {
        let parser = DtsParser::new();
        assert!(parser.codec_private().is_none());
    }
}
