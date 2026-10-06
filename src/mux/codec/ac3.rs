//! AC3 (Dolby Digital) / EAC3 (Dolby Digital Plus) frame parser.
//!
//! Every (E-)AC-3 syncframe starts with syncword 0x0B77. An E-AC-3 access unit
//! (ETSI TS 102 366 / ATSC A/52 Annex E) is a whole FRAME SET: the mandatory
//! independent substream (`substreamid` 0) plus every dependent and additional
//! substream that follows it, all covering the SAME time period, so this parser
//! groups syncframes into access units at each `substreamid`-0 independent
//! substream rather than at every syncframe. Buffers across PES boundaries so
//! access units that span two PES packets are emitted complete, never split.

use super::{CodecParser, Frame, PesPacket, pts_to_ns};

// Sample rates by fscod (0=48kHz, 1=44.1kHz, 2=32kHz). fscod=3 is reserved in AC-3 / signals
// E-AC-3 "fscod2".
const SAMPLE_RATES: [u32; 4] = [48_000, 44_100, 32_000, 48_000];

/// E-AC-3 reduced sample rates indexed by fscod2 (byte-4 bits `[5:4]`), used when
/// fscod==3. Index 3 is reserved; we fall back to 48 kHz for it.
const EAC3_REDUCED_RATES: [u32; 4] = [24_000, 22_050, 16_000, 48_000];

// Minimum byte length of a valid (E-)AC-3 frame: syncword (2) + BSI header
// (~4). Rejects the frmsiz=0/1 sub-header junk `eac3_frame_size` could
// otherwise report as a 2/4-byte "frame".
const MIN_FRAME_BYTES: usize = 6;
// Largest (E-)AC-3 frame accepted.
const MAX_FRAME_BYTES: usize = 8192;

/// AC-3 (legacy) always carries 6 audio blocks × 256 samples = 1536 samples.
const AC3_SAMPLES_PER_FRAME: u32 = 1536;

// Hard cap on the carry-over buffer: one worst-case straddling frame set (72 × 8192-byte
// syncframes, Annex E) plus slack.
const MAX_AC3_BUF: usize = 1024 * 1024;

pub struct Ac3Parser {
    /// Leftover bytes from previous PES (incomplete frame at end), each still
    /// attributable to the packet that carried it — so an access unit that
    /// began in an earlier packet takes THAT packet's source offset, not the
    /// one that happened to complete it.
    acc: super::pesbuf::PesBuf,
    /// Working copy of `acc` reused across PES (the scanner needs `&mut self.tally` while reading
    /// it); the per-PES marks snapshot still allocates.
    scratch: Vec<u8>,
    /// PTS (ns) to stamp on the frame that begins the carry-over `buf` — i.e.
    /// the running per-frame PTS at the point the partial tail was retained.
    /// Used by `flush()` to time the final buffered frame at EOS.
    flush_pts_ns: i64,
    /// Keep/drop bookkeeping for the CRC decodability gate. A frame that fails
    /// its native CRC is dropped rather than shipped as a decoder-choking glitch;
    /// the running PTS is advanced across it (see the emit loop) so the drop is a
    /// silence gap, never a shift of the following audio.
    tally: super::dropgate::DropTally,
    /// Set once a syncframe that EXTENDS an access unit (an E-AC-3 dependent
    /// substream, or an additional independent substream with `substreamid` != 0)
    /// has been seen on this track. Until then a trailing LEGACY AC-3 syncframe is
    /// closed and emitted in-call (a plain AC-3 / DVD track has no substreams at
    /// all, so holding it back would only add latency); afterwards it is held open
    /// across the PES boundary because it may be the core of an AC-3-core +
    /// E-AC-3-dependent frame set whose remaining substreams are in the next PES.
    saw_extension: bool,
    /// The access unit held open across the last PES boundary, ALREADY
    /// scanned. The carry-over begins at its first byte, so without this the
    /// next call re-scans and re-CRCs every syncframe of it from byte 0 — and
    /// an access unit that keeps gaining substreams grows to [`MAX_AC3_BUF`]
    /// (1 MiB) before the resync guard drops it, which on a ~2 KiB DVD PES is
    /// three orders of magnitude of repeated work per packet.
    held: Option<HeldAu>,
    /// Test-only: syncframes examined (sized + CRC-gated) by
    /// `scan_access_units`. Pins the resume above — the property it exists for
    /// is a WORK bound, which no frame-level assertion can observe.
    #[cfg(test)]
    frames_scanned: u64,
}

impl Default for Ac3Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Ac3Parser {
    pub fn new() -> Self {
        Self {
            scratch: Vec::new(),
            acc: super::pesbuf::PesBuf::with_capacity(4096),
            flush_pts_ns: 0,
            tally: super::dropgate::DropTally::new("ac3"),
            saw_extension: false,
            held: None,
            #[cfg(test)]
            frames_scanned: 0,
        }
    }

    /// Access units dropped as undecodable so far — surfaced to the CLI/mux.
    pub fn dropped_frames(&self) -> u64 {
        self.tally.dropped_frames()
    }

    /// Total decoded duration (ns) of dropped access units.
    pub fn dropped_duration_ns(&self) -> u64 {
        self.tally.dropped_duration_ns()
    }

    // Scan `data` for (E-)AC-3 syncframes and group them into access units, closing each only
    // at the next `substreamid`-0 independent substream.
    fn scan_access_units(
        &mut self,
        data: &[u8],
        base_pts_ns: i64,
        anchor: Option<PtsAnchor>,
        at_eos: bool,
        marks: &[(usize, super::pesbuf::PesFacts)],
        held: Option<HeldAu>,
    ) -> ScanOut {
        let mut frames = Vec::new();
        let mut pos = 0usize;
        // Running PTS for the next access unit to emit in this call.
        let mut frame_pts_ns = base_pts_ns;
        let mut anchor = anchor;
        let mut pending: Option<PendingAu> = None;
        // How far this call has proved there is no further syncframe to
        // process; carried over so the held access unit's own bytes (and the
        // junk after them) are not searched again next call.
        let mut scanned_to = 0usize;

        // Resume a held access unit instead of re-deriving it. `keep_from` was
        // its first byte, so it starts at 0 of this buffer, and every frame in
        // it was sized and CRC-gated on the call that built it.
        if let Some(h) = held {
            let mut drop_reason = h.drop_reason;
            // The one verdict that can have changed since: the track may have
            // become poisoned while this access unit was held, and a re-scan
            // would have picked that up.
            if drop_reason.is_none() && self.tally.is_poisoned() {
                drop_reason = Some("track-poisoned");
            }
            pending = Some(PendingAu {
                start: 0,
                end: h.end,
                pts_ns: base_pts_ns,
                duration_ns: h.duration_ns,
                drop_reason,
                bsid: h.bsid,
            });
            frame_pts_ns = base_pts_ns + h.duration_ns as i64;
            pos = h.scanned_to;
            scanned_to = h.scanned_to;
        }

        while pos < data.len() {
            let sync = find_ac3_sync(&data[pos..]);
            let start = match sync {
                Some(offset) => pos + offset,
                None => {
                    // No syncword in `data[pos..]` at all: every byte but the
                    // last is proved sync-free (a syncword is two bytes and the
                    // second may still arrive).
                    scanned_to = data.len().saturating_sub(1).max(pos);
                    break;
                }
            };
            scanned_to = start;

            let remaining = &data[start..];

            if remaining.len() < MIN_FRAME_BYTES {
                // Not enough data to determine frame size — keep for next PES
                break;
            }

            let bsid = get_bsid(remaining);
            let frame_size = if bsid >= 11 {
                eac3_frame_size(remaining)
            } else {
                ac3_frame_size(remaining)
            };

            if !(MIN_FRAME_BYTES..=MAX_FRAME_BYTES).contains(&frame_size) {
                // Invalid/sub-header frame size (e.g. an E-AC-3 frmsiz of 0/1
                // sizing to a 2/4-byte fragment) — skip this sync word.
                pos = start + 2;
                scanned_to = pos;
                continue;
            }

            if start + frame_size > data.len() {
                // Incomplete frame — keep for next PES
                break;
            }

            let frame = &data[start..start + frame_size];
            #[cfg(test)]
            {
                self.frames_scanned += 1;
            }
            // Decodability gate: an out-of-range bsid (> 16) or a failed native CRC
            // poisons the whole access unit — a dependent substream is useless
            // without its parent and vice versa — so it's dropped as one silence gap.
            let reason = ac3_drop_reason(&self.tally, frame, bsid);

            if substream_role(remaining, bsid) == SubstreamRole::Extends {
                match pending.as_mut() {
                    // A dependent (or additional independent, substreamid 1..7)
                    // substream extending the frame set it directly follows; byte
                    // contiguity is required to keep skipped junk out of the AU.
                    Some(au) if au.end == start => {
                        au.end = start + frame_size;
                        if au.drop_reason.is_none() {
                            au.drop_reason = reason;
                        }
                        self.saw_extension = true;
                    }
                    // No open access unit: mid-frame-set resync, or a stream whose
                    // mandatory substreamid-0 substream was never seen. Neither
                    // decodable alone nor timeable, so skip it with no PTS advance.
                    _ => {
                        tracing::debug!(
                            target: "mux",
                            "ac3: substream with no open access unit (frame set joined mid-set); skipped"
                        );
                    }
                }
            } else {
                if let Some(au) = pending.take() {
                    close_access_unit(&mut self.tally, data, &au, marks, &mut frames);
                }
                // First AU starting in this PES's own bytes: adopt its timestamp so
                // a genuine PTS jump is followed, not drifted past.
                if let Some(a) = &anchor
                    && start >= a.at
                {
                    frame_pts_ns = a.pts_ns;
                    anchor = None;
                }
                let duration_ns = frame_duration_ns(remaining, bsid);
                pending = Some(PendingAu {
                    start,
                    end: start + frame_size,
                    pts_ns: frame_pts_ns,
                    duration_ns,
                    drop_reason: reason,
                    bsid,
                });
                // Only the frame set's `substreamid`-0 independent substream
                // advances the timeline: every other substream of the set covers
                // the same time period (Annex E).
                frame_pts_ns += duration_ns as i64;
            }

            pos = start + frame_size;
            scanned_to = pos;
        }

        // Close or HOLD the trailing access unit: its frame set may still continue
        // in the next PES, so a growable AU is held and re-scanned next call, but
        // only once this track has shown a substream extending an AU (E-AC-3).
        let mut hold_from = None;
        let mut held_out = None;
        if let Some(au) = pending {
            if !at_eos && (au.bsid >= 11 || self.saw_extension) {
                frame_pts_ns = au.pts_ns;
                hold_from = Some(au.start);
                // Everything below `scanned_to` is already searched and CRC-gated;
                // record it rebased onto the carry-over (starting at `au.start`)
                // so the next call resumes instead of redoing the work.
                held_out = Some(HeldAu {
                    end: au.end - au.start,
                    scanned_to: scanned_to.max(au.end) - au.start,
                    duration_ns: au.duration_ns,
                    drop_reason: au.drop_reason,
                    bsid: au.bsid,
                });
            } else {
                close_access_unit(&mut self.tally, data, &au, marks, &mut frames);
            }
        }

        // Keep unconsumed data for the next call: re-scan from `pos`, not a
        // recomputed sync, to avoid dropping the partial frame kept across the
        // PES boundary. Held AU wins (its start is before `pos`).
        let keep_from = match hold_from {
            Some(h) => h,
            None if pos < data.len() => {
                // A syncword at/after `pos` marks the carry-over start. With no full
                // sync, retain the whole tail, including a lone trailing 0x0B that
                // may be the first half of a syncword split across the boundary.
                match find_ac3_sync(&data[pos..]) {
                    Some(o) => pos + o,
                    None if data.last() == Some(&0x0B) => data.len() - 1,
                    None => data.len(),
                }
            }
            None => data.len(),
        };

        ScanOut {
            frames,
            keep_from,
            frame_pts_ns,
            held: held_out,
        }
    }
}

// Where a PES's own timestamp takes over the running per-access-unit PTS:
// `pts_ns` applies to the first access unit starting at/after byte `at`.
struct PtsAnchor {
    at: usize,
    pts_ns: i64,
}

// What one `scan_access_units` pass produced: emitted frames, the carry-over
// offset + PTS for the next call, and the HeldAu state to resume from (if any).
struct ScanOut {
    frames: Vec<Frame>,
    keep_from: usize,
    frame_pts_ns: i64,
    held: Option<HeldAu>,
}

/// A trailing access unit held across the PES boundary, already scanned.
/// Offsets are relative to the carry-over, which begins at the access unit's
/// first byte — so the access unit occupies `0..end`.
#[derive(Clone, Copy)]
struct HeldAu {
    /// End of the access unit's bytes.
    end: usize,
    /// How far the scan that built it had searched (`>= end`). Bytes below it
    /// hold no further syncframe to process.
    scanned_to: usize,
    /// Duration contributed by the access unit's `substreamid`-0 substream.
    duration_ns: u64,
    /// Decodability verdict reached for it so far.
    drop_reason: Option<&'static str>,
    /// bsid of the substream that opened it.
    bsid: u8,
}

// An access unit (frame set) under construction: `data[start..end]` is the
// `substreamid`-0 independent substream plus every substream appended so far.
struct PendingAu {
    start: usize,
    end: usize,
    /// PTS of the `substreamid`-0 independent substream — the PTS the whole frame
    /// set carries.
    pts_ns: i64,
    /// Duration of the `substreamid`-0 independent substream; the frame set's other
    /// substreams cover the same time period and add none.
    duration_ns: u64,
    /// First decodability failure among the access unit's substreams, if any.
    drop_reason: Option<&'static str>,
    /// bsid of the substream that opened the access unit (< 11 = legacy AC-3 core).
    bsid: u8,
}

// Emit a finished access unit, or record it as a drop (still counting the
// full duration, so it reads as a silence gap, never a shift of later audio).
fn close_access_unit(
    tally: &mut super::dropgate::DropTally,
    data: &[u8],
    au: &PendingAu,
    marks: &[(usize, super::pesbuf::PesFacts)],
    out: &mut Vec<Frame>,
) {
    if let Some(reason) = au.drop_reason {
        tally.record_drop(au.pts_ns, au.duration_ns as i64, au.end - au.start, reason);
        return;
    }
    tally.record_kept();
    out.push(Frame {
        discontinuity: false,
        coding: None,
        // The packet covering this unit's FIRST byte — which is the packet its
        // PTS came from too, when the unit began in an earlier PES.
        source: super::pesbuf::facts_for(marks, au.start).source,
        pts_ns: au.pts_ns,
        keyframe: true,
        data: data[au.start..au.end].to_vec(),
        duration_ns: Some(au.duration_ns),
    });
}

/// What a syncframe does to the access unit (frame set) being assembled.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SubstreamRole {
    /// Begins a new access unit.
    Starts,
    /// Belongs to the access unit already open — it covers the same time period
    /// and must not close it or advance the timeline.
    Extends,
}

// Classify a syncframe for access-unit assembly: byte 2's strmtyp/substreamid (ETSI TS 102 366
// Annex E BSI) decide Starts vs Extends.
fn substream_role(data: &[u8], bsid: u8) -> SubstreamRole {
    if bsid < 11 || data.len() < 3 {
        return SubstreamRole::Starts;
    }
    let strmtyp = (data[2] >> 6) & 0x03;
    let substreamid = (data[2] >> 3) & 0x07;
    match strmtyp {
        // Dependent substream: always part of the open frame set.
        1 => SubstreamRole::Extends,
        // Independent substream: only id 0 begins a frame set.
        0 | 2 if substreamid != 0 => SubstreamRole::Extends,
        _ => SubstreamRole::Starts,
    }
}

use super::crc::crc16_ansi;

// Whether a fully-buffered (E-)AC-3 frame passes its native CRC-16/ANSI over the bytes after
// the syncword. `frame` must be exactly syncword..frame_size.
fn frame_crc_ok(frame: &[u8]) -> bool {
    // Need the syncword (2) plus at least one covered byte; the caller only
    // invokes this on a fully-sized frame, so this is defensive.
    if frame.len() < 4 {
        return true;
    }
    crc16_ansi(&frame[2..]) == 0
}

// Decodability verdict for a fully-sized frame: poisoned track, out-of-range
// bsid (> 16, undefined by ETSI TS 102 366), or a failed native CRC, in order.
fn ac3_drop_reason(
    tally: &super::dropgate::DropTally,
    frame: &[u8],
    bsid: u8,
) -> Option<&'static str> {
    if tally.is_poisoned() {
        Some("track-poisoned")
    } else if bsid > 16 {
        Some("bsid")
    } else if !frame_crc_ok(frame) {
        Some("crc")
    } else {
        None
    }
}

impl CodecParser for Ac3Parser {
    fn parse(&mut self, pes: &PesPacket) -> Vec<Frame> {
        // B1: a concealed/lost gap means `buf` holds a TRUNCATED frame; appending
        // post-gap bytes would splice a corrupt frame, so drop the partial and
        // resync on the next syncword. Handled before the empty-data guard.
        if pes.discontinuity {
            self.acc.clear();
            // The held access unit's bytes went with it.
            self.held = None;
        }
        if pes.data.is_empty() {
            return Vec::new();
        }

        // This PES's timestamp applies to the first AU that STARTS in its own bytes;
        // later AUs advance by the previous one's duration (avoiding A/V drift). A
        // PES with no PTS must NOT reset the timeline: `flush_pts_ns` just continues.
        let carry_len = self.acc.len();
        let anchor = pes.pts.map(|p| PtsAnchor {
            at: carry_len,
            pts_ns: pts_to_ns(p),
        });

        // Prepend leftover from the previous PES, then take the scratch buffer out
        // of `self` so the scanner can borrow `self.tally`; it's put back at the
        // end, keeping its capacity. There is no early return after this point.
        self.acc.push(pes);
        let mut buf = std::mem::take(&mut self.scratch);
        buf.clear();
        buf.extend_from_slice(self.acc.as_slice());
        let marks = self.acc.marks_snapshot();
        let data = &buf;
        let held = self.held.take();
        let ScanOut {
            frames,
            keep_from,
            frame_pts_ns,
            held: still_held,
        } = self.scan_access_units(data, self.flush_pts_ns, anchor, false, &marks, held);

        if keep_from < data.len() {
            let tail = &data[keep_from..];
            if tail.len() > MAX_AC3_BUF {
                // No frame could be parsed out of a buffer this large — this is
                // not valid AC-3 here. Drop it and resync on the next PES rather
                // than grow without bound on pathological input.
                tracing::debug!(
                    target: "mux",
                    "ac3: carry-over buffer exceeded {} bytes without a frame; dropping and resyncing",
                    MAX_AC3_BUF
                );
                self.acc.clear();
                self.held = None;
                // Advance the cadence like the other two paths out of this block.
                // No known input reaches this branch; it must still drop the held
                // AU and advance, or a stale HeldAu would resume after the resync.
                self.flush_pts_ns = frame_pts_ns;
            } else {
                self.acc.drain(keep_from);
                self.held = still_held;
                // Carried bytes, when later completed and emitted, are timed at the
                // PTS the scanner reached here — the next AU's PTS, or for a HELD
                // AU, its own PTS, so the hold never shifts it.
                self.flush_pts_ns = frame_pts_ns;
            }
        } else {
            self.acc.clear();
            self.held = None;
            // Nothing carried, but keep the cadence so a following PES with no
            // PTS (no anchor) continues the timeline instead of reusing a stale
            // value.
            self.flush_pts_ns = frame_pts_ns;
        }

        // Hand the working buffer back so the next PES reuses its capacity.
        // `data` borrowed it; that borrow ends here, at its last use.
        self.scratch = buf;
        frames
    }

    fn flush(&mut self) -> Vec<Frame> {
        // Drain the carry-over at EOS: an AU (possibly held for a dependent
        // substream) may sit there with no following PES to close it, else the
        // last ~32ms of audio is lost. `at_eos` closes it instead of holding.
        let buf = self.acc.as_slice().to_vec();
        let marks = self.acc.marks_snapshot();
        self.acc.clear();
        let held = self.held.take();
        let out = self
            .scan_access_units(&buf, self.flush_pts_ns, None, true, &marks, held)
            .frames;
        // Aggregate drop report at end-of-stream (warn-level, always visible).
        self.tally.log_summary();
        out
    }

    fn codec_private(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Number of samples per E-AC-3 frame from numblkscod (audio blocks × 256).
fn eac3_samples_per_frame(data: &[u8]) -> u32 {
    if data.len() < 5 {
        return AC3_SAMPLES_PER_FRAME;
    }
    // E-AC-3 byte 4: fscod(2) | numblkscod(2) | ... — but only when fscod != 3.
    // When fscod == 3 (fscod2 / reduced rate), numblks is fixed at 6.
    let fscod = (data[4] >> 6) & 0x03;
    if fscod == 0x03 {
        return 6 * 256;
    }
    let numblkscod = (data[4] >> 4) & 0x03;
    let numblks = match numblkscod {
        0 => 1,
        1 => 2,
        2 => 3,
        _ => 6,
    };
    numblks * 256
}

// Sample rate (Hz) from fscod (byte 4 bits 7-6); E-AC-3 fscod==3 selects a
// reduced rate via fscod2 (byte 4 bits [5:4]) to keep frame duration correct.
fn frame_sample_rate(data: &[u8], bsid: u8) -> u32 {
    if data.len() < 5 {
        return SAMPLE_RATES[0];
    }
    let fscod = (data[4] >> 6) & 0x03;
    if fscod == 0x03 && bsid >= 11 {
        let fscod2 = (data[4] >> 4) & 0x03;
        return EAC3_REDUCED_RATES[fscod2 as usize];
    }
    SAMPLE_RATES[fscod as usize]
}

/// Duration of one AC-3/E-AC-3 frame in nanoseconds: samples_per_frame /
/// sample_rate. AC-3 is always 1536 samples; E-AC-3 derives from numblkscod.
fn frame_duration_ns(data: &[u8], bsid: u8) -> u64 {
    let samples = if bsid >= 11 {
        eac3_samples_per_frame(data)
    } else {
        AC3_SAMPLES_PER_FRAME
    } as u64;
    let rate = frame_sample_rate(data, bsid) as u64;
    // samples / rate seconds → ns, rounded to nearest.
    (samples * 1_000_000_000 + rate / 2) / rate
}

// Base channel count per AC-3 `acmod` (A/52 Table 5.8), BEFORE the LFE; add 1 when `lfeon` is
// set.
const ACMOD_CHANNELS: [u8; 8] = [2, 1, 2, 3, 3, 4, 4, 5];

// Decode the channel count of an (E-)AC-3 frame from its bitstream `acmod` + `lfeon` (A/52
// §5.3.2 BSI) — the AUTHORITATIVE count over the unreliable DVD IFO nibble.
pub(crate) fn acmod_channels(data: &[u8]) -> Option<u8> {
    // Need at least bytes 0..=6 to read acmod (byte 6) and its trailing
    // optional fields + lfeon (which never spills past byte 7 for any acmod).
    if data.len() < 8 {
        return None;
    }
    let bsid = get_bsid(data);
    // E-AC-3 (bsid >= 11, Annex E) uses a different BSI layout. DVD audio is
    // always legacy AC-3 (bsid <= 8); for E-AC-3 we don't decode acmod here
    // and let the caller fall back to the passed channel count.
    if bsid >= 11 {
        return None;
    }
    // Bit cursor over `data`, MSB-first, starting at byte 6 bit 7 (= bit 48).
    let mut bit = 6 * 8;
    let read = |n: usize, bit: &mut usize| -> u32 {
        let mut v = 0u32;
        for _ in 0..n {
            let byte = data[*bit / 8];
            let shift = 7 - (*bit % 8);
            v = (v << 1) | ((byte >> shift) & 1) as u32;
            *bit += 1;
        }
        v
    };
    let acmod = read(3, &mut bit) as usize;
    // cmixlev: present when acmod has a centre channel AND is not the 1/0
    // (centre-only) mode — i.e. acmod & 0x1 != 0 && acmod != 0x1.
    if (acmod & 0x1) != 0 && acmod != 0x1 {
        let _cmixlev = read(2, &mut bit);
    }
    // surmixlev: present when acmod has a surround channel (acmod & 0x4).
    if (acmod & 0x4) != 0 {
        let _surmixlev = read(2, &mut bit);
    }
    // dsurmod: present only for the 2/0 (stereo) mode.
    if acmod == 0x2 {
        let _dsurmod = read(2, &mut bit);
    }
    let lfeon = read(1, &mut bit);
    Some(ACMOD_CHANNELS[acmod] + lfeon as u8)
}

/// Find AC3/E-AC-3 syncword (0x0B77) in data.
pub(crate) fn find_ac3_sync(data: &[u8]) -> Option<usize> {
    (0..data.len().saturating_sub(1)).find(|&i| data[i] == 0x0B && data[i + 1] == 0x77)
}

/// Extract bsid from an AC-3/E-AC-3 frame starting at the syncword.
/// bsid is at byte 5, bits 7..3.
fn get_bsid(data: &[u8]) -> u8 {
    if data.len() < 6 {
        return 0;
    }
    (data[5] >> 3) & 0x1F
}

/// Calculate E-AC-3 frame size in bytes from the frmsiz field.
fn eac3_frame_size(data: &[u8]) -> usize {
    if data.len() < 4 {
        return 0;
    }
    let frmsiz = ((data[2] as usize & 0x07) << 8) | data[3] as usize;
    (frmsiz + 1) * 2
}

// Calculate AC-3 frame size in bytes from fscod and frmsizecod (0 if
// unmappable). pub(crate) so the TrueHD parser can reuse it rather than
// duplicating the size table when skipping interleaved AC-3 frames.
pub(crate) fn ac3_frame_size(data: &[u8]) -> usize {
    if data.len() < 5 {
        return 0;
    }
    let fscod = (data[4] >> 6) & 0x03;
    let frmsizecod = (data[4] & 0x3F) as usize;
    if frmsizecod >= AC3_FRAME_SIZES.len() {
        return 0;
    }
    let words = AC3_FRAME_SIZES[frmsizecod];
    match fscod {
        0 => words[0] * 2,
        1 => words[1] * 2,
        2 => words[2] * 2,
        _ => 0,
    }
}

/// AC-3 frame size table: `[frmsizecod]` -> `[48kHz words, 44.1kHz words, 32kHz words]`
const AC3_FRAME_SIZES: [[usize; 3]; 38] = [
    [64, 69, 96],
    [64, 70, 96],
    [80, 87, 120],
    [80, 88, 120],
    [96, 104, 144],
    [96, 105, 144],
    [112, 121, 168],
    [112, 122, 168],
    [128, 139, 192],
    [128, 140, 192],
    [160, 174, 240],
    [160, 175, 240],
    [192, 208, 288],
    [192, 209, 288],
    [224, 243, 336],
    [224, 244, 336],
    [256, 278, 384],
    [256, 279, 384],
    [320, 348, 480],
    [320, 349, 480],
    [384, 417, 576],
    [384, 418, 576],
    [448, 487, 672],
    [448, 488, 672],
    [512, 557, 768],
    [512, 558, 768],
    [640, 696, 960],
    [640, 697, 960],
    [768, 835, 1152],
    [768, 836, 1152],
    [896, 975, 1344],
    [896, 976, 1344],
    [1024, 1114, 1536],
    [1024, 1115, 1536],
    [1152, 1253, 1728],
    [1152, 1254, 1728],
    [1280, 1393, 1920],
    [1280, 1394, 1920],
];

#[cfg(test)]
#[path = "ac3_tests.rs"]
mod tests;
