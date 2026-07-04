//! BD Transport Stream demuxer.
//!
//! Blu-ray uses 192-byte TS packets (not standard 188):
//! - 4-byte TP_extra_header (arrival timestamp + copy permission)
//! - 188-byte standard MPEG-TS packet
//!
//! This demuxer extracts PES packets from selected PIDs, with PTS/DTS timestamps.

use crate::consts::BD_SOURCE_PACKET_BYTES;

use crate::consts::TS_PACKET_BYTES;

/// TS sync byte.
const SYNC_BYTE: u8 = 0x47;
/// MPEG-TS null-packet PID (0x1FFF). Carries no elementary stream; the P3
/// concealment fill emits null packets on this PID, tagged with an
/// adaptation-field discontinuity_indicator to signal a concealed gap.
const NULL_PID: u16 = 0x1FFF;

/// A reassembled PES packet with timestamp info.
#[derive(Debug)]
pub struct PesPacket {
    /// MPEG-TS PID this packet belongs to.
    pub pid: u16,
    /// Presentation timestamp in 90kHz ticks (if present).
    pub pts: Option<i64>,
    /// Decode timestamp in 90kHz ticks (if present).
    pub dts: Option<i64>,
    /// Elementary stream data (video frame, audio frame, subtitle segment, etc.).
    pub data: Vec<u8>,
    /// Source position of this PES's first ES byte, stamped at the demux seam
    /// from the producer's known stream offset. `None` when the demuxer was fed
    /// without a base offset (callers that don't need provenance).
    pub source: Option<crate::pes::SourcePos>,
    /// True when one or more packets for this stream were lost before this PES —
    /// a continuity break (CC gap or adaptation-field discontinuity_indicator) on
    /// a tracked PID, or the CC-independent concealment marker the mux emits when
    /// it replaces an undecryptable unit with NULL-TS packets (P3/A2). This PES is
    /// the FIRST whose data is entirely after the gap: a mid-frame loss drops the
    /// truncated partial and flags the next complete PES; a loss landing on a PES
    /// boundary flags the PES STARTING after it (never the one just flushed). So
    /// for inter-coded video this PES — and every later frame up to the next
    /// IRAP/IDR — may reference data that is now gone. The codec-parse consumer
    /// (via the per-frame `Frame::discontinuity` its parser propagates) drops
    /// forward to the next keyframe (B1) instead of emitting dangling references.
    pub discontinuity: bool,
}

/// Per-PID PES reassembly state.
struct PesAssembler {
    pid: u16,
    buffer: Vec<u8>,
    pts: Option<i64>,
    dts: Option<i64>,
    active: bool,
    /// PES-header bytes still to be skipped on the next continuation
    /// packet(s). A PES header (9 + PES_header_data_length, up to 264
    /// bytes) can exceed a single 184-byte TS payload, spilling into the
    /// following continuation packet. Those spillover bytes are NOT
    /// elementary-stream data and must be skipped, or the PES start code
    /// (`00 00 01 …`) and timestamp bytes get injected into the ES — for
    /// HEVC/H264 that reads as a spurious start code / corrupt slice
    /// payload. Tracks how many header bytes remain across packets.
    header_remaining: usize,
    /// 4-bit continuity_counter of the last payload-bearing TS packet seen
    /// on this PID. A non-PUSI continuation whose CC is not `(prev + 1) & 0xf`
    /// — or whose adaptation field flags a discontinuity — means one or more
    /// TS packets for this PID were dropped; splicing the new payload onto the
    /// partial PES would inject corrupt bytes. The partial PES is dropped and
    /// the assembler resyncs on the next PUSI. `None` until the first packet.
    last_cc: Option<u8>,
    /// Absolute source byte offset of the in-progress PES's first byte (the
    /// PUSI packet that began it), or `None` when no source base is threaded.
    /// Stamped at PES start, emitted on the completed packet — provenance is
    /// carried, never reconstructed downstream.
    pes_source: Option<crate::pes::SourcePos>,
    /// Sticky "a gap occurred on this PID" flag. Set by a CC gap, an explicit
    /// discontinuity_indicator, or the concealment marker; consumed by the NEXT
    /// PES this assembler completes — the first whose data is entirely post-gap —
    /// then cleared. A gap detected on a PUSI sets it AFTER `start()` so it rides
    /// the new PES, not the one just flushed. Drives B1 drop-to-keyframe.
    pending_discontinuity: bool,
}

/// Initial capacity for a fresh PES buffer. Sized to cover the
/// common BD-TS audio / subtitle PES outright (a few KB to ~16 KB).
/// Video PES (typically 150–300 KB on UHD) will grow this via the
/// standard Vec doubling, but the doublings hit the allocator's
/// slab caches instead of the 64-page first-touch faults that the
/// previous `Vec::with_capacity(256 * 1024)` triggered on every PES
/// boundary.
const PES_BUFFER_INIT_CAP: usize = 16 * 1024;

/// Hard cap on a single PID's PES reassembly buffer.
///
/// A complete HEVC/UHD access unit (I-frame) is typically 1–3 MiB;
/// 64 MiB is an order of magnitude above any real disc's largest AU
/// and well below the memory a process can reasonably spare. If a
/// stream pumps continuation packets that never produce a PUSI (e.g.
/// a corrupt or crafted m2ts), the buffer would otherwise grow
/// without bound and exhaust RAM. When a `push` would push the buffer
/// past this limit the assembler drops the partial PES and resyncs on
/// the next PUSI.
const MAX_PES_BUFFER: usize = 64 * 1024 * 1024; // 64 MiB

impl PesAssembler {
    fn new(pid: u16) -> Self {
        Self {
            pid,
            buffer: Vec::with_capacity(PES_BUFFER_INIT_CAP),
            pts: None,
            dts: None,
            active: false,
            header_remaining: 0,
            last_cc: None,
            pes_source: None,
            pending_discontinuity: false,
        }
    }

    /// Start a new PES packet. Returns the completed previous packet (if any).
    /// `source` is the absolute source position of the new PES's first byte
    /// (carried onto the completed packet at the next start / flush).
    fn start(
        &mut self,
        pts: Option<i64>,
        dts: Option<i64>,
        source: Option<crate::pes::SourcePos>,
    ) -> Option<PesPacket> {
        let completed = if self.active && !self.buffer.is_empty() {
            let discontinuity = self.pending_discontinuity;
            self.pending_discontinuity = false;
            Some(PesPacket {
                pid: self.pid,
                pts: self.pts,
                dts: self.dts,
                data: std::mem::replace(&mut self.buffer, Vec::with_capacity(PES_BUFFER_INIT_CAP)),
                source: self.pes_source,
                discontinuity,
            })
        } else {
            self.buffer.clear();
            None
        };
        self.pts = pts;
        self.dts = dts;
        self.active = true;
        self.pes_source = source;
        completed
    }

    /// Append payload data to the current PES packet.
    ///
    /// If the buffer would exceed [`MAX_PES_BUFFER`] the partial PES is
    /// silently dropped and the assembler is reset. Normal traffic resumes
    /// on the next PUSI; a crafted/corrupt stream that never sends one can
    /// no longer drive unbounded allocation.
    fn push(&mut self, data: &[u8]) {
        if self.active {
            if self.buffer.len().saturating_add(data.len()) > MAX_PES_BUFFER {
                tracing::trace!(
                    target: "mux",
                    pid = self.pid,
                    bytes = self.buffer.len(),
                    "PES buffer cap exceeded; dropping partial PES and resyncing on next PUSI",
                );
                self.buffer.clear();
                self.active = false;
                self.header_remaining = 0;
                return;
            }
            self.buffer.extend_from_slice(data);
        }
    }

    /// Flush remaining data as a PES packet.
    fn flush(&mut self) -> Option<PesPacket> {
        if self.active && !self.buffer.is_empty() {
            self.active = false;
            let discontinuity = self.pending_discontinuity;
            self.pending_discontinuity = false;
            Some(PesPacket {
                pid: self.pid,
                pts: self.pts,
                dts: self.dts,
                data: std::mem::take(&mut self.buffer),
                source: self.pes_source,
                discontinuity,
            })
        } else {
            None
        }
    }
}

/// BD Transport Stream demuxer.
pub struct TsDemuxer {
    assemblers: Vec<PesAssembler>,
    pid_index: Vec<i16>, // PID → index into assemblers, -1 = not tracked
    remainder: Vec<u8>,  // leftover bytes from previous feed() call
    /// Absolute source byte offset of the NEXT byte to be fed — the running
    /// base that turns an in-buffer packet offset into a source position.
    /// Advanced by each `feed` by the bytes consumed; `feed` (no base) leaves
    /// it at 0 so non-provenance callers stamp `None`.
    feed_base: u64,
    /// True once a caller has threaded a source base via [`feed_at`]. Until
    /// then no `SourcePos` is stamped (keeps existing callers byte-identical).
    has_base: bool,
}

impl TsDemuxer {
    /// Create a new demuxer tracking the given PIDs.
    ///
    /// Allocates a flat lookup table of `i16` slots — one per possible PID
    /// up to `max(8192, max_pid + 1)`. The 8192 floor matches the BD-TS
    /// 13-bit PID space (0..0x1FFF); the variable upper bound exists for
    /// DVD program streams which may use 16-bit stream IDs above 8191.
    /// Worst-case allocation is `u16::MAX × 2 bytes ≈ 128 KB` — bounded by
    /// the type, so adversarial input can't drive this beyond predictable
    /// limits. Empty `pids` yields max_pid 0; the floor still produces a
    /// valid (wholly-unused) table.
    pub fn new(pids: &[u16]) -> Self {
        // The PID→assembler index is stored as i16 (-1 = untracked), so a
        // 32768th+ tracked PID would truncate to a negative value and be
        // silently treated as untracked. Callers pass a handful of PIDs
        // (BD-TS has at most ~8192), so this is a programmer-error guard.
        debug_assert!(
            pids.len() <= i16::MAX as usize,
            "TsDemuxer: too many PIDs for an i16 index table"
        );
        let max_pid = pids.iter().copied().max().unwrap_or(0) as usize;
        let table_size = (max_pid + 1).max(8192);
        let mut pid_index = vec![-1i16; table_size];
        let mut assemblers = Vec::with_capacity(pids.len());
        for (i, &pid) in pids.iter().enumerate() {
            pid_index[pid as usize] = i as i16;
            assemblers.push(PesAssembler::new(pid));
        }
        Self {
            assemblers,
            pid_index,
            remainder: Vec::new(),
            feed_base: 0,
            has_base: false,
        }
    }

    /// Feed a chunk of BD transport stream data. Handles non-192-byte-
    /// aligned input by buffering leftover bytes between calls. Returns
    /// completed PES packets.
    ///
    /// 16 MiB ISO batches never divide evenly into 192-byte BD-TS
    /// packets, so every call after the first carries a ~64-byte
    /// remainder. The pre-0.24 implementation handled this by building
    /// a `combined` Vec containing remainder + the entire new input —
    /// a 16 MiB+ memcpy on every call. Now we splice exactly one
    /// boundary packet from a stack buffer, then process the rest of
    /// `data` in place. Zero-copy on the bulk path; one 192-byte copy
    /// on the boundary.
    pub fn feed(&mut self, data: &[u8]) -> Vec<PesPacket> {
        // A plain feed carries no provenance. Reset any base a prior
        // `feed_at` left behind so mixing the two entry points is safe:
        // after this call no `SourcePos` is stamped, and the stale running
        // base can't leak a wrong offset into the boundary packet.
        self.feed_base = 0;
        self.has_base = false;
        self.feed_inner(data)
    }

    /// Like [`feed`](Self::feed) but records the absolute source byte offset of
    /// `data[0]` first, so every PES this batch completes is stamped with a
    /// [`crate::pes::SourcePos`]. The single provenance-stamping entry point;
    /// the highway calls this with each batch's known source offset.
    pub fn feed_at(&mut self, base_offset: u64, data: &[u8]) -> Vec<PesPacket> {
        self.feed_base = base_offset;
        self.has_base = true;
        self.feed_inner(data)
    }

    /// Source position for a packet whose first byte is at `buf_offset` within
    /// the current feed buffer — `None` until a base has been threaded.
    fn pkt_source(&self, buf_offset: usize) -> Option<crate::pes::SourcePos> {
        self.has_base
            .then(|| crate::pes::SourcePos::at_byte(self.feed_base + buf_offset as u64))
    }

    fn feed_inner(&mut self, data: &[u8]) -> Vec<PesPacket> {
        let mut completed = Vec::with_capacity(4);
        let mut offset = 0;

        // Boundary packet: if a partial packet was left from the last
        // call, complete it from the head of `data` without touching
        // the rest of `data`.
        if !self.remainder.is_empty() {
            let need = BD_SOURCE_PACKET_BYTES - self.remainder.len();
            if data.len() < need {
                // Still not a full packet — accumulate and wait.
                self.remainder.extend_from_slice(data);
                return completed;
            }
            // Capture the remainder length before clearing — it's how many of
            // the boundary packet's bytes lived in the PREVIOUS feed buffer,
            // and `feed_base` currently points at the FIRST byte of THIS buffer.
            let rem_len = self.remainder.len();
            let mut boundary = [0u8; BD_SOURCE_PACKET_BYTES];
            boundary[..rem_len].copy_from_slice(&self.remainder);
            boundary[rem_len..].copy_from_slice(&data[..need]);
            self.remainder.clear();
            // The boundary packet's first byte sat `rem_len` bytes before the
            // current feed_base (in the previous buffer). Stamp it there — not
            // at `feed_base - 1`, which would be wrong by `rem_len - 1` bytes.
            let src = self.has_base.then(|| {
                crate::pes::SourcePos::at_byte(self.feed_base.saturating_sub(rem_len as u64))
            });
            self.process_packet(&boundary, src, &mut completed);
            offset = need;
        }

        // Aligned-packets fast path — reads directly out of `data`.
        while offset + BD_SOURCE_PACKET_BYTES <= data.len() {
            let packet = &data[offset..offset + BD_SOURCE_PACKET_BYTES];
            let src = self.pkt_source(offset);
            offset += BD_SOURCE_PACKET_BYTES;
            self.process_packet(packet, src, &mut completed);
        }
        // Advance the running base past every byte consumed this feed so the
        // next batch stamps from the correct absolute offset.
        if self.has_base {
            self.feed_base += offset as u64;
        }

        // Save leftover bytes for next call (cap at one packet to
        // prevent unbounded growth on a desynchronised stream).
        if offset < data.len() {
            let leftover = &data[offset..];
            if leftover.len() < BD_SOURCE_PACKET_BYTES {
                self.remainder.extend_from_slice(leftover);
            } else {
                self.remainder.clear();
            }
        }

        completed
    }

    /// Demux a single 192-byte BD-TS packet (4-byte TP_extra_header +
    /// 188-byte TS). Routes payload bytes into the per-PID
    /// `PesAssembler`; completed PES packets are pushed onto
    /// `completed` so the caller's allocation amortises across the
    /// batch.
    fn process_packet(
        &mut self,
        packet: &[u8],
        source: Option<crate::pes::SourcePos>,
        completed: &mut Vec<PesPacket>,
    ) {
        // Sync byte check skips malformed packets.
        if packet[4] != SYNC_BYTE {
            return;
        }
        let ts = &packet[4..]; // 188-byte standard TS packet

        let pid = (((ts[1] & 0x1F) as u16) << 8) | ts[2] as u16;
        let pusi = ts[1] & 0x40 != 0; // Payload Unit Start Indicator
        let adaptation = (ts[3] >> 4) & 0x03;

        // P3/B1 CONCEALMENT MARKER. The decrypt layer fills an undecryptable
        // aligned unit with NULL-TS packets (PID 0x1FFF) that carry an
        // adaptation-field discontinuity_indicator (see `aacs::content::fill_null_ts_unit`).
        // This is the authoritative loss signal — unlike a tracked PID's 4-bit
        // continuity_counter it is CC-INDEPENDENT, so it survives a loss that is
        // an exact multiple of 16 packets and a loss at the very start of a PID
        // (no prior CC to diff against). The decrypt layer cannot know which
        // elementary PID(s) the lost unit carried (the data was undecryptable),
        // so force a pending discontinuity on EVERY tracked assembler: the next
        // completed PES of each resyncs. Harmless for audio/subtitle (the codec
        // gate is a no-op there) and at most one extra GOP on a video track that
        // did not actually lose packets — bounded, and only on a degraded disc.
        if pid == NULL_PID
            && (adaptation == 0x02 || adaptation == 0x03)
            && (ts[4] as usize) > 0
            && (ts[5] & 0x80) != 0
        {
            for a in &mut self.assemblers {
                // A concealed unit may have dropped packets belonging to a PES
                // currently open on any PID — so that partial is potentially
                // TRUNCATED (a hole in the middle of its access unit). Drop it
                // like a mid-PES continuity break, and flag pending so the NEXT
                // completed PES (the first frame whose data is entirely post-gap)
                // resyncs. Mirrors the non-PUSI cc_gap path, applied to every PID
                // because the lost unit's PID(s) are unknowable (undecryptable).
                a.buffer.clear();
                a.active = false;
                a.header_remaining = 0;
                a.pending_discontinuity = true;
            }
            return;
        }

        let idx = if (pid as usize) < self.pid_index.len() {
            self.pid_index[pid as usize]
        } else {
            -1
        };
        if idx < 0 {
            return;
        }
        // adaptation_field_control == 0b00 is reserved (ISO 13818-1) and
        // carries no payload; discard so a corrupt/desynced packet can't
        // inject its 184 bytes into the PES assembler.
        if adaptation == 0x00 {
            return;
        }

        let asm = &mut self.assemblers[idx as usize];

        let payload_start = if adaptation == 0x03 || adaptation == 0x02 {
            let af_len = ts[4] as usize;
            if af_len > 183 {
                return; // Malformed: AF length exceeds TS payload
            }
            5 + af_len
        } else {
            4
        };

        if payload_start >= TS_PACKET_BYTES {
            return;
        }
        // adaptation == 0x02 → AF only, no payload.
        if adaptation == 0x02 {
            return;
        }

        let payload = &ts[payload_start..];

        // Continuity check. The 4-bit continuity_counter increments by 1 on
        // every payload-bearing packet of a PID; a gap means dropped TS
        // packets. The adaptation field's discontinuity_indicator (first AF
        // byte, bit 0x80) explicitly flags an intentional break. On a non-PUSI
        // continuation that is discontinuous, the partial PES has a hole in it
        // — splicing the new payload would corrupt the elementary stream — so
        // drop the partial and resync on the next PUSI.
        let cc = ts[3] & 0x0f;
        // adaptation == 0x02 (AF only) already returned above, so only 0x03
        // (AF + payload) can carry an adaptation field here.
        let discontinuity_flag = adaptation == 0x03 && ts[4] > 0 && (ts[5] & 0x80) != 0;
        // A gap is a CC that is neither the expected `(prev + 1) & 0xf` nor a
        // duplicate `prev` (ISO 13818-1 permits a packet to repeat its CC; a
        // duplicate is not a loss). Anything else means one or more packets for
        // this PID were dropped.
        let cc_gap = match asm.last_cc {
            Some(prev) => cc != ((prev + 1) & 0x0f) && cc != prev,
            None => false,
        };
        asm.last_cc = Some(cc);
        // A continuity gap means packets for THIS PID were lost (a damaged source,
        // or — for the conceal path — a loss that the CC-independent NULL-TS marker
        // above did not already flag). The flag is sticky and rides to the FIRST
        // post-gap PES so the codec consumer drops forward to the next keyframe
        // (B1). Attribution differs by where the gap lands (see below).
        let gap = discontinuity_flag || cc_gap;

        if pusi {
            // `header_len` is the FULL (uncapped) PES-header length:
            // 0 = malformed (payload is not a PES start), else 6/9+N.
            let (pts, dts, header_len) = parse_pes_header(payload);
            // Flush the previous PES FIRST — a gap detected on this PUSI packet
            // belongs to the PES STARTING now (its data begins after the lost
            // packets), NOT the one just completing. So set `pending_discontinuity`
            // AFTER start(): it rides the new PES to its own completion. (Setting
            // it before would stamp the pre-gap frame; if that frame were a
            // keyframe the gate would arm-then-disarm on it and admit the real
            // post-gap inter frame with a dangling reference.)
            if let Some(prev) = asm.start(pts, dts, source) {
                completed.push(prev);
            }
            if gap {
                asm.pending_discontinuity = true;
            }
            if header_len == 0 {
                // PUSI packet whose payload is not a valid PES start. Do
                // NOT push it — those bytes are not elementary-stream data
                // and would inject a spurious start code / garbage.
                asm.header_remaining = 0;
            } else if header_len <= payload.len() {
                // Header fits in this packet (the common case).
                asm.header_remaining = 0;
                if header_len < payload.len() {
                    asm.push(&payload[header_len..]);
                }
            } else {
                // Header spills past this packet — skip the remainder on
                // the following continuation packet(s).
                asm.header_remaining = header_len - payload.len();
            }
        } else {
            // Non-PUSI continuation.
            if gap {
                // Mid-PES hole: the open partial has a gap, so splicing this
                // payload would corrupt the ES. Flag pending (consumed at the
                // NEXT completed PES — the first post-gap frame) and drop the
                // open partial; resync on the next PUSI.
                asm.pending_discontinuity = true;
                if asm.active {
                    tracing::trace!(
                        target: "mux",
                        pid = asm.pid,
                        "TS continuity break on non-PUSI continuation; dropping partial PES",
                    );
                    asm.buffer.clear();
                    asm.active = false;
                    asm.header_remaining = 0;
                    return;
                }
            }
            if asm.header_remaining > 0 {
                // Continuation packet still inside a PES header that spanned
                // the boundary — consume header bytes before any ES data.
                let skip = asm.header_remaining.min(payload.len());
                asm.header_remaining -= skip;
                if skip < payload.len() {
                    asm.push(&payload[skip..]);
                }
            } else {
                asm.push(payload);
            }
        }
    }

    /// Flush all assemblers, returning any remaining PES packets.
    pub fn flush(&mut self) -> Vec<PesPacket> {
        let mut completed = Vec::new();
        for asm in &mut self.assemblers {
            if let Some(pkt) = asm.flush() {
                completed.push(pkt);
            }
        }
        completed
    }
}

/// Parse a PES packet header, extracting PTS and DTS.
///
/// Returns `(pts, dts, header_len)` where `header_len` is the FULL,
/// UNCAPPED PES-header length in bytes (`9 + PES_header_data_length`, or
/// 6 for stream IDs without the standard extension). `0` signals the
/// payload is not a valid PES start (malformed / too short). The caller
/// must treat `header_len` as bytes-to-skip and carry any remainder past
/// this packet's payload into the next continuation packet — the header
/// can exceed one TS payload, and the spillover is header, not ES data.
fn parse_pes_header(data: &[u8]) -> (Option<i64>, Option<i64>, usize) {
    // PES packet: 00 00 01 [stream_id] [length:2] [flags...]
    if data.len() < 9 || data[0] != 0x00 || data[1] != 0x00 || data[2] != 0x01 {
        return (None, None, 0);
    }

    let stream_id = data[3];

    // Some stream IDs don't carry the standard PES header extension
    // (ISO 13818-1 Table 2-22: program_stream_map, padding, private_stream_2,
    // ECM, EMM, DSMCC_stream 0xF2, H.222.1 type E 0xF8, program_stream_directory).
    if stream_id == 0xBC
        || stream_id == 0xBE
        || stream_id == 0xBF
        || stream_id == 0xF0
        || stream_id == 0xF1
        || stream_id == 0xF2
        || stream_id == 0xF8
        || stream_id == 0xFF
    {
        return (None, None, 6);
    }

    // Standard PES header: [6] = flags1, [7] = flags2, [8] = header_data_length.
    // The `data.len() < 9` precondition was already checked at the top of
    // this function and nothing shrinks `data` since, so no re-check here.
    let pts_dts_flags = (data[7] >> 6) & 0x03;
    let header_data_len = data[8] as usize;
    // Full, uncapped header length. PTS/DTS (if present) live in the
    // first ~19 bytes, always within this packet's payload, so they parse
    // here; only the *skip* length may extend into the next packet.
    let header_len = 9 + header_data_len;

    let mut pts = None;
    let mut dts = None;

    if pts_dts_flags >= 2 && header_data_len >= 5 && data.len() >= 14 {
        pts = parse_timestamp(&data[9..14]);
    }
    if pts_dts_flags == 3 && header_data_len >= 10 && data.len() >= 19 {
        dts = parse_timestamp(&data[14..19]);
    }

    (pts, dts, header_len)
}

/// Parse a 5-byte PTS/DTS timestamp (33 bits in 90kHz).
/// Validates marker bits per MPEG-2 spec. Returns None on invalid encoding.
fn parse_timestamp(data: &[u8]) -> Option<i64> {
    if data.len() < 5 {
        return None;
    }
    // Validate marker bits: per MPEG-2 Systems (Table 2-17) bit 0 of
    // bytes 0, 2 and 4 of the 5-byte PTS/DTS field must all be 1.
    if (data[0] & 0x01) == 0 || (data[2] & 0x01) == 0 || (data[4] & 0x01) == 0 {
        return None;
    }
    let b0 = data[0] as i64;
    let b1 = data[1] as i64;
    let b2 = data[2] as i64;
    let b3 = data[3] as i64;
    let b4 = data[4] as i64;

    Some(((b0 >> 1) & 0x07) << 30 | b1 << 22 | (b2 >> 1) << 15 | b3 << 7 | b4 >> 1)
}

// ============================================================
// Stream scanning (PAT/PMT → stream list)
// ============================================================

/// Whether `offset` is a credible BD-TS packet boundary in the PSI scanner.
///
/// Requires the sync byte at `data[offset + 4]`, and — to avoid latching onto
/// a stray 0x47 inside a TP_extra_header or payload during a desync — also
/// requires the next 192-spaced position to carry a sync byte when one exists
/// in the buffer. A lone trailing packet (no follower in range) is accepted on
/// its single sync byte.
fn is_resync_point(data: &[u8], offset: usize) -> bool {
    if data.get(offset + 4) != Some(&SYNC_BYTE) {
        return false;
    }
    match data.get(offset + BD_SOURCE_PACKET_BYTES + 4) {
        Some(&b) => b == SYNC_BYTE,
        None => true, // last packet in the buffer — no follower to corroborate
    }
}

/// Compute the byte offset of the PSI payload (the pointer_field) for a BD-TS
/// packet starting at `pkt` (the 4-byte TP_extra_header + 188-byte TS packet).
///
/// Accounts for the adaptation_field_control (bits 5:4 of the 4th TS header
/// byte). Returns `None` when the packet carries no payload (AFC 0b10 = AF
/// only, or the reserved 0b00) or when the adaptation field length runs past
/// the packet. `pkt` must be at least [`BD_SOURCE_PACKET_BYTES`] bytes.
fn psi_payload_base(pkt: &[u8]) -> Option<usize> {
    // TS header is pkt[4..]; byte pkt[7] holds AFC in bits 5:4.
    let afc = (pkt[7] >> 4) & 0x03;
    match afc {
        0x01 => Some(8), // payload only: 4 (TP_extra) + 4 (TS header)
        0x03 => {
            // Adaptation field present + payload. AF length byte is pkt[8];
            // payload starts after it.
            let af_len = pkt[8] as usize;
            let base = 9 + af_len; // 4 + 4 + 1(length byte) + af_len
            if base < BD_SOURCE_PACKET_BYTES {
                Some(base)
            } else {
                None // AF overruns the packet
            }
        }
        // 0x02 = AF only (no payload), 0x00 = reserved.
        _ => None,
    }
}

/// Reassemble a single PSI section (PAT / PMT) for `target_pid` with
/// the expected `table_id`, respecting TS-packet boundaries.
///
/// The section pointed at by `pointer_field` in the PUSI packet may be
/// longer than the 184-byte TS payload (PSI sections can reach 1021
/// bytes; a PMT with many ES entries spans 2+ packets). Reading a flat
/// slice of the input would walk straight through the next packet's
/// TP_extra_header + TS header as if it were table content, yielding a
/// wrong PID / garbage stream_type. This walks the PUSI packet, applies
/// `pointer_field` bounded to within that packet's payload, then appends
/// the payload of each subsequent continuation packet (same PID, no
/// PUSI) until `3 + section_length` bytes have been collected.
///
/// The PUSI packet's payload base is computed with [`psi_payload_base`]
/// so a PSI section carried behind an adaptation field is located
/// correctly rather than assuming the payload starts at `offset + 8`.
///
/// Returns the section bytes (starting at the table_id) or `None` if no
/// matching section is found.
fn collect_psi_section(data: &[u8], target_pid: u16, table_id: u8) -> Option<Vec<u8>> {
    let mut offset = 0;
    while offset + BD_SOURCE_PACKET_BYTES <= data.len() {
        if !is_resync_point(data, offset) {
            offset += 1;
            continue;
        }
        let pid = (((data[offset + 5] & 0x1F) as u16) << 8) | data[offset + 6] as u16;
        let pusi = data[offset + 5] & 0x40 != 0;

        if pid == target_pid && pusi {
            // Locate the payload (pointer_field) accounting for any
            // adaptation field. A packet with no payload (AF only) or an
            // AF that overruns the packet is skipped.
            let Some(payload_off) =
                psi_payload_base(&data[offset..offset + BD_SOURCE_PACKET_BYTES])
            else {
                offset += BD_SOURCE_PACKET_BYTES;
                continue;
            };
            let payload = &data[offset + payload_off..offset + BD_SOURCE_PACKET_BYTES];
            // pointer_field is the FIRST payload byte; the section starts
            // pointer_field bytes after it. Bound the start to within
            // THIS packet's payload — a pointer that runs into the next
            // packet is malformed.
            let pointer = payload[0] as usize;
            let sec_start = 1 + pointer;
            if sec_start + 3 > payload.len() || payload[sec_start] != table_id {
                offset += BD_SOURCE_PACKET_BYTES;
                continue;
            }
            let section_len =
                (((payload[sec_start + 1] & 0x0F) as usize) << 8) | payload[sec_start + 2] as usize;
            let total = 3 + section_len; // table_id + 2 length bytes + body
            let mut section = Vec::with_capacity(total);
            section.extend_from_slice(&payload[sec_start..]);
            if section.len() >= total {
                section.truncate(total);
                return Some(section);
            }
            // Need continuation packets: same PID, no PUSI, with a
            // monotonically incrementing continuity counter. The CC lives in
            // the low nibble of the 4th TS-header byte (offset+7 here: the
            // BD-TS 4-byte prefix precedes the sync byte). A CC gap means a
            // dropped/duplicated packet → the assembled section is corrupt, so
            // abandon it rather than splicing in misordered payload.
            let mut expected_cc = ((data[offset + 7] & 0x0F) + 1) & 0x0F;
            let mut scan = offset + BD_SOURCE_PACKET_BYTES;
            let mut desync = false;
            while scan + BD_SOURCE_PACKET_BYTES <= data.len() && section.len() < total {
                // Require a corroborated resync point (this sync byte plus the
                // follower one packet ahead) before trusting the header. A
                // stray 0x47 in corrupt payload would otherwise misread the CC
                // and fire a false desync.
                if !is_resync_point(data, scan) {
                    scan += 1;
                    continue;
                }
                let cpid = (((data[scan + 5] & 0x1F) as u16) << 8) | data[scan + 6] as u16;
                let cpusi = data[scan + 5] & 0x40 != 0;
                if cpid == target_pid && !cpusi {
                    let cc = data[scan + 7] & 0x0F;
                    if cc != expected_cc {
                        desync = true;
                        break;
                    }
                    expected_cc = (cc + 1) & 0x0F;
                    // Continuation packets may also carry an adaptation
                    // field; compute their payload base the same way.
                    if let Some(cbase) =
                        psi_payload_base(&data[scan..scan + BD_SOURCE_PACKET_BYTES])
                    {
                        section
                            .extend_from_slice(&data[scan + cbase..scan + BD_SOURCE_PACKET_BYTES]);
                    }
                }
                scan += BD_SOURCE_PACKET_BYTES;
            }
            if desync {
                // Restart PSI assembly from the next packet after this PUSI;
                // a later clean copy of the section may still appear.
                offset += BD_SOURCE_PACKET_BYTES;
                continue;
            }
            if section.len() >= total {
                section.truncate(total);
                return Some(section);
            }
            // Incomplete section (truncated input) — stop looking.
            return None;
        }
        offset += BD_SOURCE_PACKET_BYTES;
    }
    None
}

/// Scan BD-TS data for streams by parsing PAT and PMT tables.
/// Returns None if no valid program is found.
pub fn scan_streams(data: &[u8]) -> Option<Vec<crate::disc::Stream>> {
    use crate::disc::*;

    // Pass 1: find PMT PID from PAT (table_id 0x00 on PID 0).
    let pat = collect_psi_section(data, 0, 0x00)?;
    let pat_section_len = (((pat[1] & 0x0F) as usize) << 8) | pat[2] as usize;
    if pat_section_len < 4 {
        return None;
    }
    let mut pat_pmt_pid: Option<u16> = None;
    {
        let entries_start = 8;
        // section_length counts bytes after the length field, incl. the
        // 4-byte CRC; the program loop stops before the CRC.
        let entries_end = (3 + pat_section_len - 4).min(pat.len());
        let mut e = entries_start;
        while e + 4 <= entries_end {
            let prog_num = ((pat[e] as u16) << 8) | pat[e + 1] as u16;
            let p = (((pat[e + 2] & 0x1F) as u16) << 8) | pat[e + 3] as u16;
            if prog_num != 0 {
                pat_pmt_pid = Some(p);
                break;
            }
            e += 4;
        }
    }

    let pmt_pid = pat_pmt_pid?;

    // Pass 2: parse PMT for stream entries (table_id 0x02 on pmt_pid).
    let mut streams = Vec::new();
    let pmt = collect_psi_section(data, pmt_pid, 0x02)?;
    if pmt.len() >= 12 {
        let section_len = (((pmt[1] & 0x0F) as usize) << 8) | pmt[2] as usize;
        // section_length counts the bytes after this field, including the
        // trailing 4-byte CRC; `< 4` would underflow `end` below.
        if section_len < 4 {
            return None;
        }
        // Clamp the section end to the reassembled bytes; a malformed
        // section_len must never drive reads past `pmt`.
        let end = (3 + section_len - 4).min(pmt.len());
        // Clamp prog_info_len so it cannot push `pos` past `end`.
        // ISO 13818-1 requires program_info to fit within the PMT section;
        // a crafted value larger than the remaining section would skip all
        // ES entries and, in pathological cases, wrap or mis-index.
        let prog_info_len =
            ((((pmt[10] & 0x0F) as usize) << 8) | pmt[11] as usize).min(end.saturating_sub(12));
        let mut pos = 12 + prog_info_len;

        while pos + 5 <= end {
            let stream_type = pmt[pos];
            let es_pid = (((pmt[pos + 1] & 0x1F) as u16) << 8) | pmt[pos + 2] as u16;
            let es_info_len = (((pmt[pos + 3] & 0x0F) as usize) << 8) | pmt[pos + 4] as usize;

            // Single source of truth for stream_type → Codec: reuse
            // `Codec::from_coding_type` (the same table the BD STN /
            // disc scanner uses) so the two mappings can never drift.
            // We only retain the category (video/audio/subtitle) and
            // per-kind default attribute logic here.
            let codec = Codec::from_coding_type(stream_type);
            let stream = match codec.kind() {
                CodecKind::Video => {
                    // Default resolution by codec generation (HEVC →
                    // UHD, MPEG-2 → 1080i, else 1080p); refined later
                    // from the actual elementary stream.
                    let resolution = match codec {
                        Codec::Hevc => Resolution::R2160p,
                        Codec::Mpeg2 => Resolution::R1080i,
                        _ => Resolution::R1080p,
                    };
                    Some(Stream::Video(VideoStream {
                        pid: es_pid,
                        codec,
                        resolution,
                        frame_rate: FrameRate::Unknown,
                        hdr: HdrFormat::Sdr,
                        color_space: ColorSpace::Bt709,
                        // TS is a passthrough container — aspect stays in the ES.
                        display_aspect: None,
                        secondary: false,
                        label: String::new(),
                        measured_cicp: None,
                    }))
                }
                CodecKind::Audio => Some(Stream::Audio(AudioStream {
                    pid: es_pid,
                    codec,
                    channels: AudioChannels::Surround51,
                    language: "und".into(),
                    sample_rate: SampleRate::S48,
                    secondary: false,
                    purpose: crate::disc::LabelPurpose::Normal,
                    label: String::new(),
                })),
                CodecKind::Subtitle => Some(Stream::Subtitle(SubtitleStream {
                    pid: es_pid,
                    codec,
                    language: "und".into(),
                    forced: false,
                    qualifier: crate::disc::LabelQualifier::None,
                    codec_data: None,
                })),
                CodecKind::Unknown => {
                    tracing::warn!(
                        target: "mux",
                        "dropping PMT stream entry with unknown stream_type {:#04x} (PID {:#06x})",
                        stream_type,
                        es_pid,
                    );
                    None
                }
            };

            if let Some(s) = stream {
                streams.push(s);
            }
            pos += 5 + es_info_len;
        }
    }

    if streams.is_empty() {
        None
    } else {
        Some(streams)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_timestamp() {
        // Example: PTS = 0 → encoded as 21 00 01 00 01
        let data = [0x21, 0x00, 0x01, 0x00, 0x01];
        assert_eq!(parse_timestamp(&data), Some(0));

        // Example: PTS = 90000 (1 second at 90kHz)
        // Manual encoding: 33 bits = 0x00015F90
        // This is just a sanity check that the parser doesn't crash
        let data2 = [0x21, 0x00, 0x07, 0xE9, 0x01]; // approximate
        let pts = parse_timestamp(&data2);
        assert!(pts.is_some() && pts.unwrap() >= 0);

        // Invalid marker bits → returns None
        let bad = [0x00, 0x00, 0x00, 0x00, 0x00]; // marker bits wrong
        assert_eq!(parse_timestamp(&bad), None);
    }

    /// Build a 192-byte BD-TS payload packet for `pid` with explicit PUSI and
    /// continuity_counter, carrying `payload` (truncated/padded to 184 bytes,
    /// payload-only adaptation).
    fn ts_payload_packet(pid: u16, pusi: bool, cc: u8, payload: &[u8]) -> Vec<u8> {
        let mut pkt = vec![0u8; BD_SOURCE_PACKET_BYTES];
        pkt[4] = SYNC_BYTE;
        pkt[5] = ((pid >> 8) as u8) & 0x1F;
        if pusi {
            pkt[5] |= 0x40;
        }
        pkt[6] = (pid & 0xFF) as u8;
        pkt[7] = 0x10 | (cc & 0x0f); // payload-only adaptation + CC
        let n = payload.len().min(184);
        pkt[8..8 + n].copy_from_slice(&payload[..n]);
        pkt
    }

    /// A minimal valid PES start for a video stream id, with no PTS/DTS flags,
    /// followed by `es` elementary-stream bytes. header_len = 9.
    fn pes_start(es: &[u8]) -> Vec<u8> {
        let mut v = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        v.extend_from_slice(es);
        v
    }

    /// Regression (finding 3): a non-PUSI continuation whose continuity_counter
    /// is not (prev+1)&0xf means TS packets were dropped — the partial PES has a
    /// hole and must be discarded, not spliced. We start a PES (cc=0), then feed
    /// a continuation with a CC gap (cc=5 instead of 1); the assembler drops the
    /// partial. A clean follow-on PUSI then produces exactly that next PES,
    /// proving the corrupt splice didn't happen.
    #[test]
    fn continuity_gap_drops_partial_pes() {
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);

        // Start a PES (cc=0) carrying "AAAA".
        let mut out = demux.feed(&ts_payload_packet(pid, true, 0, &pes_start(b"AAAA")));
        assert!(
            out.is_empty(),
            "first PES still open, nothing completed yet"
        );

        // Discontinuous continuation (cc jumps 0 -> 5) carrying "BBBB". The gap
        // must drop the partial PES rather than append "BBBB".
        out = demux.feed(&ts_payload_packet(pid, false, 5, b"BBBB"));
        assert!(out.is_empty(), "dropped partial PES is not emitted here");

        // A fresh PUSI (cc=6) starts the next PES "CCCC"; starting it would
        // normally flush the previous one — but it was dropped, so nothing is
        // flushed yet.
        out = demux.feed(&ts_payload_packet(pid, true, 6, &pes_start(b"CCCC")));
        assert!(
            out.is_empty(),
            "the dropped partial must NOT be flushed by the next PUSI"
        );

        // Flush: only the clean "CCCC" PES comes out — it must NOT begin with
        // the dropped "AAAA" payload. (Payload-only packets pad to 184 bytes,
        // so compare the leading ES bytes, not the whole padded buffer.)
        let final_out = demux.flush();
        assert_eq!(final_out.len(), 1, "exactly one clean PES");
        assert_eq!(
            &final_out[0].data[..4],
            b"CCCC",
            "surviving PES is the clean one, not the dropped partial"
        );
        // The dropped "BBBB" continuation must not have been spliced anywhere.
        assert!(
            !final_out[0].data.windows(4).any(|w| w == b"BBBB"),
            "dropped continuation must not appear in any emitted PES"
        );
    }

    /// B1 plumbing: a continuity gap detected on a PUSI must stamp
    /// `discontinuity = true` on the PES STARTING after the gap, NOT the one
    /// flushed at the boundary — the post-gap PES is the one whose data begins
    /// after the lost packets and references them. (Attribution fix: stamping the
    /// pre-gap PES would, if it were a keyframe, arm-then-disarm the gate and let
    /// the real post-gap inter frame through with a dangling reference.) A clean
    /// in-sequence PES carries `discontinuity = false`.
    #[test]
    fn continuity_gap_stamps_discontinuity_on_next_pes() {
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);

        // Open A (cc=0): nothing completed yet.
        let out = demux.feed(&ts_payload_packet(pid, true, 0, &pes_start(b"AAAA")));
        assert!(out.is_empty());

        // B's PUSI (cc=1, in sequence) flushes A. A saw no gap → clean.
        let out = demux.feed(&ts_payload_packet(pid, true, 1, &pes_start(b"BBBB")));
        assert_eq!(out.len(), 1, "A completes");
        assert_eq!(&out[0].data[..4], b"AAAA");
        assert!(
            !out[0].discontinuity,
            "in-sequence PES is not a discontinuity"
        );

        // C's PUSI jumps cc 1 -> 5: packets were lost between B and C. B (flushed
        // here) is PRE-gap and stays clean — the gap belongs to C, which starts
        // after the lost packets.
        let out = demux.feed(&ts_payload_packet(pid, true, 5, &pes_start(b"CCCC")));
        assert_eq!(out.len(), 1, "B completes");
        assert_eq!(&out[0].data[..4], b"BBBB");
        assert!(
            !out[0].discontinuity,
            "the pre-gap PES flushed at the boundary must NOT be flagged"
        );

        // C carries the discontinuity — it is the first post-gap PES.
        let out = demux.flush();
        assert_eq!(out.len(), 1);
        assert_eq!(&out[0].data[..4], b"CCCC");
        assert!(
            out[0].discontinuity,
            "the post-gap PES must be flagged so B1 resyncs at/after it"
        );
    }

    /// One 192-byte BD source packet that is a B1 concealment marker: a PID-0x1FFF
    /// null packet carrying the adaptation-field discontinuity_indicator (the byte
    /// shape `fill_null_ts_unit` writes for every packet of a concealed unit).
    fn null_marker_packet() -> Vec<u8> {
        let mut pkt = vec![0u8; BD_SOURCE_PACKET_BYTES];
        pkt[4] = SYNC_BYTE; // 0x47
        pkt[5] = 0x1F; // PID 0x1FFF
        pkt[6] = 0xFF;
        pkt[7] = 0x20; // adaptation-field only
        pkt[8] = 0xB7; // af_len 183
        pkt[9] = 0x80; // discontinuity_indicator
        for b in &mut pkt[10..] {
            *b = 0xFF;
        }
        pkt
    }

    /// HOLE 3 (16-multiple CC blind spot) + the truncated-partial drop. A concealed
    /// unit can drop an exact multiple of 16 packets on a PID, leaving its 4-bit
    /// continuity_counter looking IN-SEQUENCE — so CC-based detection is blind. The
    /// CC-INDEPENDENT marker flags the loss anyway, drops the (potentially
    /// truncated) open PES, and stamps the first post-gap PES.
    #[test]
    fn conceal_marker_forces_discontinuity_with_in_sequence_cc() {
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);
        // A (cc=0) opens; B's PUSI (cc=1) flushes A clean.
        demux.feed(&ts_payload_packet(pid, true, 0, &pes_start(b"AAAA")));
        let out = demux.feed(&ts_payload_packet(pid, true, 1, &pes_start(b"BBBB")));
        assert_eq!(&out[0].data[..4], b"AAAA");
        assert!(!out[0].discontinuity);

        // Concealment marker: a unit was dropped. B is open → potentially truncated
        // → dropped. CC is NOT consulted.
        assert!(
            demux.feed(&null_marker_packet()).is_empty(),
            "marker emits nothing itself"
        );

        // C (cc=2) is EXACTLY in-sequence after B's cc=1 — as if a multiple of 16
        // packets were lost, so `cc_gap` is false. The marker is the only signal.
        let out = demux.feed(&ts_payload_packet(pid, true, 2, &pes_start(b"CCCC")));
        assert!(
            out.is_empty(),
            "the truncated open PES (B) is dropped, not emitted"
        );
        let out = demux.flush();
        assert_eq!(out.len(), 1);
        assert_eq!(&out[0].data[..4], b"CCCC");
        assert!(
            out[0].discontinuity,
            "marker flags the post-gap PES despite in-sequence CC (16-aligned blind spot)"
        );
    }

    /// HOLE 4 (leading loss). If the disc's very first unit is undecryptable the
    /// first surviving packet has no predecessor CC (`last_cc == None`), so CC
    /// detection is blind. The marker still flags the first PES.
    #[test]
    fn conceal_marker_at_stream_start_flags_first_pes() {
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);
        // Marker FIRST — no prior CC exists for this PID.
        assert!(demux.feed(&null_marker_packet()).is_empty());
        // The first real PES of the PID.
        let out = demux.feed(&ts_payload_packet(pid, true, 0, &pes_start(b"AAAA")));
        assert!(out.is_empty());
        let out = demux.flush();
        assert_eq!(out.len(), 1);
        assert_eq!(&out[0].data[..4], b"AAAA");
        assert!(
            out[0].discontinuity,
            "leading concealed loss flags the first PES (last_cc == None case)"
        );
    }

    /// In-sequence continuation (cc 0 -> 1) must still splice normally — the
    /// continuity check must not break the happy path.
    #[test]
    fn continuity_in_sequence_splices() {
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);
        demux.feed(&ts_payload_packet(pid, true, 0, &pes_start(b"AAAA")));
        demux.feed(&ts_payload_packet(pid, false, 1, b"BBBB"));
        let out = demux.flush();
        assert_eq!(out.len(), 1);
        // First payload's ES leads, and the in-sequence continuation's "BBBB"
        // is present (spliced) — the padding zeros sit between them.
        assert_eq!(&out[0].data[..4], b"AAAA", "first PES ES leads");
        assert!(
            out[0].data.windows(4).any(|w| w == b"BBBB"),
            "in-sequence continuation must be spliced in"
        );
    }

    #[test]
    fn test_demuxer_empty() {
        let mut demux = TsDemuxer::new(&[0x1011]);
        let result = demux.feed(&[]);
        assert!(result.is_empty());
    }

    /// A boundary packet (one split across two feeds) must be stamped with the
    /// source offset of its FIRST byte, which sat `remainder.len()` bytes before
    /// the current feed's base — not at `feed_base - 1`. We feed two 192-byte
    /// packets via `feed_at`, splitting mid-second-packet so the second packet
    /// is reassembled at the boundary, and assert its provenance lands exactly
    /// on its first byte.
    #[test]
    fn boundary_packet_source_is_first_byte_not_base_minus_one() {
        let pid = 0x1011;
        let base: u64 = 20480; // sector-aligned (10 × 2048)
        let mut demux = TsDemuxer::new(&[pid]);

        let pkt0 = ts_payload_packet(pid, true, 0, &pes_start(b"AAAA"));
        let pkt1 = ts_payload_packet(pid, true, 1, &pes_start(b"BBBB"));
        let mut full = pkt0;
        full.extend_from_slice(&pkt1);

        // Split mid-pkt1 → pkt1 is reassembled from a 100-byte remainder + the
        // next feed's head. pkt1's first byte is at absolute offset base + 192.
        let split = BD_SOURCE_PACKET_BYTES + 100;
        let out1 = demux.feed_at(base, &full[..split]);
        assert!(out1.is_empty(), "pkt0's PES is still open");

        // Second feed carries the rest; data[0] is at absolute base + split.
        let out2 = demux.feed_at(base + split as u64, &full[split..]);
        // pkt1 (PUSI) flushes pkt0's "AAAA" PES, stamped at pkt0's first byte.
        assert_eq!(out2.len(), 1, "pkt0's PES completes when pkt1 starts");
        assert_eq!(
            out2[0].source.map(|s| s.byte),
            Some(base),
            "AAAA PES provenance is pkt0's first byte"
        );

        // Flush emits pkt1's "BBBB" PES — its source is the boundary stamp.
        let out3 = demux.flush();
        assert_eq!(out3.len(), 1, "pkt1's PES flushes out");
        assert_eq!(
            out3[0].source.map(|s| s.byte),
            Some(base + BD_SOURCE_PACKET_BYTES as u64),
            "boundary packet provenance must be its first byte (base + 192), \
             not feed_base - 1"
        );
    }

    /// Owner decision #7: a plain `feed()` must reset/ignore any base a prior
    /// `feed_at()` left behind, so mixing the two is safe. After a `feed_at`
    /// primes a base, the next plain `feed` must stamp `None` on PES packets it
    /// begins.
    #[test]
    fn plain_feed_resets_prior_feed_at_base() {
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);

        // Prime a base via feed_at; pkt0's PES stays open.
        let out1 = demux.feed_at(20480, &ts_payload_packet(pid, true, 0, &pes_start(b"AAAA")));
        assert!(out1.is_empty());

        // Plain feed must clear the base. pkt1 (PUSI) flushes "AAAA" (which was
        // stamped during feed_at) and starts "BBBB" with NO provenance.
        let out2 = demux.feed(&ts_payload_packet(pid, true, 1, &pes_start(b"BBBB")));
        assert_eq!(out2.len(), 1, "AAAA completes");

        let out3 = demux.flush();
        assert_eq!(out3.len(), 1, "BBBB flushes");
        assert_eq!(
            out3[0].source, None,
            "PES begun by a plain feed must carry no source after a prior feed_at"
        );
    }

    // ── scan_streams PMT parsing ──────────────────────────────────────────

    /// Wrap a 188-byte TS packet body in a 192-byte BD-TS packet
    /// (4-byte timecode prefix the scanner skips).
    fn bdts_packet(body: [u8; 184], pid: u16, pusi: bool) -> Vec<u8> {
        let mut pkt = vec![0u8; BD_SOURCE_PACKET_BYTES];
        // 4-byte timecode prefix is ignored; leave zero.
        pkt[4] = SYNC_BYTE;
        pkt[5] = ((pid >> 8) as u8) & 0x1F;
        if pusi {
            pkt[5] |= 0x40;
        }
        pkt[6] = (pid & 0xFF) as u8;
        pkt[7] = 0x10; // payload only, no adaptation field
        pkt[8..8 + 184].copy_from_slice(&body);
        pkt
    }

    /// Build a PAT TS packet pointing program 1 at `pmt_pid`.
    fn pat_packet(pmt_pid: u16) -> Vec<u8> {
        let mut body = [0xFFu8; 184];
        let mut i = 0;
        body[i] = 0x00; // pointer_field
        i += 1;
        body[i] = 0x00; // table_id = PAT
        // section_length counts bytes after the length field: tsid(2) +
        // version/current_next(1) + section_number(1) + last_section(1) +
        // one 4-byte program entry + 4-byte CRC = 13.
        body[i + 1] = 0xB0; // section_syntax + reserved + len high nibble
        body[i + 2] = 0x0D; // section_length low byte = 13
        body[i + 3] = 0x00; // tsid hi
        body[i + 4] = 0x01; // tsid lo
        body[i + 5] = 0xC1; // version/current_next
        body[i + 6] = 0x00; // section_number
        body[i + 7] = 0x00; // last_section_number
        // program entry: program_number=1 → pmt_pid
        body[i + 8] = 0x00;
        body[i + 9] = 0x01;
        body[i + 10] = 0xE0 | (((pmt_pid >> 8) as u8) & 0x1F);
        body[i + 11] = (pmt_pid & 0xFF) as u8;
        // (CRC bytes left as 0xFF — scanner doesn't validate CRC)
        let _ = &mut i;
        bdts_packet(body, 0, true)
    }

    /// Build a PMT TS packet listing the given `(stream_type, es_pid)` entries.
    fn pmt_packet(pmt_pid: u16, entries: &[(u8, u16)]) -> Vec<u8> {
        let mut body = [0xFFu8; 184];
        body[0] = 0x00; // pointer_field
        let s = 1; // table start
        body[s] = 0x02; // table_id = PMT
        // Fixed PMT fields after section_length: 2(prog) +1 +2 +2(pcr)
        // +2(prog_info_len=0) = 9, then per-entry 5 bytes, then 4 CRC.
        let entries_len = entries.len() * 5;
        let section_length = 9 + entries_len + 4;
        body[s + 1] = 0xB0 | (((section_length >> 8) as u8) & 0x0F);
        body[s + 2] = (section_length & 0xFF) as u8;
        body[s + 3] = 0x00; // program_number hi
        body[s + 4] = 0x01; // program_number lo
        body[s + 5] = 0xC1; // version/current_next
        body[s + 6] = 0x00; // section_number
        body[s + 7] = 0x00; // last_section_number
        body[s + 8] = 0xE0; // PCR PID hi (reserved bits)
        body[s + 9] = 0x00; // PCR PID lo
        body[s + 10] = 0xF0; // program_info_length hi (=0)
        body[s + 11] = 0x00; // program_info_length lo
        let mut p = s + 12;
        for &(stype, es_pid) in entries {
            body[p] = stype;
            body[p + 1] = 0xE0 | (((es_pid >> 8) as u8) & 0x1F);
            body[p + 2] = (es_pid & 0xFF) as u8;
            body[p + 3] = 0xF0; // ES_info_length hi (=0)
            body[p + 4] = 0x00; // ES_info_length lo
            p += 5;
        }
        bdts_packet(body, pmt_pid, true)
    }

    /// Build a 192-byte BD-TS data packet on `pid` carrying `payload`
    /// (payload-only adaptation, truncated/padded to fit one packet).
    fn data_packet(pid: u16, pusi: bool, payload: &[u8]) -> Vec<u8> {
        let mut pkt = vec![0u8; BD_SOURCE_PACKET_BYTES];
        pkt[4] = SYNC_BYTE;
        pkt[5] = ((pid >> 8) as u8) & 0x1F;
        if pusi {
            pkt[5] |= 0x40;
        }
        pkt[6] = (pid & 0xFF) as u8;
        pkt[7] = 0x10; // payload only, no adaptation field
        let room = TS_PACKET_BYTES - 4; // 184 ES bytes after the 4-byte TS header
        let n = payload.len().min(room);
        pkt[8..8 + n].copy_from_slice(&payload[..n]);
        pkt
    }

    /// Like `pmt_packet` but with a 2-byte adaptation field (AFC=0b11) of
    /// stuffing before the payload, to exercise the adaptation-field-aware
    /// payload base computation in scan_streams.
    fn pmt_packet_with_af(pmt_pid: u16, entries: &[(u8, u16)]) -> Vec<u8> {
        let af_len: u8 = 2; // 1 flags byte + 1 stuffing byte
        let mut pkt = vec![0u8; BD_SOURCE_PACKET_BYTES];
        pkt[4] = SYNC_BYTE;
        pkt[5] = (((pmt_pid >> 8) as u8) & 0x1F) | 0x40; // PUSI set
        pkt[6] = (pmt_pid & 0xFF) as u8;
        pkt[7] = 0x30; // AFC = 0b11 (adaptation + payload)
        pkt[8] = af_len; // adaptation_field_length
        pkt[9] = 0x00; // AF flags
        pkt[10] = 0xFF; // stuffing
        // Payload (PSI) begins at 4 + 4 + 1 + af_len = 11.
        let payload_off = 4 + 4 + 1 + af_len as usize;
        let mut body = vec![0xFFu8; BD_SOURCE_PACKET_BYTES - payload_off];
        body[0] = 0x00; // pointer_field
        let s = 1;
        body[s] = 0x02; // table_id = PMT
        let entries_len = entries.len() * 5;
        let section_length = 9 + entries_len + 4;
        body[s + 1] = 0xB0 | (((section_length >> 8) as u8) & 0x0F);
        body[s + 2] = (section_length & 0xFF) as u8;
        body[s + 3] = 0x00;
        body[s + 4] = 0x01;
        body[s + 5] = 0xC1;
        body[s + 6] = 0x00;
        body[s + 7] = 0x00;
        body[s + 8] = 0xE0;
        body[s + 9] = 0x00;
        body[s + 10] = 0xF0;
        body[s + 11] = 0x00;
        let mut p = s + 12;
        for &(stype, es_pid) in entries {
            body[p] = stype;
            body[p + 1] = 0xE0 | (((es_pid >> 8) as u8) & 0x1F);
            body[p + 2] = (es_pid & 0xFF) as u8;
            body[p + 3] = 0xF0;
            body[p + 4] = 0x00;
            p += 5;
        }
        pkt[payload_off..].copy_from_slice(&body);
        pkt
    }

    #[test]
    fn short_pes_payload_injects_no_header_bytes() {
        // A PUSI packet whose payload is NOT a valid PES start
        // (no 00 00 01 start code / too short) must contribute ZERO bytes to
        // the assembled elementary stream — otherwise a stray 00 00 01 in the
        // garbage masquerades as an Annex-B NAL / PES start code in the codec
        // parser. Only the following well-formed continuation bytes survive.
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);

        // Garbage PUSI payload with NO valid PES start code (no leading
        // 00 00 01). It must parse as malformed → header_len 0 → nothing
        // pushed. The bytes include a 00 00 01 03 sequence mid-payload that,
        // if leaked, would masquerade as an Annex-B NAL / PES start code.
        let mut garbage = vec![0xAAu8; 32];
        garbage[8] = 0x00;
        garbage[9] = 0x00;
        garbage[10] = 0x01;
        garbage[11] = 0x03;
        let mut stream = demux.feed(&data_packet(pid, true, &garbage));
        assert!(
            stream.is_empty(),
            "garbage PUSI packet must not complete a PES on its own"
        );

        // Continuation packet (no PUSI) carrying real ES bytes.
        let es = [0xDEu8, 0xAD, 0xBE, 0xEF];
        stream.extend(demux.feed(&data_packet(pid, false, &es)));
        stream.extend(demux.flush());

        assert_eq!(stream.len(), 1, "one PES assembled from the continuation");
        let pes = &stream[0];
        // The continuation ES bytes survive…
        assert!(
            pes.data.windows(es.len()).any(|w| w == es),
            "continuation ES bytes present, got {:02X?}",
            pes.data
        );
        // …but none of the garbage PUSI payload leaked in. In particular the
        // 0xAA filler and the embedded 00 00 01 sequence must be absent — the
        // malformed PES header contributed ZERO bytes to the elementary stream.
        assert!(
            !pes.data.iter().any(|&b| b == 0xAA),
            "garbage PES-header bytes must not appear in the elementary stream"
        );
        assert!(
            !pes.data.windows(3).any(|w| w == [0x00, 0x00, 0x01]),
            "no injected start code leaked from the malformed PES header"
        );
    }

    #[test]
    fn scan_streams_handles_adaptation_field_in_pmt() {
        use crate::disc::{Codec, Stream};
        let pmt_pid = 0x0100;
        let mut data = pat_packet(pmt_pid);
        // PMT carried in a packet with an adaptation field — payload base must
        // account for af_len, not assume offset+8.
        data.extend(pmt_packet_with_af(pmt_pid, &[(0x1B, 0x1011)]));
        // Follower sync byte so is_resync_point corroborates the PMT packet.
        data.extend(pat_packet(pmt_pid));

        let streams = scan_streams(&data).expect("PMT with AF should parse");
        assert!(
            streams
                .iter()
                .any(|s| matches!(s, Stream::Video(v) if v.codec == Codec::H264)),
            "H.264 video must be found past the adaptation field"
        );
    }

    #[test]
    fn scan_streams_maps_lpcm_via_from_coding_type() {
        use crate::disc::{Codec, Stream};
        let pmt_pid = 0x0100;
        let mut data = pat_packet(pmt_pid);
        // 0x80 = LPCM (present in from_coding_type, was MISSING from the
        // old duplicate table in scan_streams). 0x1B = H.264 video.
        data.extend(pmt_packet(pmt_pid, &[(0x1B, 0x1011), (0x80, 0x1100)]));

        let streams = scan_streams(&data).expect("PMT should parse");
        assert_eq!(streams.len(), 2, "video + LPCM audio");

        let lpcm = streams
            .iter()
            .find(|s| matches!(s, Stream::Audio(a) if a.pid == 0x1100))
            .expect("LPCM audio stream present");
        if let Stream::Audio(a) = lpcm {
            assert_eq!(a.codec, Codec::Lpcm, "0x80 must map to LPCM");
        }

        assert!(
            streams
                .iter()
                .any(|s| matches!(s, Stream::Video(v) if v.codec == Codec::H264)),
            "H.264 video present"
        );
    }

    /// Build a PMT whose reassembled section spans MORE than one 184-byte
    /// TS payload, returned as two BD-TS packets: a PUSI packet carrying
    /// the section head and a continuation (no-PUSI) packet carrying the
    /// tail. The reassembler must stitch them back together; a flat-slice
    /// parser would read the continuation packet's TS header as table
    /// content and mis-type or drop the trailing entries.
    fn pmt_two_packets(pmt_pid: u16, entries: &[(u8, u16)]) -> Vec<u8> {
        // Assemble the raw PSI section (table_id + length + body + CRC).
        let entries_len = entries.len() * 5;
        let section_length = 9 + entries_len + 4; // fixed PMT fields + entries + CRC
        let mut section = Vec::new();
        section.push(0x02); // table_id
        section.push(0xB0 | (((section_length >> 8) as u8) & 0x0F));
        section.push((section_length & 0xFF) as u8);
        section.extend_from_slice(&[0x00, 0x01]); // program_number
        section.push(0xC1); // version/current_next
        section.push(0x00); // section_number
        section.push(0x00); // last_section_number
        section.extend_from_slice(&[0xE0, 0x00]); // PCR PID
        section.extend_from_slice(&[0xF0, 0x00]); // program_info_length = 0
        for &(stype, es_pid) in entries {
            section.push(stype);
            section.push(0xE0 | (((es_pid >> 8) as u8) & 0x1F));
            section.push((es_pid & 0xFF) as u8);
            section.extend_from_slice(&[0xF0, 0x00]); // ES_info_length = 0
        }
        section.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]); // CRC (unchecked)

        // First packet payload: pointer_field(0) + as much section as fits.
        let first_cap = 184 - 1; // minus pointer_field
        let head_len = first_cap.min(section.len());
        let mut p0 = [0xFFu8; 184];
        p0[0] = 0x00; // pointer_field
        p0[1..1 + head_len].copy_from_slice(&section[..head_len]);
        let pkt0 = bdts_packet(p0, pmt_pid, true);

        // Continuation packet (no PUSI) carries the rest.
        let mut p1 = [0xFFu8; 184];
        let tail = &section[head_len..];
        assert!(!tail.is_empty(), "test must actually span two packets");
        p1[..tail.len()].copy_from_slice(tail);
        let mut pkt1 = bdts_packet(p1, pmt_pid, false);
        // Continuity counter must increment from the PUSI packet (CC=0) to its
        // continuation (CC=1) — `collect_psi_section` rejects a CC gap as a
        // desync. The CC lives in the low nibble of TS-header byte 4 (offset 7
        // here, after the 4-byte BD-TS timecode prefix).
        pkt1[7] = (pkt1[7] & 0xF0) | 0x01;

        let mut out = pkt0;
        out.extend(pkt1);
        out
    }

    #[test]
    fn scan_streams_reassembles_pmt_across_packets() {
        use crate::disc::{Codec, Stream};
        let pmt_pid = 0x0100;
        // Enough entries that the section exceeds one 183-byte payload:
        // 12 fixed + 4*N*... at 5 bytes/entry; 40 entries = 200 bytes of
        // entries alone, forcing a continuation packet.
        let mut entries: Vec<(u8, u16)> = Vec::new();
        entries.push((0x1B, 0x1011)); // H.264 video
        for i in 0..40u16 {
            entries.push((0x80, 0x1100 + i)); // LPCM audio tracks
        }
        let mut data = pat_packet(pmt_pid);
        data.extend(pmt_two_packets(pmt_pid, &entries));

        let streams = scan_streams(&data).expect("multi-packet PMT should parse");
        // All entries must survive reassembly (video + 40 audio).
        assert_eq!(streams.len(), entries.len(), "every PMT entry reassembled");
        assert!(
            streams
                .iter()
                .any(|s| matches!(s, Stream::Video(v) if v.codec == Codec::H264)),
            "video survives the split"
        );
        // The LAST audio entry lives in the continuation packet — proves
        // the tail was stitched in, not read from a TS header.
        assert!(
            streams.iter().any(
                |s| matches!(s, Stream::Audio(a) if a.pid == 0x1100 + 39 && a.codec == Codec::Lpcm)
            ),
            "trailing audio entry from the continuation packet survives"
        );
    }

    /// Regression for the PSI continuity-counter guard: a continuation packet
    /// whose CC does NOT increment from the PUSI packet is a desync (dropped or
    /// reordered packet). `collect_psi_section` must abandon that assembly
    /// rather than splice misordered payload. Here the only continuation has a
    /// bad CC, so the section never completes and no streams are found.
    #[test]
    fn scan_streams_rejects_pmt_with_cc_desync() {
        let pmt_pid = 0x0100;
        let mut entries: Vec<(u8, u16)> = Vec::new();
        entries.push((0x1B, 0x1011));
        for i in 0..40u16 {
            entries.push((0x80, 0x1100 + i));
        }
        let mut pmt = pmt_two_packets(pmt_pid, &entries);
        // Corrupt the continuation packet's CC. pmt is exactly two BD-TS
        // packets; the second starts at BD_SOURCE_PACKET_BYTES. Its CC (low nibble
        // of offset+7) was set to 1 by pmt_two_packets; flip it to a gap (5).
        let cc_off = BD_SOURCE_PACKET_BYTES + 7;
        pmt[cc_off] = (pmt[cc_off] & 0xF0) | 0x05;

        let mut data = pat_packet(pmt_pid);
        data.extend(pmt);
        // The PMT section can't be reassembled (CC gap) → no program found.
        assert!(
            scan_streams(&data).is_none(),
            "a CC-desynced PMT continuation must not yield streams"
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // Added hardening tests
    // ════════════════════════════════════════════════════════════════════

    /// Build a 192-byte BD-TS packet whose TS payload region is EXACTLY
    /// `payload` (no trailing zero padding). When `payload` is shorter than
    /// the 184-byte TS payload area, the remainder is consumed by a
    /// stuffing adaptation field (AFC 0b11) — the standard BD-TS way to
    /// fill a short payload packet. This lets a test assert the exact ES
    /// bytes the demuxer must produce, unlike `data_packet` which leaves
    /// zero padding that a length-0 (unbounded) PES would absorb as ES.
    fn es_packet_exact(pid: u16, pusi: bool, payload: &[u8]) -> Vec<u8> {
        use crate::consts::TS_PAYLOAD_BYTES;
        assert!(payload.len() <= TS_PAYLOAD_BYTES);
        let mut pkt = vec![0u8; BD_SOURCE_PACKET_BYTES];
        pkt[4] = SYNC_BYTE;
        pkt[5] = ((pid >> 8) as u8) & 0x1F;
        if pusi {
            pkt[5] |= 0x40;
        }
        pkt[6] = (pid & 0xFF) as u8;
        let pad = TS_PAYLOAD_BYTES - payload.len();
        if pad == 0 {
            pkt[7] = 0x10; // payload only
            pkt[8..8 + payload.len()].copy_from_slice(payload);
        } else {
            pkt[7] = 0x30; // AFC 0b11: adaptation + payload
            // adaptation_field consumes `pad` bytes total: 1 length byte +
            // (pad-1) of [flags + stuffing]. payload starts at 8 + pad.
            let af_field_len = pad - 1; // bytes after the length byte
            pkt[8] = af_field_len as u8;
            if af_field_len >= 1 {
                pkt[9] = 0x00; // AF flags (all zero)
                for b in pkt.iter_mut().skip(10).take(af_field_len - 1) {
                    *b = 0xFF; // stuffing
                }
            }
            let payload_off = 8 + pad;
            pkt[payload_off..payload_off + payload.len()].copy_from_slice(payload);
        }
        pkt
    }

    // ── parse_timestamp: marker bits + 33-bit field (ISO 13818-1 Tbl 2-17) ─

    /// Encode a 33-bit PTS/DTS value into the 5-byte field with the
    /// standard 4-bit prefix and all three marker bits (LSB of bytes
    /// 0, 2, 4) set to 1, per ISO/IEC 13818-1 Table 2-17.
    fn encode_pts_i64(pts: i64, prefix: u8) -> [u8; 5] {
        let p = pts as u64;
        [
            prefix | (((p >> 30) as u8) & 0x07) << 1 | 1,
            ((p >> 22) & 0xFF) as u8,
            (((p >> 15) & 0x7F) as u8) << 1 | 1,
            ((p >> 7) & 0xFF) as u8,
            (((p) & 0x7F) as u8) << 1 | 1,
        ]
    }

    #[test]
    fn parse_timestamp_decodes_known_value_90000() {
        // 1 second @ 90 kHz = 90000 ticks. Round-trip through the canonical
        // encoder (markers set) so the bit layout is grounded in the spec,
        // not in whatever the parser happens to emit.
        let enc = encode_pts_i64(90_000, 0x20);
        assert_eq!(parse_timestamp(&enc), Some(90_000));
    }

    #[test]
    fn parse_timestamp_max_33bit_value() {
        // 33-bit max is 2^33-1 = 8_589_934_591. The field carries exactly
        // 33 bits, so the maximum representable PTS must round-trip.
        let max = (1i64 << 33) - 1;
        let enc = encode_pts_i64(max, 0x20);
        assert_eq!(parse_timestamp(&enc), Some(max));
    }

    #[test]
    fn parse_timestamp_rejects_each_missing_marker_bit() {
        // ISO 13818-1 Table 2-17: marker bit (LSB) of bytes 0, 2 and 4 must
        // each be 1. A zero in ANY of the three is an invalid encoding and
        // must yield None — not a misparsed timestamp.
        let good = encode_pts_i64(12_345, 0x20);
        for &byte_idx in &[0usize, 2, 4] {
            let mut bad = good;
            bad[byte_idx] &= 0xFE; // clear the marker bit
            assert_eq!(
                parse_timestamp(&bad),
                None,
                "marker bit cleared in byte {byte_idx} must reject"
            );
        }
        // Bytes 1 and 3 have NO marker bit — clearing their LSB is legal and
        // must still parse.
        for &byte_idx in &[1usize, 3] {
            let mut still_ok = good;
            still_ok[byte_idx] &= 0xFE;
            assert!(
                parse_timestamp(&still_ok).is_some(),
                "byte {byte_idx} has no marker bit; clearing LSB must still parse"
            );
        }
    }

    #[test]
    fn parse_timestamp_too_short_returns_none() {
        // The PTS/DTS field is fixed 5 bytes; fewer than 5 cannot be parsed.
        assert_eq!(parse_timestamp(&[0x21, 0x00, 0x01, 0x00]), None);
        assert_eq!(parse_timestamp(&[]), None);
    }

    // ── parse_pes_header: stream-id classes, flags, lengths ───────────────

    #[test]
    fn parse_pes_header_rejects_bad_start_code() {
        // Per ISO 13818-1 the PES start prefix is exactly 00 00 01. Any
        // other leading bytes → header_len 0 (not a PES start). A wrong
        // first byte must be rejected so garbage isn't injected as ES.
        let mut buf = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x80, 0x05];
        buf.extend_from_slice(&encode_pts_i64(0, 0x20));
        let (pts, dts, hl) = parse_pes_header(&buf);
        assert!(pts.is_some() && dts.is_none() && hl == 14);
        // Corrupt the prefix.
        buf[2] = 0x02;
        assert_eq!(parse_pes_header(&buf), (None, None, 0));
    }

    #[test]
    fn parse_pes_header_too_short_is_malformed() {
        // < 9 bytes cannot hold the fixed PES header — must report
        // header_len 0 rather than reading past the slice.
        let short = [0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x80];
        assert_eq!(parse_pes_header(&short), (None, None, 0));
    }

    #[test]
    fn parse_pes_header_extension_less_stream_ids_report_len_6() {
        // ISO 13818-1 Table 2-22: program_stream_map(0xBC), padding(0xBE),
        // private_stream_2(0xBF), ECM(0xF0), EMM(0xF1), DSMCC(0xF2),
        // H.222.1 type E(0xF8), program_stream_directory(0xFF) carry NO
        // standard PES header extension → header_len 6, no PTS/DTS.
        for sid in [0xBCu8, 0xBE, 0xBF, 0xF0, 0xF1, 0xF2, 0xF8, 0xFF] {
            let buf = [0x00, 0x00, 0x01, sid, 0x00, 0x00, 0x80, 0xC0, 0x0A];
            let (pts, dts, hl) = parse_pes_header(&buf);
            assert_eq!(
                (pts, dts, hl),
                (None, None, 6),
                "stream_id {sid:#04x} must be extension-less (len 6, no timestamps)"
            );
        }
    }

    #[test]
    fn parse_pes_header_pts_only_vs_pts_dts() {
        // pts_dts_flags (bits 7:6 of flags2 / data[7]): 0b10 = PTS only,
        // 0b11 = PTS+DTS. header_data_length must cover the fields (>=5 PTS,
        // >=10 PTS+DTS) per Table 2-21.
        let mut pts_only = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x80, 0x05];
        pts_only.extend_from_slice(&encode_pts_i64(90_000, 0x20));
        let (p, d, hl) = parse_pes_header(&pts_only);
        assert_eq!((p, d, hl), (Some(90_000), None, 14));

        let mut both = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0xC0, 0x0A];
        both.extend_from_slice(&encode_pts_i64(180_000, 0x30));
        both.extend_from_slice(&encode_pts_i64(90_000, 0x10));
        let (p, d, hl) = parse_pes_header(&both);
        assert_eq!((p, d, hl), (Some(180_000), Some(90_000), 19));
    }

    #[test]
    fn parse_pes_header_dts_flag_without_room_skips_dts() {
        // pts_dts_flags == 0b11 but header_data_length only 5 (< 10) — the
        // declared header cannot hold the DTS field, so DTS must be dropped
        // (reading data[14..19] would consume payload as a bogus timestamp).
        let mut buf = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0xC0, 0x05];
        buf.extend_from_slice(&encode_pts_i64(90_000, 0x30));
        // pad so data.len() >= 19 to prove the gate is on header_data_len,
        // not on slice length.
        buf.extend_from_slice(&[0xAA; 10]);
        let (p, d, hl) = parse_pes_header(&buf);
        assert_eq!(p, Some(90_000), "PTS present");
        assert_eq!(d, None, "DTS dropped: header_data_length too short for it");
        assert_eq!(hl, 14, "header_len = 9 + header_data_length(5)");
    }

    #[test]
    fn parse_pes_header_len_is_uncapped() {
        // header_len must be the FULL 9 + header_data_length even when it
        // exceeds the slice — the caller relies on this to skip header bytes
        // that spill into continuation packets. A capped length would leak
        // header bytes into the ES.
        let buf = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x80, 200];
        let (_, _, hl) = parse_pes_header(&buf);
        assert_eq!(
            hl,
            9 + 200,
            "header_len uncapped at 209 even though slice is 9"
        );
    }

    // ── process_packet routing: sync, PID, AFC, PUSI ──────────────────────

    #[test]
    fn untracked_pid_produces_nothing() {
        // A demuxer tracking only PID 0x1011 must ignore packets on any
        // other PID — they belong to other elementary streams.
        let mut demux = TsDemuxer::new(&[0x1011]);
        let mut pes = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        pes.extend_from_slice(&[0xDE, 0xAD]);
        let out = demux.feed(&data_packet(0x1012, true, &pes)); // wrong PID
        assert!(out.is_empty());
        assert!(demux.flush().is_empty());
    }

    #[test]
    fn bad_sync_byte_skips_packet() {
        // TS sync byte (ISO 13818-1) is 0x47 at TS offset 0 (= BD offset 4).
        // A packet with the wrong sync byte must be discarded, not parsed.
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);
        let mut pkt = data_packet(pid, true, &{
            let mut v = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
            v.extend_from_slice(&[0x11, 0x22, 0x33]);
            v
        });
        pkt[4] = 0x46; // corrupt sync byte
        let out = demux.feed(&pkt);
        assert!(
            out.is_empty(),
            "bad sync byte must drop the packet entirely"
        );
        assert!(demux.flush().is_empty());
    }

    #[test]
    fn afc_reserved_zero_drops_payload() {
        // adaptation_field_control == 0b00 is reserved (ISO 13818-1
        // Table 2-5) and carries no payload — its 184 bytes must NOT be
        // injected into the assembler.
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);
        let mut pkt = data_packet(pid, true, &{
            let mut v = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
            v.extend_from_slice(&[0xCA, 0xFE]);
            v
        });
        // Force AFC = 0b00 while keeping PUSI: byte 5 (TS byte1) holds PUSI;
        // byte 7 (TS byte3) holds scrambling(2) AFC(2) CC(4).
        pkt[7] = 0x00; // AFC 0b00, CC 0
        let out = demux.feed(&pkt);
        assert!(out.is_empty());
        assert!(demux.flush().is_empty(), "reserved AFC contributes no ES");
    }

    #[test]
    fn afc_adaptation_only_carries_no_payload() {
        // AFC == 0b10 = adaptation field only, no payload (ISO 13818-1).
        // Even with a valid AF length, no ES bytes may be produced.
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);
        // Build a PUSI packet that starts a PES…
        let mut start = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        start.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]);
        demux.feed(&es_packet_exact(pid, true, &start));
        // …then an AF-only continuation packet whose "payload" bytes must
        // be discarded.
        let mut afonly = vec![0u8; BD_SOURCE_PACKET_BYTES];
        afonly[4] = SYNC_BYTE;
        afonly[5] = ((pid >> 8) as u8) & 0x1F; // no PUSI
        afonly[6] = (pid & 0xFF) as u8;
        afonly[7] = 0x20; // AFC = 0b10 (AF only)
        afonly[8] = 5; // adaptation_field_length
        for b in afonly.iter_mut().skip(9).take(183) {
            *b = 0xEE; // would be ES if (wrongly) treated as payload
        }
        demux.feed(&afonly);
        let out = demux.flush();
        assert_eq!(out.len(), 1);
        // None of the 0xEE AF-only bytes may appear.
        assert!(
            !out[0].data.iter().any(|&b| b == 0xEE),
            "AF-only packet bytes must never be appended as ES"
        );
        assert_eq!(out[0].data, vec![0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn adaptation_field_len_skipped_before_payload() {
        // AFC == 0b11: payload starts at 5 + adaptation_field_length within
        // the TS packet. The AF bytes must NOT appear in the ES.
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);
        let mut pkt = vec![0u8; BD_SOURCE_PACKET_BYTES];
        pkt[4] = SYNC_BYTE;
        pkt[5] = (((pid >> 8) as u8) & 0x1F) | 0x40; // PUSI
        pkt[6] = (pid & 0xFF) as u8;
        pkt[7] = 0x30; // AFC = 0b11
        let pes = [
            0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00, // PES header (hdr_len 0)
            0x77, 0x88,
        ];
        // TS payload area is 184 bytes. Size the AF so it consumes exactly
        // everything except the PES, leaving no zero padding for the
        // length-0 (unbounded) video PES to absorb. AF stuffing = 0xBB to
        // prove it never leaks into the ES.
        let payload_area = 184usize;
        let af_total = payload_area - pes.len(); // bytes incl. length byte
        let af_field_len = af_total - 1; // bytes after the length byte
        pkt[8] = af_field_len as u8;
        pkt[9] = 0x00; // AF flags
        for b in pkt.iter_mut().skip(10).take(af_field_len - 1) {
            *b = 0xBB; // AF stuffing (must not leak)
        }
        // Payload (PES) begins at 4 + 4 + af_total.
        let payload_off = 4 + 4 + af_total;
        pkt[payload_off..payload_off + pes.len()].copy_from_slice(&pes);
        demux.feed(&pkt);
        let out = demux.flush();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].data, vec![0x77, 0x88]);
        assert!(
            !out[0].data.iter().any(|&b| b == 0xBB),
            "adaptation-field stuffing must not appear in the ES"
        );
    }

    #[test]
    fn malformed_af_length_over_183_drops_packet() {
        // adaptation_field_length can be at most 183 (the TS payload area).
        // A larger value runs past the packet and must be discarded.
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);
        let mut pkt = vec![0u8; BD_SOURCE_PACKET_BYTES];
        pkt[4] = SYNC_BYTE;
        pkt[5] = (((pid >> 8) as u8) & 0x1F) | 0x40;
        pkt[6] = (pid & 0xFF) as u8;
        pkt[7] = 0x30; // AFC 0b11
        pkt[8] = 184; // > 183 — malformed
        let out = demux.feed(&pkt);
        assert!(out.is_empty());
        assert!(demux.flush().is_empty());
    }

    // ── PES reassembly across packets ─────────────────────────────────────

    #[test]
    fn pes_reassembled_from_continuation_packets() {
        // A PES spanning multiple TS packets: PUSI starts it, subsequent
        // no-PUSI packets append payload, and the NEXT PUSI completes the
        // previous PES (ISO 13818-1 §2.4.3.6 PUSI semantics).
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);
        let mut start = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        start.extend_from_slice(&[0xA1, 0xA2]);
        let mut out = demux.feed(&es_packet_exact(pid, true, &start));
        assert!(out.is_empty(), "first PES not yet completed");
        out.extend(demux.feed(&es_packet_exact(pid, false, &[0xB1, 0xB2])));
        out.extend(demux.feed(&es_packet_exact(pid, false, &[0xC1, 0xC2])));
        // New PUSI completes the previous PES.
        let mut start2 = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        start2.extend_from_slice(&[0xD1]);
        out.extend(demux.feed(&es_packet_exact(pid, true, &start2)));
        assert_eq!(out.len(), 1, "previous PES completed by new PUSI");
        assert_eq!(out[0].data, vec![0xA1, 0xA2, 0xB1, 0xB2, 0xC1, 0xC2]);
        out.extend(demux.flush());
        assert_eq!(out.last().unwrap().data, vec![0xD1]);
    }

    #[test]
    fn pes_header_spanning_two_packets_is_fully_skipped() {
        // A PES header (9 + header_data_length) can exceed the 184-byte
        // payload of one TS packet. The spillover header bytes on the next
        // continuation packet must be skipped, NOT appended as ES — else a
        // bogus 00 00 01 start code corrupts the codec stream.
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);
        // header_data_length = 184 → header_len = 193 > 184 payload.
        // Fill the declared header area with 0xAA filler.
        let mut start = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 184];
        start.extend(std::iter::repeat_n(0xAAu8, 175)); // 9 + 175 = 184 bytes in pkt
        demux.feed(&es_packet_exact(pid, true, &start));
        // header_remaining = 193 - 184 = 9 bytes spill into the next packet.
        // Continuation: 9 header-spill bytes (0xAA) then real ES.
        let mut cont = vec![0xAAu8; 9]; // remaining header bytes
        cont.extend_from_slice(&[0xEF, 0xBE]); // real ES
        demux.feed(&es_packet_exact(pid, false, &cont));
        let out = demux.flush();
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].data,
            vec![0xEF, 0xBE],
            "only post-header ES survives; spillover header bytes skipped"
        );
    }

    #[test]
    fn unaligned_feed_reassembles_across_call_boundary() {
        // 16 MiB ISO batches never divide evenly into 192-byte BD-TS
        // packets, so a packet may straddle two feed() calls. The remainder
        // buffer must splice the boundary packet without losing data.
        let pid = 0x1011;
        let mut full = es_packet_exact(pid, true, &{
            let mut v = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
            v.extend_from_slice(&[0x10, 0x20, 0x30, 0x40]);
            v
        });
        full.extend(es_packet_exact(pid, false, &[0x50, 0x60]));
        // Split mid-first-packet (not on a 192 boundary).
        let mut demux = TsDemuxer::new(&[pid]);
        let cut = 100;
        let mut out = demux.feed(&full[..cut]);
        out.extend(demux.feed(&full[cut..]));
        out.extend(demux.flush());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].data, vec![0x10, 0x20, 0x30, 0x40, 0x50, 0x60]);
    }

    #[test]
    fn feed_holds_sub_packet_remainder_without_emitting() {
        // A feed() shorter than one full boundary packet must buffer and
        // emit nothing until the rest arrives — never emit a truncated PES.
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);
        // Seed a remainder by feeding most of a packet, then feed < need.
        let pkt = es_packet_exact(pid, true, &{
            let mut v = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
            v.extend_from_slice(&[0xAB, 0xCD]);
            v
        });
        let out1 = demux.feed(&pkt[..50]); // partial: 50 < 192
        assert!(out1.is_empty());
        let out2 = demux.feed(&pkt[50..100]); // still partial: 100 < 192
        assert!(out2.is_empty(), "sub-packet remainder must not emit");
        let mut out = demux.feed(&pkt[100..]);
        out.extend(demux.flush());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].data, vec![0xAB, 0xCD]);
    }

    #[test]
    fn two_pids_route_independently_no_collision() {
        // Distinct PIDs route to distinct assemblers; interleaved packets on
        // two PIDs must not cross-contaminate (ISO 13818-1 PID demux).
        let (v, a) = (0x1011u16, 0x1100u16);
        let mut demux = TsDemuxer::new(&[v, a]);
        let mut vstart = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        vstart.extend_from_slice(&[0x11, 0x11]);
        let mut astart = vec![0x00, 0x00, 0x01, 0xBD, 0x00, 0x00, 0x80, 0x00, 0x00];
        astart.extend_from_slice(&[0x22, 0x22]);
        let mut out = Vec::new();
        out.extend(demux.feed(&es_packet_exact(v, true, &vstart)));
        out.extend(demux.feed(&es_packet_exact(a, true, &astart)));
        out.extend(demux.feed(&es_packet_exact(v, false, &[0x33])));
        out.extend(demux.feed(&es_packet_exact(a, false, &[0x44])));
        out.extend(demux.flush());
        let vpes = out.iter().find(|p| p.pid == v).unwrap();
        let apes = out.iter().find(|p| p.pid == a).unwrap();
        assert_eq!(
            vpes.data,
            vec![0x11, 0x11, 0x33],
            "video ES not contaminated"
        );
        assert_eq!(
            apes.data,
            vec![0x22, 0x22, 0x44],
            "audio ES not contaminated"
        );
    }

    #[test]
    fn pusi_with_pts_is_extracted() {
        // A PUSI PES carrying a PTS must surface that PTS on the completed
        // packet (ISO 13818-1 §2.4.3.7). Grounds the PTS path in process_packet.
        let pid = 0x1011;
        let mut demux = TsDemuxer::new(&[pid]);
        let mut pes = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x80, 0x05];
        pes.extend_from_slice(&encode_pts_i64(90_000, 0x20));
        pes.extend_from_slice(&[0xFE, 0xED]);
        demux.feed(&es_packet_exact(pid, true, &pes));
        let out = demux.flush();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pts, Some(90_000));
        assert_eq!(out[0].data, vec![0xFE, 0xED]);
    }

    #[test]
    fn flush_on_empty_assembler_yields_nothing() {
        // Flushing a demuxer that never saw a started PES must yield no
        // packets — never a spurious empty PES.
        let mut demux = TsDemuxer::new(&[0x1011]);
        assert!(demux.flush().is_empty());
    }

    #[test]
    fn new_with_empty_pids_tracks_nothing() {
        // Empty PID list → max_pid 0, table floored to 8192, all untracked.
        // Feeding well-formed packets must produce nothing and not panic.
        let mut demux = TsDemuxer::new(&[]);
        let mut pes = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        pes.extend_from_slice(&[0xAA]);
        assert!(demux.feed(&data_packet(0x1011, true, &pes)).is_empty());
        assert!(demux.flush().is_empty());
    }

    #[test]
    fn high_pid_above_table_floor_is_tracked() {
        // The flat PID table is sized to max(8192, max_pid+1). A PID at the
        // top of the 13-bit BD-TS space (0x1FFF) must still route correctly.
        let pid = 0x1FFFu16; // 13-bit max
        let mut demux = TsDemuxer::new(&[pid]);
        let mut pes = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        pes.extend_from_slice(&[0x5A, 0xA5]);
        demux.feed(&es_packet_exact(pid, true, &pes));
        let out = demux.flush();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].pid, pid);
        assert_eq!(out[0].data, vec![0x5A, 0xA5]);
    }

    // ── scan_streams error / boundary paths ───────────────────────────────

    #[test]
    fn scan_streams_no_pat_returns_none() {
        // Without a PAT (table_id 0x00 on PID 0) there is no program to find.
        let data = vec![0u8; BD_SOURCE_PACKET_BYTES * 2]; // all zero, no sync bytes
        assert!(scan_streams(&data).is_none());
    }

    #[test]
    fn scan_streams_pat_but_no_pmt_returns_none() {
        // PAT points at a PMT PID, but no PMT section is present in the
        // stream → scan must return None, not a partial/garbage stream list.
        let pmt_pid = 0x0100;
        let mut data = pat_packet(pmt_pid);
        data.extend(pat_packet(pmt_pid)); // follower sync corroboration
        assert!(scan_streams(&data).is_none());
    }

    #[test]
    fn scan_streams_drops_unknown_stream_type() {
        // A PMT entry with an unknown stream_type maps to Codec::Unknown
        // (CodecKind::Unknown) and must be dropped, not emitted as a stream.
        use crate::disc::Stream;
        let pmt_pid = 0x0100;
        let mut data = pat_packet(pmt_pid);
        // 0x1B = H.264 (kept), 0x7F = unassigned/unknown (dropped).
        data.extend(pmt_packet(pmt_pid, &[(0x1B, 0x1011), (0x7F, 0x1500)]));
        data.extend(pat_packet(pmt_pid)); // follower
        let streams = scan_streams(&data).expect("known stream survives");
        assert_eq!(streams.len(), 1, "unknown stream_type entry dropped");
        assert!(matches!(streams[0], Stream::Video(_)));
    }

    #[test]
    fn scan_streams_hevc_defaults_to_uhd_resolution() {
        // scan_streams seeds a default resolution by codec generation:
        // HEVC → R2160p (UHD). Grounded in the resolution-seed branch.
        use crate::disc::{Resolution, Stream};
        let pmt_pid = 0x0100;
        let mut data = pat_packet(pmt_pid);
        data.extend(pmt_packet(pmt_pid, &[(0x24, 0x1011)])); // 0x24 = HEVC
        data.extend(pat_packet(pmt_pid));
        let streams = scan_streams(&data).expect("HEVC video parses");
        let v = streams
            .iter()
            .find_map(|s| match s {
                Stream::Video(v) => Some(v),
                _ => None,
            })
            .expect("video present");
        assert_eq!(v.resolution, Resolution::R2160p, "HEVC defaults to UHD");
    }

    #[test]
    fn scan_streams_mpeg2_defaults_to_1080i() {
        // MPEG-2 video (stream_type 0x02) defaults to R1080i in scan_streams.
        use crate::disc::{Resolution, Stream};
        let pmt_pid = 0x0100;
        let mut data = pat_packet(pmt_pid);
        data.extend(pmt_packet(pmt_pid, &[(0x02, 0x1011)])); // 0x02 = MPEG-2
        data.extend(pat_packet(pmt_pid));
        let streams = scan_streams(&data).expect("MPEG-2 video parses");
        let v = streams
            .iter()
            .find_map(|s| match s {
                Stream::Video(v) => Some(v),
                _ => None,
            })
            .expect("video present");
        assert_eq!(v.resolution, Resolution::R1080i, "MPEG-2 defaults to 1080i");
    }

    #[test]
    fn scan_streams_oversized_prog_info_len_does_not_panic() {
        // Regression: a PMT with prog_info_len larger than the section body
        // must not panic, index out of bounds, or silently corrupt `pos`.
        // The parser must clamp it and still return None (no valid ES entries
        // past the inflated descriptor region).
        let pmt_pid = 0x0100u16;

        // Build a minimal PAT pointing at pmt_pid.
        let mut data = pat_packet(pmt_pid);

        // Craft a raw PMT TS packet with prog_info_len = 0x0FFF (4095),
        // which is far larger than the actual section content.  The section
        // itself only holds a single H.264 ES entry (5 bytes) so the real
        // prog_info_len must be 0.
        let mut body = [0xFFu8; 184];
        body[0] = 0x00; // pointer_field
        let s = 1;
        body[s] = 0x02; // table_id = PMT
        // section_length = 9 (fixed fields) + 5 (one ES entry) + 4 (CRC) = 18
        let section_length: usize = 9 + 5 + 4;
        body[s + 1] = 0xB0 | (((section_length >> 8) as u8) & 0x0F);
        body[s + 2] = (section_length & 0xFF) as u8;
        body[s + 3] = 0x00; // program_number hi
        body[s + 4] = 0x01; // program_number lo
        body[s + 5] = 0xC1; // version/current_next
        body[s + 6] = 0x00; // section_number
        body[s + 7] = 0x00; // last_section_number
        body[s + 8] = 0xE0; // PCR PID hi
        body[s + 9] = 0x00; // PCR PID lo
        // prog_info_len = 0x0FFF — crafted oversized value
        body[s + 10] = 0xFF; // 0xF0 reserved | 0x0F high nibble of 0xFFF
        body[s + 11] = 0xFF; // low byte of 0xFFF
        // ES entry: H.264 (0x1B) on PID 0x1011, es_info_len=0
        let p = s + 12;
        body[p] = 0x1B;
        body[p + 1] = 0xE0 | ((0x1011u16 >> 8) as u8 & 0x1F);
        body[p + 2] = (0x1011u16 & 0xFF) as u8;
        body[p + 3] = 0xF0; // es_info_len hi = 0
        body[p + 4] = 0x00; // es_info_len lo = 0
        data.extend(bdts_packet(body, pmt_pid, true));
        data.extend(pat_packet(pmt_pid)); // corroboration packet

        // Must not panic.  The oversized prog_info_len causes the ES entry to
        // be skipped after clamping, so the result is None or an empty stream
        // list (both are acceptable; the critical invariant is no panic/OOB).
        let _ = scan_streams(&data);
    }

    // ── PES reassembly buffer cap (DoS hardening) ─────────────────────────

    #[test]
    fn pes_buffer_cap_resets_on_overflow_and_recovers_on_next_pusi() {
        // Feed continuation-only packets that would exceed MAX_PES_BUFFER if
        // allowed to accumulate, then verify:
        //   (a) the assembler buffer never grows past the cap,
        //   (b) a subsequent valid PUSI + continuation produces a correct PES.
        //
        // Each continuation packet carries 184 ES bytes.  We need enough packets
        // to exceed MAX_PES_BUFFER even after the cap resets the buffer between
        // overflows.  Sending (MAX_PES_BUFFER / 184) + 2 packets guarantees at
        // least one cap-trigger regardless of internal doubling.
        let pid = 0x1011u16;
        let mut demux = TsDemuxer::new(&[pid]);

        // Start a PES so the assembler is `active` before we hammer it.
        let mut pes_start = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        pes_start.extend_from_slice(&[0xAB; 10]);
        demux.feed(&es_packet_exact(pid, true, &pes_start));

        // Continuation packets with 184-byte payloads, no PUSI.  Each call to
        // feed() processes one 192-byte BD-TS packet.
        let payload = [0xCCu8; 184];
        let cont_pkt = data_packet(pid, false, &payload);
        let packets_needed = MAX_PES_BUFFER / 184 + 2;
        let mut mid_out: Vec<PesPacket> = Vec::new();
        for _ in 0..packets_needed {
            mid_out.extend(demux.feed(&cont_pkt));
            // Verify the internal buffer is bounded: no assembler may hold
            // more than MAX_PES_BUFFER bytes at any point.
            for asm in &demux.assemblers {
                assert!(
                    asm.buffer.len() <= MAX_PES_BUFFER,
                    "assembler buffer exceeded cap: {} > {MAX_PES_BUFFER}",
                    asm.buffer.len()
                );
            }
        }
        // The demuxer must not have completed any PES during the flood
        // (the cap resets the partial PES rather than emitting garbage).
        assert!(
            mid_out.is_empty(),
            "no PES must be emitted during a cap-overflow continuation flood"
        );

        // Recovery: a new valid PUSI followed by a continuation packet must
        // produce exactly one well-formed PES with the correct ES bytes.
        let mut good_start = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        good_start.extend_from_slice(&[0x11u8, 0x22]);
        let mut out = demux.feed(&es_packet_exact(pid, true, &good_start));
        out.extend(demux.feed(&es_packet_exact(pid, false, &[0x33u8, 0x44])));
        // Flush to complete the in-progress PES.
        out.extend(demux.flush());
        assert_eq!(out.len(), 1, "exactly one PES after recovery");
        assert_eq!(
            out[0].data,
            vec![0x11, 0x22, 0x33, 0x44],
            "recovered PES carries only the post-reset ES bytes"
        );
    }
}
