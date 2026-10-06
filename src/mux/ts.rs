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
// MPEG-TS null-packet PID (0x1FFF); carries no elementary stream. A 0x1FFF
// packet with adaptation-field discontinuity_indicator is treated as a
// concealed-gap signal; only externally-authored markers reach this now.
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
    /// a continuity break (CC gap or discontinuity_indicator) on a tracked PID,
    /// or an externally-authored NULL-TS concealment marker (the mux itself no
    /// longer emits such markers). This PES is the FIRST whose data is entirely
    /// after the gap: a mid-frame loss drops the truncated partial and flags the
    /// next complete PES; a loss on a PES boundary flags the PES starting after
    /// it. Inter-coded video may reference lost data up to the next IRAP/IDR;
    /// codec-parse consumers should drop forward to the next keyframe.
    pub discontinuity: bool,
}

/// Per-PID PES reassembly state.
struct PesAssembler {
    pid: u16,
    /// This PID's PES-reassembly ceiling: its share of [`MAX_PES_BUFFER_TOTAL`],
    /// clamped to [`MAX_PES_BUFFER`]. Resolved once by `TsDemuxer::new` so the
    /// per-PID caps sum to a bounded total no matter how many streams the disc
    /// declares.
    cap: usize,
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
    /// Payload bytes since the PUSI while the PES header (fixed part or PTS/DTS) is still
    /// incomplete: h222 lets it span packets. Empty otherwise.
    head: Vec<u8>,
    /// 4-bit continuity_counter of the last payload-bearing TS packet seen
    /// on this PID. A non-PUSI continuation whose CC is not `(prev + 1) & 0xf`
    /// — or whose adaptation field flags a discontinuity — means one or more
    /// TS packets for this PID were dropped; splicing the new payload onto the
    /// partial PES would inject corrupt bytes. The partial PES is dropped and
    /// the assembler resyncs on the next PUSI. `None` until the first packet.
    last_cc: Option<u8>,
    /// PUSI bit and payload of that packet: a same-CC packet is a duplicate (h222 2.4.3.3)
    /// only if it repeats them; otherwise it is new data after a gap (e.g. a clip join).
    last_pusi: bool,
    last_payload: Vec<u8>,
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

// Initial PES buffer cap: covers common audio/subtitle PES outright (~16 KB);
// video PES grows via Vec doubling, hitting allocator slab caches instead of
// the 64-page first-touch faults the old with_capacity(256 KiB) caused per PES.
const PES_BUFFER_INIT_CAP: usize = 16 * 1024;

// Hard cap on a single PID's PES buffer: an HEVC/UHD AU is ~1-3 MiB, so 64 MiB
// is a wide margin. Guards against a corrupt/crafted stream that never sends a
// PUSI; push() drops the partial PES and resyncs on the next PUSI past this cap.
const MAX_PES_BUFFER: usize = 64 * 1024 * 1024; // 64 MiB

// Aggregate ceiling across every tracked PID; each PID's share (`pes_cap`) is derived from this
// so a crafted MPLS declaring many streams can't multiply MAX_PES_BUFFER by the stream count.
const MAX_PES_BUFFER_TOTAL: usize = 512 * 1024 * 1024; // 512 MiB

impl PesAssembler {
    /// `cap` is this PID's SHARE of [`MAX_PES_BUFFER_TOTAL`], resolved by
    /// `TsDemuxer::new` from the tracked-PID count.
    fn new(pid: u16, cap: usize) -> Self {
        Self {
            pid,
            cap,
            buffer: Vec::with_capacity(PES_BUFFER_INIT_CAP),
            pts: None,
            dts: None,
            active: false,
            header_remaining: 0,
            head: Vec::new(),
            last_cc: None,
            last_pusi: false,
            last_payload: Vec::new(),
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

    // Append payload data. If the buffer would exceed this PID's `cap` (its share of
    // MAX_PES_BUFFER_TOTAL, at most MAX_PES_BUFFER) the partial PES is dropped and the
    // assembler resyncs on the next PUSI — bounds allocation per-PID and in aggregate.
    fn push(&mut self, data: &[u8]) {
        if self.active {
            if self.buffer.len().saturating_add(data.len()) > self.cap {
                tracing::trace!(
                    target: "mux",
                    pid = self.pid,
                    bytes = self.buffer.len(),
                    "PES buffer cap exceeded; dropping partial PES and resyncing on next PUSI",
                );
                self.drop_partial();
                return;
            }
            // Grow by doubling but never past `cap`, so the allocation honours the share too.
            let need = self.buffer.len() + data.len();
            if need > self.buffer.capacity() {
                let target = need.max(self.buffer.capacity() * 2).min(self.cap);
                self.buffer.reserve_exact(target - self.buffer.len());
            }
            self.buffer.extend_from_slice(data);
        }
    }

    // Drop the open PES (its data has a hole) and flag the next one; resyncs on the next PUSI.
    fn drop_partial(&mut self) {
        self.buffer.clear();
        self.active = false;
        self.header_remaining = 0;
        self.head.clear();
        self.pending_discontinuity = true;
    }

    // Take PUSI payload bytes into the PES header; once it and its PTS/DTS are complete, set
    // the timestamps and pass the rest on as ES. A payload that is not a PES start opens no
    // PES (its continuations would form a headless frame) and flags the loss.
    fn take_head(&mut self, bytes: &[u8]) {
        self.head.extend_from_slice(bytes);
        let Some((pts, dts, header_len)) = pes_header_complete(&self.head) else {
            return;
        };
        let head = std::mem::take(&mut self.head);
        if header_len == 0 {
            self.drop_partial();
        } else {
            self.pts = pts;
            self.dts = dts;
            if header_len < head.len() {
                self.push(&head[header_len..]);
            } else {
                self.header_remaining = header_len - head.len();
            }
        }
        self.head = head;
        self.head.clear();
    }

    /// Flush remaining data as a PES packet.
    fn flush(&mut self) -> Option<PesPacket> {
        self.head.clear();
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
    pid_index: Vec<i32>, // PID → index into assemblers, -1 = not tracked
    remainder: Vec<u8>,  // leftover bytes from previous feed() call
    /// Absolute source byte offset of the NEXT byte to be fed — the running
    /// base that turns an in-buffer packet offset into a source position.
    /// Advanced by each `feed` by the bytes consumed; `feed` (no base) leaves
    /// it at 0 so non-provenance callers stamp `None`.
    feed_base: u64,
    /// True once a caller has threaded a source base via [`Self::feed_at`]. Until
    /// then no `SourcePos` is stamped (keeps existing callers byte-identical).
    has_base: bool,
    /// Set once sync loss has been logged, so a damaged input warns only once.
    sync_lost_logged: bool,
}

impl TsDemuxer {
    /// Create a new demuxer tracking the given PIDs.
    ///
    /// Allocates a flat lookup table of `i32` slots — one per possible PID up
    /// to `max(8192, max_pid + 1)`. The 8192 floor matches the BD-TS 13-bit
    /// PID space (0..0x1FFF); the variable upper bound covers DVD program
    /// streams, which may use 16-bit stream IDs above 8191. Worst case is
    /// `u16::MAX × 4 bytes ≈ 256 KB`, bounded by the type. Empty `pids`
    /// yields max_pid 0; the floor still produces a valid, unused table.
    pub fn new(pids: &[u16]) -> Self {
        // PID→assembler index is i32 (-1 = untracked); both PID count and index are
        // far below i32::MAX, so `i as i32` can never wrap negative and be misread as untracked.
        let max_pid = pids.iter().copied().max().unwrap_or(0) as usize;
        let table_size = (max_pid + 1).max(8192);
        let mut pid_index = vec![-1i32; table_size];
        let mut assemblers = Vec::with_capacity(pids.len());
        // Per-PID cap = this PID's share of the AGGREGATE ceiling. Without this the
        // caps were per-PID only and never saw the total, so a disc-declared stream
        // list could multiply 64 MiB by its own length.
        let pes_cap = (MAX_PES_BUFFER_TOTAL / pids.len().max(1)).min(MAX_PES_BUFFER);
        for (i, &pid) in pids.iter().enumerate() {
            pid_index[pid as usize] = i as i32;
            assemblers.push(PesAssembler::new(pid, pes_cap));
        }
        Self {
            assemblers,
            pid_index,
            remainder: Vec::new(),
            feed_base: 0,
            has_base: false,
            sync_lost_logged: false,
        }
    }

    /// Feed a chunk of BD transport stream data. Handles non-192-byte-
    /// aligned input by buffering leftover bytes between calls. Returns
    /// completed PES packets.
    ///
    /// 16 MiB ISO batches never divide evenly into 192-byte BD-TS packets, so
    /// every call after the first carries a remainder. Rather than copying
    /// remainder + new input into a combined Vec, this splices exactly one
    /// boundary packet from a stack buffer, then processes the rest of `data`
    /// in place — zero-copy on the bulk path, one 192-byte copy at the boundary.
    pub fn feed(&mut self, data: &[u8]) -> Vec<PesPacket> {
        // Reset any base a prior `feed_at` left behind, so mixing the two entry
        // points is safe and a stale running base can't leak into the next packet.
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
            if data[offset + 4] != SYNC_BYTE {
                // No sync byte: a zero-filled gap or a byte slip. Packets lost in a slip
                // surface as CC gaps on their PIDs.
                offset = self.resync(data, offset);
                continue;
            }
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

    // Next packet start after the unsynced `offset`: the next grid slot if it has a sync byte,
    // else the first grid slot with one or an off-grid point confirmed 192 bytes on (a slip).
    // With neither, the last grid slot, so alignment carries over.
    fn resync(&mut self, data: &[u8], offset: usize) -> usize {
        let step = BD_SOURCE_PACKET_BYTES;
        if data.get(offset + step + 4) == Some(&SYNC_BYTE) {
            return offset + step;
        }
        for p in offset + 1..data.len().saturating_sub(4) {
            if (p - offset).is_multiple_of(step) {
                if data[p + 4] == SYNC_BYTE {
                    return p;
                }
            } else if data[p + 4] == SYNC_BYTE && data.get(p + step + 4) == Some(&SYNC_BYTE) {
                if !self.sync_lost_logged {
                    self.sync_lost_logged = true;
                    tracing::warn!(target: "mux", offset = p, "bd-ts: packet sync slipped; resynced");
                }
                return p;
            }
        }
        offset + (data.len() - offset) / step * step
    }

    // Demux a single 192-byte BD-TS packet (4-byte TP_extra_header + 188-byte
    // TS). Routes payload bytes into the per-PID `PesAssembler`; completed PES
    // packets are pushed onto `completed` so the caller's alloc amortises.
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

        // P3/B1 concealment marker: NULL-TS (0x1FFF) with a discontinuity indicator.
        // Only fires on externally-authored markers now (in-tree writer removed with
        // pure-decrypt passthrough). Lost unit's PID is unknowable, so force pending discontinuity on every assembler.
        if pid == NULL_PID
            && (adaptation == 0x02 || adaptation == 0x03)
            && (ts[4] as usize) > 0
            && (ts[5] & 0x80) != 0
        {
            for a in &mut self.assemblers {
                // A concealed unit may have dropped packets from any open PES, leaving a
                // hole mid-access-unit. Drop it like a mid-PES continuity break and flag
                // pending so the next completed PES resyncs (mirrors the non-PUSI cc_gap path).
                a.drop_partial();
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
        // transport_error_indicator: the packet is damaged, so its payload and CC are not
        // trusted. Drop the open PES like a continuity break; the next PUSI resyncs.
        if ts[1] & 0x80 != 0 {
            self.assemblers[idx as usize].drop_partial();
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

        // Continuity check: the 4-bit CC increments per payload packet, so a gap means
        // dropped packets. On a discontinuous non-PUSI continuation the partial PES has
        // a hole, so splicing would corrupt the ES — drop it and resync on the next PUSI.
        let cc = ts[3] & 0x0f;
        // adaptation == 0x02 (AF only) already returned above, so only 0x03
        // (AF + payload) can carry an adaptation field here.
        let discontinuity_flag = adaptation == 0x03 && ts[4] > 0 && (ts[5] & 0x80) != 0;
        // A duplicate repeats the previous packet (same CC and payload; the adaptation field,
        // e.g. PCR, may differ): nothing new. A same-CC packet with other data is a gap.
        let same_cc = asm.last_cc == Some(cc) && !discontinuity_flag;
        if same_cc && asm.last_pusi == pusi && asm.last_payload == payload {
            return;
        }
        let cc_gap = same_cc || cc_is_gap(asm.last_cc, cc);
        asm.last_cc = Some(cc);
        asm.last_pusi = pusi;
        asm.last_payload.clear();
        asm.last_payload.extend_from_slice(payload);
        // A gap means packets for this PID were lost. Sticky flag rides to the first
        // post-gap PES so the codec consumer drops forward to the next keyframe (B1).
        let gap = discontinuity_flag || cc_gap;

        if pusi {
            // Flush the previous PES first — a gap on this PUSI packet belongs to the PES
            // starting now (as does a lost PES whose header never completed). Set
            // `pending_discontinuity` after start(), else it stamps the pre-gap frame.
            let lost_head = !asm.head.is_empty();
            if let Some(prev) = asm.start(None, None, source) {
                completed.push(prev);
            }
            if gap || lost_head {
                asm.pending_discontinuity = true;
            }
            asm.head.clear();
            asm.header_remaining = 0;
            asm.take_head(payload);
        } else {
            // Non-PUSI continuation.
            if gap {
                // Mid-PES hole: splicing this payload would corrupt the ES. Flag
                // pending (consumed at the next completed PES) and drop the partial.
                asm.pending_discontinuity = true;
                if asm.active {
                    tracing::trace!(
                        target: "mux",
                        pid = asm.pid,
                        "TS continuity break on non-PUSI continuation; dropping partial PES",
                    );
                    asm.drop_partial();
                    return;
                }
            }
            if !asm.head.is_empty() {
                // The PES header is still arriving.
                asm.take_head(payload);
            } else if asm.header_remaining > 0 {
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

// Stream ids whose PES has no header extension (ISO 13818-1 Table 2-22: program_stream_map,
// padding, private_stream_2, ECM, EMM, DSMCC, H.222.1 type E, program_stream_directory).
fn no_pes_extension(stream_id: u8) -> bool {
    matches!(
        stream_id,
        0xBC | 0xBE | 0xBF | 0xF0 | 0xF1 | 0xF2 | 0xF8 | 0xFF
    )
}

// `parse_pes_header` of `data` once it holds the fixed header and any flagged PTS/DTS;
// `None` while more bytes are needed (and what arrived still matches a start code).
fn pes_header_complete(data: &[u8]) -> Option<(Option<i64>, Option<i64>, usize)> {
    let need = match data {
        [_, _, _, id, _, _, _, flags, hdl, ..] if !no_pes_extension(*id) => {
            let (f, hdl) = (flags >> 6, *hdl as usize);
            9 + if f >= 2 && hdl >= 5 { 5 } else { 0 } + if f == 3 && hdl >= 10 { 5 } else { 0 }
        }
        _ => 9,
    };
    let prefix = data.len().min(3);
    if data.len() < need && data[..prefix] == [0, 0, 1][..prefix] {
        return None;
    }
    Some(parse_pes_header(data))
}

// Parse a PES header, extracting PTS/DTS. Returns `(pts, dts, header_len)` where
// `header_len` is the FULL uncapped header length (9 + data_length, or 6 without
// the extension; 0 = not a valid PES start) — caller skips it, carrying remainder.
fn parse_pes_header(data: &[u8]) -> (Option<i64>, Option<i64>, usize) {
    // PES packet: 00 00 01 [stream_id] [length:2] [flags...]
    if data.len() < 9 || data[0] != 0x00 || data[1] != 0x00 || data[2] != 0x01 {
        return (None, None, 0);
    }

    let stream_id = data[3];

    if no_pes_extension(stream_id) {
        return (None, None, 6);
    }

    // Standard PES header: [6] = flags1, [7] = flags2, [8] = header_data_length.
    // The `data.len() < 9` precondition was already checked at the top of
    // this function and nothing shrinks `data` since, so no re-check here.
    let pts_dts_flags = (data[7] >> 6) & 0x03;
    let header_data_len = data[8] as usize;
    // Full, uncapped header length. `pes_header_complete` gathers the first 19 bytes (PTS/DTS)
    // before parsing; only the *skip* length may extend into later packets.
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

// Canonical continuity-counter gap test (ISO/IEC 13818-1 S2.4.3.3), shared by the PES assembler
// and the PSI section reassembler so they can't drift; a legal duplicate CC repeat is not a
// gap.
fn cc_is_gap(last_cc: Option<u8>, cc: u8) -> bool {
    match last_cc {
        Some(prev) => cc != ((prev + 1) & 0x0f) && cc != prev,
        None => false,
    }
}

// Stream scanning (PAT/PMT → stream list): whether `offset` is a credible
// BD-TS packet boundary in the PSI scanner. Requires the sync byte at
// data[offset+4] AND a sync byte 192 bytes on (rejects a stray 0x47 inside).
fn is_resync_point(data: &[u8], offset: usize) -> bool {
    if data.get(offset + 4) != Some(&SYNC_BYTE) {
        return false;
    }
    match data.get(offset + BD_SOURCE_PACKET_BYTES + 4) {
        Some(&b) => b == SYNC_BYTE,
        None => true, // last packet in the buffer — no follower to corroborate
    }
}

// Byte offset of the PSI payload (pointer_field) for a BD-TS packet at `pkt`.
// `None` when the packet carries no payload (AFC 0b10 = AF only, or reserved
// 0b00) or the adaptation field runs past the packet. `pkt` >= BD_SOURCE_PACKET_BYTES.
fn ts_payload_base(pkt: &[u8]) -> Option<usize> {
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

// The audio sync at the head of a PES's ES: ADTS (13818-7 layer '00') or MPEG audio Layer 1-3.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum AudioSync {
    Adts,
    Layer(u8),
}

// Per PID, the stream_id and audio sync opening its first PES in `data` (`None` when that
// PES has no PES/sync header), found in one walk over `data`.
fn first_pes_heads(data: &[u8]) -> std::collections::HashMap<u16, Option<(u8, Option<AudioSync>)>> {
    let mut heads = std::collections::HashMap::new();
    let mut offset = 0;
    while offset + BD_SOURCE_PACKET_BYTES <= data.len() {
        if !is_resync_point(data, offset) {
            offset += 1;
            continue;
        }
        let pkt = &data[offset..offset + BD_SOURCE_PACKET_BYTES];
        let pkt_pid = (((pkt[5] & 0x1F) as u16) << 8) | pkt[6] as u16;
        if pkt[5] & 0x40 != 0 {
            heads.entry(pkt_pid).or_insert_with(|| first_pes_head(pkt));
        }
        offset += BD_SOURCE_PACKET_BYTES;
    }
    heads
}

// stream_id and audio sync of the PES that starts in `pkt`.
fn first_pes_head(pkt: &[u8]) -> Option<(u8, Option<AudioSync>)> {
    let pes = &pkt[ts_payload_base(pkt)?..];
    if pes.get(..3)? != [0, 0, 1] {
        return None;
    }
    let id = *pes.get(3)?;
    let es = pes.get(9 + *pes.get(8)? as usize..);
    let sync = es.and_then(|es| {
        let (b0, b1) = (*es.first()?, *es.get(1)?);
        if b0 != 0xFF || b1 & 0xE0 != 0xE0 {
            return None;
        }
        match (b1 >> 1) & 0x03 {
            0 if b1 & 0xF0 == 0xF0 => Some(AudioSync::Adts),
            0 => None,
            l => Some(AudioSync::Layer(4 - l)),
        }
    });
    Some((id, sync))
}

// Reassemble a single PSI section (PAT/PMT) for `target_pid`/`table_id` across TS-packet
// boundaries. Returns the section bytes (from table_id) or None.
fn collect_psi_section(data: &[u8], target_pid: u16, table_id: u8) -> Option<Vec<u8>> {
    let mut offset = 0;
    // First complete copy, used only if no copy passes `psi_section_ok`.
    let mut fallback = None;
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
            let Some(payload_off) = ts_payload_base(&data[offset..offset + BD_SOURCE_PACKET_BYTES])
            else {
                offset += BD_SOURCE_PACKET_BYTES;
                continue;
            };
            let payload = &data[offset + payload_off..offset + BD_SOURCE_PACKET_BYTES];
            // pointer_field is the first payload byte; bound the section start to
            // within this packet's payload — a pointer into the next packet is malformed.
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
                if psi_section_ok(&section) {
                    return Some(section);
                }
                fallback.get_or_insert(section);
                offset += BD_SOURCE_PACKET_BYTES;
                continue;
            }
            // Need continuation packets: same PID, no PUSI. Use the canonical `cc_is_gap`
            // (§2.4.3.3) shared with `process_packet`, not a local test — a prior local
            // `cc != expected` rejected legal duplicates and miscounted AF-only packets as gaps.
            let mut last_cc = Some(data[offset + 7] & 0x0F);
            let mut scan = offset + BD_SOURCE_PACKET_BYTES;
            let mut desync = false;
            while scan + BD_SOURCE_PACKET_BYTES <= data.len() && section.len() < total {
                // Require a corroborated resync point before trusting the header — a
                // stray 0x47 in corrupt payload could otherwise misread the CC.
                if !is_resync_point(data, scan) {
                    scan += 1;
                    continue;
                }
                let cpid = (((data[scan + 5] & 0x1F) as u16) << 8) | data[scan + 6] as u16;
                let cpusi = data[scan + 5] & 0x40 != 0;
                if cpid == target_pid && !cpusi {
                    // `None` = no payload (AF-only or malformed AF): §2.4.3.3 doesn't
                    // increment the CC for those, so they skip the continuity check.
                    let Some(cbase) = ts_payload_base(&data[scan..scan + BD_SOURCE_PACKET_BYTES])
                    else {
                        scan += BD_SOURCE_PACKET_BYTES;
                        continue;
                    };
                    let cc = data[scan + 7] & 0x0F;
                    if cc_is_gap(last_cc, cc) {
                        desync = true;
                        break;
                    }
                    // A repeated CC is the spec's duplicate packet: identical
                    // payload, already collected. Skip it — appending it again
                    // would corrupt the section it is meant to protect.
                    let duplicate = last_cc == Some(cc);
                    last_cc = Some(cc);
                    if !duplicate {
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
                if psi_section_ok(&section) {
                    return Some(section);
                }
                fallback.get_or_insert(section);
                offset += BD_SOURCE_PACKET_BYTES;
                continue;
            }
            // Incomplete section (truncated input) — stop looking.
            return fallback;
        }
        offset += BD_SOURCE_PACKET_BYTES;
    }
    fallback
}

// A long-form PSI section that is current (current_next_indicator 1) and passes its
// CRC_32 (H.222.0 Annex A: over the whole section including the CRC, the register ends at 0).
fn psi_section_ok(section: &[u8]) -> bool {
    section.len() >= 12 && section[5] & 0x01 == 1 && super::mpg::pack::crc32(section) == 0
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
        // Clamp prog_info_len so it can't push `pos` past `end`: a crafted value
        // larger than the remaining section would skip all ES entries or mis-index.
        let prog_info_len =
            ((((pmt[10] & 0x0F) as usize) << 8) | pmt[11] as usize).min(end.saturating_sub(12));
        let mut pos = 12 + prog_info_len;
        let mut heads = None;

        while pos + 5 <= end {
            let stream_type = pmt[pos];
            let es_pid = (((pmt[pos + 1] & 0x1F) as u16) << 8) | pmt[pos + 2] as u16;
            let es_info_len = (((pmt[pos + 3] & 0x0F) as usize) << 8) | pmt[pos + 4] as usize;

            // PMTs may also carry ISO/IEC 13818-1 audio stream types that are
            // absent from Blu-ray's STN table. Keep that distinction local to TS.
            let mut head = || {
                heads
                    .get_or_insert_with(|| first_pes_heads(data))
                    .get(&es_pid)
                    .copied()
                    .flatten()
            };
            let codec = match stream_type {
                // MPEG-1/2 audio covers Layers I-III; only the ES says which.
                0x03 | 0x04 if head().and_then(|h| h.1) == Some(AudioSync::Layer(3)) => Codec::Mp3,
                0x03 | 0x04 => Codec::Mp2,
                // PES private data (as other m2ts muxers write it): an MPEG-audio stream_id plus the ES
                // sync names AAC/MP2/MP3 (13818-1 Table 2-22); anything else stays unknown.
                0x06 => match head() {
                    Some((0xC0..=0xDF, Some(AudioSync::Adts))) => Codec::Aac,
                    Some((0xC0..=0xDF, Some(AudioSync::Layer(3)))) => Codec::Mp3,
                    Some((0xC0..=0xDF, Some(AudioSync::Layer(_)))) => Codec::Mp2,
                    _ => Codec::Unknown(stream_type),
                },
                0x0f => Codec::Aac,
                0x87 => Codec::Ac3Plus,
                _ => Codec::from_coding_type(stream_type),
            };
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
#[path = "ts_tests.rs"]
mod tests;
