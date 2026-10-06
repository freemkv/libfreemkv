//! MPEG-2 Program Stream (PS) demuxer.
//!
//! DVDs use MPEG-2 Program Stream, which has:
//! - Pack headers (00 00 01 BA) with SCR timestamps, system headers (00 00 01 BB)
//! - PES packets (00 00 01 `[stream_id]`) with variable length
//! - Program end code (00 00 01 B9)
//!
//! Stream IDs: 0xE0-0xEF video (usually 0xE0), 0xC0-0xDF MPEG audio, 0xBD
//! private stream 1 (AC3, DTS, LPCM, subtitles via sub-stream ID)

use super::codec::startcode::find_start_code;

/// Pack header start code suffix.
const PACK_HEADER_ID: u8 = 0xBA;

/// System header start code suffix.
const SYSTEM_HEADER_ID: u8 = crate::consts::pes_stream_id::SYSTEM_HEADER;

/// Program end start code suffix.
const PROGRAM_END_ID: u8 = 0xB9;

/// program_stream_map start code suffix (MS-27 `1011 1100`).
const PROGRAM_STREAM_MAP_ID: u8 = 0xBC;

/// Private stream 1 (AC3, DTS, LPCM, subtitles).
const PRIVATE_STREAM_1: u8 = crate::consts::pes_stream_id::PRIVATE_STREAM_1;
/// Private stream 2 (0xBF) — DVD navigation (PCI/DSI). Carries no muxable
/// elementary stream; expected to be dropped on every disc.
const PRIVATE_STREAM_2: u8 = crate::consts::pes_stream_id::PRIVATE_STREAM_2;
// Extended stream id (0xFD) — H.222.0 escape: real id is the `stream_id_extension` in the PES
// extension. HD-DVD `.evo` VC-1/HD audio.
const EXTENDED_STREAM_ID: u8 = 0xFD;

// Hard cap on the demuxer's reassembly buffer, so a corrupt unbounded PES can't drive unbounded
// allocation.
const MAX_PS_BUFFER: usize = 4 * 1024 * 1024;

/// A demuxed PES packet from the Program Stream.
#[derive(Debug, Clone)]
pub struct PsPacket {
    /// PES stream ID (0xE0 for video, 0xC0 for audio, 0xBD for private, etc.).
    pub stream_id: u8,
    /// Sub-stream ID for private stream 1 (AC3: 0x80-0x87, DTS: 0x88-0x8F,
    /// LPCM: 0xA0-0xA7, E-AC-3: 0xC0-0xCF, subtitles: 0x20-0x3F); for extended stream
    /// id 0xFD, the PES `stream_id_extension`.
    pub sub_stream_id: Option<u8>,
    /// Presentation timestamp in 90kHz ticks.
    pub pts: Option<u64>,
    /// Decode timestamp in 90kHz ticks.
    pub dts: Option<u64>,
    /// Elementary stream payload data.
    pub data: Vec<u8>,
    /// Source position of this PES's start code, stamped at the demux seam
    /// from the producer's known stream offset. `None` when the demuxer was fed
    /// without a base offset.
    pub source: Option<crate::pes::SourcePos>,
}

/// Canonical DVD video PID. DVD-Video carries a single MPEG-2 video
/// elementary stream; both the scanner and the muxer use this PID.
pub const DVD_VIDEO_PID: u16 = 0xE0;

/// Canonical PID for a `private_stream_1` audio stream identified by its
/// on-wire sub-stream id. Returns `None` for sub-ids outside the AC-3 /
/// DTS / LPCM / HD-DVD E-AC-3 ranges.
///
/// The PID is `0xBD00 | sub_stream_id`, unique per sub-stream id (AC-3 / DTS `0x80..=0x8F`,
/// LPCM `0xA0..=0xA7`, E-AC-3 `0xC0..=0xCF` (G17)) — the single source of truth shared with
/// `Disc::scan_dvd_titles` (`src/disc/dvd.rs`).
pub fn dvd_audio_pid(sub_stream_id: u8) -> Option<u16> {
    match sub_stream_id {
        0x80..=0x8F | 0xA0..=0xA7 | 0xC0..=0xCF => Some(0xBD00 | sub_stream_id as u16),
        _ => None,
    }
}

/// Canonical PID for a DVD MPEG-audio stream, keyed by its PES `stream_id` (`0xC0|n`, DVD's
/// eight audio streams): the stream id itself, disjoint from `0xBD00..` (which already holds
/// HD-DVD E-AC-3 sub-ids `0xC0..`), VobSub `0x20..` and video `0xE0`.
pub fn dvd_mpeg_audio_pid(stream_id: u8) -> Option<u16> {
    match stream_id {
        0xC0..=0xC7 => Some(stream_id as u16),
        _ => None,
    }
}

/// Canonical PID for a DVD MPEG-2 audio extension bit stream (PES `0xD0|n`): the stream id,
/// paired with its base `0xC0|n` (the same `n`). Inference, not spec: US5987417 gives MPEG audio
/// packets "stream id ... 1100 0***b or 1101 0***b"; which one carries the extension is ours.
pub fn dvd_mpeg_audio_extension_pid(stream_id: u8) -> Option<u16> {
    match stream_id {
        0xD0..=0xD7 => Some(stream_id as u16),
        _ => None,
    }
}

/// The base MPEG audio PID (`0xC0|n`) an extension PID (`0xD0|n`) belongs to.
pub fn dvd_mpeg_audio_extension_base(ext_pid: u16) -> Option<u16> {
    let id = u8::try_from(ext_pid).ok()?;
    dvd_mpeg_audio_extension_pid(id).map(|_| ext_pid & !0x0010)
}

/// PS packets dropped for want of a track, per `(stream_id, sub_stream_id)`. A deselected
/// or undeclared DVD stream drops every packet, so each id warns once at first sight and
/// its count is reported at end of stream.
#[derive(Debug, Default)]
pub(crate) struct DroppedPs {
    seen: Vec<((u8, Option<u8>), u64)>,
}

impl DroppedPs {
    /// Count one dropped packet; `pid` is its DVD PID (`None`: no DVD mapping at all).
    pub(crate) fn drop_packet(&mut self, ps: &PsPacket, pid: Option<u16>) {
        let key = (ps.stream_id, ps.sub_stream_id);
        if let Some((_, n)) = self.seen.iter_mut().find(|(k, _)| *k == key) {
            *n += 1;
            return;
        }
        self.seen.push((key, 1));
        match pid {
            Some(pid) => tracing::warn!(
                target: "mux",
                "dropping PS packets for unmapped PID {pid:#06x} (stream_id={:#04x}, sub_stream_id={:?}); counted until EOF",
                ps.stream_id,
                ps.sub_stream_id,
            ),
            None => tracing::warn!(
                target: "mux",
                "dropping unmappable PS packets (stream_id={:#04x}, sub_stream_id={:?}); counted until EOF",
                ps.stream_id,
                ps.sub_stream_id,
            ),
        }
    }

    /// The per-id drop counts, once at end of stream.
    pub(crate) fn report(&self) {
        for ((stream_id, sub_stream_id), n) in &self.seen {
            tracing::debug!(
                target: "mux",
                "dropped PS stream_id={stream_id:#04x} sub_stream_id={sub_stream_id:?} packets={n}",
            );
        }
    }
}

/// Reads a DVD title the way its navigation packs say to play it.
///
/// A DVD cell is a sector range, and in an interleaved block that range also holds the units of
/// another program (a second language version, say). Every VOBU opens with a navigation pack
/// naming its VOB and cell; a cell's first sector is its own first VOBU, so the first one read
/// in each extent names the cell, and any later VOBU naming another belongs to the other program.
///
/// The same pack gives the VOBU's start and end time. Each VOB runs its own clock, so where a
/// kept VOBU does not start where the last one ended, every later packet is shifted to close
/// the jump: the title plays as one timeline, on every track at once.
#[derive(Debug, Default)]
pub(crate) struct VobuNav {
    // Feed byte offset at which each extent ends, in read order.
    ends: Vec<u64>,
    extent: usize,
    cell: Option<(u16, u8)>,
    keep: bool,
    // The current VOBU's start/end time from its PCI packet, until its DSI decides it is kept.
    pci: Option<(i64, i64)>,
    // Where the last kept VOBU ended, in shifted time, and the shift in force (90 kHz).
    end: Option<i64>,
    shift: i64,
    pub(crate) dropped_vobus: u64,
    pub(crate) joins: u64,
}

// A jump smaller than this (1 ms) is rounding, not a new clock.
const JOIN_SLACK_TICKS: i64 = 90;

impl VobuNav {
    /// Navigation for a title read as `extents`, back to back.
    pub(crate) fn new(extents: &[crate::disc::Extent]) -> Self {
        let mut end = 0u64;
        let ends = extents
            .iter()
            .map(|e| {
                end += e.sector_count as u64 * 2048;
                end
            })
            .collect();
        VobuNav {
            ends,
            keep: true,
            ..Default::default()
        }
    }

    /// Whether `ps` belongs to the cell being read; a kept packet is moved onto the title's
    /// timeline. Packets without a source position pass untouched.
    pub(crate) fn admit(&mut self, ps: &mut PsPacket) -> bool {
        let Some(at) = ps.source.map(|s| s.byte) else {
            return true;
        };
        while self.ends.get(self.extent).is_some_and(|&end| at >= end) {
            self.extent += 1;
            self.cell = None;
            self.keep = true;
        }
        if ps.is_nav() {
            match ps.data.first() {
                Some(0x00) => self.pci = pci_times(&ps.data),
                Some(0x01) => self.on_dsi(&ps.data),
                _ => {}
            }
            return self.keep;
        }
        if self.keep && self.shift != 0 {
            let moved = |t: u64| (t as i64).saturating_add(self.shift).max(0) as u64;
            ps.pts = ps.pts.map(moved);
            ps.dts = ps.dts.map(moved);
        }
        self.keep
    }

    fn on_dsi(&mut self, dsi: &[u8]) {
        let Some(id) = dsi_cell(dsi) else {
            return;
        };
        let cell = *self.cell.get_or_insert(id);
        self.keep = id == cell;
        if !self.keep {
            self.dropped_vobus += 1;
            return;
        }
        let Some((start, end)) = self.pci.take() else {
            return;
        };
        if let Some(prev) = self.end
            && (start + self.shift - prev).abs() > JOIN_SLACK_TICKS
        {
            self.shift = prev - start;
            self.joins += 1;
        }
        self.end = Some(end + self.shift);
    }
}

// The VOBU start and end presentation times a PCI packet gives (90 kHz).
fn pci_times(payload: &[u8]) -> Option<(i64, i64)> {
    // Substream 0x00, then PCI_GI: LBN (4), category (2), reserved (2), UOP control (4),
    // start time (4), end time (4).
    let word = |o: usize| Some(u32::from_be_bytes(payload.get(o..o + 4)?.try_into().ok()?) as i64);
    let (start, end) = (word(13)?, word(17)?);
    (end > start).then_some((start, end))
}

// The (VOB id, cell id) a DSI packet's general information names.
fn dsi_cell(payload: &[u8]) -> Option<(u16, u8)> {
    // Substream 0x01, then DSI_GI: SCR, LBN, VOBU end and three reference ends (4 bytes each),
    // VOB id (2), a reserved byte and the cell id.
    if payload.first() != Some(&0x01) || payload.len() < 29 {
        return None;
    }
    Some((u16::from_be_bytes([payload[25], payload[26]]), payload[28]))
}

/// Reports, once at end of stream, MPEG-2 audio extension packets (`0xD0|n`, index `n`) that
/// had no declared extension track (the IFO did not say coding mode 3) and were left out.
pub(crate) fn warn_undeclared_extensions(packets: &[u64; 8]) {
    for (n, &count) in packets.iter().enumerate().filter(|(_, c)| **c > 0) {
        tracing::warn!(
            target: "mux",
            "tag=mp2.extension stream_id={:#04x} packets={count}: MPEG-2 audio extension \
             packets with no declared extension track (IFO coding mode 3) were left out; an \
             ISO or raw copy keeps them",
            0xD0 | n,
        );
    }
}

/// The DVD MPEG-2 multichannel extension tracks of a title that a sink cannot store. IFO
/// coding mode 3 only declares them; the loss is reported (warned once, then listed by
/// `undelivered_streams`) only for a track whose `0xD0|n` packets actually arrived.
#[derive(Debug, Default)]
pub(crate) struct UnstoredExtensions {
    /// (`title.streams` index, PID, frames seen).
    tracks: Vec<(usize, u16, bool)>,
    container: &'static str,
}

impl UnstoredExtensions {
    /// Every extension track of `title`, for a sink writing `container`.
    pub(crate) fn new(title: &crate::disc::DiscTitle, container: &'static str) -> Self {
        let tracks = (title.streams.iter().enumerate())
            .filter_map(|(i, s)| match s {
                crate::disc::Stream::Audio(a) if a.is_mp2_extension() => Some((i, a.pid, false)),
                _ => None,
            })
            .collect();
        Self { tracks, container }
    }

    /// Whether `track` is an extension track (never written).
    pub(crate) fn contains(&self, track: usize) -> bool {
        self.tracks.iter().any(|&(i, _, _)| i == track)
    }

    /// Whether the title has no extension track at all.
    pub(crate) fn is_empty(&self) -> bool {
        self.tracks.is_empty()
    }

    /// Notes a frame for `track`; `true` when it is an extension track the sink must drop.
    /// The first frame of each one warns.
    pub(crate) fn drop_frame(&mut self, track: usize) -> bool {
        let Some(t) = self.tracks.iter_mut().find(|t| t.0 == track) else {
            return false;
        };
        if !t.2 {
            t.2 = true;
            tracing::warn!(
                target: "mux",
                track,
                "MPEG-2 multichannel extension {:#04x} has no {} mapping; left out (the stereo \
                 base is kept; an ISO copy keeps the surround)",
                t.1,
                self.container,
            );
        }
        true
    }

    /// Add extension track `track` (PID `pid`) as one this sink does not write.
    pub(crate) fn add(&mut self, track: usize, pid: u16) {
        if !self.contains(track) {
            self.tracks.push((track, pid, false));
        }
    }

    /// Keep only the extension tracks `unstored` says this sink does not write.
    pub(crate) fn retain(&mut self, unstored: impl Fn(usize) -> bool) {
        self.tracks.retain(|t| unstored(t.0));
    }

    /// Extension tracks whose packets arrived and were left out.
    pub(crate) fn seen(&self) -> Vec<usize> {
        (self.tracks.iter()).filter(|t| t.2).map(|t| t.0).collect()
    }
}

/// Canonical PID for a VobSub subtitle stream identified by its on-wire
/// sub-stream id (`0x20..=0x3F`). The PID is the sub-id itself (identity),
/// which never overlaps the `0xBD..` audio PID space.
pub fn dvd_subtitle_pid(sub_stream_id: u8) -> Option<u16> {
    match sub_stream_id {
        0x20..=0x3F => Some(sub_stream_id as u16),
        _ => None,
    }
}

/// Canonical PID for an HD-DVD extended-stream-id (`0xFD`) stream, keyed by its
/// `stream_id_extension`: `0xFD00 | ext`. Disjoint from the DVD video (`0xE0`) and
/// `private_stream_1` (`0xBD00..`) PID spaces, so several elementary streams
/// multiplexed on `0xFD` (VC-1 video, MLP/TrueHD audio) never collide. The
/// scanner's head probe and `PsPacket::dvd_pid` derive the same PID from the same
/// `stream_id_extension`, so demux output routes through the title's
/// `pid_to_track`.
pub fn hddvd_extended_pid(stream_id_extension: u8) -> u16 {
    0xFD00 | stream_id_extension as u16
}

impl PsPacket {
    /// Map this packet to the canonical DVD PID assigned by
    /// `Disc::scan_dvd_titles` (`src/disc/dvd.rs`), so demux output can be
    /// looked up in the title's `pid_to_track` map.
    ///
    /// Routes by the REAL on-wire `(stream_id, sub_stream_id)` via the
    /// shared [`dvd_audio_pid`] / [`dvd_mpeg_audio_pid`] / [`dvd_subtitle_pid`] tables the scanner
    /// also uses.
    ///
    /// Returns `None` for combinations the DVD title scanner does not assign a PID to; the
    /// caller should WARN-and-drop.
    pub fn dvd_pid(&self) -> Option<u16> {
        match self.stream_id {
            crate::consts::pes_stream_id::VIDEO..=0xEF => Some(DVD_VIDEO_PID),
            0xC0..=0xDF => dvd_mpeg_audio_pid(self.stream_id)
                .or_else(|| dvd_mpeg_audio_extension_pid(self.stream_id)),
            PRIVATE_STREAM_1 => {
                let sub = self.sub_stream_id?;
                dvd_audio_pid(sub).or_else(|| dvd_subtitle_pid(sub))
            }
            // HD-DVD extended-stream-id (0xFD): route by stream_id_extension (in
            // `sub_stream_id`) to a distinct `0xFD00 | ext` PID so several elementary
            // streams on 0xFD stay apart. Routing key only — scanner's probe picks codec.
            EXTENDED_STREAM_ID => self.sub_stream_id.map(hddvd_extended_pid),
            _ => None,
        }
    }

    /// Whether this is a DVD navigation packet (private_stream_2, 0xBF —
    /// PCI/DSI). These carry no muxable elementary stream and are EXPECTED to
    /// be dropped on every DVD, so a per-packet WARN is noise: the mux loops
    /// count them and emit one finalize summary instead. A `dvd_pid()` of
    /// `None` for any OTHER stream_id is unexpected (a possibly-dropped real
    /// stream) and stays an individual WARN.
    pub fn is_nav(&self) -> bool {
        self.stream_id == PRIVATE_STREAM_2
    }
}

/// MPEG-2 Program Stream demuxer.
///
/// Accepts raw PS bytes via `feed()` and produces demuxed PES packets.
/// Handles non-aligned input by buffering leftover bytes between calls.
pub struct PsDemuxer {
    buffer: Vec<u8>,
    /// Absolute source byte offset of `buffer[0]` — the running base that turns
    /// an in-buffer unit position into a [`crate::pes::SourcePos`]. Advanced as
    /// the buffer drains. `has_base` gates stamping so non-provenance callers
    /// stay byte-identical.
    buffer_base: u64,
    has_base: bool,
    /// Boundary-scan cursor for an unbounded (length-0) PES still waiting
    /// for its terminating PS-layer unit: `(buffer offset of the PES start
    /// code, buffer offset up to which the search has already proved there
    /// is no boundary)`. Buffer-relative; rebased when the buffer drains,
    /// cleared when the PES is emitted.
    ///
    /// Without it, every `feed` would re-search the whole accumulated payload from the PES
    /// header.
    pending_scan: Option<(usize, usize)>,
    /// Test-only: total bytes examined by `find_ps_boundary`. Pins the cursor
    /// above — the property it exists for is a WORK bound, which no
    /// packet-level assertion can observe.
    #[cfg(test)]
    boundary_bytes_scanned: u64,
    /// The current pack is an ISO/IEC 11172-1 (MPEG-1) pack: its packets use the MPEG-1
    /// header form (mpg-output-design v5 §4 step 1, chosen per pack, J6).
    mpeg1: bool,
    /// The first program stream map (0xBC) seen, for the `mpg://` stream scan.
    psm: Option<Vec<u8>>,
}

impl Default for PsDemuxer {
    fn default() -> Self {
        Self::new()
    }
}

impl PsDemuxer {
    /// Create a new Program Stream demuxer.
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(64 * 1024),
            buffer_base: 0,
            has_base: false,
            pending_scan: None,
            #[cfg(test)]
            boundary_bytes_scanned: 0,
            mpeg1: false,
            psm: None,
        }
    }

    /// The first program stream map seen, start code included.
    pub(crate) fn psm(&self) -> Option<&[u8]> {
        self.psm.as_deref()
    }

    /// Feed raw MPEG-2 PS bytes, returning any completely parsed PES packets.
    pub fn feed(&mut self, data: &[u8]) -> Vec<PsPacket> {
        self.buffer.extend_from_slice(data);
        self.extract_packets(false)
    }

    /// Like [`feed`](Self::feed) but records the absolute source byte offset of
    /// `data[0]`, so every PES this call completes is stamped with a
    /// [`crate::pes::SourcePos`]. The provenance-stamping entry point; the
    /// highway calls this with each batch's known source offset.
    pub fn feed_at(&mut self, base_offset: u64, data: &[u8]) -> Vec<PsPacket> {
        if !self.has_base {
            // First base seen: the offset of data[0] is base_offset, and data[0]
            // lands at buffer[buffer.len()], so buffer[0] is base_offset minus
            // the bytes already buffered.
            self.buffer_base = base_offset.saturating_sub(self.buffer.len() as u64);
            self.has_base = true;
        }
        self.buffer.extend_from_slice(data);
        self.extract_packets(false)
    }

    /// Flush remaining buffered data, returning any final PES packets.
    pub fn flush(&mut self) -> Vec<PsPacket> {
        // At EOF, an unbounded (length 0) PES with no trailing start code is a
        // complete-but-unterminated final packet — emit it rather than dropping the last
        // frame's tail. Length-bounded PES short of its declared size is still discarded.
        let packets = self.extract_packets(true);
        self.buffer.clear();
        // The buffer the cursor indexes into is gone.
        self.pending_scan = None;
        packets
    }

    // Scan the buffer for complete start-code-delimited units and parse them.
    // When `flushing`, a trailing unbounded PES with no following start code
    // is emitted using the rest of the buffer as its payload (EOF terminates it).
    fn extract_packets(&mut self, flushing: bool) -> Vec<PsPacket> {
        let mut packets = Vec::with_capacity(4);
        let mut pos = 0;

        while let Some(sc) = find_start_code(&self.buffer, pos) {
            if sc + 3 >= self.buffer.len() {
                // Not enough bytes to read the start code ID.
                break;
            }

            let code = self.buffer[sc + 3];

            match code {
                PROGRAM_END_ID => {
                    // 00 00 01 B9 — 4 bytes, no payload.
                    pos = sc + 4;
                }
                PACK_HEADER_ID => {
                    if sc + 5 > self.buffer.len() {
                        break; // wait for the marker byte
                    }
                    // The byte after 0x000001BA picks the layout per pack (design §4 step 1):
                    // '01' is an H.222.0 pack (MS-2), '0010' an 11172-1 pack of 12 bytes with
                    // no stuffing field; anything else is a lost sync, resynced below.
                    let marker = self.buffer[sc + 4];
                    let pack_len = if marker >> 6 == 0b01 {
                        if sc + 14 > self.buffer.len() {
                            break; // wait for more data
                        }
                        14 + (self.buffer[sc + 13] & 0x07) as usize
                    } else if marker >> 4 == 0b0010 {
                        12
                    } else {
                        pos = sc + 4;
                        continue;
                    };
                    if sc + pack_len > self.buffer.len() {
                        break;
                    }
                    self.mpeg1 = pack_len == 12;
                    pos = sc + pack_len;
                }
                PROGRAM_STREAM_MAP_ID => {
                    // The map is length-prefixed (MS-8); kept once, never parsed as packets.
                    if sc + 6 > self.buffer.len() {
                        break;
                    }
                    let len = ((self.buffer[sc + 4] as usize) << 8) | self.buffer[sc + 5] as usize;
                    if sc + 6 + len > self.buffer.len() {
                        break;
                    }
                    if self.psm.is_none() {
                        self.psm = Some(self.buffer[sc..sc + 6 + len].to_vec());
                    }
                    pos = sc + 6 + len;
                }
                SYSTEM_HEADER_ID => {
                    // System header: 00 00 01 BB [length:2] ...
                    if sc + 6 > self.buffer.len() {
                        break;
                    }
                    let header_len =
                        ((self.buffer[sc + 4] as usize) << 8) | self.buffer[sc + 5] as usize;
                    let total = 6 + header_len;
                    if sc + total > self.buffer.len() {
                        break;
                    }
                    pos = sc + total;
                }
                id if is_pes_stream_id(id) => {
                    // PES packet: 00 00 01 [stream_id] [length:2] ...
                    if sc + 6 > self.buffer.len() {
                        break;
                    }
                    let pes_packet_len =
                        ((self.buffer[sc + 4] as usize) << 8) | self.buffer[sc + 5] as usize;

                    // Length 0 means unbounded (video): the packet runs to the next PS-LAYER
                    // boundary (pack / system header / program end / next PES), NOT the next
                    // raw start code — the video ES payload is full of 00 00 01 xx codes.
                    let end = if pes_packet_len == 0 && !self.mpeg1 {
                        // Resume where the last call stopped searching for
                        // THIS PES's terminating unit; anything before that is
                        // already proved boundary-free.
                        let from = match self.pending_scan {
                            Some((pes_at, searched_to)) if pes_at == sc => searched_to,
                            _ => sc + 4,
                        };
                        let (found, searched_to) = find_ps_boundary(&self.buffer, from);
                        #[cfg(test)]
                        {
                            self.boundary_bytes_scanned += searched_to.saturating_sub(from) as u64;
                        }
                        match found {
                            Some(next) => {
                                self.pending_scan = None;
                                next
                            }
                            // At EOF the rest of the buffer is this PES's
                            // payload — emit it.
                            None if flushing => {
                                self.pending_scan = None;
                                self.buffer.len()
                            }
                            None => {
                                // No boundary buffered yet — normally wait for more data, but
                                // a corrupt unbounded PES could stream endless non-boundary
                                // bytes; cap the buffer to stop unbounded alloc, then flush.
                                if self.buffer.len() - sc > MAX_PS_BUFFER {
                                    tracing::warn!(
                                        target: "mux",
                                        stream_id = id,
                                        "ps: unbounded PES passed the buffer cap; emitted truncated"
                                    );
                                    self.pending_scan = None;
                                    self.buffer.len()
                                } else {
                                    self.pending_scan = Some((sc, searched_to));
                                    break; // wait for more data
                                }
                            }
                        }
                    } else {
                        let e = sc + 6 + pes_packet_len;
                        if e > self.buffer.len() {
                            break; // wait for more data
                        }
                        e
                    };

                    let parsed = if self.mpeg1 {
                        parse_mpeg1_packet(&self.buffer[sc..end])
                    } else {
                        parse_pes_packet(&self.buffer[sc..end])
                    };
                    if let Some(mut pkt) = parsed {
                        if self.has_base {
                            pkt.source =
                                Some(crate::pes::SourcePos::at_byte(self.buffer_base + sc as u64));
                        }
                        packets.push(pkt);
                    }
                    pos = end;
                }
                _ => {
                    // Unknown start code — skip past it.
                    pos = sc + 4;
                }
            }
        }

        if pos > 0 {
            self.buffer.drain(..pos);
            // Advance the absolute base past the drained bytes so subsequent
            // units stamp from the correct offset.
            if self.has_base {
                self.buffer_base += pos as u64;
            }
            // The cursor is a BUFFER offset, so it moves with the drain. A pending PES
            // always starts at or after `pos` (the loop broke on it, having consumed
            // everything before it), so neither component can underflow.
            self.pending_scan = self
                .pending_scan
                .map(|(pes_at, searched_to)| (pes_at - pos, searched_to - pos));
        }

        // Trim a start-code-free tail: a buffer holding no `00 00 01` never drains, so
        // zero-filled VOB or AACS ciphertext would grow it to whole-title size. Only a
        // 2-byte `00 00` prefix straddling the feed boundary can start a unit — keep it.
        if self.buffer.len() > START_CODE_PREFIX_KEEP && find_start_code(&self.buffer, 0).is_none()
        {
            let drop = self.buffer.len() - START_CODE_PREFIX_KEEP;
            self.buffer.drain(..drop);
            if self.has_base {
                self.buffer_base += drop as u64;
            }
            // A pending PES implies a start code IS in the buffer, so this
            // branch cannot run while one is open; drop the cursor anyway
            // rather than leave a stale offset behind this drain.
            self.pending_scan = None;
        }

        packets
    }
}

/// Bytes retained when the buffer holds no start code: a `00 00 01` prefix can
/// straddle a feed boundary by at most its first two bytes.
const START_CODE_PREFIX_KEEP: usize = 2;

// Next PS-layer unit boundary at/after `from` — NOT the next raw `00 00 01`, which the video ES
// payload is full of. Returns `(boundary, searched_to)`.
fn find_ps_boundary(data: &[u8], from: usize) -> (Option<usize>, usize) {
    let mut pos = from;
    while let Some(sc) = find_start_code(data, pos) {
        if sc + 3 >= data.len() {
            // A start code whose ID byte has not arrived yet: undecided, so
            // the next scan must look at it again.
            return (None, sc);
        }
        let id = data[sc + 3];
        if id == PACK_HEADER_ID
            || id == SYSTEM_HEADER_ID
            || id == PROGRAM_END_ID
            || is_pes_stream_id(id)
        {
            return (Some(sc), sc);
        }
        pos = sc + 4;
    }
    (None, data.len().saturating_sub(2).max(from))
}

/// Check whether a start code byte is a valid PES stream ID that carries payload.
fn is_pes_stream_id(id: u8) -> bool {
    // Video: 0xE0-0xEF, MPEG audio: 0xC0-0xDF, private stream 1: 0xBD,
    // private stream 2: 0xBF, padding: 0xBE, ECM/EMM etc. — plus the HD-DVD
    // extended-stream-id (0xFD), which carries VC-1 video / HD audio.
    crate::consts::pes_stream_id::PAYLOAD_RANGE.contains(&id) || id == EXTENDED_STREAM_ID
}

// For an extended-stream-id (0xFD) PES, walk the optional PES-header fields to
// the PES extension and read the 7-bit stream_id_extension — the real stream
// id. `data` starts at the start code; fields are bounds-checked against `header_end`.
fn parse_stream_id_extension(data: &[u8], flags2: u8, header_end: usize) -> Option<u8> {
    let get = |p: usize| -> Option<u8> {
        if p < header_end {
            data.get(p).copied()
        } else {
            None
        }
    };
    let mut pos = 9usize;
    let pts_dts = (flags2 >> 6) & 0x03;
    if pts_dts & 0x02 != 0 {
        pos += 5; // PTS
    }
    if pts_dts == 0x03 {
        pos += 5; // DTS
    }
    if flags2 & 0x20 != 0 {
        pos += 6; // ESCR
    }
    if flags2 & 0x10 != 0 {
        pos += 3; // ES_rate
    }
    if flags2 & 0x08 != 0 {
        pos += 1; // DSM_trick_mode
    }
    if flags2 & 0x04 != 0 {
        pos += 1; // additional_copy_info
    }
    if flags2 & 0x02 != 0 {
        pos += 2; // PES_CRC
    }
    if flags2 & 0x01 == 0 {
        return None; // no PES_extension
    }
    let ext_flags = get(pos)?;
    pos += 1;
    if ext_flags & 0x80 != 0 {
        pos += 16; // PES_private_data
    }
    if ext_flags & 0x40 != 0 {
        // pack_header_field: 1-byte length + that many bytes.
        pos += 1 + get(pos)? as usize;
    }
    if ext_flags & 0x20 != 0 {
        pos += 2; // program_packet_sequence_counter
    }
    if ext_flags & 0x10 != 0 {
        pos += 2; // P-STD_buffer
    }
    if ext_flags & 0x01 == 0 {
        return None; // no PES_extension_flag_2
    }
    // PES_extension_field_length (7 bits, marker in the top bit), then the
    // stream_id_extension byte: present when its top bit (the extension flag) is 0.
    let _field_len = get(pos)? & 0x7F;
    pos += 1;
    let b = get(pos)?;
    (b & 0x80 == 0).then_some(b & 0x7F)
}

/// Parse an ISO/IEC 11172-1 packet (design §4 step 1, cited: 11172-1 §2.4.3.3, F4): up to 16
/// `0xFF` stuffing bytes, an optional `'01'` STD buffer field, then `'0010'` + PTS, `'0011'` +
/// PTS + `'0001'` + DTS, or `0000 1111`; the sub-stream handling is the MPEG-2 path's.
fn parse_mpeg1_packet(data: &[u8]) -> Option<PsPacket> {
    if data.len() < 7 || data[..3] != [0, 0, 1] {
        return None;
    }
    let stream_id = data[3];
    if stream_id == crate::consts::pes_stream_id::PADDING_STREAM {
        return None;
    }
    if stream_id == PRIVATE_STREAM_2 {
        return parse_pes_packet(data);
    }
    let mut q = 6;
    while q < data.len() && q < 6 + 16 && data[q] == 0xFF {
        q += 1;
    }
    if data.get(q).is_some_and(|b| b & 0xC0 == 0x40) {
        q += 2;
    }
    let (mut pts, mut dts) = (None, None);
    match data.get(q).map(|b| b >> 4)? {
        0b0010 if data.len() >= q + 5 => {
            pts = parse_pts(&data[q..q + 5]);
            q += 5;
        }
        0b0011 if data.len() >= q + 10 => {
            pts = parse_pts(&data[q..q + 5]);
            dts = parse_pts(&data[q + 5..q + 10]);
            q += 10;
        }
        _ if data[q] == 0x0F => q += 1,
        _ => return None,
    }
    finish_packet(stream_id, pts, dts, data, q)
}

/// Parse a single PES packet from a byte slice that starts at the start code.
fn parse_pes_packet(data: &[u8]) -> Option<PsPacket> {
    // Minimum: 00 00 01 [id] [len:2] = 6 bytes
    if data.len() < 6 {
        return None;
    }
    if data[0] != 0x00 || data[1] != 0x00 || data[2] != 0x01 {
        return None;
    }

    let stream_id = data[3];

    // Padding stream — skip entirely.
    if stream_id == crate::consts::pes_stream_id::PADDING_STREAM {
        return None;
    }

    // Streams without standard PES header extension.
    if stream_id == PRIVATE_STREAM_2 {
        let payload = if data.len() > 6 { &data[6..] } else { &[] };
        return Some(PsPacket {
            stream_id,
            sub_stream_id: None,
            pts: None,
            dts: None,
            data: payload.to_vec(),
            // Stamped by the demuxer (extract_packets) when a source base is
            // threaded; the free function has no absolute offset of its own.
            source: None,
        });
    }

    // Standard PES header: [6]=flags1, [7]=flags2, [8]=header_data_length
    if data.len() < 9 {
        return None;
    }

    let pts_dts_flags = (data[7] >> 6) & 0x03;
    let header_data_len = data[8] as usize;
    let header_end = 9 + header_data_len;

    if header_end > data.len() {
        return None;
    }

    let mut pts = None;
    let mut dts = None;

    // PTS (data[9..14]) and DTS (data[14..19]) live INSIDE the PES header, so gate on
    // header_data_len covering them (>=5 PTS, >=10 PTS+DTS), not total length: a packet
    // setting the flags with a too-short header would read payload as a bogus timestamp.
    if pts_dts_flags >= 2 && header_data_len >= 5 && data.len() >= 14 {
        pts = parse_pts(&data[9..14]);
    }
    if pts_dts_flags == 3 && header_data_len >= 10 && data.len() >= 19 {
        dts = parse_pts(&data[14..19]);
    }

    let flags2 = data[7];
    let mut pkt = finish_packet(stream_id, pts, dts, data, header_end)?;
    if stream_id == EXTENDED_STREAM_ID {
        pkt.sub_stream_id = parse_stream_id_extension(data, flags2, header_end);
    }
    Some(pkt)
}

// The payload after a (MPEG-1 or MPEG-2) packet header ending at `header_end`: the
// private_stream_1 sub-stream header stripped, the ES kept.
fn finish_packet(
    stream_id: u8,
    pts: Option<u64>,
    dts: Option<u64>,
    data: &[u8],
    header_end: usize,
) -> Option<PsPacket> {
    let payload = &data[header_end..];

    // For private stream 1, the first payload byte is the sub-stream ID,
    // followed by a sub-header whose length depends on the sub-stream type.
    let (sub_stream_id, es_data) = if stream_id == EXTENDED_STREAM_ID {
        // HD-DVD extended-stream-id: real stream id is the stream_id_extension inside
        // the PES extension (filled in by the MPEG-2 caller). No leading sub-header byte on
        // the payload (unlike private_stream_1), so the ES is the payload verbatim.
        (None, payload.to_vec())
    } else if stream_id == PRIVATE_STREAM_1 && !payload.is_empty() {
        let sub_id = payload[0];
        let skip = match sub_id {
            0x80..=0x8F => 4, // AC3/DTS: sub_id + frame_count + access_unit_ptr(2)
            // E-AC-3 (G17: 0xC0..=0xCF): a 4-byte sub-header like DVD AC-3, verified on a
            // real HD-DVD EVO; a shorter skip splices sub-header into a frame.
            0xC0..=0xCF => 4,
            // LPCM: sub_id + frames + ptr(2); the 3-byte audio header (quant/rate/channels)
            // is left for `LpcmParser`, which needs it to unpack 20/24-bit samples.
            0xA0..=0xA7 => 4,
            _ => 1,
        };
        let start = skip.min(payload.len());
        (Some(sub_id), payload[start..].to_vec())
    } else {
        (None, payload.to_vec())
    };

    Some(PsPacket {
        stream_id,
        sub_stream_id,
        pts,
        dts,
        data: es_data,
        // Stamped by the demuxer (extract_packets) when a source base is threaded.
        source: None,
    })
}

// Parse a 5-byte PTS/DTS timestamp field (33 bits at 90kHz), layout per ISO/IEC 13818-1 Table
// 2-17.
fn parse_pts(buf: &[u8]) -> Option<u64> {
    debug_assert!(buf.len() >= 5);
    // Validate the three marker bits (bit 0 of bytes 0, 2, 4) per MPEG-2
    // Systems Table 2-17. A timestamp with a cleared marker is malformed —
    // matching ts.rs::parse_timestamp, reject it rather than decode garbage.
    if (buf[0] & 0x01) == 0 || (buf[2] & 0x01) == 0 || (buf[4] & 0x01) == 0 {
        return None;
    }
    let b0 = buf[0] as u64;
    let b1 = buf[1] as u64;
    let b2 = buf[2] as u64;
    let b3 = buf[3] as u64;
    let b4 = buf[4] as u64;

    Some(((b0 >> 1) & 0x07) << 30 | b1 << 22 | (b2 >> 1) << 15 | b3 << 7 | b4 >> 1)
}

#[cfg(test)]
#[path = "ps_tests.rs"]
mod tests;
