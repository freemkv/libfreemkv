//! IFO parser — DVD title structure.
//!
//! DVD discs use IFO files to describe the title structure:
//!   - `VIDEO_TS/VIDEO_TS.IFO` — top-level VMG with title search pointer table
//!   - `VIDEO_TS/VTS_XX_0.IFO` — per-title-set with PGC chains, cell addresses, streams
//!
//! The parser reads IFO files via UDF and extracts enough information
//! to build DiscTitle structs (parallel to MPLS for Blu-ray).

use crate::disc::{Codec, Resolution};
use crate::error::{Error, Result};
use crate::sector::SectorSource;
use crate::udf::UdfFs;

// ── Public types ────────────────────────────────────────────────────────────

/// Top-level DVD info parsed from VIDEO_TS.IFO + all VTS IFO files.
#[derive(Debug)]
pub struct DvdInfo {
    pub title_sets: Vec<DvdTitleSet>,
}

/// One title set (VTS_XX_0.IFO).
#[derive(Debug)]
pub struct DvdTitleSet {
    /// 1-based title set number (XX in VTS_XX_0.IFO)
    pub vts_number: u8,
    /// First VOB sector in UDF
    pub vob_start_sector: u32,
    /// Video stream attributes
    pub video: DvdVideoAttr,
    /// Audio stream attributes (up to 8)
    pub audio_streams: Vec<DvdAudioAttr>,
    /// Subtitle stream attributes (up to 32)
    pub subtitle_streams: Vec<DvdSubtitleAttr>,
    /// Titles within this set
    pub titles: Vec<DvdTitle>,
}

/// A single title (from PGC + TT_SRPT chapter count).
#[derive(Debug)]
#[allow(dead_code)]
pub struct DvdTitle {
    /// Number of chapters (PTTs)
    pub chapters: u16,
    /// Total playback duration in seconds
    pub duration_secs: f64,
    /// Cell sector ranges
    pub cells: Vec<DvdCell>,
    /// Chapter start times in seconds (derived from program map + cell times)
    pub chapter_times: Vec<f64>,
    /// Subtitle palette from PGC: 16 entries of [padding, Y, Cr, Cb].
    pub palette: Option<Vec<[u8; 4]>>,
}

/// A cell — contiguous sector range within a VOB.
#[derive(Debug, Clone)]
pub struct DvdCell {
    pub first_sector: u32,
    pub last_sector: u32,
    /// Raw cell-category byte at `cell_playback + 0` (DVD-Video IFO layout).
    /// Packs block_mode (bits 7-6), block_type (bits 5-4), seamless_play
    /// (bit 3), interleaved (bit 2), stc_discontinuity (bit 1),
    /// seamless_angle (bit 0). Carried so the extent builder can recognise
    /// non-feature leading cells (interleaved angle sub-blocks) and the
    /// diagnostic dump can show why a cell was kept or dropped.
    pub category: u8,
    /// Per-cell playback duration in seconds (BCD time at `cell_playback + 4`).
    /// Used by the diagnostic dump and the conservative leading-cell filter
    /// (a short leading scene-index cell vs the multi-minute feature).
    pub duration_secs: f64,
}

/// Decoded view of a cell-category byte (`cell_playback + 0`), per the
/// DVD-Video IFO cell-playback layout. Byte-0 bitfields,
/// MSB-first: `block_mode`(7-6), `block_type`(5-4), `seamless_play`(3),
/// `interleaved`(2), `stc_discontinuity`(1), `seamless_angle`(0). (The real
/// `cell_type` is a karaoke-only field in byte 1, not used here.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CellCategory {
    /// bits 7-6: 0=not in block, 1=first cell of block, 2=in block, 3=last cell.
    pub block_mode: u8,
    /// bits 5-4: 0=not part of a block, 1=angle block.
    pub block_type: u8,
    /// bit 3: seamless playback (STC continuous).
    pub seamless_play: bool,
    /// bit 2: interleaved (multi-angle / seamless-branch interleave).
    pub interleaved: bool,
    /// bit 1: STC discontinuity at the start of this cell.
    pub stc_discontinuity: bool,
    /// bit 0: seamless angle change.
    pub seamless_angle: bool,
}

impl CellCategory {
    /// Decode the raw `cell_playback + 0` byte (DVD-Video IFO cell playback).
    pub fn decode(raw: u8) -> Self {
        CellCategory {
            block_mode: (raw >> 6) & 0x03,
            block_type: (raw >> 4) & 0x03,
            seamless_play: (raw & 0x08) != 0,
            interleaved: (raw & 0x04) != 0,
            stc_discontinuity: (raw & 0x02) != 0,
            seamless_angle: (raw & 0x01) != 0,
        }
    }

    /// A plain feature cell: not part of any angle/interleave block. Every cell
    /// of a normal single-angle feature decodes to this (`block_mode` and
    /// `block_type` both 0, only the seamless/interleaved flags possibly set).
    /// Such a cell is NEVER dropped by the leading-cell filter.
    pub fn is_plain_feature(&self) -> bool {
        self.block_mode == 0 && self.block_type == 0
    }

    /// Marks a non-first piece of an angle block: an "in-block" or "last of
    /// block" cell (`block_mode ∈ {2,3}`) of an angle block (`block_type==1`).
    /// Concatenating these back-to-back with the first angle duplicates content
    /// at the head of the feature. Conservative: the FIRST cell of a block
    /// (`block_mode==1`) is NOT flagged — it is the angle we keep.
    pub fn is_secondary_block_piece(&self) -> bool {
        self.block_type == 1 && matches!(self.block_mode, 2 | 3)
    }
}

impl DvdTitle {
    /// Index of the first cell to include in the muxed feature.
    ///
    /// Bug-4 (scene-selection / logo at the head of the feature): the main
    /// feature's PGC can open with leading cells that are NOT part of the
    /// movie — a scene-index segment or an interleaved-angle sub-block. Those
    /// are recognisable by their cell-category byte: a leading cell flagged as
    /// a *secondary* piece of an angle/interleave block
    /// ([`CellCategory::is_secondary_block_piece`]) is not feature content.
    ///
    /// This walks the leading run and returns the index of the first cell that
    /// is a plain feature cell (category `0x00`-class). Cells before it that
    /// are secondary block pieces are dropped from the feature extents.
    ///
    /// **Conservative by construction — it can NEVER truncate a normal
    /// feature:**
    /// - It only ever skips a *prefix*; the scan stops at the first
    ///   plain-feature cell and keeps everything from there on.
    /// - A normal single-angle feature has category `0x00` on cell 0, so the
    ///   scan stops immediately at index 0 and drops nothing.
    /// - It never drops on duration or any heuristic — only on the spec
    ///   category bits — and it never drops the FIRST cell of an angle block
    ///   (the angle we keep).
    /// - As a final guard it never returns past the last cell, and never drops
    ///   when that would leave zero cells.
    ///
    /// For "The Silence of the Lambs" (every feature cell category `0x00`,
    /// chapter 1 at 00:00:00) this returns 0 — a no-op — which is the correct
    /// result: the disc's scene-index lives in a separate menu/title PGC, not
    /// in leading cells of the feature PGC, so there is nothing to drop here.
    pub fn feature_start_cell(&self) -> usize {
        let n = self.cells.len();
        if n == 0 {
            return 0;
        }
        let mut idx = 0;
        while idx < n {
            let cat = CellCategory::decode(self.cells[idx].category);
            // Stop at the first cell that is genuine feature content.
            if !cat.is_secondary_block_piece() {
                break;
            }
            idx += 1;
        }
        // Never drop everything: if every leading cell looked like a secondary
        // block piece (pathological/corrupt category bytes), fall back to
        // keeping all cells rather than producing an empty feature.
        if idx >= n { 0 } else { idx }
    }

    /// The feature cells after the leading-cell filter ([`feature_start_cell`]).
    pub fn feature_cells(&self) -> &[DvdCell] {
        &self.cells[self.feature_start_cell()..]
    }
}

/// DVD TV system, from VTS_V_ATR `video_format` (byte 0 bits 5-4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TvSystem {
    Ntsc,
    Pal,
}

/// DVD display aspect ratio, from VTS_V_ATR `display_aspect_ratio`
/// (byte 0 bits 3-2). The pixels are anamorphic 720x480/576 either way;
/// this is the intended *display* shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DvdAspect {
    R4x3,
    R16x9,
}

/// DVD video stream attributes.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DvdVideoAttr {
    pub codec: Codec,
    pub resolution: Resolution,
    pub aspect: DvdAspect,
    pub standard: TvSystem,
}

/// DVD audio stream attributes.
#[derive(Debug, Clone)]
pub struct DvdAudioAttr {
    pub codec: Codec,
    pub channels: u8,
    pub sample_rate: u32,
    pub language: String,
    /// The PES `private_stream_1` sub-stream id this audio stream carries
    /// on the wire (AC-3: `0x80..=0x87`, DTS: `0x88..=0x8F`, LPCM:
    /// `0xA0..=0xA7`), assigned by per-codec ordinal during the scan.
    /// `None` for codecs carried as a regular MPEG-audio PES (MP1/MP2,
    /// stream_id `0xC0..`) which don't use a private-stream-1 sub-id.
    /// This is the single routing key shared with the muxer's `dvd_pid()`
    /// so the two never disagree on a mixed-codec title.
    pub sub_stream_id: Option<u8>,
}

/// DVD subtitle stream attributes.
#[derive(Debug, Clone)]
pub struct DvdSubtitleAttr {
    pub language: String,
}

// ── Constants ───────────────────────────────────────────────────────────────

const VMG_MAGIC: &[u8; 12] = b"DVDVIDEO-VMG";
const VTS_MAGIC: &[u8; 12] = b"DVDVIDEO-VTS";
use crate::consts::SECTOR_BYTES;

// ── Helper: safe binary reads ───────────────────────────────────────────────

/// Read a big-endian u16 from `data` at `offset`, with bounds check.
fn be_u16(data: &[u8], offset: usize) -> Result<u16> {
    if offset + 2 > data.len() {
        return Err(Error::IfoParse);
    }
    Ok(u16::from_be_bytes([data[offset], data[offset + 1]]))
}

/// Read a big-endian u32 from `data` at `offset`, with bounds check.
fn be_u32(data: &[u8], offset: usize) -> Result<u32> {
    if offset + 4 > data.len() {
        return Err(Error::IfoParse);
    }
    Ok(u32::from_be_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]))
}

/// Read a single byte with bounds check.
fn byte_at(data: &[u8], offset: usize) -> Result<u8> {
    data.get(offset).copied().ok_or(Error::IfoParse)
}

/// Get a sub-slice with bounds check.
fn sub_slice(data: &[u8], offset: usize, len: usize) -> Result<&[u8]> {
    if offset.saturating_add(len) > data.len() {
        return Err(Error::IfoParse);
    }
    Ok(&data[offset..offset + len])
}

// ── BCD time parsing ────────────────────────────────────────────────────────

/// The nominal (timecode) frame rate and the exact real-time frame duration
/// implied by a DVD `dvd_time_t` rate flag.
///
/// This distinction is the whole point of the type: `nominal_fps` is the rate
/// at which the BCD *seconds* field advances, while `frame_num / frame_den` is
/// how long a frame actually lasts. For PAL the two agree (25 frames = 1.000 s).
/// For NTSC they do not: the seconds field advances every 30 frames, but the
/// video runs at 30000/1001 fps, so 30 frames occupy 1001/1000 real seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DvdRate {
    /// Frames per timecode second: 25 (PAL) or 30 (NTSC — *not* 29.97).
    pub nominal_fps: u32,
    /// Real seconds per frame as an exact rational, `frame_num / frame_den`.
    pub frame_num: u32,
    /// Denominator of the exact frame duration.
    pub frame_den: u32,
}

impl DvdRate {
    /// Decode bits 7-6 of `dvd_time_t.frame_u`.
    ///
    /// `0b01` = 25 fps (PAL), `0b11` = 30000/1001 fps (NTSC).
    /// `0b00` and `0b10` are unspecified/reserved and yield `None`.
    pub fn from_flag(flag: u8) -> Option<DvdRate> {
        match flag {
            0x01 => Some(DvdRate {
                nominal_fps: 25,
                frame_num: 1,
                frame_den: 25,
            }),
            0x03 => Some(DvdRate {
                nominal_fps: 30,
                frame_num: 1001,
                frame_den: 30000,
            }),
            _ => None,
        }
    }

    /// Exact real-time seconds for a count of timecode frames.
    pub fn frames_to_secs(&self, frames: u64) -> f64 {
        (frames as f64) * (self.frame_num as f64) / (self.frame_den as f64)
    }
}

/// Total non-drop-frame timecode frames in a 4-byte DVD BCD time.
///
/// Format: `[hours_bcd, minutes_bcd, seconds_bcd, rate_and_frames]`
///   - Byte 0: hours in BCD (e.g. 0x01 = 1 hour, 0x12 = 12 hours)
///   - Byte 1: minutes in BCD
///   - Byte 2: seconds in BCD
///   - Byte 3: bits 7-6 = frame rate flag (01=25fps, 11=29.97fps),
///     bits 5-0 = frame count in BCD
///
/// **`dvd_time_t` is a timecode, not elapsed wall-clock time.** The seconds
/// field advances once every [`DvdRate::nominal_fps`] frames. On NTSC discs
/// that is every 30 frames, but 30 frames of 30000/1001 fps video last
/// 1001/1000 s — so reading H:M:S as literal seconds under-reports real time
/// by exactly 0.1% (3.6 s per hour). Callers that need real seconds must go
/// through the frame count, which is what [`bcd_to_secs`] does.
///
/// Returns `None` when the slice is short or the rate flag is unspecified,
/// since without a nominal rate the frame count cannot be interpreted.
pub fn bcd_to_frames(bcd: &[u8]) -> Option<(u64, DvdRate)> {
    if bcd.len() < 4 {
        return None;
    }
    let rate = DvdRate::from_flag((bcd[3] >> 6) & 0x03)?;
    let hours = bcd_byte(bcd[0]) as u64;
    let minutes = bcd_byte(bcd[1]) as u64;
    let seconds = bcd_byte(bcd[2]) as u64;
    let frames = bcd_byte(bcd[3] & 0x3F) as u64;
    let tc_secs = hours * 3600 + minutes * 60 + seconds;
    Some((tc_secs * rate.nominal_fps as u64 + frames, rate))
}

/// Convert DVD BCD playback time (4 bytes) to real elapsed seconds.
///
/// See [`bcd_to_frames`] for the layout and for why the timecode must be
/// converted through a frame count rather than read as literal seconds.
///
/// Returns 0.0 for invalid BCD digits rather than erroring, since some
/// authoring tools produce malformed time fields. When the rate flag is
/// unspecified the nominal rate is unknowable, so this falls back to reading
/// H:M:S as literal seconds and ignoring the frame field.
pub fn bcd_to_secs(bcd: &[u8]) -> f64 {
    match bcd_to_frames(bcd) {
        Some((frames, rate)) => rate.frames_to_secs(frames),
        None => {
            if bcd.len() < 4 {
                return 0.0;
            }
            (bcd_byte(bcd[0]) as f64) * 3600.0
                + (bcd_byte(bcd[1]) as f64) * 60.0
                + (bcd_byte(bcd[2]) as f64)
        }
    }
}

/// Decode one BCD byte to its decimal value.
/// Returns 0 for invalid BCD (digit > 9).
fn bcd_byte(b: u8) -> u32 {
    let hi = (b >> 4) as u32;
    let lo = (b & 0x0F) as u32;
    if hi > 9 || lo > 9 {
        return 0;
    }
    hi * 10 + lo
}

// ── Top-level entry point ───────────────────────────────────────────────────

/// Parse VIDEO_TS.IFO and all VTS_XX_0.IFO files to build a complete DvdInfo.
///
/// Reads the VMG (Video Manager) to discover title sets, then reads each
/// VTS IFO to extract PGC chains, cell addresses, and stream attributes.
pub fn parse_vmg(reader: &mut dyn SectorSource, udf: &UdfFs) -> Result<DvdInfo> {
    let vmg_data = udf.read_file(reader, "/VIDEO_TS/VIDEO_TS.IFO")?;

    // Validate VMG magic
    if vmg_data.len() < 12 || &vmg_data[0..12] != VMG_MAGIC {
        return Err(Error::IfoParse);
    }

    // Minimum size: need at least through the TT_SRPT pointer at offset 0xC4
    if vmg_data.len() < 0xC8 {
        return Err(Error::IfoParse);
    }

    // The TT_SRPT pointer is at 0xC4 per the DVD-Video VMGI spec.
    // (An informal '62-65' / 0x3E note seen elsewhere is wrong — do not use it.)
    let tt_srpt_sector = be_u32(&vmg_data, 0xC4)?;

    // Read TT_SRPT — it's at the given sector offset relative to the start of VIDEO_TS.IFO.
    // In the IFO file data we already have, sector offsets are relative to the IFO start.
    let tt_srpt_offset = (tt_srpt_sector as usize)
        .checked_mul(SECTOR_BYTES)
        .ok_or(Error::IfoParse)?;

    // TT_SRPT may be beyond what we read; if so, it's embedded in the file data
    // (IFO files are typically small, a few sectors). Check bounds.
    if tt_srpt_offset + 8 > vmg_data.len() {
        return Err(Error::IfoParse);
    }

    let title_set_map = parse_tt_srpt(&vmg_data, tt_srpt_offset)?;

    // Parse each VTS IFO
    let mut title_sets = Vec::new();
    let mut skipped = 0usize;
    for (&vts_number, titles_info) in &title_set_map {
        match parse_vts(reader, udf, vts_number, titles_info) {
            Ok(ts) => title_sets.push(ts),
            Err(e) => {
                // Some discs carry placeholder TT_SRPT entries for title sets
                // that are not really there, so one failure is not fatal. But
                // the failure must not be INVISIBLE: every skipped set is a
                // title the user will never see, and swallowing the reason made
                // a disc that enumerated 38 titles from one image and 10 from
                // another look like a scan difference rather than 28 dropped
                // reads.
                skipped += 1;
                tracing::warn!(
                    target: "freemkv::scan",
                    vts = vts_number,
                    titles = titles_info.len(),
                    error = %e,
                    "title set could not be parsed; its titles are omitted"
                );
                continue;
            }
        }
    }

    if skipped > 0 {
        tracing::warn!(
            target: "freemkv::scan",
            skipped,
            kept = title_sets.len(),
            declared = title_set_map.len(),
            "some title sets were omitted from the scan"
        );
    }
    Ok(DvdInfo { title_sets })
}

/// Maximum TT_SRPT entries honoured. DVD-Video caps a disc at 99 titles
/// (VMGI TT_SRPT `TT_Ns`, and the 99-title / 99-title-set structure the format
/// is built around), but the on-disc count is an untrusted `u16`: a ~800 KB
/// crafted IFO can declare 65535 entries, each re-parsing a PGC into a full
/// `DvdTitle` (~540 MB of `DvdInfo`).
///
/// Headroom: this IS the format maximum, so it clips no conformant disc — a
/// real DVD cannot address a 100th title through TT_SRPT.
const MAX_TT_SRPT_TITLES: usize = 99;

/// Parse the VMG TT_SRPT into a per-title-set map of
/// `(chapter_count, vts_title_number)`.
///
/// Two bounds on untrusted input: the declared entry count is clamped to
/// [`MAX_TT_SRPT_TITLES`], and entries naming a `(vts_number, vts_title_num)`
/// pair already seen are dropped. De-duplication is a correctness fix as well
/// as a bound: two TT_SRPT entries pointing at the same VTS title are the same
/// title, and `parse_pgcit` would otherwise re-parse that one PGC into a
/// separate `DvdTitle` per entry.
fn parse_tt_srpt(
    vmg_data: &[u8],
    tt_srpt_offset: usize,
) -> Result<std::collections::BTreeMap<u8, Vec<(u16, u8)>>> {
    let num_titles = be_u16(vmg_data, tt_srpt_offset)? as usize;
    if num_titles > MAX_TT_SRPT_TITLES {
        tracing::warn!(
            declared = num_titles,
            cap = MAX_TT_SRPT_TITLES,
            "TT_SRPT title count exceeds the DVD-Video maximum, clamping"
        );
    }
    let num_titles = num_titles.min(MAX_TT_SRPT_TITLES);

    // Title entries are 12 bytes each, starting at tt_srpt_offset + 8.
    let entries_start = tt_srpt_offset + 8;
    let mut title_set_map: std::collections::BTreeMap<u8, Vec<(u16, u8)>> =
        std::collections::BTreeMap::new();
    let mut seen: std::collections::HashSet<(u8, u8)> = std::collections::HashSet::new();

    for i in 0..num_titles {
        let base = entries_start + i * 12;
        if base + 12 > vmg_data.len() {
            break; // truncated — parse what we can
        }

        let num_chapters = be_u16(vmg_data, base + 2)?;
        let vts_number = byte_at(vmg_data, base + 6)?;
        let vts_title_num = byte_at(vmg_data, base + 7)?;

        if vts_number == 0 {
            continue; // invalid
        }
        if !seen.insert((vts_number, vts_title_num)) {
            continue; // duplicate entry for the same VTS title
        }

        title_set_map
            .entry(vts_number)
            .or_default()
            .push((num_chapters, vts_title_num));
    }

    Ok(title_set_map)
}

// ── VTS parser ──────────────────────────────────────────────────────────────

/// Parse VTS_XX_0.IFO for one title set.
///
/// `titles_info` is a list of (chapter_count, vts_title_number) from TT_SRPT.
fn parse_vts(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    vts_number: u8,
    titles_info: &[(u16, u8)],
) -> Result<DvdTitleSet> {
    let path = format!("/VIDEO_TS/VTS_{vts_number:02}_0.IFO");
    let vts_data = udf.read_file(reader, &path)?;

    // Validate VTS magic
    if vts_data.len() < 12 || &vts_data[0..12] != VTS_MAGIC {
        return Err(Error::IfoParse);
    }

    // Need at least 0x204 bytes for header fields
    if vts_data.len() < 0x204 {
        return Err(Error::IfoParse);
    }

    // VTSI_MAT (VTS_xx_0.IFO header) field offsets — fixed by the DVD-Video
    // spec (the VTSI management table). The offsets are constant; the sector
    // values they point to are per-disc.
    const VTSTT_VOBS_OFFSET: usize = 0xC4; // VTS title VOBS start sector (feature)
    const VTS_PGCIT_OFFSET: usize = 0xCC; // VTS_PGCIT sector pointer

    // VTS_PGCIT sector pointer
    let pgcit_sector = be_u32(&vts_data, VTS_PGCIT_OFFSET)?;

    // First sector of the VTS **Title** VOBS (`vtstt_vobs`, VTSTT_VOBS_OFFSET).
    // The cell `first_sector` / `last_sector` values in the title PGCs are
    // relative to this. Offset 0xC0 is `vtsm_vobs` — the VTS *menu* VOBS
    // (VTS_xx_0.VOB), which on discs with a per-title menu (e.g. a Universal
    // "the parental level has been set, press yes" first-play still) holds that
    // interactive prompt. Reading the menu base instead prepended the menu VOB
    // to the feature and shifted every cell extent back by
    // `vtstt_vobs - vtsm_vobs` sectors, so the rip opened on the parental
    // prompt instead of the movie. The title content lives at `vtstt_vobs`.
    //
    // `vtstt_vobs` is a sector address **relative to the start of this VTS_xx_0.IFO
    // file**, not an absolute disc LBA. The cell `first_sector`/`last_sector`
    // values are in turn relative to `vtstt_vobs`. To turn them into the absolute
    // disc LBAs the reader needs, add the IFO file's own on-disc location (from
    // the UDF FS). Without this rebase every extent started `ifo_lba` sectors too
    // early — for THESILENCEOFTHELAMBS the feature began at LBA 126 (the VMGI /
    // VIDEO_TS.VOB main-menu region) instead of 132886 (VTS_03_1.VOB), so the
    // first ~4.5 min of muxed video was the disc's main menu before the stream
    // drifted into the movie.
    let vtstt_vobs = be_u32(&vts_data, VTSTT_VOBS_OFFSET)?;
    let ifo_lba = udf.file_start_lba(reader, &path)?;
    let vob_start_sector = ifo_lba.saturating_add(vtstt_vobs);

    // Video attributes at offset 0x200 (2 bytes)
    let video = parse_video_attr(&vts_data)?;

    // Audio streams: count at 0x202 (u16 BE), then 8 bytes each starting at 0x204
    let num_audio = be_u16(&vts_data, 0x200 + 2)?;
    let num_audio = std::cmp::min(num_audio, 8) as usize; // cap at 8
    let mut audio_streams = Vec::with_capacity(num_audio);
    for i in 0..num_audio {
        let aoff = 0x204 + i * 8;
        if aoff + 8 > vts_data.len() {
            break;
        }
        audio_streams.push(parse_audio_attr(&vts_data, aoff)?);
    }
    // Assign each audio stream its on-wire private_stream_1 sub-stream id
    // by per-codec ordinal — the same convention DVD authoring uses (AC-3
    // 0x80+, DTS 0x88+, LPCM 0xA0+). This is the routing key shared with
    // the muxer; per-codec ordinals (not the positional index) are what
    // keep mixed-codec titles from colliding.
    assign_audio_sub_stream_ids(&mut audio_streams);

    // Subtitle streams: count at 0x254 (u16 BE), then 6 bytes each starting at 0x256
    let num_subs = if vts_data.len() >= 0x256 {
        be_u16(&vts_data, 0x254).unwrap_or(0)
    } else {
        0
    };
    let num_subs = std::cmp::min(num_subs, 32) as usize; // cap at 32
    let mut subtitle_streams = Vec::with_capacity(num_subs);
    for i in 0..num_subs {
        let soff = 0x256 + i * 6;
        if soff + 6 > vts_data.len() {
            break;
        }
        subtitle_streams.push(parse_subtitle_attr(&vts_data, soff)?);
    }

    // Parse PGC information table
    let pgcit_offset = (pgcit_sector as usize)
        .checked_mul(SECTOR_BYTES)
        .ok_or(Error::IfoParse)?;
    let titles = parse_pgcit(&vts_data, pgcit_offset, titles_info)?;

    Ok(DvdTitleSet {
        vts_number,
        vob_start_sector,
        video,
        audio_streams,
        subtitle_streams,
        titles,
    })
}

// ── Attribute parsers ───────────────────────────────────────────────────────

// ── VTS_V_ATR byte 0 bitfield layout (DVD-Video spec, MSB first) ──────────
//   bits 7-6 mpeg_version | bits 5-4 video_format | bits 3-2 display_aspect
//   | bits 1-0 permitted_df
// Naming the positions is the guard against the original bug: video_format is
// bits 5-4, NOT bits 1-0 (those are the pan&scan/letterbox permission). Reading
// the low two bits mis-detected every PAL disc as NTSC → 720x480 not 720x576.
const V_ATR_VIDEO_FORMAT_SHIFT: u8 = 4;
const V_ATR_ASPECT_SHIFT: u8 = 2;
const V_ATR_FIELD_MASK: u8 = 0x03;
// video_format field values (2/3 are reserved → parsed as NTSC).
pub(crate) const VIDEO_FORMAT_NTSC: u8 = 0;
pub(crate) const VIDEO_FORMAT_PAL: u8 = 1;
// display_aspect_ratio field values (1/2 are reserved → parsed as 4:3).
pub(crate) const ASPECT_4X3: u8 = 0;
pub(crate) const ASPECT_16X9: u8 = 3;

/// Compose a VTS_V_ATR byte 0 from its `video_format` / `display_aspect`
/// fields, mirroring the layout [`parse_video_attr`] reads. Test-only — keeps
/// fixtures self-documenting (`v_atr_byte(VIDEO_FORMAT_PAL, ASPECT_16X9)`)
/// instead of opaque packed hex.
#[cfg(test)]
pub(crate) fn v_atr_byte(video_format: u8, display_aspect: u8) -> u8 {
    (video_format << V_ATR_VIDEO_FORMAT_SHIFT) | (display_aspect << V_ATR_ASPECT_SHIFT)
}

/// Parse video attributes from VTS header offset 0x200.
fn parse_video_attr(data: &[u8]) -> Result<DvdVideoAttr> {
    let b0 = byte_at(data, 0x200)?;

    // video_format (bits 5-4): NTSC / PAL; reserved values (2/3) → NTSC.
    let standard = match (b0 >> V_ATR_VIDEO_FORMAT_SHIFT) & V_ATR_FIELD_MASK {
        VIDEO_FORMAT_PAL => TvSystem::Pal,
        VIDEO_FORMAT_NTSC => TvSystem::Ntsc,
        _ => TvSystem::Ntsc,
    };

    // display_aspect_ratio (bits 3-2): 4:3 / 16:9; reserved values (1/2) → 4:3.
    let aspect = match (b0 >> V_ATR_ASPECT_SHIFT) & V_ATR_FIELD_MASK {
        ASPECT_16X9 => DvdAspect::R16x9,
        ASPECT_4X3 => DvdAspect::R4x3,
        _ => DvdAspect::R4x3,
    };

    let resolution = match standard {
        TvSystem::Pal => Resolution::R576i,
        TvSystem::Ntsc => Resolution::R480i,
    };

    Ok(DvdVideoAttr {
        codec: Codec::Mpeg2,
        resolution,
        aspect,
        standard,
    })
}

/// Parse one audio stream attribute block (8 bytes at `offset`).
/// `pub(crate)` for the CROSS-MODULE tests only. The sole production caller is
/// `parse_vts_attributes` in this file; `src/mux/mkv.rs`'s `#[cfg(test)]` block
/// calls it directly so its language-mapping tests run the real parser over real
/// on-disc IFO bytes end to end, instead of a hand-built `DvdAudioAttr` that
/// could agree with the muxer while both disagree with the disc. Narrowing this
/// would mean either a `#[cfg(test)]`/`#[cfg(not(test))]` pair of signatures
/// that can drift apart, or moving those tests away from the code they exist to
/// pin — both worse than the widened crate-internal visibility, which reaches no
/// public API.
pub(crate) fn parse_audio_attr(data: &[u8], offset: usize) -> Result<DvdAudioAttr> {
    let b0 = byte_at(data, offset)?;
    let b1 = byte_at(data, offset + 1)?;

    let coding_mode = (b0 >> 5) & 0x07;
    // DVD-Video audio_coding_mode. Modes 2 and 3 are both MPEG audio Layer II
    // (3 adds the MPEG-2 multichannel extension), so both are Codec::Mp2.
    // Mode 2 previously mapped to Codec::Mpeg1 — a VIDEO variant, so
    // Codec::kind() reported Video and the audio stream was classified and
    // handled as video everywhere downstream.
    let codec = match coding_mode {
        0 => Codec::Ac3,
        2 | 3 => Codec::Mp2,
        4 => Codec::Lpcm,
        6 => Codec::Dts,
        _ => Codec::Unknown(coding_mode),
    };

    let sample_rate_flag = (b1 >> 4) & 0x03; // sample_frequency: byte 1 bits 5-4 (DVD-Video audio attributes)
    let sample_rate = match sample_rate_flag {
        0 => 48000,
        1 => 96000,
        _ => 48000,
    };

    let channels = (b1 & 0x07) + 1; // (channels - 1) in low 3 bits of byte 1

    // Language code: bytes 2-3 as ISO 639-1 (the DVD-Video spec's form).
    let lang_bytes = sub_slice(data, offset + 2, 2)?;
    let language = dvd_lang_to_iso639_2(&parse_raw_dvd_lang_bytes(lang_bytes));

    Ok(DvdAudioAttr {
        codec,
        channels,
        sample_rate,
        language,
        // Assigned by `assign_audio_sub_stream_ids` once all streams in the
        // title set are known (the sub-id is a per-codec ordinal).
        sub_stream_id: None,
    })
}

/// Assign the on-wire `private_stream_1` sub-stream id to each audio
/// stream. On DVD-Video the sub-id's **low nibble is the audio-stream
/// *number* (0-7), shared across all codecs** — the single stream index the
/// PGC `audio_control` table / navigation registers select — and the high
/// nibble is the codec base. So the sub-id is `codec_base | position`, where
/// `position` is the stream's index in the IFO audio-attribute table (NOT a
/// per-codec running count):
///   - AC-3  → `0x80 | i`
///   - DTS   → `0x88 | i`
///   - LPCM  → `0xA0 | i`
///   - MP1/MP2 and anything else → `None` (regular MPEG-audio PES, not a
///     private-stream-1 sub-id).
///
/// A per-codec ordinal was wrong: it only coincides with the wire id when a
/// codec's first stream is also the disc's audio stream #0. Any codec that is
/// not the first audio stream (e.g. a DTS track after an AC-3 track) then got
/// a sub-id one-too-low, so the demux routing key (`0xBD00 | sub_id`) never
/// matched and the track muxed silent. The positional index is the real wire
/// number, so distinct positions still give distinct sub-ids (no collision).
///
/// Position saturates at 7 so a malformed over-count never produces an
/// out-of-range sub-id.
fn assign_audio_sub_stream_ids(streams: &mut [DvdAudioAttr]) {
    for (i, s) in streams.iter_mut().enumerate() {
        let n = (i as u8).min(7);
        s.sub_stream_id = match s.codec {
            Codec::Ac3 => Some(0x80 | n),
            Codec::Dts => Some(0x88 | n),
            Codec::Lpcm => Some(0xA0 | n),
            _ => None,
        };
    }
}

/// Parse one subtitle stream attribute block (6 bytes at `offset`).
fn parse_subtitle_attr(data: &[u8], offset: usize) -> Result<DvdSubtitleAttr> {
    // Language code: bytes 2-3 as ISO 639-1 (the DVD-Video spec's form).
    let lang_bytes = sub_slice(data, offset + 2, 2)?;
    let language = dvd_lang_to_iso639_2(&parse_raw_dvd_lang_bytes(lang_bytes));

    Ok(DvdSubtitleAttr { language })
}

/// Decode the raw 2-byte on-disc language code shared by the audio and
/// subtitle attribute blocks. A pair of lowercase a-z bytes is taken
/// verbatim (the ISO 639-1 code the DVD-Video spec puts there); an all-zero
/// pair means unspecified (empty string); anything else falls through to an
/// ASCII-alphanumeric salvage — letters (either case) and digits are kept,
/// everything else (control bytes, punctuation, high bytes from a corrupt or
/// hostile disc) is dropped.
///
/// The salvage is deliberately not narrowed to a-z: whatever survives is only
/// ever a lookup key for [`dvd_lang_to_iso639_2`], which degrades anything it
/// does not recognize to `und`, so a stray `X` or `5` costs nothing and cannot
/// reach an output stream as a language code.
fn parse_raw_dvd_lang_bytes(lang_bytes: &[u8]) -> String {
    if lang_bytes[0] >= b'a'
        && lang_bytes[0] <= b'z'
        && lang_bytes[1] >= b'a'
        && lang_bytes[1] <= b'z'
    {
        String::from_utf8_lossy(lang_bytes).to_string()
    } else if lang_bytes[0] == 0 && lang_bytes[1] == 0 {
        String::new()
    } else {
        lang_bytes
            .iter()
            .filter(|&&b| b.is_ascii_alphanumeric())
            .map(|&b| b as char)
            .collect()
    }
}

/// Convert a DVD IFO audio/subtitle attribute's language code — ISO 639-1
/// (2 lowercase letters) per the DVD-Video spec, or empty when unspecified —
/// to the ISO 639-2 form every downstream consumer (`disc::AudioStream` /
/// `disc::SubtitleStream::language`, and in turn Matroska's `Language`
/// element per RFC 9559 §12 and the MP4 sink's `mdhd` language) requires.
///
/// Uses `labels::vocab::iso639_1_to_iso639_2`, which spans the WHOLE of ISO
/// 639-1 (plus the withdrawn spellings `iw`/`in`/`ji` that DVD-Video's
/// frozen-1988 language list still puts on disc). The narrower
/// `vocab::menu_lang` table is deliberately NOT used here: it exists for
/// Blu-ray menu-graphic filename tokens and knows only 25 languages, so a
/// Region-2 disc's Romanian, Bulgarian, Croatian, Serbian, Slovak, Slovenian,
/// Hebrew, Estonian, Latvian, Lithuanian and Icelandic tracks would all fold
/// onto `und` together. DVD streams carry an empty `label`, so the language
/// is the only thing distinguishing one subtitle track from the next — a
/// valid code that is identical for six tracks is worse for the user than the
/// invalid one it replaced. Both tables normalize to ISO 639-2/T, so they
/// agree wherever they overlap.
///
/// An empty or unrecognized code degrades to `"und"` (ISO 639-2 / Matroska's
/// own "undetermined" value) — a valid element value — rather than passing
/// through an invalid 2-letter code or an empty string. Never guesses.
fn dvd_lang_to_iso639_2(raw: &str) -> String {
    crate::labels::vocab::iso639_1_to_iso639_2(raw)
        .unwrap_or("und")
        .to_string()
}

// ── PGC parser ──────────────────────────────────────────────────────────────

/// Parse VTS_PGCIT (Program Chain Information Table) to extract titles.
fn parse_pgcit(
    data: &[u8],
    pgcit_offset: usize,
    titles_info: &[(u16, u8)],
) -> Result<Vec<DvdTitle>> {
    if pgcit_offset + 8 > data.len() {
        return Err(Error::IfoParse);
    }

    let num_pgcs = be_u16(data, pgcit_offset)?;

    // PGC info entries start at pgcit_offset + 8, each 8 bytes
    let entries_start = pgcit_offset + 8;

    let mut titles = Vec::new();
    // Every `continue` below drops a title the user will never see. None of
    // them may be silent: `parse_vmg` already counts and warns per skipped
    // title SET, and this function was the remaining place where a disc could
    // quietly report fewer titles than it has.
    let mut skipped = 0usize;

    for &(chapter_count, vts_title_num) in titles_info {
        // VTS title numbers are 1-based; map to PGC index (typically 1:1)
        let pgc_index = vts_title_num.saturating_sub(1) as usize;
        if pgc_index >= num_pgcs as usize {
            skipped += 1;
            tracing::warn!(
                target: "freemkv::scan",
                pgc_index,
                num_pgcs,
                "title points past the end of the PGC table; its title is omitted"
            );
            continue;
        }

        let entry_offset = entries_start + pgc_index * 8;
        if entry_offset + 8 > data.len() {
            skipped += 1;
            tracing::warn!(
                target: "freemkv::scan",
                pgc_index,
                entry_offset,
                len = data.len(),
                "PGC entry table is truncated; this title is omitted"
            );
            continue;
        }

        // PGC byte offset relative to VTS_PGCIT start
        let pgc_byte_offset = be_u32(data, entry_offset + 4)? as usize;
        let pgc_abs = pgcit_offset
            .checked_add(pgc_byte_offset)
            .ok_or(Error::IfoParse)?;

        match parse_pgc(data, pgc_abs, chapter_count) {
            Ok(title) => titles.push(title),
            Err(e) => {
                // By design a single unparseable PGC (truncated/corrupt entry,
                // authoring-tool quirk) must not lose the whole title list. But
                // "not fatal" is not the same as "not worth saying": every PGC
                // skipped here is a title the user will never see, and this was
                // the only remaining silent one — `parse_vmg` above already
                // counts and warns per skipped title SET for exactly this
                // reason. A disc quietly reporting fewer titles than it has is
                // the failure this release exists to stop.
                skipped += 1;
                tracing::warn!(
                    target: "freemkv::scan",
                    pgc = pgc_index + 1,
                    of = num_pgcs,
                    error = %e,
                    "PGC could not be parsed; its title is omitted"
                );
                continue;
            }
        }
    }

    if skipped > 0 {
        tracing::warn!(
            target: "freemkv::scan",
            skipped,
            kept = titles.len(),
            declared = titles_info.len(),
            "some titles were omitted from this title set"
        );
    }

    Ok(titles)
}

/// Parse a single PGC (Program Chain) to extract duration and cells.
fn parse_pgc(data: &[u8], pgc_offset: usize, chapters: u16) -> Result<DvdTitle> {
    // PGC needs at least 0xE8 bytes for the cell playback info offset
    if pgc_offset + 0xEA > data.len() {
        return Err(Error::IfoParse);
    }

    // PGC layout:
    //   0x00-0x01: misc flags
    //   0x02:      nr_of_programs
    //   0x03:      nr_of_cells
    //   0x04-0x07: playback_time (4 BCD bytes)
    let num_cells = byte_at(data, pgc_offset + 0x03)? as usize;
    let time_bytes = sub_slice(data, pgc_offset + 0x04, 4)?;
    let duration_secs = bcd_to_secs(time_bytes);

    // Cell playback info table offset (relative to PGC start)
    let cell_playback_offset = be_u16(data, pgc_offset + 0xE8)? as usize;

    // Parse cells
    let mut cells = Vec::with_capacity(num_cells);
    if cell_playback_offset > 0 && num_cells > 0 {
        let cell_base = pgc_offset
            .checked_add(cell_playback_offset)
            .ok_or(Error::IfoParse)?;
        for i in 0..num_cells {
            let co = cell_base + i * 24;
            if co + 24 > data.len() {
                break;
            }
            let category = byte_at(data, co)?;
            let duration_secs = bcd_to_secs(&data[co + 4..co + 8]);
            let first_sector = be_u32(data, co + 8)?;
            let last_sector = be_u32(data, co + 20)?;
            cells.push(DvdCell {
                first_sector,
                last_sector,
                category,
                duration_secs,
            });
        }
    }

    // Recalculate duration from cell times if PGC-level time is zero
    let duration_secs = if duration_secs == 0.0 && !cells.is_empty() && cell_playback_offset > 0 {
        let cell_base = pgc_offset + cell_playback_offset;
        let mut total = 0.0;
        for i in 0..cells.len() {
            // Cell playback info: 24 bytes per cell, BCD time at offset 4-7
            let co = cell_base + i * 24;
            if co + 8 <= data.len() {
                total += bcd_to_secs(&data[co + 4..co + 8]);
            }
        }
        total
    } else {
        duration_secs
    };

    // Extract chapter times from program map + cell durations
    // PGC program map offset at 0xE6, maps program_number → first cell_number
    let chapter_times = {
        let pgm_map_offset = be_u16(data, pgc_offset + 0xE6).unwrap_or(0) as usize;
        let nr_of_programs = byte_at(data, pgc_offset + 0x02).unwrap_or(0) as usize;
        let mut times = Vec::new();
        if pgm_map_offset > 0 && nr_of_programs > 0 && cell_playback_offset > 0 {
            let pgm_base = pgc_offset + pgm_map_offset;
            // Collect per-cell durations as exact timecode FRAME counts. Summing
            // frames (integers) and converting once per chapter keeps every mark
            // on an exact frame boundary — summing f64 seconds would let rounding
            // drift accumulate across a two-hour title.
            //
            // A cell whose rate flag is unspecified has no frame count, so it
            // contributes plain seconds instead; that is tracked separately and
            // added on top. In practice a PGC never mixes rates.
            let mut cell_frames: Vec<(u64, f64)> = Vec::with_capacity(num_cells);
            let mut rate: Option<DvdRate> = None;
            let cell_base = pgc_offset + cell_playback_offset;
            for i in 0..num_cells {
                let co = cell_base + i * 24;
                if co + 8 > data.len() {
                    cell_frames.push((0, 0.0));
                    continue;
                }
                let t = &data[co + 4..co + 8];
                match bcd_to_frames(t) {
                    Some((f, r)) => {
                        // First rate wins; a mixed-rate PGC is malformed, and
                        // reinterpreting earlier cells would be worse than
                        // staying on the rate the title started in.
                        rate.get_or_insert(r);
                        cell_frames.push((f, 0.0));
                    }
                    None => cell_frames.push((0, bcd_to_secs(t))),
                }
            }
            // Program map: each byte is the first cell number (1-based) for that program
            for p in 0..nr_of_programs {
                if pgm_base + p >= data.len() {
                    break;
                }
                let first_cell = data[pgm_base + p] as usize;
                // Chapter time = sum of cell durations before this program's first cell.
                // Clamp to cell_frames.len(): a crafted/corrupt IFO can set first_cell
                // beyond the actual cell count, which would panic the slice index.
                let end = first_cell.saturating_sub(1).min(cell_frames.len());
                let frames: u64 = cell_frames[..end].iter().map(|&(f, _)| f).sum();
                let extra: f64 = cell_frames[..end].iter().map(|&(_, s)| s).sum();
                let time = rate.map_or(0.0, |r| r.frames_to_secs(frames)) + extra;
                times.push(time);
            }
        }
        times
    };

    // Extract subtitle palette at PGC offset 0xA4: 16 colors × 4 bytes [padding, Y, Cr, Cb]
    let palette = if pgc_offset + 0xA4 + 64 <= data.len() {
        let mut colors = Vec::with_capacity(16);
        for i in 0..16 {
            let co = pgc_offset + 0xA4 + i * 4;
            colors.push([data[co], data[co + 1], data[co + 2], data[co + 3]]);
        }
        // Only include palette if it's not all zeros (some DVDs have empty palettes)
        if colors.iter().any(|c| c[1] != 0 || c[2] != 0 || c[3] != 0) {
            Some(colors)
        } else {
            None
        }
    } else {
        None
    };

    Ok(DvdTitle {
        chapters,
        duration_secs,
        cells,
        chapter_times,
        palette,
    })
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a synthetic TT_SRPT at offset 0: a declared `u16` title count
    /// followed by `entries` 12-byte records of
    /// `(num_chapters, vts_number, vts_title_num)`.
    fn tt_srpt_bytes(declared: u16, entries: &[(u16, u8, u8)]) -> Vec<u8> {
        let mut data = vec![0u8; 8 + entries.len() * 12];
        data[0..2].copy_from_slice(&declared.to_be_bytes());
        for (i, &(chapters, vts, title)) in entries.iter().enumerate() {
            let base = 8 + i * 12;
            data[base + 2..base + 4].copy_from_slice(&chapters.to_be_bytes());
            data[base + 6] = vts;
            data[base + 7] = title;
        }
        data
    }

    #[test]
    fn tt_srpt_title_count_is_capped_and_deduplicated() {
        // The TT_SRPT count is an untrusted u16. A ~800 KB crafted IFO declares
        // 65535 entries, and each one re-parses a PGC into another full DvdTitle.
        //
        // The entries are DISTINCT, which matters: with 65535 identical entries
        // the de-duplication collapses them to one title on its own and the count
        // cap is never what bounds the result — the test would then pass with the
        // cap removed entirely, proving only that dedup works. Distinct entries
        // defeat dedup, so the cap is the only thing left holding the line.
        // De-duplication gets its own assertion below, on its own fixture.
        // Both title fields are u8, so vary each within its own range to get
        // ~65025 distinct pairs out of the 65535 entries.
        let entries: Vec<(u16, u8, u8)> = (0..u16::MAX)
            .map(|i| (5u16, (i % 255) as u8 + 1, ((i / 255) % 255) as u8 + 1))
            .collect();
        let data = tt_srpt_bytes(u16::MAX, &entries);
        let map = parse_tt_srpt(&data, 0).expect("well-formed TT_SRPT");
        let total: usize = map.values().map(|v| v.len()).sum();
        // Asserted against the DVD-Video format maximum as a LITERAL, not against
        // MAX_TT_SRPT_TITLES. Comparing a result to the very constant under test
        // passes vacuously the moment someone raises that constant, which is the
        // most likely regression here — and is the tautology class this audit has
        // now found six times. 99 is the format's own ceiling, so this pins the
        // cap to the spec rather than to whatever the code currently says.
        assert!(
            total <= 99,
            "TT_SRPT produced {total} titles from a crafted 65535-entry table; \
             DVD-Video allows at most 99"
        );
        // De-duplication, on its own fixture: the same VTS title named 65535
        // times is one title, not many. Kept separate from the cap assertion
        // above so neither guard can mask the other.
        let dupes = vec![(5u16, 1u8, 1u8); u16::MAX as usize];
        let dup_map =
            parse_tt_srpt(&tt_srpt_bytes(u16::MAX, &dupes), 0).expect("well-formed TT_SRPT");
        assert_eq!(
            dup_map.get(&1).map(Vec::len),
            Some(1),
            "duplicate (vts_number, vts_title_num) entries must collapse"
        );
    }

    #[test]
    fn tt_srpt_admits_a_full_99_title_disc() {
        // The DVD-Video maximum must still parse in full: 99 distinct titles
        // spread over two title sets.
        let entries: Vec<(u16, u8, u8)> = (0..99u8)
            .map(|i| (3u16, if i < 50 { 1 } else { 2 }, i + 1))
            .collect();
        let data = tt_srpt_bytes(99, &entries);
        let map = parse_tt_srpt(&data, 0).expect("well-formed TT_SRPT");
        let total: usize = map.values().map(|v| v.len()).sum();
        assert_eq!(total, 99, "a full 99-title disc must survive the cap");
        assert_eq!(map.get(&1).map(Vec::len), Some(50));
        assert_eq!(map.get(&2).map(Vec::len), Some(49));
    }

    #[test]
    fn bcd_to_secs_basic() {
        // 1 hour, 23 minutes, 45 seconds, 0 frames at 25fps
        let bcd = [0x01, 0x23, 0x45, 0b01_000000];
        let secs = bcd_to_secs(&bcd);
        let expected = 1.0 * 3600.0 + 23.0 * 60.0 + 45.0;
        assert!((secs - expected).abs() < 0.01, "got {}", secs);
    }

    #[test]
    fn bcd_to_secs_with_frames() {
        // 0 hours, 1 minute, 30 seconds, 15 frames of NTSC timecode.
        let bcd = [0x00, 0x01, 0x30, 0b11_010101];
        let secs = bcd_to_secs(&bcd);
        // 0b010101 = 0x15, BCD = 15 frames.
        // Timecode 00:01:30:15 is 90*30 + 15 = 2715 frames, and each frame
        // lasts 1001/30000 s, so real time is 2715*1001/30000 = 90.5905 s
        // (NOT 90.5 s — NTSC timecode runs 0.1% slow against the clock).
        let expected = 2715.0 * 1001.0 / 30000.0;
        assert!((secs - expected).abs() < 1e-9, "got {}", secs);
        assert!((secs - 90.5905).abs() < 1e-9, "got {}", secs);
    }

    /// The NTSC 0.1% pull-down must be applied to the whole timecode, not just
    /// the frame field. Regression test for issue freemkv#25: a title's chapter marks
    /// drifted ~3.6 s per hour because H:M:S was read as literal seconds.
    #[test]
    fn bcd_ntsc_timecode_is_not_literal_seconds() {
        // One hour of NTSC timecode = 3603.6 s of real time.
        let one_hour = [0x01, 0x00, 0x00, 0b11_000000];
        assert!((bcd_to_secs(&one_hour) - 3603.6).abs() < 1e-6);
        // 00:01:02:00 -> 1860 frames -> 62.062 s (the issue freemkv#25 chapter 2 value).
        let ch2 = [0x00, 0x01, 0x02, 0b11_000000];
        assert!((bcd_to_secs(&ch2) - 62.062).abs() < 1e-9);
        // A 20-minute NTSC episode cell is 00:19:58:24 = 35964 frames.
        let ep = [0x00, 0x19, 0x58, 0b11_100100];
        assert_eq!(bcd_to_frames(&ep).unwrap().0, 35964);
        assert!((bcd_to_secs(&ep) - 1199.9988).abs() < 1e-6);
    }

    /// PAL timecode is exact (25 frames = 1.000 s), so the fix must leave every
    /// PAL value bit-identical.
    #[test]
    fn bcd_pal_unaffected_by_ntsc_fix() {
        let pal = [0x01, 0x23, 0x45, 0b01_000000];
        assert!((bcd_to_secs(&pal) - (3600.0 + 23.0 * 60.0 + 45.0)).abs() < 1e-9);
        let pal_frames = [0x00, 0x00, 0x10, 0b01_010010]; // 10 s + 12 frames
        assert!((bcd_to_secs(&pal_frames) - 10.48).abs() < 1e-9);
        let r = DvdRate::from_flag(0x01).unwrap();
        assert_eq!((r.nominal_fps, r.frame_num, r.frame_den), (25, 1, 25));
    }

    #[test]
    fn dvd_rate_from_flag_rejects_reserved() {
        assert!(DvdRate::from_flag(0x00).is_none());
        assert!(DvdRate::from_flag(0x02).is_none());
        let n = DvdRate::from_flag(0x03).unwrap();
        assert_eq!((n.nominal_fps, n.frame_num, n.frame_den), (30, 1001, 30000));
    }

    #[test]
    fn bcd_to_frames_none_on_short_or_unknown_rate() {
        assert!(bcd_to_frames(&[0x00, 0x00]).is_none());
        assert!(bcd_to_frames(&[0x00, 0x00, 0x00, 0b00_000000]).is_none());
        assert!(bcd_to_frames(&[0x00, 0x00, 0x00, 0b10_000000]).is_none());
    }

    #[test]
    fn bcd_to_secs_zero() {
        let bcd = [0x00, 0x00, 0x00, 0x00];
        assert_eq!(bcd_to_secs(&bcd), 0.0);
    }

    #[test]
    fn bcd_to_secs_short_input() {
        assert_eq!(bcd_to_secs(&[0x01, 0x02]), 0.0);
        assert_eq!(bcd_to_secs(&[]), 0.0);
    }

    #[test]
    fn bcd_to_secs_invalid_bcd_digits() {
        // 0xFF has hi=15, lo=15 — both > 9, should return 0 for that byte
        let bcd = [0xFF, 0x01, 0x02, 0b01_000000];
        let secs = bcd_to_secs(&bcd);
        // hours=0 (invalid), minutes=1, seconds=2
        let expected = 0.0 + 60.0 + 2.0;
        assert!((secs - expected).abs() < 0.01, "got {}", secs);
    }

    #[test]
    fn bcd_byte_valid() {
        assert_eq!(bcd_byte(0x00), 0);
        assert_eq!(bcd_byte(0x09), 9);
        assert_eq!(bcd_byte(0x10), 10);
        assert_eq!(bcd_byte(0x59), 59);
        assert_eq!(bcd_byte(0x99), 99);
    }

    #[test]
    fn bcd_byte_invalid() {
        assert_eq!(bcd_byte(0xAA), 0);
        assert_eq!(bcd_byte(0x0F), 0);
        assert_eq!(bcd_byte(0xF0), 0);
    }

    #[test]
    fn be_helpers_bounds_check() {
        let data = [0x00, 0x01, 0x02];
        assert!(be_u16(&data, 0).is_ok());
        assert!(be_u16(&data, 1).is_ok());
        assert!(be_u16(&data, 2).is_err()); // only 1 byte left
        assert!(be_u32(&data, 0).is_err()); // only 3 bytes
    }

    #[test]
    fn struct_construction() {
        let cell = DvdCell {
            first_sector: 100,
            last_sector: 200,
            category: 0,
            duration_secs: 0.0,
        };
        assert_eq!(cell.first_sector, 100);
        assert_eq!(cell.last_sector, 200);

        let title = DvdTitle {
            chapters: 5,
            duration_secs: 3600.0,
            cells: vec![cell.clone()],
            chapter_times: Vec::new(),
            palette: None,
        };
        assert_eq!(title.chapters, 5);
        assert!((title.duration_secs - 3600.0).abs() < 0.01);
        assert_eq!(title.cells.len(), 1);

        let video = DvdVideoAttr {
            codec: Codec::Mpeg2,
            resolution: Resolution::R480i,
            aspect: DvdAspect::R16x9,
            standard: TvSystem::Ntsc,
        };
        assert_eq!(video.codec, Codec::Mpeg2);

        let audio = DvdAudioAttr {
            codec: Codec::Ac3,
            channels: 6,
            sample_rate: 48000,
            language: "en".to_string(),
            sub_stream_id: Some(0x80),
        };
        assert_eq!(audio.channels, 6);

        let ts = DvdTitleSet {
            vts_number: 1,
            vob_start_sector: 512,
            video,
            audio_streams: vec![audio],
            subtitle_streams: Vec::new(),
            titles: vec![title],
        };
        assert_eq!(ts.vts_number, 1);
        assert_eq!(ts.audio_streams.len(), 1);

        let info = DvdInfo {
            title_sets: vec![ts],
        };
        assert_eq!(info.title_sets.len(), 1);
    }

    #[test]
    fn pgc_parses_duration_from_correct_offset() {
        // Build a minimal PGC: 0xEA bytes minimum
        // PGC layout: 0x02 = nr_programs, 0x03 = nr_cells, 0x04-0x07 = BCD time
        let mut pgc = vec![0u8; 0xEA];
        pgc[0x02] = 1; // 1 program
        pgc[0x03] = 2; // 2 cells
        // 1h 59m 30s at 29.97fps, 0 frames
        pgc[0x04] = 0x01; // hours BCD
        pgc[0x05] = 0x59; // minutes BCD
        pgc[0x06] = 0x30; // seconds BCD
        pgc[0x07] = 0b11_000000; // 29.97fps, 0 frames
        // Cell playback info offset at PGC+0xE8
        let cell_offset: u16 = 0xEA; // right after minimum header
        pgc[0xE8] = (cell_offset >> 8) as u8;
        pgc[0xE9] = cell_offset as u8;
        // Add 2 cells (24 bytes each)
        pgc.resize(pgc.len() + 48, 0);
        // Cell 0: sectors 100-200
        let co = 0xEA;
        pgc[co + 8] = 0;
        pgc[co + 9] = 0;
        pgc[co + 10] = 0;
        pgc[co + 11] = 100; // first sector
        pgc[co + 20] = 0;
        pgc[co + 21] = 0;
        pgc[co + 22] = 0;
        pgc[co + 23] = 200; // last sector
        // Cell 1: sectors 300-400
        let co = 0xEA + 24;
        pgc[co + 8] = 0;
        pgc[co + 9] = 0;
        pgc[co + 10] = 1;
        pgc[co + 11] = 44; // first sector = 300
        pgc[co + 20] = 0;
        pgc[co + 21] = 0;
        pgc[co + 22] = 1;
        pgc[co + 23] = 144; // last sector = 400

        let title = parse_pgc(&pgc, 0, 5).unwrap();
        // 01:59:30:00 is NTSC *timecode*, so 7170 timecode seconds = 215100
        // frames, and the real running time is 215100 * 1001/30000 = 7177.17 s.
        // Reading the BCD as literal seconds (7170.0) was the issue freemkv#25 bug.
        let expected = 7170.0 * 30.0 * 1001.0 / 30000.0;
        assert!(
            (title.duration_secs - expected).abs() < 1e-6,
            "expected ~{expected}s, got {}s",
            title.duration_secs
        );
        assert!((title.duration_secs - 7177.17).abs() < 1e-6);
        assert_eq!(title.chapters, 5);
        assert_eq!(title.cells.len(), 2);
        assert_eq!(title.cells[0].first_sector, 100);
        assert_eq!(title.cells[0].last_sector, 200);
        assert_eq!(title.cells[1].first_sector, 300);
        assert_eq!(title.cells[1].last_sector, 400);
    }

    #[test]
    fn video_attr_parsing() {
        let mut data = vec![0u8; 0x204];
        data[0x200] = v_atr_byte(VIDEO_FORMAT_NTSC, ASPECT_16X9);
        let attr = parse_video_attr(&data).unwrap();
        assert_eq!(attr.standard, TvSystem::Ntsc);
        assert_eq!(attr.aspect, DvdAspect::R16x9);
        assert_eq!(attr.resolution, Resolution::R480i);
        assert_eq!(attr.codec, Codec::Mpeg2);
    }

    #[test]
    fn video_attr_pal() {
        let mut data = vec![0u8; 0x204];
        data[0x200] = v_atr_byte(VIDEO_FORMAT_PAL, ASPECT_4X3);
        let attr = parse_video_attr(&data).unwrap();
        assert_eq!(attr.standard, TvSystem::Pal);
        assert_eq!(attr.aspect, DvdAspect::R4x3);
        assert_eq!(attr.resolution, Resolution::R576i);
    }

    /// Real-world regression: a PAL 16:9 anamorphic disc (the Silence of the
    /// Lambs UK SKU). Must parse as PAL / 16:9 / 576i. The old code read the TV
    /// system from bits 1-0 (permitted_df, here 0) and reported NTSC/480i — the
    /// case that shipped broken because only NTSC discs (where the wrong bits
    /// coincide on 0) were ever tested.
    #[test]
    fn video_attr_pal_16x9_anamorphic() {
        let mut data = vec![0u8; 0x204];
        data[0x200] = v_atr_byte(VIDEO_FORMAT_PAL, ASPECT_16X9);
        let attr = parse_video_attr(&data).unwrap();
        assert_eq!(attr.standard, TvSystem::Pal);
        assert_eq!(attr.aspect, DvdAspect::R16x9);
        assert_eq!(attr.resolution, Resolution::R576i);
    }

    /// ABSOLUTE-BYTE pin (audit §3 #2): the existing video-attr tests build the
    /// byte via `v_atr_byte(...)`, which uses the SAME shift constants the parser
    /// reads with — a co-edit of constant + helper would silently re-introduce
    /// the PAL-as-NTSC bug and every test would still pass. This test feeds
    /// `parse_video_attr` HARDCODED bytes captured from real DVD-Video layouts
    /// (DVD-Video video attributes: mpeg_version[7-6] video_format[5-4]
    /// display_aspect[3-2] permitted_df[1-0]) — no `v_atr_byte`. If the parser's
    /// bit positions drift, these fail.
    #[test]
    fn video_attr_absolute_bytes_pin_real_layout() {
        // (byte @0x200, expected standard, expected aspect, expected resolution).
        // PAL 16:9 anamorphic = mpeg(00) format(01=PAL) aspect(11=16:9) df(00)
        //   = 0b0001_1100 = 0x1C  (e.g. a PAL 16:9 R2 feature disc).
        // PAL 4:3          = 0b0001_0000 = 0x10.
        // NTSC 16:9        = 0b0000_1100 = 0x0C.
        // NTSC 4:3         = 0b0000_0000 = 0x00.
        // A real disc also sets mpeg_version=01 (MPEG-2) in bits 7-6, which the
        // parser must IGNORE; OR it in (|0x40) to prove it doesn't leak into the
        // video_format read.
        let cases: &[(u8, TvSystem, DvdAspect, Resolution)] = &[
            (0x1C, TvSystem::Pal, DvdAspect::R16x9, Resolution::R576i),
            (0x10, TvSystem::Pal, DvdAspect::R4x3, Resolution::R576i),
            (0x0C, TvSystem::Ntsc, DvdAspect::R16x9, Resolution::R480i),
            (0x00, TvSystem::Ntsc, DvdAspect::R4x3, Resolution::R480i),
            // mpeg_version=2 (MPEG-2) in bits 7-6 must not perturb the read.
            (0x5C, TvSystem::Pal, DvdAspect::R16x9, Resolution::R576i),
        ];
        for &(b0, std, aspect, res) in cases {
            let mut data = vec![0u8; 0x204];
            data[0x200] = b0;
            let attr = parse_video_attr(&data).unwrap();
            assert_eq!(attr.standard, std, "byte {b0:#04x} → standard");
            assert_eq!(attr.aspect, aspect, "byte {b0:#04x} → aspect");
            assert_eq!(attr.resolution, res, "byte {b0:#04x} → resolution");
        }
        // Anti-bug anchor: the original bug read the TV system from bits 1-0
        // (permitted_df). A PAL byte whose low 2 bits are 0 (0x1C) must NOT be
        // misread as NTSC — and a byte with low bits set but format=NTSC
        // (0x03 = NTSC, df=11) must stay NTSC, proving the low bits are ignored.
        let mut df = vec![0u8; 0x204];
        df[0x200] = 0x03; // format=NTSC(00), df=11
        assert_eq!(
            parse_video_attr(&df).unwrap().standard,
            TvSystem::Ntsc,
            "permitted_df bits (1-0) must NOT be read as the TV system"
        );
    }

    #[test]
    fn audio_attr_parsing() {
        let mut data = vec![0u8; 16];
        // AC3 (coding=0), 48kHz (rate=0), 6 channels (stored as 5)
        // b0: bits 7-5=000(AC3), bits 4-3=00(48k) => 0x00
        data[0] = 0x00;
        // b1: bits 2-0=101 (channels-1=5) => 0x05
        data[1] = 0x05;
        // on-disc language "en" (ISO 639-1) -> parsed as ISO 639-2 "eng"
        data[2] = b'e';
        data[3] = b'n';

        let attr = parse_audio_attr(&data, 0).unwrap();
        assert_eq!(attr.codec, Codec::Ac3);
        assert_eq!(attr.sample_rate, 48000);
        assert_eq!(attr.channels, 6);
        assert_eq!(attr.language, "eng");
    }

    #[test]
    fn mixed_codec_sub_stream_ids_are_distinct() {
        // A title mixing AC-3, DTS and LPCM: the sub-id low nibble is the
        // POSITIONAL audio-stream number (shared across codecs), OR'd with the
        // codec base. So idx 1 (DTS) → 0x89, idx 3 (AC-3) → 0x83 — the real
        // wire ids the demux routes on. All distinct (positions are unique).
        let mut streams = vec![
            DvdAudioAttr {
                codec: Codec::Ac3,
                channels: 6,
                sample_rate: 48000,
                language: "en".into(),
                sub_stream_id: None,
            },
            DvdAudioAttr {
                codec: Codec::Dts,
                channels: 6,
                sample_rate: 48000,
                language: "en".into(),
                sub_stream_id: None,
            },
            DvdAudioAttr {
                codec: Codec::Lpcm,
                channels: 2,
                sample_rate: 48000,
                language: "fr".into(),
                sub_stream_id: None,
            },
            DvdAudioAttr {
                codec: Codec::Ac3,
                channels: 2,
                sample_rate: 48000,
                language: "es".into(),
                sub_stream_id: None,
            },
        ];
        assign_audio_sub_stream_ids(&mut streams);
        assert_eq!(streams[0].sub_stream_id, Some(0x80)); // AC-3 @ pos 0
        assert_eq!(streams[1].sub_stream_id, Some(0x89)); // DTS  @ pos 1
        assert_eq!(streams[2].sub_stream_id, Some(0xA2)); // LPCM @ pos 2
        assert_eq!(streams[3].sub_stream_id, Some(0x83)); // AC-3 @ pos 3
        // All sub-ids unique.
        let ids: Vec<u8> = streams.iter().filter_map(|s| s.sub_stream_id).collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(ids.len(), sorted.len(), "sub-stream ids must be unique");
    }

    /// Regression (The Punisher 2004): audio[0]=AC-3 5.1, audio[1]=DTS 5.0.
    /// The DTS track sits at audio position 1, so its wire sub-id is 0x89
    /// (0x88 | 1), NOT the per-codec 0x88. With the old per-codec ordinal it
    /// got 0x88 → demux routing key 0xBD88 had no match → every DTS packet
    /// (which carries 0x89) was dropped → the track muxed present-but-silent
    /// while the AC-3 (at position 0, where ordinal and position coincide)
    /// played fine. Positional numbering fixes it end-to-end.
    #[test]
    fn dts_after_ac3_uses_positional_substream_id() {
        let mut streams = vec![
            DvdAudioAttr {
                codec: Codec::Ac3,
                channels: 6,
                sample_rate: 48000,
                language: "en".into(),
                sub_stream_id: None,
            },
            DvdAudioAttr {
                codec: Codec::Dts,
                channels: 5,
                sample_rate: 48000,
                language: "en".into(),
                sub_stream_id: None,
            },
        ];
        assign_audio_sub_stream_ids(&mut streams);
        assert_eq!(streams[0].sub_stream_id, Some(0x80));
        assert_eq!(
            streams[1].sub_stream_id,
            Some(0x89),
            "DTS at audio position 1 routes to 0x89 on the wire, not 0x88"
        );
        // The routing key the muxer actually uses must resolve for 0x89.
        assert_eq!(crate::mux::ps::dvd_audio_pid(0x89), Some(0xBD89));
    }

    #[test]
    fn every_audio_coding_mode_maps_to_an_audio_codec() {
        // An audio attribute block must never yield a codec whose kind() is Video.
        // Mode 2 (MPEG-1 audio Layer II) mapped to Codec::Mpeg1 — the MPEG-1 VIDEO
        // variant — so a DVD MPEG-audio stream was classified as video downstream.
        for (mode, want) in [
            (0u8, Codec::Ac3),
            (2, Codec::Mp2),
            (3, Codec::Mp2),
            (4, Codec::Lpcm),
            (6, Codec::Dts),
        ] {
            let mut data = vec![0u8; 16];
            data[0] = mode << 5;
            data[2] = b'e';
            data[3] = b'n';
            let attr = parse_audio_attr(&data, 0).unwrap();
            assert_eq!(attr.codec, want, "coding_mode {mode} must map to {want:?}");
            assert_eq!(
                attr.codec.kind(),
                crate::disc::CodecKind::Audio,
                "coding_mode {mode} produced {:?}, whose kind is not Audio",
                attr.codec
            );
        }
    }

    #[test]
    fn audio_attr_dts() {
        let mut data = vec![0u8; 16];
        // DTS (coding=6), 96kHz (rate=1, byte1 bits 5-4), 2 channels (stored as 1)
        // b0: bits 7-5=110(DTS) => 0b110_00000 = 0xC0
        data[0] = 0xC0;
        // b1: bits 5-4=01(96k), bits 2-0=001(channels-1=1) => 0b00_01_0_001 = 0x11
        data[1] = 0x11;
        data[2] = b'f';
        data[3] = b'r';

        let attr = parse_audio_attr(&data, 0).unwrap();
        assert_eq!(attr.codec, Codec::Dts);
        assert_eq!(attr.sample_rate, 96000);
        assert_eq!(attr.channels, 2);
        assert_eq!(attr.language, "fra");
    }

    // ─────────────────────────────────────────────────────────────────────
    // Added hardening tests. Grounded in the DVD-Video IFO spec
    // (DVD-Video IFO format; http://dvd.sourceforge.net).
    // ─────────────────────────────────────────────────────────────────────

    /// BCD frame-rate flag: bits 7-6 of byte[3]. 0b01 = 25fps (PAL),
    /// 0b11 = 29.97fps (NTSC). 0b00/0b10 are "unknown" → frames ignored.
    /// Verify the 25fps branch contributes frames correctly.
    #[test]
    fn bcd_25fps_frame_contribution() {
        // 0h 0m 0s, 12 frames at 25fps → 12/25 = 0.48s.
        let bcd = [0x00, 0x00, 0x00, 0b01_010010]; // frame BCD 0x12 = 12
        let secs = bcd_to_secs(&bcd);
        assert!((secs - 12.0 / 25.0).abs() < 1e-9, "got {secs}");
    }

    /// BCD rate_flag 0b00 (and 0b10) → fps 0.0 → frame count ignored
    /// entirely (only H/M/S counted). Source: `_ => 0.0` arm.
    #[test]
    fn bcd_unknown_rate_ignores_frames() {
        // 0h 1m 0s with frame bits set but rate_flag 0b00.
        let bcd = [0x00, 0x01, 0x00, 0b00_011001]; // frames present, rate unknown
        let secs = bcd_to_secs(&bcd);
        assert!((secs - 60.0).abs() < 0.001, "got {secs}");
        // rate_flag 0b10 also unknown.
        let bcd2 = [0x00, 0x01, 0x00, 0b10_011001];
        assert!((bcd_to_secs(&bcd2) - 60.0).abs() < 0.001);
    }

    /// BCD frame count is the LOW 6 bits of byte[3] (bits 5-0), decoded as
    /// BCD. The 2 high bits (rate flag) must not leak into the frame value.
    /// 0b11_100101: rate=29.97, frame BCD = 0x25 = 25 frames.
    #[test]
    fn bcd_frame_count_masks_rate_bits() {
        let bcd = [0x00, 0x00, 0x00, 0b11_100101]; // 0x25 BCD = 25 frames
        assert_eq!(bcd_to_frames(&bcd).unwrap().0, 25);
        let secs = bcd_to_secs(&bcd);
        assert!((secs - 25.0 * 1001.0 / 30000.0).abs() < 1e-9, "got {secs}");
    }

    /// BCD hours can exceed 12 (long titles): 0x12 BCD = 12 → but test a
    /// value where hi/lo are both valid digits, e.g. 0x10 = 10 hours.
    /// Ensures hours aren't capped or treated as hex.
    #[test]
    fn bcd_double_digit_hours() {
        let bcd = [0x10, 0x00, 0x00, 0x00]; // 10 hours BCD
        let secs = bcd_to_secs(&bcd);
        assert!((secs - 10.0 * 3600.0).abs() < 0.01, "got {secs}");
    }

    /// sub_slice uses saturating_add so an offset near usize::MAX cannot
    /// wrap and bypass the bounds check. Must return Err, not panic/OOB.
    #[test]
    fn sub_slice_no_overflow_wrap() {
        let data = [0u8; 8];
        assert!(sub_slice(&data, usize::MAX, 4).is_err());
        assert!(sub_slice(&data, 4, 4).is_ok());
        assert!(sub_slice(&data, 5, 4).is_err()); // 5+4 > 8
    }

    /// byte_at returns Err for an out-of-range index (uses .get()).
    #[test]
    fn byte_at_out_of_range() {
        let data = [0xAA, 0xBB];
        assert_eq!(byte_at(&data, 0).unwrap(), 0xAA);
        assert_eq!(byte_at(&data, 1).unwrap(), 0xBB);
        assert!(byte_at(&data, 2).is_err());
    }

    /// A reserved video_format value (2/3) falls into the NTSC default.
    #[test]
    fn video_attr_reserved_standard_defaults_ntsc() {
        let mut data = vec![0u8; 0x204];
        // A reserved value is anything past PAL (2 or 3).
        data[0x200] = v_atr_byte(VIDEO_FORMAT_PAL + 1, ASPECT_4X3);
        let attr = parse_video_attr(&data).unwrap();
        assert_eq!(attr.standard, TvSystem::Ntsc);
        assert_eq!(attr.resolution, Resolution::R480i);
    }

    /// A reserved display_aspect value (1/2) falls into the 4:3 default.
    #[test]
    fn video_attr_reserved_aspect_defaults_4_3() {
        let mut data = vec![0u8; 0x204];
        // A reserved aspect value is between 4:3 (0) and 16:9 (3).
        data[0x200] = v_atr_byte(VIDEO_FORMAT_NTSC, ASPECT_4X3 + 1);
        let attr = parse_video_attr(&data).unwrap();
        assert_eq!(attr.aspect, DvdAspect::R4x3);
    }

    /// Audio coding_mode (b0>>5 & 0x07): 0=AC3, 2=MPEG1, 3=MP2, 4=LPCM,
    /// 6=DTS; everything else → Unknown(mode). Verify LPCM (4) and an
    /// unknown mode (1) map per the spec table.
    #[test]
    fn audio_attr_lpcm_and_unknown_coding() {
        let mut data = vec![0u8; 8];
        // LPCM: coding=4 → b0 bits 7-5 = 0b100 → 0x80
        data[0] = 0x80;
        data[2] = b'e';
        data[3] = b'n';
        let attr = parse_audio_attr(&data, 0).unwrap();
        assert_eq!(attr.codec, Codec::Lpcm);

        // coding=1 (reserved/unknown) → Unknown(1)
        let mut data2 = vec![0u8; 8];
        data2[0] = 0b001_00000; // coding=1
        let attr2 = parse_audio_attr(&data2, 0).unwrap();
        assert_eq!(attr2.codec, Codec::Unknown(1));
    }

    /// Audio language bytes [offset+2..+4]: when both bytes are 0x00 the
    /// on-disc code is unspecified, and `dvd_lang_to_iso639_2` maps that to
    /// the valid ISO 639-2 "und" (undetermined) rather than an empty string,
    /// which is not a legal Matroska `Language` element value.
    #[test]
    fn audio_attr_zero_language_becomes_und() {
        let mut data = vec![0u8; 8];
        data[0] = 0x00;
        data[2] = 0x00;
        data[3] = 0x00;
        let attr = parse_audio_attr(&data, 0).unwrap();
        assert_eq!(attr.language, "und");
    }

    /// Audio sample_rate flag (b0>>3 & 0x03): 0=48kHz, 1=96kHz, else 48kHz.
    /// Verify flag 2/3 fall back to 48kHz (catch-all).
    #[test]
    fn audio_attr_reserved_rate_defaults_48k() {
        let mut data = vec![0u8; 8];
        data[0] = 0b0001_0000; // sample-rate flag (bits 4-3) = 0b10
        let attr = parse_audio_attr(&data, 0).unwrap();
        assert_eq!(attr.sample_rate, 48000);
    }

    /// Subtitle language is at [offset+2..+4]. Verify a valid 2-letter code
    /// and the all-zero → empty case.
    #[test]
    fn subtitle_attr_language() {
        let mut data = vec![0u8; 6];
        data[2] = b'd';
        data[3] = b'e';
        let attr = parse_subtitle_attr(&data, 0).unwrap();
        assert_eq!(attr.language, "deu");

        let zero = vec![0u8; 6];
        let attr2 = parse_subtitle_attr(&zero, 0).unwrap();
        assert_eq!(attr2.language, "und");
    }

    /// assign_audio_sub_stream_ids: MP1/MP2 and other non-private-stream-1
    /// codecs must get `None` (regular MPEG-audio PES, not a sub-id).
    /// Source maps only AC3/DTS/LPCM to Some(_).
    #[test]
    fn mp2_audio_gets_no_sub_stream_id() {
        let mut streams = vec![
            DvdAudioAttr {
                codec: Codec::Mp2,
                channels: 2,
                sample_rate: 48000,
                language: "en".into(),
                sub_stream_id: None,
            },
            DvdAudioAttr {
                codec: Codec::Ac3,
                channels: 6,
                sample_rate: 48000,
                language: "en".into(),
                sub_stream_id: None,
            },
        ];
        assign_audio_sub_stream_ids(&mut streams);
        assert_eq!(streams[0].sub_stream_id, None); // MP2 → no sub-id
        assert_eq!(streams[1].sub_stream_id, Some(0x81)); // AC3 @ pos 1
    }

    /// assign_audio_sub_stream_ids saturates the positional index at the
    /// range ceiling (min(7)) so a malformed over-count never produces an
    /// out-of-range sub-id. 9 AC-3 streams: the 9th still ≤ 0x87.
    #[test]
    fn audio_sub_stream_id_saturates_at_ceiling() {
        let mut streams: Vec<DvdAudioAttr> = (0..9)
            .map(|_| DvdAudioAttr {
                codec: Codec::Ac3,
                channels: 2,
                sample_rate: 48000,
                language: String::new(),
                sub_stream_id: None,
            })
            .collect();
        assign_audio_sub_stream_ids(&mut streams);
        for s in &streams {
            let id = s.sub_stream_id.unwrap();
            assert!(
                (0x80..=0x87).contains(&id),
                "AC-3 sub-id out of range: {id:#x}"
            );
        }
        // 8th and 9th both saturate at 0x87.
        assert_eq!(streams[7].sub_stream_id, Some(0x87));
        assert_eq!(streams[8].sub_stream_id, Some(0x87));
    }

    /// parse_pgc requires `pgc_offset + 0xEA <= data.len()` (needs the cell
    /// playback offset at 0xE8). A PGC shorter than 0xEA → IfoParse error,
    /// not panic.
    #[test]
    fn pgc_too_short_errs() {
        let pgc = vec![0u8; 0xE9]; // one byte short of 0xEA
        assert!(parse_pgc(&pgc, 0, 1).is_err());
    }

    /// parse_pgc cell loop stops when a cell record runs past the buffer
    /// (`co + 24 > data.len()` → break), parsing only complete cells.
    /// Declare 3 cells but supply bytes for 2.
    #[test]
    fn pgc_truncated_cell_table_stops() {
        let mut pgc = vec![0u8; 0xEA];
        pgc[0x02] = 1;
        pgc[0x03] = 3; // claims 3 cells
        pgc[0xE8] = 0x00;
        pgc[0xE9] = 0xEA;
        // Only room for 2 full cells (48 bytes).
        pgc.resize(0xEA + 48, 0);
        pgc[0xEA + 8..0xEA + 12].copy_from_slice(&10u32.to_be_bytes());
        pgc[0xEA + 24 + 8..0xEA + 24 + 12].copy_from_slice(&20u32.to_be_bytes());
        let title = parse_pgc(&pgc, 0, 1).unwrap();
        // Only 2 cells parsed; the 3rd had no bytes.
        assert_eq!(title.cells.len(), 2);
        assert_eq!(title.cells[0].first_sector, 10);
        assert_eq!(title.cells[1].first_sector, 20);
    }

    /// parse_pgc palette: at PGC+0xA4, 16 colors × 4 bytes [pad, Y, Cr, Cb].
    /// A palette with at least one non-zero Y/Cr/Cb is returned as Some;
    /// an all-zero palette returns None (source filters empty palettes).
    #[test]
    fn pgc_palette_present_and_empty() {
        let mut pgc = vec![0u8; 0xEA];
        pgc[0x03] = 0; // no cells
        // Set color 0's Y byte (offset 0xA4 + 1) non-zero.
        pgc[0xA4 + 1] = 0x80;
        let title = parse_pgc(&pgc, 0, 1).unwrap();
        let pal = title.palette.expect("non-empty palette should be Some");
        assert_eq!(pal.len(), 16);
        assert_eq!(pal[0], [0x00, 0x80, 0x00, 0x00]);

        // All-zero palette → None.
        let mut pgc2 = vec![0u8; 0xEA];
        pgc2[0x03] = 0;
        let title2 = parse_pgc(&pgc2, 0, 1).unwrap();
        assert!(title2.palette.is_none());
    }

    /// parse_pgc palette layout: each color is [padding, Y, Cr, Cb] and the
    /// "non-empty" test ignores the padding byte (index 0). A palette whose
    /// ONLY non-zero bytes are padding must still be treated as empty (None).
    #[test]
    fn pgc_palette_padding_only_is_empty() {
        let mut pgc = vec![0u8; 0xEA];
        pgc[0x03] = 0;
        // Set padding byte (index 0) of color 0 non-zero, but Y/Cr/Cb zero.
        pgc[0xA4] = 0xFF;
        let title = parse_pgc(&pgc, 0, 1).unwrap();
        assert!(
            title.palette.is_none(),
            "padding-only palette must be treated as empty"
        );
    }

    /// parse_pgc chapter_times: the program map (at PGC+0xE6) holds, per
    /// program, the 1-based first cell number. chapter_time[p] = sum of
    /// cell durations BEFORE that program's first cell. Verify a 2-program,
    /// 3-cell layout: program 0 starts at cell 1 (time 0), program 1 starts
    /// at cell 3 (time = dur(cell0)+dur(cell1)).
    #[test]
    fn pgc_chapter_times_from_program_map() {
        let mut pgc = vec![0u8; 0xEA];
        pgc[0x02] = 2; // nr_programs = 2
        pgc[0x03] = 3; // nr_cells = 3
        // program map offset at 0xE6 (u16 BE)
        let pgm_off: u16 = 0xEA;
        pgc[0xE6] = (pgm_off >> 8) as u8;
        pgc[0xE7] = pgm_off as u8;
        // cell playback offset at 0xE8
        let cell_off: u16 = 0xEA + 2; // after the 2-byte program map
        pgc[0xE8] = (cell_off >> 8) as u8;
        pgc[0xE9] = cell_off as u8;

        // Layout: [0xEA..0xEC] = program map (2 bytes), then 3 cells × 24.
        pgc.resize(cell_off as usize + 3 * 24, 0);
        // Program map: program0 first cell = 1, program1 first cell = 3.
        pgc[0xEA] = 1;
        pgc[0xEB] = 3;
        // Cell durations: cell0 = 5s, cell1 = 7s, cell2 = 9s (BCD seconds).
        let cb = cell_off as usize;
        pgc[cb + 6] = 0x05; // cell0 sec
        pgc[cb + 24 + 6] = 0x07; // cell1 sec
        pgc[cb + 48 + 6] = 0x09; // cell2 sec

        let title = parse_pgc(&pgc, 0, 2).unwrap();
        assert_eq!(title.chapter_times.len(), 2);
        // Program 0 → before cell 1 → 0s.
        assert!((title.chapter_times[0] - 0.0).abs() < 0.01);
        // Program 1 → before cell 3 → dur(cell0)+dur(cell1) = 5+7 = 12s.
        assert!(
            (title.chapter_times[1] - 12.0).abs() < 0.01,
            "got {}",
            title.chapter_times[1]
        );
    }

    /// Regression test for issue freemkv#25 (NTSC chapter drift).
    ///
    /// Builds a PGC from the real cell table of the DOLL_MASTERS_7 disc in the
    /// bug report and checks the chapter marks against MakeMKV's values. Before
    /// the fix, chapter 14 landed 4.019 s early; the drift was proportional to
    /// elapsed time (0.1%), so a short synthetic fixture would not have caught it.
    #[test]
    fn pgc_chapter_times_ntsc_no_pulldown_drift() {
        // (minutes, seconds, frames) of NTSC non-drop-frame timecode per cell.
        let cells: [(u8, u8, u8); 13] = [
            (1, 2, 0),
            (4, 44, 12),
            (1, 32, 11),
            (13, 42, 1),
            (0, 59, 28),
            (0, 31, 8),
            (1, 2, 0),
            (19, 58, 24),
            (0, 59, 28),
            (0, 31, 8),
            (1, 2, 0),
            (19, 58, 26),
            (0, 59, 28),
        ];
        // Program map: 14 chapters, one per cell boundary (1-based first cell).
        let pgm: [u8; 14] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14];

        fn bcd(v: u8) -> u8 {
            ((v / 10) << 4) | (v % 10)
        }

        let mut pgc = vec![0u8; 0xEA];
        pgc[0x02] = pgm.len() as u8;
        pgc[0x03] = cells.len() as u8;
        let pgm_off: u16 = 0xEA;
        pgc[0xE6] = (pgm_off >> 8) as u8;
        pgc[0xE7] = pgm_off as u8;
        let cell_off: u16 = pgm_off + pgm.len() as u16;
        pgc[0xE8] = (cell_off >> 8) as u8;
        pgc[0xE9] = cell_off as u8;
        pgc.resize(cell_off as usize + cells.len() * 24, 0);
        pgc[pgm_off as usize..pgm_off as usize + pgm.len()].copy_from_slice(&pgm);
        for (i, &(m, s, f)) in cells.iter().enumerate() {
            let co = cell_off as usize + i * 24;
            pgc[co + 4] = 0x00; // hours
            pgc[co + 5] = bcd(m);
            pgc[co + 6] = bcd(s);
            pgc[co + 7] = 0xC0 | bcd(f); // rate flag 0b11 = NTSC
        }

        let title = parse_pgc(&pgc, 0, pgm.len() as u16).unwrap();
        // MakeMKV / source truth, in seconds.
        let expected = [
            0.0,
            62.062,
            346.7464,
            439.205_433_333,
            1262.0608,
            1_322.054_066_666,
            1353.352,
            1415.414,
            2615.4128,
            2_675.406_066_666,
            2706.704,
            2768.766,
            3_968.831_533_333,
            4028.8248,
        ];
        assert_eq!(title.chapter_times.len(), expected.len());
        for (i, (&got, &want)) in title.chapter_times.iter().zip(&expected).enumerate() {
            assert!(
                (got - want).abs() < 1e-6,
                "chapter {} drifted: got {got}, want {want}",
                i + 1
            );
        }
        // Every mark must sit on an exact 30000/1001 frame boundary.
        for (i, &t) in title.chapter_times.iter().enumerate() {
            let frames = t * 30000.0 / 1001.0;
            assert!(
                (frames - frames.round()).abs() < 1e-6,
                "chapter {} not frame-aligned: {t}",
                i + 1
            );
        }
    }

    /// parse_pgc duration: when the PGC-level BCD time is NON-zero it is
    /// used directly and NOT overwritten by cell-sum recomputation
    /// (the recompute only fires when duration_secs == 0.0).
    #[test]
    fn pgc_nonzero_duration_not_recomputed() {
        let mut pgc = vec![0u8; 0xEA];
        pgc[0x02] = 1;
        pgc[0x03] = 1;
        // PGC-level time = 1m 0s at 25fps.
        pgc[0x05] = 0x01; // minutes BCD 1
        pgc[0x07] = 0b01_000000; // 25fps, 0 frames
        pgc[0xE8] = 0x00;
        pgc[0xE9] = 0xEA;
        pgc.resize(0xEA + 24, 0);
        // Give the cell a bogus huge duration that must be IGNORED.
        pgc[0xEA + 6] = 0x59; // 59s — would change result if recomputed
        let title = parse_pgc(&pgc, 0, 1).unwrap();
        assert!(
            (title.duration_secs - 60.0).abs() < 0.01,
            "PGC-level 60s must win, got {}",
            title.duration_secs
        );
    }

    /// parse_pgc with cell_playback_offset == 0 must produce NO cells (the
    /// `cell_playback_offset > 0 && num_cells > 0` guard). Even with
    /// nr_cells set, a zero offset means the table is absent.
    #[test]
    fn pgc_zero_cell_offset_no_cells() {
        let mut pgc = vec![0u8; 0xEA];
        pgc[0x03] = 5; // claims 5 cells
        // cell_playback_offset (0xE8) left 0.
        let title = parse_pgc(&pgc, 0, 1).unwrap();
        assert!(title.cells.is_empty());
    }

    // ─────────────────────────────────────────────────────────────────────
    // Cell-category decode + bug-4 leading-cell filter.
    // ─────────────────────────────────────────────────────────────────────

    fn cell(first: u32, last: u32, category: u8) -> DvdCell {
        DvdCell {
            first_sector: first,
            last_sector: last,
            category,
            duration_secs: 0.0,
        }
    }

    /// CellCategory decodes the DVD-Video cell-category byte-0 bitfields: block_mode (7-6),
    /// block_type (5-4), seamless_play (3), interleaved (2),
    /// stc_discontinuity (1), seamless_angle (0).
    #[test]
    fn cell_category_decode_bits() {
        // 0x00 → plain feature, nothing set.
        let c = CellCategory::decode(0x00);
        assert_eq!(c.block_mode, 0);
        assert_eq!(c.block_type, 0);
        assert!(!c.seamless_play);
        assert!(!c.interleaved);
        assert!(c.is_plain_feature());
        assert!(!c.is_secondary_block_piece());

        // block_mode=1 (first cell of block), block_type=1 (angle block):
        // 0b0101_0000 = 0x50. This is the angle we KEEP — not secondary.
        let c = CellCategory::decode(0b0101_0000);
        assert_eq!(c.block_mode, 1);
        assert_eq!(c.block_type, 1);
        assert!(!c.is_plain_feature());
        assert!(!c.is_secondary_block_piece());

        // block_mode=2 (in block) / 3 (last of block) of an angle block
        // (block_type=1) → secondary.
        assert!(CellCategory::decode(0b1001_0000).is_secondary_block_piece());
        assert!(CellCategory::decode(0b1101_0000).is_secondary_block_piece());
        // First cell of the block (block_mode=1) is NEVER secondary.
        assert!(!CellCategory::decode(0b0101_0000).is_secondary_block_piece());

        // The low flags (seamless_play bit3, interleaved bit2, stc bit1,
        // seamless_angle bit0) on an otherwise-plain cell must NOT make it
        // secondary — they don't mark non-feature content.
        let c = CellCategory::decode(0b0000_1111);
        assert!(c.seamless_play);
        assert!(c.interleaved);
        assert!(c.stc_discontinuity);
        assert!(c.seamless_angle);
        assert!(c.is_plain_feature());
        assert!(!c.is_secondary_block_piece());
    }

    /// A normal single-angle feature (every cell category 0x00) is never
    /// filtered: feature_start_cell == 0, feature_cells == all cells. This is
    /// the "Silence of the Lambs" case — the filter must be a no-op.
    #[test]
    fn feature_filter_noop_on_plain_feature() {
        let t = DvdTitle {
            chapters: 3,
            duration_secs: 6780.0,
            cells: vec![
                cell(0, 99, 0x00),
                cell(100, 199, 0x00),
                cell(200, 299, 0x00),
            ],
            chapter_times: vec![0.0, 100.0, 200.0],
            palette: None,
        };
        assert_eq!(t.feature_start_cell(), 0);
        assert_eq!(t.feature_cells().len(), 3);
    }

    /// A leading interleaved/angle-block sub-cell (category marks a secondary
    /// block piece) is dropped; the scan stops at the first plain cell and
    /// keeps the rest.
    #[test]
    fn feature_filter_drops_leading_secondary_block_cells() {
        let t = DvdTitle {
            chapters: 2,
            duration_secs: 100.0,
            cells: vec![
                cell(0, 9, 0b1001_0000),   // in-block cell of angle block → drop
                cell(10, 19, 0b1101_0000), // last cell of angle block → drop
                cell(20, 119, 0x00),       // feature starts here
                cell(120, 219, 0x00),
            ],
            chapter_times: vec![0.0, 50.0],
            palette: None,
        };
        assert_eq!(t.feature_start_cell(), 2);
        let fc = t.feature_cells();
        assert_eq!(fc.len(), 2);
        assert_eq!(fc[0].first_sector, 20);
    }

    /// Conservative guard: if EVERY cell looks like a secondary block piece
    /// (corrupt/pathological category bytes), the filter refuses to drop them
    /// all — it returns 0 and keeps every cell rather than emit an empty
    /// feature.
    #[test]
    fn feature_filter_never_empties_title() {
        let t = DvdTitle {
            chapters: 1,
            duration_secs: 100.0,
            cells: vec![cell(0, 9, 0b1001_0000), cell(10, 19, 0b1101_0000)],
            chapter_times: vec![0.0],
            palette: None,
        };
        assert_eq!(t.feature_start_cell(), 0);
        assert_eq!(t.feature_cells().len(), 2);
    }

    /// An empty title (no cells) returns 0 and an empty slice — no panic.
    #[test]
    fn feature_filter_empty_cells() {
        let t = DvdTitle {
            chapters: 0,
            duration_secs: 0.0,
            cells: vec![],
            chapter_times: vec![],
            palette: None,
        };
        assert_eq!(t.feature_start_cell(), 0);
        assert!(t.feature_cells().is_empty());
    }

    /// parse_pgc populates the new `category` + `duration_secs` cell fields
    /// from `cell_playback + 0` and the BCD time at `cell_playback + 4`.
    #[test]
    fn pgc_reads_cell_category_and_duration() {
        let mut pgc = vec![0u8; 0xEA];
        pgc[0x02] = 1;
        pgc[0x03] = 2; // 2 cells
        pgc[0xE8] = 0x00;
        pgc[0xE9] = 0xEA;
        pgc.resize(0xEA + 48, 0);
        // Cell 0: category byte = 0x90 (in-block cell of angle block), 5s BCD.
        pgc[0xEA] = 0x90;
        pgc[0xEA + 6] = 0x05;
        pgc[0xEA + 8..0xEA + 12].copy_from_slice(&10u32.to_be_bytes());
        // Cell 1: category 0x00 (plain feature), 7s BCD.
        pgc[0xEA + 24] = 0x00;
        pgc[0xEA + 24 + 6] = 0x07;
        pgc[0xEA + 24 + 8..0xEA + 24 + 12].copy_from_slice(&20u32.to_be_bytes());
        let title = parse_pgc(&pgc, 0, 2).unwrap();
        assert_eq!(title.cells[0].category, 0x90);
        assert!((title.cells[0].duration_secs - 5.0).abs() < 0.01);
        assert_eq!(title.cells[1].category, 0x00);
        assert!((title.cells[1].duration_secs - 7.0).abs() < 0.01);
        // The leading secondary-block cell is filtered out of the feature.
        assert_eq!(title.feature_start_cell(), 1);
    }

    /// Regression: a crafted IFO whose program-map byte names a first_cell
    /// index larger than the actual cell count must NOT panic. Before the fix,
    /// `cell_durations[..first_cell.saturating_sub(1)]` would panic with an
    /// out-of-bounds slice index when first_cell > cell_durations.len().
    ///
    /// Layout: 1 real cell, but the program map byte is 0xFF (255) — an
    /// attacker-controlled value that exceeds the cell_durations Vec length.
    /// Expected: parse_pgc returns Ok (the clamped sum is simply the full
    /// cell duration) without panicking.
    #[test]
    fn pgc_program_map_oob_cell_index_no_panic() {
        let mut pgc = vec![0u8; 0xEA];
        pgc[0x02] = 1; // nr_programs = 1
        pgc[0x03] = 1; // nr_cells = 1

        // program map offset at PGC+0xE6 (u16 BE) → right after the header
        let pgm_off: u16 = 0xEA;
        pgc[0xE6] = (pgm_off >> 8) as u8;
        pgc[0xE7] = pgm_off as u8;

        // cell playback offset at PGC+0xE8 → after the 1-byte program map
        let cell_off: u16 = 0xEA + 1;
        pgc[0xE8] = (cell_off >> 8) as u8;
        pgc[0xE9] = cell_off as u8;

        // Allocate space: 1 program-map byte + 1 cell × 24 bytes
        pgc.resize(cell_off as usize + 24, 0);

        // Craft: program 0's first_cell = 0xFF (255) — far past the 1 real cell
        pgc[0xEA] = 0xFF;

        // Cell 0 duration = 10s (BCD seconds byte at cell_base + 6)
        pgc[cell_off as usize + 6] = 0x10; // BCD 0x10 = 10 seconds

        // Must return Ok; must not panic.
        let title = parse_pgc(&pgc, 0, 1).unwrap();
        // With first_cell=255, end = min(254, 1) = 1, so chapter_times[0] = dur(cell0) = 10s.
        assert_eq!(title.chapter_times.len(), 1);
        assert!(
            (title.chapter_times[0] - 10.0).abs() < 0.01,
            "got {}",
            title.chapter_times[0]
        );
    }

    // ─────────────────────────────────────────────────────────────────────
    // Added: cell-category bit isolation, palette/program-map arithmetic,
    // and the malformed-language salvage paths.
    // ─────────────────────────────────────────────────────────────────────

    /// Each low flag of the cell-category byte comes from its OWN bit and no
    /// other: bit3 seamless_play, bit2 interleaved, bit1 stc_discontinuity,
    /// bit0 seamless_angle (DVD-Video cell playback `cell_category`).
    /// Setting one bit must leave all three siblings clear.
    #[test]
    fn cell_category_low_flags_are_bit_isolated() {
        let c = CellCategory::decode(0x00);
        assert_eq!(
            [
                c.seamless_play,
                c.interleaved,
                c.stc_discontinuity,
                c.seamless_angle
            ],
            [false, false, false, false],
            "category 0x00 sets no flag"
        );
        for bit in 0..4u8 {
            let c = CellCategory::decode(1u8 << bit);
            assert_eq!(
                [
                    c.seamless_play,
                    c.interleaved,
                    c.stc_discontinuity,
                    c.seamless_angle
                ],
                [bit == 3, bit == 2, bit == 1, bit == 0],
                "only bit {bit} may be set"
            );
        }
    }

    /// `is_plain_feature` requires BOTH block_mode and block_type to be zero.
    /// A cell inside a block is not plain feature content even when the other
    /// field happens to be zero.
    #[test]
    fn is_plain_feature_requires_both_block_fields_zero() {
        assert!(CellCategory::decode(0x00).is_plain_feature());
        // block_type = 1, block_mode = 0
        assert!(!CellCategory::decode(0b0001_0000).is_plain_feature());
        // block_mode = 1, block_type = 0
        assert!(!CellCategory::decode(0b0100_0000).is_plain_feature());
        assert!(!CellCategory::decode(0b0101_0000).is_plain_feature());
    }

    /// be_u32 reads four CONSECUTIVE big-endian bytes from `offset`.
    /// All four values are distinct so a repeated or skipped index shows up.
    #[test]
    fn be_u32_reads_four_consecutive_be_bytes() {
        let data = [0xFFu8, 0x01, 0x02, 0x03, 0x04, 0xFF];
        assert_eq!(be_u32(&data, 1).unwrap(), 0x0102_0304);
    }

    // ── malformed language codes ─────────────────────────────────────────

    /// The ISO 639 language code in the audio/subtitle attribute block is
    /// two bytes at +2. Only a pair of lowercase a-z bytes is taken verbatim;
    /// anything else falls through to the ASCII-alphanumeric salvage, which
    /// keeps only the usable characters. A byte outside a-z must never end up
    /// in the raw salvaged string. This exercises `parse_raw_dvd_lang_bytes`
    /// directly — the byte-level salvage step — separately from the ISO
    /// 639-1 -> 639-2 mapping `parse_audio_attr`/`parse_subtitle_attr` apply
    /// on top (see `dvd_two_letter_and_malformed_language_becomes_iso639_2`).
    #[test]
    fn language_code_rejects_non_lowercase_bytes() {
        // (byte0, byte1, expected raw salvage)
        let cases: [(u8, u8, &str); 8] = [
            (b'e', b'n', "en"), // both in range → verbatim
            (0x21, b'n', "n"),  // '!' is below 'a'
            (0x7B, b'n', "n"),  // '{' is above 'z'
            (b'e', 0x21, "e"),  // second byte below 'a'
            (b'e', 0x7B, "e"),  // second byte above 'z'
            (b'E', 0x00, "E"),  // only the second byte is zero
            (0x00, b'E', "E"),  // only the first byte is zero
            (0x00, 0x00, ""),   // both zero → unset
        ];
        for (b0, b1, want) in cases {
            assert_eq!(
                parse_raw_dvd_lang_bytes(&[b0, b1]),
                want,
                "raw language salvage for ({b0:#04x}, {b1:#04x})"
            );
        }
    }

    /// The full pipeline `parse_audio_attr`/`parse_subtitle_attr` apply on
    /// top of the raw salvage: a valid ISO 639-1 code maps to its ISO 639-2
    /// equivalent, and anything the raw salvage does NOT produce a mapped
    /// code for (empty, or a single leftover letter from a malformed byte
    /// pair) degrades to "und" — never an invalid 2-letter/1-letter code and
    /// never an empty string, both of which are illegal Matroska `Language`
    /// element values (RFC 9559 §12).
    #[test]
    fn dvd_two_letter_and_malformed_language_becomes_iso639_2() {
        // (byte0, byte1, expected final language)
        let cases: [(u8, u8, &str); 4] = [
            (b'e', b'n', "eng"), // valid ISO 639-1 → mapped
            (0x21, b'n', "und"), // malformed → salvage "n", unmapped → und
            (b'E', 0x00, "und"), // malformed → salvage "E", unmapped → und
            (0x00, 0x00, "und"), // unspecified → und
        ];
        for (b0, b1, want) in cases {
            let mut audio = vec![0u8; 8];
            audio[2] = b0;
            audio[3] = b1;
            assert_eq!(
                parse_audio_attr(&audio, 0).unwrap().language,
                want,
                "audio language for ({b0:#04x}, {b1:#04x})"
            );

            let mut sub = vec![0u8; 6];
            sub[2] = b0;
            sub[3] = b1;
            assert_eq!(
                parse_subtitle_attr(&sub, 0).unwrap().language,
                want,
                "subtitle language for ({b0:#04x}, {b1:#04x})"
            );
        }
    }

    // ── PGC fixtures ─────────────────────────────────────────────────────

    /// Build a standalone PGC starting at offset 0.
    ///
    /// Layout: 0xEA-byte header, then the program map (one byte per program,
    /// the 1-based first cell number), then the 24-byte cell playback table.
    /// `cells` are `(category, BCD time, first_sector, last_sector)`.
    fn build_pgc(
        pgc_time: [u8; 4],
        cells: &[(u8, [u8; 4], u32, u32)],
        programs: &[u8],
        palette: Option<[[u8; 4]; 16]>,
    ) -> Vec<u8> {
        let mut d = vec![0u8; 0xEA];
        d[0x02] = programs.len() as u8;
        d[0x03] = cells.len() as u8;
        d[0x04..0x08].copy_from_slice(&pgc_time);
        if let Some(p) = palette {
            for (i, c) in p.iter().enumerate() {
                d[0xA4 + i * 4..0xA4 + i * 4 + 4].copy_from_slice(c);
            }
        }
        let pgm_off = 0xEAusize;
        let cell_off = pgm_off + programs.len();
        if !programs.is_empty() {
            d[0xE6..0xE8].copy_from_slice(&(pgm_off as u16).to_be_bytes());
        }
        if !cells.is_empty() {
            d[0xE8..0xEA].copy_from_slice(&(cell_off as u16).to_be_bytes());
        }
        d.extend_from_slice(programs);
        for &(cat, time, first, last) in cells {
            let mut c = vec![0u8; 24];
            c[0] = cat;
            c[4..8].copy_from_slice(&time);
            c[8..12].copy_from_slice(&first.to_be_bytes());
            c[20..24].copy_from_slice(&last.to_be_bytes());
            d.extend_from_slice(&c);
        }
        d
    }

    /// BCD playback time of `secs` TIMECODE seconds (< 60) at the NTSC rate
    /// flag.
    ///
    /// The argument is timecode, not real time: every fixture built from this
    /// helper carries the 0b11 flag, so `secs` timecode seconds are
    /// `secs * 30` frames and last `secs * 1.001` real seconds. Expectations
    /// below are written in real seconds and so read 1.001x the argument —
    /// `bcd_secs(10)` is 10.01 s, not 10 s.
    fn bcd_secs(secs: u8) -> [u8; 4] {
        assert!(secs < 60);
        [0, 0, ((secs / 10) << 4) | (secs % 10), 0b11_000000]
    }

    /// When the PGC-level playback time is zero, the duration is recomputed
    /// as the SUM of every cell's own BCD time, each read from its own
    /// 24-byte cell playback record at +4.
    #[test]
    fn pgc_zero_duration_recomputed_as_sum_of_cell_times() {
        let pgc = build_pgc(
            [0, 0, 0, 0],
            &[
                (0x00, bcd_secs(10), 100, 199),
                (0x00, bcd_secs(20), 200, 299),
                (0x00, bcd_secs(31), 300, 399),
            ],
            &[],
            None,
        );
        let title = parse_pgc(&pgc, 0, 3).unwrap();
        assert_eq!(title.cells.len(), 3);
        assert_eq!(title.cells[0].duration_secs, 10.01);
        assert_eq!(title.cells[1].duration_secs, 20.02);
        assert_eq!(title.cells[2].duration_secs, 31.031);
        assert!(
            (title.duration_secs - 61.061).abs() < 1e-9,
            "recomputed duration must be the sum of the distinct cell times, got {}",
            title.duration_secs
        );
    }

    /// Chapter times come from the program map: each program's byte is the
    /// 1-based number of its first cell, and the chapter time is the sum of
    /// the durations of every cell BEFORE it. Distinct cell durations mean a
    /// misread program byte or a mis-strided cell table changes the result.
    #[test]
    fn pgc_chapter_times_sum_preceding_cell_durations() {
        let pgc = build_pgc(
            bcd_secs(59),
            &[
                (0x00, bcd_secs(10), 0, 9),
                (0x00, bcd_secs(20), 10, 19),
                (0x00, bcd_secs(31), 20, 29),
            ],
            &[1, 2, 3],
            None,
        );
        let title = parse_pgc(&pgc, 0, 3).unwrap();
        assert_eq!(title.chapter_times, vec![0.0, 10.01, 30.03]);
    }

    /// A program map offset of 0 means there is no program map, so no chapter
    /// times may be produced — the PGC header must not be read as one. Same
    /// for a cell playback offset of 0.
    #[test]
    fn pgc_absent_program_map_or_cell_table_yields_no_chapter_times() {
        // programs declared, but pgm_map_offset patched to 0
        let mut pgc = build_pgc(
            bcd_secs(30),
            &[(0x00, bcd_secs(10), 0, 9), (0x00, bcd_secs(20), 10, 19)],
            &[1, 2],
            None,
        );
        pgc[0xE6..0xE8].copy_from_slice(&0u16.to_be_bytes());
        let title = parse_pgc(&pgc, 0, 2).unwrap();
        assert!(
            title.chapter_times.is_empty(),
            "no program map → no chapter times, got {:?}",
            title.chapter_times
        );

        // program map present, but cell_playback_offset patched to 0
        let mut pgc = build_pgc(
            bcd_secs(30),
            &[(0x00, bcd_secs(10), 0, 9), (0x00, bcd_secs(20), 10, 19)],
            &[1, 2],
            None,
        );
        pgc[0xE8..0xEA].copy_from_slice(&0u16.to_be_bytes());
        let title = parse_pgc(&pgc, 0, 2).unwrap();
        assert!(title.cells.is_empty());
        assert!(
            title.chapter_times.is_empty(),
            "no cell table → no chapter times, got {:?}",
            title.chapter_times
        );
    }

    /// A declared cell count larger than the cell table actually holds must
    /// not read past the end while collecting durations for the chapter-time
    /// calculation; the missing cells contribute zero.
    #[test]
    fn pgc_cell_count_overshoot_does_not_read_past_end() {
        let mut pgc = build_pgc(
            bcd_secs(30),
            &[(0x00, bcd_secs(10), 0, 9), (0x00, bcd_secs(20), 10, 19)],
            &[1, 2],
            None,
        );
        pgc[0x03] = 8; // declare 8 cells; only 2 records exist
        let title = parse_pgc(&pgc, 0, 2).unwrap();
        assert_eq!(title.cells.len(), 2, "only the readable cells are kept");
        // Program 2 starts at cell 2, so its time is cell 0's duration.
        assert_eq!(title.chapter_times, vec![0.0, 10.01]);
    }

    /// A program map that runs past the end of the data must stop at the
    /// buffer end rather than reading past it. The fixture's map begins at
    /// PGC+0xEA and the buffer holds 50 bytes from there, so a declared
    /// count of 255 programs must yield exactly 50 chapter times.
    #[test]
    fn pgc_program_map_past_end_stops() {
        let mut pgc = build_pgc(
            bcd_secs(30),
            &[(0x00, bcd_secs(10), 0, 9), (0x00, bcd_secs(20), 10, 19)],
            &[1, 2],
            None,
        );
        pgc[0x02] = 255; // declare 255 programs
        let available = pgc.len() - 0xEA;
        assert_eq!(available, 50, "fixture: 2 map bytes + 2 cells of 24");
        let title = parse_pgc(&pgc, 0, 2).unwrap();
        assert_eq!(
            title.chapter_times.len(),
            available,
            "the program map walk must stop exactly at the buffer end"
        );
        assert_eq!(title.chapter_times[0], 0.0);
        assert_eq!(title.chapter_times[1], 10.01);
    }

    /// The subtitle palette is 16 entries of 4 bytes at PGC+0xA4, each
    /// `[padding, Y, Cr, Cb]`. Every byte of every entry is distinct here, so
    /// a wrong stride, a wrong base or a shifted component shows up.
    #[test]
    // The loop variable is the DOMAIN VALUE being checked (a palette entry number), not a
    // cursor into a collection: it is what the assertion message names and
    // what the table is keyed by. `.iter().enumerate()` would rename the
    // thing under test to `i` and read worse.
    #[allow(clippy::needless_range_loop)]
    fn pgc_palette_entries_read_at_correct_stride() {
        let mut pal = [[0u8; 4]; 16];
        for (i, c) in pal.iter_mut().enumerate() {
            let b = i as u8;
            *c = [0x10 + b, 0x30 + b, 0x50 + b, 0x70 + b];
        }
        let pgc = build_pgc(bcd_secs(30), &[], &[], Some(pal));
        let title = parse_pgc(&pgc, 0, 1).unwrap();
        let got = title.palette.expect("palette present");
        assert_eq!(got.len(), 16);
        for i in 0..16 {
            let b = i as u8;
            assert_eq!(
                got[i],
                [0x10 + b, 0x30 + b, 0x50 + b, 0x70 + b],
                "palette entry {i}"
            );
        }
    }

    /// A palette is "present" when ANY of Y, Cr or Cb is non-zero in ANY
    /// entry — a single non-zero chroma component is enough. Only the
    /// padding byte [0] is ignored.
    #[test]
    fn pgc_palette_present_on_any_single_nonzero_component() {
        for comp in 1..4usize {
            let mut pal = [[0u8; 4]; 16];
            pal[7][comp] = 0x40; // exactly one non-zero component, in one entry
            let pgc = build_pgc(bcd_secs(30), &[], &[], Some(pal));
            let title = parse_pgc(&pgc, 0, 1).unwrap();
            assert!(
                title.palette.is_some(),
                "component {comp} alone must mark the palette present"
            );
        }
        // Only the padding byte set → still empty.
        let mut pal = [[0u8; 4]; 16];
        for c in pal.iter_mut() {
            c[0] = 0xFF;
        }
        let pgc = build_pgc(bcd_secs(30), &[], &[], Some(pal));
        assert!(parse_pgc(&pgc, 0, 1).unwrap().palette.is_none());
    }

    // ── PGCIT ────────────────────────────────────────────────────────────

    /// Build a VTS_PGCIT at offset 0: VTS_PGC_Ns(2) + reserved(2) +
    /// VTS_PGCIT_EA(4), then one 8-byte VTS_PGCI_SRP per PGC
    /// (VTS_PGC_CAT(4) + VTS_PGCI_SA(4), the PGC's byte offset from the
    /// VTS_PGCIT start). `pgcs` are appended after the SRP table.
    fn build_pgcit(pgcs: &[Vec<u8>]) -> Vec<u8> {
        let mut d = vec![0u8; 8 + pgcs.len() * 8];
        d[0..2].copy_from_slice(&(pgcs.len() as u16).to_be_bytes());
        let mut off = d.len();
        for (i, p) in pgcs.iter().enumerate() {
            let e = 8 + i * 8;
            d[e + 4..e + 8].copy_from_slice(&(off as u32).to_be_bytes());
            off += p.len();
        }
        for p in pgcs {
            d.extend_from_slice(p);
        }
        d
    }

    /// Each VTS_PGCI_SRP is 8 bytes and the PGC start address is the second
    /// word of its own entry, so title N must resolve to PGC N. Two PGCs
    /// with distinct durations catch an entry read at the wrong stride or
    /// from the wrong entry.
    #[test]
    fn pgcit_entry_stride_selects_the_right_pgc() {
        let pgc0 = build_pgc(bcd_secs(11), &[(0x00, bcd_secs(11), 0, 9)], &[], None);
        let pgc1 = build_pgc(bcd_secs(22), &[(0x00, bcd_secs(22), 50, 59)], &[], None);
        let data = build_pgcit(&[pgc0, pgc1]);

        // vts_title_num is 1-based: title 2 → PGC index 1.
        let titles = parse_pgcit(&data, 0, &[(5, 2)]).unwrap();
        assert_eq!(titles.len(), 1);
        assert_eq!(titles[0].duration_secs, 22.022);
        assert_eq!(titles[0].cells[0].first_sector, 50);

        let titles = parse_pgcit(&data, 0, &[(5, 1)]).unwrap();
        assert_eq!(titles.len(), 1);
        assert_eq!(titles[0].duration_secs, 11.011);
        assert_eq!(titles[0].cells[0].first_sector, 0);

        // Both titles, in order.
        let titles = parse_pgcit(&data, 0, &[(5, 1), (7, 2)]).unwrap();
        assert_eq!(titles.len(), 2);
        assert_eq!(titles[0].duration_secs, 11.011);
        assert_eq!(titles[1].duration_secs, 22.022);
    }

    /// A VTS_PGCIT whose 8-byte header ends exactly at the end of the data is
    /// a complete header with no SRP entries: an empty title list, not an
    /// error. Below 8 bytes the header itself is truncated → IfoParse.
    #[test]
    fn pgcit_header_boundary() {
        let data = vec![0u8; 8];
        let titles = parse_pgcit(&data, 0, &[(5, 1)]).expect("complete header parses");
        assert!(titles.is_empty());
        for len in 0..8usize {
            assert!(
                parse_pgcit(&vec![0u8; len], 0, &[(5, 1)]).is_err(),
                "len={len}"
            );
        }
    }

    /// An SRP entry that runs past the end of the data is SKIPPED, leaving
    /// an empty title list — a truncated entry table must not turn into a
    /// parse error for the whole PGCIT.
    #[test]
    fn pgcit_entry_past_end_is_skipped_not_an_error() {
        // 8-byte header declaring 3 PGCs, then only 12 bytes of table: the
        // entry for PGC index 2 (offset 24..32) is entirely past the end.
        let mut data = vec![0u8; 20];
        data[0..2].copy_from_slice(&3u16.to_be_bytes());
        let titles = parse_pgcit(&data, 0, &[(5, 3)])
            .expect("a truncated SRP entry is skipped, not an error");
        assert!(titles.is_empty());
    }

    /// A TT_SRP entry is 12 bytes: playback_type(1) + angles(1) +
    /// number_of_PTTs(2) + parental_mask(2) + VTSN(1) + VTS_TTN(1) +
    /// VTS_start_sector(4). The chapter count is the 16-bit field at +2, and
    /// each entry's own value must be carried through. Distinct counts per
    /// entry catch a read from a neighbouring offset.
    #[test]
    fn tt_srpt_chapter_count_read_from_entry_offset_2() {
        let data = tt_srpt_bytes(3, &[(7, 1, 1), (13, 1, 2), (0x0102, 2, 1)]);
        let map = parse_tt_srpt(&data, 0).unwrap();
        assert_eq!(map[&1], vec![(7u16, 1u8), (13, 2)]);
        assert_eq!(map[&2], vec![(0x0102u16, 1u8)]);
    }

    // ── TT_SRPT: the title table (`parse_tt_srpt` / `parse_vmg`) ────────────
    //
    // This is the table that says which titles a DVD has, and it had no tests
    // at all until 1.6.2 — while being at the centre of both DVD stories in
    // 1.6.1. The CSS descramble bug destroyed the sector carrying it, so a disc
    // enumerating 38 titles produced an image enumerating 10, silently, at exit
    // 0. And `dir://` was pulled from the release on 2026-08-05 because a
    // folder produced the wrong title list — the same table, reached through a
    // synthesized image.
    //
    // Both were found on real discs. Nothing hermetic covered the table itself.

    /// Build a VMG with a TT_SRPT at `tt_srpt_sector` declaring `entries`
    /// of `(num_chapters, vts_number, vts_title_num)`.
    fn vmg_with_tt_srpt(tt_srpt_sector: u32, entries: &[(u16, u8, u8)]) -> Vec<u8> {
        let off = tt_srpt_sector as usize * crate::consts::SECTOR_BYTES;
        let mut v = vec![0u8; off + 8 + entries.len() * 12 + 16];
        v[0..12].copy_from_slice(b"DVDVIDEO-VMG");
        v[0xC4..0xC8].copy_from_slice(&tt_srpt_sector.to_be_bytes());
        v[off..off + 2].copy_from_slice(&(entries.len() as u16).to_be_bytes());
        for (i, &(chapters, vts, title)) in entries.iter().enumerate() {
            let b = off + 8 + i * 12;
            v[b + 2..b + 4].copy_from_slice(&chapters.to_be_bytes());
            v[b + 6] = vts;
            v[b + 7] = title;
        }
        v
    }

    #[test]
    fn tt_srpt_groups_titles_by_their_title_set() {
        // Three titles: two in VTS 1, one in VTS 2 — the ordinary shape.
        let vmg = vmg_with_tt_srpt(1, &[(12, 1, 1), (5, 1, 2), (20, 2, 1)]);
        let map = parse_tt_srpt(&vmg, crate::consts::SECTOR_BYTES).expect("parse");
        assert_eq!(map.len(), 2, "two title sets");
        assert_eq!(
            map[&1],
            vec![(12, 1), (5, 2)],
            "VTS 1 keeps both titles in order"
        );
        assert_eq!(map[&2], vec![(20, 1)], "VTS 2 keeps its one");
    }

    /// The pointer at 0xC4 is a SECTOR offset from the start of VIDEO_TS.IFO,
    /// not a byte offset. Reading it as bytes would find the table at 1/2048th
    /// of its real position — which is a zero-filled region on most discs, so
    /// it would report a disc with no titles rather than fail loudly.
    #[test]
    fn tt_srpt_pointer_is_a_sector_offset_not_a_byte_offset() {
        for sector in [1u32, 2, 5] {
            let vmg = vmg_with_tt_srpt(sector, &[(3, 1, 1)]);
            let at_sector = parse_tt_srpt(&vmg, sector as usize * crate::consts::SECTOR_BYTES)
                .expect("reading at the sector offset works");
            assert_eq!(at_sector[&1], vec![(3, 1)], "sector {sector}");
            // Reading the same pointer as a BYTE offset lands 1/2048th of the
            // way in — inside the header here — and yields a different table.
            // It does not error: it silently reports the wrong titles, which is
            // how 28 dropped titles looked like a scan difference rather than a
            // failure.
            let as_bytes = parse_tt_srpt(&vmg, sector as usize).expect("does not error");
            assert_ne!(
                as_bytes, at_sector,
                "sector {sector}: a byte-offset read must not coincidentally \
                 agree with the correct one, or this test proves nothing"
            );
        }
    }

    /// A VTS number of 0 is not a title set; entries carrying it are padding.
    #[test]
    fn tt_srpt_skips_entries_with_no_title_set() {
        let vmg = vmg_with_tt_srpt(1, &[(9, 0, 1), (4, 3, 1)]);
        let map = parse_tt_srpt(&vmg, crate::consts::SECTOR_BYTES).expect("parse");
        assert_eq!(map.len(), 1, "the vts=0 entry is dropped");
        assert_eq!(map[&3], vec![(4, 1)]);
    }

    /// The same (title set, title) twice is one title, not two. Real discs
    /// carry duplicate TT_SRPT rows.
    #[test]
    fn tt_srpt_collapses_a_duplicate_title() {
        let vmg = vmg_with_tt_srpt(1, &[(7, 2, 1), (7, 2, 1), (8, 2, 2)]);
        let map = parse_tt_srpt(&vmg, crate::consts::SECTOR_BYTES).expect("parse");
        assert_eq!(
            map[&2],
            vec![(7, 1), (8, 2)],
            "the repeat is dropped, the distinct one kept"
        );
    }

    /// A truncated table parses what is there rather than failing the whole
    /// disc — one unreadable row should not cost a user every other title.
    #[test]
    fn tt_srpt_truncated_keeps_the_entries_it_can_read() {
        let mut vmg = vmg_with_tt_srpt(1, &[(1, 1, 1), (2, 2, 1), (3, 3, 1)]);
        vmg.truncate(crate::consts::SECTOR_BYTES + 8 + 12 + 6); // 1 whole entry + a fragment
        let map = parse_tt_srpt(&vmg, crate::consts::SECTOR_BYTES).expect("parse");
        assert_eq!(map.len(), 1, "the complete entry survives");
        assert_eq!(map[&1], vec![(1, 1)]);
    }

    /// The count is an untrusted u16 off the disc. DVD-Video caps a disc at 99
    /// titles, and each entry re-parses a PGC into a full DvdTitle — so an
    /// unclamped 65535 is a ~540 MB allocation from a ~800 KB crafted file.
    #[test]
    fn tt_srpt_clamps_an_absurd_declared_title_count() {
        // The fixture must actually CONTAIN more entries than the cap, or the
        // walk stops when the buffer runs out and the clamp is never what
        // bounded it. An earlier version of this test declared u16::MAX over a
        // two-kilobyte fixture: the loop broke on `base + 12 > len` after three
        // iterations and the assertion held with the clamp deleted entirely.
        const PRESENT: usize = MAX_TT_SRPT_TITLES + 40;
        let entries: Vec<(u16, u8, u8)> = (0..PRESENT)
            .map(|i| (1u16, 1u8, (i % 250 + 1) as u8))
            .collect();
        let mut vmg = vmg_with_tt_srpt(1, &entries);
        let off = crate::consts::SECTOR_BYTES;
        vmg[off..off + 2].copy_from_slice(&u16::MAX.to_be_bytes());

        let map = parse_tt_srpt(&vmg, off).expect("parse");
        let total: usize = map.values().map(|v| v.len()).sum();
        assert!(
            total <= MAX_TT_SRPT_TITLES,
            "a declared count of u16::MAX over {PRESENT} real entries must be \
             clamped to {MAX_TT_SRPT_TITLES}, got {total}"
        );
    }
}
