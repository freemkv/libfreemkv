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
    /// Chapter names from the VMG text data, keyed by (vts_number, vts_title_num) and
    /// indexed by 0-based chapter; `None` where the disc names no chapter.
    pub chapter_names: std::collections::BTreeMap<(u8, u8), Vec<Option<String>>>,
    /// VMGI_MAT byte 0x23, the second byte of VMG_CATEGORY (0x22): bit n set means the disc
    /// does NOT play in region n+1 (libdvdread ifo_print.c; mpucoder "byte1=prohibited region
    /// mask"). 0x00 is region-free.
    pub region_mask: u8,
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
    /// PGC_AST_CTL (PGC+0x0C): per logical audio stream, its presence bit (15) and
    /// physical stream number (bits 10-8).
    pub ast_ctl: [u16; 8],
    /// PGC_SPST_CTL (PGC+0x1C): per logical subpicture stream, its presence bit
    /// and physical sub-stream ids for 4:3 / wide / letterbox / pan-scan.
    pub spst_ctl: [u32; 32],
    /// This title's 1-based `vts_title_num` (TTN within its VTS). Carried so nav
    /// resolution joins on the REAL title number rather than the position in the
    /// titles vec, which desyncs when a sibling PGC is dropped as unparseable.
    pub vts_title_num: u8,
    /// Distinct PGCs this title's part-of-title entries point at. Only the first is read,
    /// so a title with more is missing the rest.
    pub pgcs: usize,
    /// The program (PGN) each chapter (part of title) starts at, from VTS_PTT_SRPT. `None`
    /// when the table does not hold the title; empty when a chapter lies in another PGC.
    pub ptt_programs: Option<Vec<u16>>,
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
    /// bit 3: seamless playback linked in PCI (not a statement that the STC continues).
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

// Count of leading secondary angle-block cells (see `DvdTitle::feature_start_cell`).
fn leading_secondary_cells(cells: &[DvdCell]) -> usize {
    let n = cells.len();
    let mut idx = 0;
    while idx < n {
        // Stop at the first cell that is genuine feature content.
        if !CellCategory::decode(cells[idx].category).is_secondary_block_piece() {
            break;
        }
        idx += 1;
    }
    // Never drop everything: if every leading cell looked like a secondary
    // block piece (pathological/corrupt category bytes), keep all cells.
    if idx >= n { 0 } else { idx }
}

impl DvdTitle {
    /// Index of the first cell to include in the muxed feature.
    ///
    /// Some PGCs open with leading cells that are not part of the movie, identified by a
    /// cell-category flagged as a *secondary* piece of an angle/interleave block
    /// ([`CellCategory::is_secondary_block_piece`]). Returns the index of the first
    /// plain-feature cell; cells before it are dropped from the feature extents. Conservative:
    /// only ever skips a prefix, never past the last cell or to zero cells.
    pub fn feature_start_cell(&self) -> usize {
        leading_secondary_cells(&self.cells)
    }

    /// The feature cells after the leading-cell filter ([`Self::feature_start_cell`]).
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
    /// Coding mode 3: mpucoder IFO "3 Mpeg-2ext"; EP0867877A2 "011b MPEG-2 with extension
    /// bitstream" (mode 2 is "MPEG-1 or MPEG-2 without extension bit stream").
    pub mpeg_ext: bool,
    /// Code extension, byte 5 (libdvdread `audio_attr_t.code_extension`; mpucoder "5 code
    /// extension", SPRM 17): 0 unspecified, 1 normal, 2 visually impaired, 3/4 director's
    /// comments.
    pub code_extension: u8,
}

/// The on-wire VobSub sub-stream id (0x20..=0x3F) of one PGC_SPST_CTL entry, or `None` when
/// the stream is absent from the PGC. 16:9 takes the wide id (the rip keeps the anamorphic
/// frame); 4:3 takes the 4:3 id. Letterbox/pan-scan variants are display-side downscales.
pub(crate) fn subpicture_stream_id(ctl: u32, aspect: DvdAspect) -> Option<u8> {
    // libdvdnav vm_get_subp_stream: "if((vm->state).pgc->subp_control[subpN] & (1u<<31))";
    // mpucoder PGC_SPST_CTL byte 0: "1 = stream available".
    if ctl & 0x8000_0000 == 0 {
        return None;
    }
    let shift = match aspect {
        DvdAspect::R4x3 => 24, // vm_get_subp_stream "/* 4:3 */": "subp_control[subpN] >> 24) & 0x1f"
        DvdAspect::R16x9 => 16, // "mode == 0 - widescreen": "subp_control[subpN] >> 16) & 0x1f"
    };
    // VLC ps.h: "( i_id&0xe0 ) == 0x20 ) /* 0x20 -> 0x3f */" is the subpicture sub-stream.
    Some(0x20 | ((ctl >> shift) & 0x1F) as u8)
}

/// DVD subtitle stream attributes.
#[derive(Debug, Clone)]
pub struct DvdSubtitleAttr {
    pub language: String,
    /// Code extension, byte 5 (libdvdread `subp_attr_t.code_extension`; mpucoder "5 code
    /// extension", SPRM 19): 1 normal, 2 large, 3 children, 5-7 captions, 9 forced, 13-15
    /// director's comments.
    pub code_extension: u8,
}

// ── Constants ───────────────────────────────────────────────────────────────

const VMG_MAGIC: &[u8; 12] = b"DVDVIDEO-VMG";
// VMG_CATEGORY is the u32 at 0x22 (libdvdread vmgi_mat_t); its second byte is the region mask.
const VMG_REGION_MASK_OFFSET: usize = 0x23;
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

// An attribute block's code-extension byte (offset 5 in both the 8-byte audio and 6-byte
// subpicture blocks); 0 "unspecified" when a short test buffer stops before it.
fn code_extension_at(data: &[u8], offset: usize) -> u8 {
    offset
        .checked_add(5)
        .and_then(|o| data.get(o))
        .copied()
        .unwrap_or(0)
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

/// Total non-drop-frame timecode frames in a 4-byte DVD BCD time
/// (`[hours_bcd, minutes_bcd, seconds_bcd, rate_and_frames]`; byte 3 packs
/// the frame-rate flag in bits 7-6 and the frame count in bits 5-0).
///
/// `dvd_time_t` is a timecode, not elapsed wall-clock time.
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

/// Read-and-parse convenience over [`parse_vmg_with`] (reads VIDEO_TS.IFO
/// itself). Retained for the diagnostic image tests; production callers reuse
/// already-read bytes via `parse_vmg_with`.
#[cfg(test)]
pub fn parse_vmg(reader: &mut dyn SectorSource, udf: &UdfFs) -> Result<DvdInfo> {
    parse_vmg_with(reader, udf, None)
}

// Parses VIDEO_TS.IFO + all VTS_XX_0.IFO into a DvdInfo. `vmg_bytes`, if
// `Some`, reuses already-read VIDEO_TS.IFO bytes instead of re-reading the
// file; the parse is byte-for-byte identical either way.
pub(crate) fn parse_vmg_with(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    vmg_bytes: Option<&[u8]>,
) -> Result<DvdInfo> {
    let read;
    let vmg_data: &[u8] = match vmg_bytes {
        Some(bytes) => bytes,
        None => {
            read = udf.read_file(reader, "/VIDEO_TS/VIDEO_TS.IFO")?;
            &read
        }
    };

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
    let tt_srpt_sector = be_u32(vmg_data, 0xC4)?;

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

    let title_set_map = parse_tt_srpt(vmg_data, tt_srpt_offset)?;

    // Parse each VTS IFO
    let mut title_sets = Vec::new();
    let mut skipped = 0usize;
    for (&vts_number, titles_info) in &title_set_map {
        match parse_vts(reader, udf, vts_number, titles_info) {
            Ok(ts) => title_sets.push(ts),
            // Halted means the drive itself stopped, not that this title set is a
            // placeholder — once set, every later parse_vts call also fails, so
            // swallowing it here would silently truncate the scan. Propagate it.
            Err(Error::Halted) => return Err(Error::Halted),
            Err(e) => {
                // Some discs carry placeholder TT_SRPT entries for title sets that
                // aren't really there, so one failure isn't fatal — but it must not
                // be silent, since swallowing it can hide many dropped titles.
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
    let chapter_names = parse_txtdt_chapter_names(vmg_data, tt_srpt_offset);
    Ok(DvdInfo {
        title_sets,
        chapter_names,
        region_mask: vmg_data[VMG_REGION_MASK_OFFSET],
    })
}

// Maximum TT_SRPT entries honoured (DVD-Video's own 99-title cap). The on-disc count is an
// untrusted u16; without this, a crafted IFO could declare 65535 entries and blow up memory
// re-parsing PGCs.
pub(crate) const MAX_TT_SRPT_TITLES: usize = 99;

// Parses the VMG TT_SRPT into a per-title-set map of (chapter_count, vts_title_number). Clamps
// the declared entry count to MAX_TT_SRPT_TITLES and drops duplicate (vts_number,
// vts_title_num) pairs.
pub(crate) fn parse_tt_srpt(
    vmg_data: &[u8],
    tt_srpt_offset: usize,
) -> Result<std::collections::BTreeMap<u8, Vec<(u16, u8)>>> {
    let num_titles = be_u16(vmg_data, tt_srpt_offset)? as usize;
    if num_titles > MAX_TT_SRPT_TITLES {
        tracing::warn!(
            target: "freemkv::scan",
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

        // Both numbers are 1-based; a 0 title number would alias title 1's PGC.
        if vts_number == 0 || vts_title_num == 0 {
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

// ── Text data (chapter names) ───────────────────────────────────────────────

// The text data manager (TXTDT_MG) has no public spec; this layout matches libdvdread's
// partial structs and a disc dump. VMGI 0xD4 is its sector, relative to VIDEO_TS.IFO.
const TXTDT_MG_PTR: usize = 0xD4;
// 12-byte id, u16 reserved, u16 unit count, u32 last byte; then 8-byte unit pointers
// (u16 language, u8 reserved, u8 charset, u32 start byte from the TXTDT_MG).
const TXTDT_MG_HDR: usize = 0x14;
// A unit opens with 204 bytes, then u16 count (two per item), u16 reserved, 8-byte items
// (u8 kind, 5 bytes, u16 offset from the count) and tab-separated strings.
const TXTDT_LU_ITEMS: usize = 204;
const TXTDT_ITEM_TITLE: u8 = 0x02;
const TXTDT_ITEM_CHAPTER: u8 = 0x04;
const TXTDT_CHARSET_ISO646: u8 = 0x01;
const TXTDT_CHARSET_ISO8859_1: u8 = 0x11;
// Longest name accepted; a missing terminator would otherwise swallow the table.
const TXTDT_MAX_NAME: usize = 255;

// Chapter names per TT_SRPT title, mapped to (vts_number, vts_title_num). A title
// whose name count differs from its TT_SRPT chapter count is dropped rather than
// risk naming the wrong chapters. Never fails: absent or bad text data is no names.
fn parse_txtdt_chapter_names(
    vmg: &[u8],
    tt_srpt_offset: usize,
) -> std::collections::BTreeMap<(u8, u8), Vec<Option<String>>> {
    let mut out = std::collections::BTreeMap::new();
    let Some(text_titles) = txtdt_titles(vmg) else {
        return out;
    };
    let declared = be_u16(vmg, tt_srpt_offset).map_or(0, usize::from);
    for (i, names) in text_titles
        .into_iter()
        .enumerate()
        .take(declared.min(MAX_TT_SRPT_TITLES))
    {
        let e = tt_srpt_offset + 8 + i * 12;
        let (Ok(chapters), Ok(vts), Ok(ttn)) =
            (be_u16(vmg, e + 2), byte_at(vmg, e + 6), byte_at(vmg, e + 7))
        else {
            break;
        };
        if names.len() != usize::from(chapters) {
            tracing::debug!(
                target: "freemkv::scan",
                title = i + 1,
                chapters,
                names = names.len(),
                "dvd text data chapter names do not match the title; ignored"
            );
            continue;
        }
        if names.iter().any(Option::is_some) {
            out.entry((vts, ttn)).or_insert(names);
        }
    }
    out
}

// The first language unit's chapter names, grouped by title in text-data order. Every
// read is bounded by the TXTDT_MG's own last byte and the IFO length.
fn txtdt_titles(vmg: &[u8]) -> Option<Vec<Vec<Option<String>>>> {
    let sector = be_u32(vmg, TXTDT_MG_PTR).ok()?;
    if sector == 0 {
        return None;
    }
    let mg = (sector as usize).checked_mul(SECTOR_BYTES)?;
    let last = be_u32(vmg, mg.checked_add(0x10)?).ok()? as usize;
    let end = mg.checked_add(last)?.saturating_add(1).min(vmg.len());
    let data = vmg.get(mg..end)?;
    if be_u16(data, 0x0E).ok()? == 0 {
        return None;
    }
    let charset = byte_at(data, TXTDT_MG_HDR + 3).ok()?;
    let lu = be_u32(data, TXTDT_MG_HDR + 4).ok()? as usize;
    let base = lu.checked_add(TXTDT_LU_ITEMS)?;
    let items = usize::from(be_u16(data, base).ok()?) / 2;

    let mut titles: Vec<Vec<Option<String>>> = Vec::new();
    for i in 0..items {
        let item = base + 4 + i * 8;
        let (Ok(kind), Ok(off)) = (byte_at(data, item), be_u16(data, item + 6)) else {
            break;
        };
        match kind {
            TXTDT_ITEM_TITLE => titles.push(Vec::new()),
            TXTDT_ITEM_CHAPTER => {
                if let Some(t) = titles.last_mut() {
                    t.push(txtdt_name(data, base + usize::from(off), charset));
                }
            }
            _ => {}
        }
    }
    Some(titles)
}

// One tab- or NUL-terminated name. Unknown charsets pass only plain ASCII; empty,
// overlong or control-bearing names are `None` so the caller keeps its fallback.
fn txtdt_name(data: &[u8], at: usize, charset: u8) -> Option<String> {
    // Never look past the longest name: thousands of items can share one unterminated run.
    let raw = data.get(at..)?;
    let raw = &raw[..raw.len().min(TXTDT_MAX_NAME + 1)];
    let raw = &raw[..raw
        .iter()
        .position(|&b| b == b'\t' || b == 0)
        .unwrap_or(raw.len())];
    if raw.len() > TXTDT_MAX_NAME {
        return None;
    }
    let latin1 = matches!(charset, TXTDT_CHARSET_ISO646 | TXTDT_CHARSET_ISO8859_1);
    if !latin1 && !raw.is_ascii() {
        return None;
    }
    let name: String = raw.iter().map(|&b| char::from(b)).collect();
    if name.chars().any(char::is_control) {
        return None;
    }
    let name = name.trim();
    (!name.is_empty()).then(|| name.to_string())
}

// Builds a one-language TXTDT_MG of `(kind, text)` items in the layout parsed above.
#[cfg(test)]
pub(crate) fn build_txtdt_mg(charset: u8, items: &[(u8, &[u8])]) -> Vec<u8> {
    let lu = TXTDT_MG_HDR + 8;
    let base = lu + TXTDT_LU_ITEMS;
    let mut d = vec![0u8; base + 4 + items.len() * 8];
    d[0x0E..0x10].copy_from_slice(&1u16.to_be_bytes());
    d[TXTDT_MG_HDR + 3] = charset;
    d[TXTDT_MG_HDR + 4..TXTDT_MG_HDR + 8].copy_from_slice(&(lu as u32).to_be_bytes());
    d[base..base + 2].copy_from_slice(&(items.len() as u16 * 2).to_be_bytes());
    for (i, (kind, text)) in items.iter().enumerate() {
        let item = base + 4 + i * 8;
        let off = (d.len() - base) as u16;
        d[item] = *kind;
        d[item + 4] = 0x30;
        d[item + 6..item + 8].copy_from_slice(&off.to_be_bytes());
        d.extend_from_slice(text);
        d.push(b'\t');
    }
    let last = (d.len() - 1) as u32;
    d[0x10..0x14].copy_from_slice(&last.to_be_bytes());
    d
}

// Appends `txtdt` to a VMG at the next sector boundary and points 0xD4 at it.
#[cfg(test)]
pub(crate) fn with_txtdt(mut vmg: Vec<u8>, txtdt: &[u8]) -> Vec<u8> {
    let sector = vmg.len().div_ceil(SECTOR_BYTES);
    vmg.resize(sector * SECTOR_BYTES, 0);
    vmg[TXTDT_MG_PTR..TXTDT_MG_PTR + 4].copy_from_slice(&(sector as u32).to_be_bytes());
    vmg.extend_from_slice(txtdt);
    vmg
}

// ── VTS parser ──────────────────────────────────────────────────────────────

/// Parse VTS_XX_0.IFO for one title set, falling back to its VTS_XX_0.BUP copy when the
/// IFO fails or keeps fewer titles than `titles_info` declares.
///
/// `titles_info` is a list of (chapter_count, vts_title_number) from TT_SRPT.
fn parse_vts(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    vts_number: u8,
    titles_info: &[(u16, u8)],
) -> Result<DvdTitleSet> {
    let path = format!("/VIDEO_TS/VTS_{vts_number:02}_0.IFO");
    let mut partial = None;
    let mut err = None;
    match parse_vts_file(reader, udf, &path, &path, vts_number, titles_info) {
        Ok(ts) if ts.titles.len() >= titles_info.len() => return Ok(ts),
        Ok(ts) => partial = Some(ts),
        Err(Error::Halted) => return Err(Error::Halted),
        Err(e) => err = Some(e),
    }
    let bup = format!("/VIDEO_TS/VTS_{vts_number:02}_0.BUP");
    match parse_vts_file(reader, udf, &bup, &path, vts_number, titles_info) {
        Ok(ts)
            if partial
                .as_ref()
                .is_none_or(|p| ts.titles.len() > p.titles.len()) =>
        {
            tracing::warn!(target: "freemkv::scan", vts = vts_number, "VTS IFO bad; using BUP");
            Ok(ts)
        }
        Err(Error::Halted) => Err(Error::Halted),
        _ => match (partial, err) {
            (Some(ts), _) => Ok(ts),
            (None, Some(e)) => Err(e),
            (None, None) => Err(Error::IfoParse),
        },
    }
}

// Parses the IFO bytes at `src` (IFO or BUP). Cell sectors are relative to the IFO's LBA, so
// `ifo_path` anchors the title VOBS even when the bytes come from the BUP.
fn parse_vts_file(
    reader: &mut dyn SectorSource,
    udf: &UdfFs,
    src: &str,
    ifo_path: &str,
    vts_number: u8,
    titles_info: &[(u16, u8)],
) -> Result<DvdTitleSet> {
    let path = ifo_path;
    let vts_data = udf.read_file(reader, src)?;

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
    const VTS_PTT_SRPT_OFFSET: usize = 0xC8; // VTS_PTT_SRPT sector pointer
    const VTS_PGCIT_OFFSET: usize = 0xCC; // VTS_PGCIT sector pointer

    // VTS_PGCIT sector pointer
    let pgcit_sector = be_u32(&vts_data, VTS_PGCIT_OFFSET)?;

    // First sector of the VTS **Title** VOBS (`vtstt_vobs`), which PGC cell extents
    // are relative to (offset 0xC0/`vtsm_vobs` is the *menu* VOBS, not the movie).
    // It's relative to this IFO file, so rebase by the IFO's on-disc LBA (UDF FS).
    let vtstt_vobs = be_u32(&vts_data, VTSTT_VOBS_OFFSET)?;
    let ifo_lba = udf.file_start_lba(reader, path)?;
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
    // PTT_SRPT maps each title number to its PGC; sector 0 means absent.
    let ptt_offset = (be_u32(&vts_data, VTS_PTT_SRPT_OFFSET)? as usize)
        .checked_mul(SECTOR_BYTES)
        .ok_or(Error::IfoParse)?;
    let titles = parse_pgcit(&vts_data, pgcit_offset, ptt_offset, titles_info)?;

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

// VTS_V_ATR byte 0: bits 7-6 mpeg_version | 5-4 video_format | 3-2
// display_aspect | 1-0 permitted_df (the earlier bug misread bits 1-0).
const V_ATR_VIDEO_FORMAT_SHIFT: u8 = 4;
const V_ATR_ASPECT_SHIFT: u8 = 2;
const V_ATR_FIELD_MASK: u8 = 0x03;
// video_format field values (2/3 are reserved → parsed as NTSC).
pub(crate) const VIDEO_FORMAT_NTSC: u8 = 0;
pub(crate) const VIDEO_FORMAT_PAL: u8 = 1;
// display_aspect_ratio field values (1/2 are reserved → parsed as 4:3).
pub(crate) const ASPECT_4X3: u8 = 0;
pub(crate) const ASPECT_16X9: u8 = 3;

// Composes a VTS_V_ATR byte 0 from its video_format/display_aspect fields,
// mirroring parse_video_attr's layout. Test-only, for self-documenting fixtures.
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

// Parses one audio stream attribute block (8 bytes at `offset`). pub(crate) so src/mux/mkv.rs's
// cross-module tests can call the real parser directly.
pub(crate) fn parse_audio_attr(data: &[u8], offset: usize) -> Result<DvdAudioAttr> {
    let b0 = byte_at(data, offset)?;
    let b1 = byte_at(data, offset + 1)?;

    let coding_mode = (b0 >> 5) & 0x07;
    // Modes 2 and 3 are both MPEG audio Layer II (3 adds the multichannel
    // extension), so both map to Codec::Mp2. Mode 2 previously mapped to the
    // video codec Codec::Mpeg1, so the audio stream was classified as video.
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
        mpeg_ext: coding_mode == 3,
        code_extension: code_extension_at(data, offset),
    })
}

/// The on-wire `private_stream_1` sub-stream id of physical audio stream `n` (0..=7):
/// AC-3 `0x80|n`, DTS `0x88|n`, LPCM `0xA0|n`; `None` for MPEG audio (its own PES id).
pub(crate) fn audio_sub_stream_id(codec: Codec, n: u8) -> Option<u8> {
    match codec {
        Codec::Ac3 => Some(0x80 | n), // VLC ps.h: "( i_id&0xf8 ) == 0x80 || /* 0x80 -> 0x87 */"
        Codec::Dts => Some(0x88 | n), // VLC ps.h: "( i_id&0xf8 ) == 0x88 || /* 0x88 -> 0x8f"
        Codec::Lpcm => Some(0xA0 | n), // mpucoder LPCM: "1010 0***b *** = Audio stream number"
        _ => None,
    }
}

/// The routing PID of physical audio stream `n` (0..=7), shared with the demuxer's
/// `dvd_pid()`: `0xBD00 | sub-id` on `private_stream_1`, or MPEG audio's own PES id `0xC0|n`.
pub(crate) fn audio_pid(codec: Codec, n: u8) -> Option<u16> {
    match codec {
        // mpucoder PES: "0xC0 - 0xDF MPEG-1 or MPEG-2 audio stream number x xxxx".
        Codec::Mp2 => crate::mux::ps::dvd_mpeg_audio_pid(0xC0 | n),
        _ => audio_sub_stream_id(codec, n).and_then(crate::mux::ps::dvd_audio_pid),
    }
}

/// The physical stream number (0..=7) of one PGC_AST_CTL entry, or `None` when the
/// stream is absent from the PGC. Bits 14-11 are reserved (libdvdnav masks `& 0x07`).
pub(crate) fn audio_stream_number(ctl: u16) -> Option<u8> {
    // libdvdnav vm_get_audio_stream: "if((vm->state).pgc->audio_control[audioN] & (1<<15))"
    // then "streamN = ((vm->state).pgc->audio_control[audioN] >> 8) & 0x07;".
    (ctl & 0x8000 != 0).then_some(((ctl >> 8) & 0x07) as u8)
}

/// Parse one subtitle stream attribute block (6 bytes at `offset`).
fn parse_subtitle_attr(data: &[u8], offset: usize) -> Result<DvdSubtitleAttr> {
    // Language code: bytes 2-3 as ISO 639-1 (the DVD-Video spec's form).
    let lang_bytes = sub_slice(data, offset + 2, 2)?;
    let language = dvd_lang_to_iso639_2(&parse_raw_dvd_lang_bytes(lang_bytes));

    Ok(DvdSubtitleAttr {
        language,
        code_extension: code_extension_at(data, offset),
    })
}

// Decodes the raw 2-byte on-disc language code: lowercase a-z taken verbatim, all-zero means
// unspecified (empty), else an ASCII-alphanumeric salvage.
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

// Converts a DVD IFO audio/subtitle language code (ISO 639-1, or empty) to ISO 639-2 for
// Matroska/MP4 language fields; unrecognized/empty -> "und".
fn dvd_lang_to_iso639_2(raw: &str) -> String {
    crate::labels::vocab::iso639_1_to_iso639_2(raw)
        .unwrap_or("und")
        .to_string()
}

// ── PGC parser ──────────────────────────────────────────────────────────────

// 0-based PGC index of title `ttn`'s first part-of-title, from VTS_PTT_SRPT at `ptt_offset`
// (count, reserved, last byte, u32 offsets to (pgcn, pgn) entries); None if absent/unusable.
fn ptt_srpt_pgc_index(data: &[u8], ptt_offset: usize, ttn: u8) -> Option<usize> {
    if ptt_offset == 0 || ttn == 0 {
        return None;
    }
    let count = be_u16(data, ptt_offset).ok()? as usize;
    let last_byte = be_u32(data, ptt_offset + 4).ok()? as usize;
    if ttn as usize > count {
        return None;
    }
    let rel = be_u32(data, ptt_offset + 8 + (ttn as usize - 1) * 4).ok()? as usize;
    if rel.checked_add(4)? > last_byte.checked_add(1)? {
        return None; // title has no PTT entry inside the table
    }
    let pgcn = be_u16(data, ptt_offset.checked_add(rel)?).ok()? as usize;
    pgcn.checked_sub(1)
}

// Title `ttn`'s part-of-title entries, (PGCN, PGN) each, in chapter order; `None` when the
// table is absent or does not hold the title.
fn ptt_srpt_parts(data: &[u8], ptt_offset: usize, ttn: u8) -> Option<Vec<(u16, u16)>> {
    if ptt_offset == 0 || ttn == 0 {
        return None;
    }
    let count = be_u16(data, ptt_offset).ok()? as usize;
    let last_byte = be_u32(data, ptt_offset + 4).ok()? as usize;
    if ttn as usize > count {
        return None;
    }
    let at = |t: usize| {
        be_u32(data, ptt_offset + 8 + (t - 1) * 4)
            .ok()
            .map(|r| r as usize)
    };
    let start = at(ttn as usize)?;
    let end = if (ttn as usize) < count {
        at(ttn as usize + 1)?
    } else {
        last_byte.checked_add(1)?
    };
    let mut parts = Vec::new();
    let mut rel = start;
    while rel.checked_add(4)? <= end {
        let entry = ptt_offset.checked_add(rel)?;
        parts.push((be_u16(data, entry).ok()?, be_u16(data, entry + 2).ok()?));
        rel += 4;
    }
    Some(parts)
}

// Distinct PGCs title `ttn`'s part-of-title entries point at. A title split across
// PGCs (one per chapter) is read from its first PGC only; the caller warns on > 1.
fn ptt_srpt_pgc_count(data: &[u8], ptt_offset: usize, ttn: u8) -> usize {
    let mut pgcns: Vec<u16> = ptt_srpt_parts(data, ptt_offset, ttn)
        .unwrap_or_default()
        .into_iter()
        .map(|(pgcn, _)| pgcn)
        .collect();
    pgcns.sort_unstable();
    pgcns.dedup();
    pgcns.len()
}

/// Parse VTS_PGCIT (Program Chain Information Table) to extract titles.
/// `ptt_offset` is the VTS_PTT_SRPT byte offset (0 = absent).
pub(crate) fn parse_pgcit(
    data: &[u8],
    pgcit_offset: usize,
    ptt_offset: usize,
    titles_info: &[(u16, u8)],
) -> Result<Vec<DvdTitle>> {
    if pgcit_offset + 8 > data.len() {
        return Err(Error::IfoParse);
    }

    let num_pgcs = be_u16(data, pgcit_offset)?;

    // PGC info entries start at pgcit_offset + 8, each 8 bytes
    let entries_start = pgcit_offset + 8;

    let mut titles = Vec::new();
    // Every `continue` below drops a title the user will never see and must not
    // be silent: `parse_vmg` warns per skipped title SET, but this was the
    // remaining place a disc could quietly report fewer titles than it has.
    let mut skipped = 0usize;

    for &(chapter_count, vts_title_num) in titles_info {
        // TTN -> PGC goes through PTT_SRPT; without it (or when its PGCN is past the
        // table) assume 1:1 (TTN is 1-based).
        let pgc_index = ptt_srpt_pgc_index(data, ptt_offset, vts_title_num)
            .filter(|&i| i < num_pgcs as usize)
            .unwrap_or(vts_title_num.saturating_sub(1) as usize);
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

        // Warned once per disc by the DVD scan, which sees every title set.
        let pgcs = ptt_srpt_pgc_count(data, ptt_offset, vts_title_num);
        // The program each chapter starts at, when every chapter lies in this PGC.
        let ptt_programs = ptt_srpt_parts(data, ptt_offset, vts_title_num).map(|parts| match parts
            .iter()
            .all(|&(pgcn, _)| usize::from(pgcn) == pgc_index + 1)
        {
            true => parts.into_iter().map(|(_, pgn)| pgn).collect(),
            false => Vec::new(),
        });

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
            Ok(mut title) => {
                // Stamp the REAL title number so downstream nav joins survive a
                // dropped sibling PGC (position in `titles` is not vts_title_num).
                title.vts_title_num = vts_title_num;
                title.pgcs = pgcs;
                title.ptt_programs = ptt_programs;
                titles.push(title);
            }
            Err(e) => {
                // A single unparseable PGC (truncated/corrupt entry, authoring quirk)
                // must not lose the whole title list, but must not be silent either:
                // a disc quietly reporting fewer titles than it has is what this warns.
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
pub(crate) fn parse_pgc(data: &[u8], pgc_offset: usize, chapters: u16) -> Result<DvdTitle> {
    // PGC needs at least 0xE8 bytes for the cell playback info offset
    if pgc_offset + 0xEA > data.len() {
        return Err(Error::IfoParse);
    }

    // PGC layout: 0x00-0x01 misc flags, 0x02 nr_of_programs,
    // 0x03 nr_of_cells, 0x04-0x07 playback_time (4 BCD bytes).
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

    if cells.len() < num_cells {
        tracing::warn!(
            target: "freemkv::scan",
            declared = num_cells,
            kept = cells.len(),
            "PGC cell table runs past the IFO; its title keeps only the leading cells"
        );
    }

    // Recalculate duration from cell times if PGC-level time is zero. Like the PGC time and
    // the chapter marks, it counts an angle block once: its other angles' cells add nothing
    // (unless every cell is one, which `leading_secondary_cells` also keeps whole).
    let duration_secs = if duration_secs == 0.0 && !cells.is_empty() {
        let primary = |c: &&DvdCell| !CellCategory::decode(c.category).is_secondary_block_piece();
        match cells.iter().any(|c| primary(&c)) {
            true => cells.iter().filter(primary).map(|c| c.duration_secs).sum(),
            false => cells.iter().map(|c| c.duration_secs).sum(),
        }
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
            // Collect durations as exact FRAME counts, converting once per chapter so
            // marks stay on frame boundaries (summing f64 seconds would drift). Cells
            // with no rate flag fall back to plain seconds, tracked separately.
            let mut cell_frames: Vec<(u64, f64)> = Vec::with_capacity(num_cells);
            let mut rate: Option<DvdRate> = None;
            let cell_base = pgc_offset + cell_playback_offset;
            for i in 0..num_cells {
                let co = cell_base + i * 24;
                // Same bound as the cell loop above: a cell that is not kept adds nothing.
                if co + 24 > data.len() {
                    cell_frames.push((0, 0.0));
                    continue;
                }
                // An angle block plays one angle: its other angles' cells take no time.
                if CellCategory::decode(data[co]).is_secondary_block_piece() {
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

    // mpucoder PGC: "000C PGC_AST_CTL 8*2", "001C PGC_SPST_CTL 32*4" (libdvdread pgc_t:
    // "uint16_t audio_control[8];", "uint32_t subp_control[32];"); inside the 0xEA bound.
    let mut ast_ctl = [0u16; 8];
    for (i, c) in ast_ctl.iter_mut().enumerate() {
        *c = be_u16(data, pgc_offset + 0x0C + i * 2)?;
    }
    let mut spst_ctl = [0u32; 32];
    for (i, c) in spst_ctl.iter_mut().enumerate() {
        *c = be_u32(data, pgc_offset + 0x1C + i * 4)?;
    }

    // Subtitle palette at PGC offset 0xA4: 16 colors × 4 bytes [padding, Y, Cr, Cb].
    // Chroma order is Cr (byte 2) BEFORE Cb (byte 3) per the DVD-Video PGC CLUT format;
    // `mux::codec::dvdsub::ycbcr_to_rgb` must read it the same way.
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
        ast_ctl,
        spst_ctl,
        // Set by the caller (parse_pgcit) which knows the TT_SRPT title number.
        vts_title_num: 0,
        pgcs: 0,
        ptt_programs: None,
    })
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "ifo_tests.rs"]
mod tests;
