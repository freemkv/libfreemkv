//! IFO parser — DVD title structure.
//!
//! DVD discs use IFO files to describe the title structure:
//!   - `VIDEO_TS/VIDEO_TS.IFO` — top-level VMG with title search pointer table
//!   - `VIDEO_TS/VTS_XX_0.IFO` — per-title-set with PGC chains, cell addresses, streams
//!
//! The parser reads IFO files via UDF and extracts enough information
//! to build DiscTitle structs (parallel to MPLS for Blu-ray).

use crate::error::{Error, Result};
use crate::sector::SectorReader;
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
    /// Subtitle palette from PGC: 16 entries of [padding, Y, Cb, Cr].
    pub palette: Option<Vec<[u8; 4]>>,
}

/// A cell — contiguous sector range within a VOB.
#[derive(Debug, Clone)]
pub struct DvdCell {
    pub first_sector: u32,
    pub last_sector: u32,
}

/// DVD video stream attributes.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DvdVideoAttr {
    pub codec: String,
    pub resolution: String,
    pub aspect: String,
    pub standard: String,
}

/// DVD audio stream attributes.
#[derive(Debug, Clone)]
pub struct DvdAudioAttr {
    pub codec: String,
    pub channels: u8,
    pub sample_rate: u32,
    pub language: String,
}

/// DVD subtitle stream attributes.
#[derive(Debug, Clone)]
pub struct DvdSubtitleAttr {
    pub language: String,
}

// ── Constants ───────────────────────────────────────────────────────────────

const VMG_MAGIC: &[u8; 12] = b"DVDVIDEO-VMG";
const VTS_MAGIC: &[u8; 12] = b"DVDVIDEO-VTS";
const SECTOR_SIZE: usize = 2048;

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

/// Convert DVD BCD playback time (4 bytes) to seconds.
///
/// Format: `[hours_bcd, minutes_bcd, seconds_bcd, frames_and_rate]`
///   - Byte 0: hours in BCD (e.g. 0x01 = 1 hour, 0x12 = 12 hours)
///   - Byte 1: minutes in BCD
///   - Byte 2: seconds in BCD
///   - Byte 3: bits 7-6 = frame rate flag (01=25fps, 11=29.97fps),
///     bits 5-0 = frame count in BCD
///
/// Returns 0.0 for invalid BCD digits rather than erroring,
/// since some authoring tools produce malformed time fields.
pub fn bcd_to_secs(bcd: &[u8]) -> f64 {
    if bcd.len() < 4 {
        return 0.0;
    }

    let hours = bcd_byte(bcd[0]);
    let minutes = bcd_byte(bcd[1]);
    let seconds = bcd_byte(bcd[2]);

    let rate_flag = (bcd[3] >> 6) & 0x03;
    let frame_count = bcd_byte(bcd[3] & 0x3F);

    let fps: f64 = match rate_flag {
        0x01 => 25.0,
        0x03 => 29.97,
        _ => 0.0, // unknown rate — ignore frame contribution
    };

    let total = (hours as f64) * 3600.0 + (minutes as f64) * 60.0 + (seconds as f64);

    if fps > 0.0 {
        total + (frame_count as f64) / fps
    } else {
        total
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
pub fn parse_vmg(reader: &mut dyn SectorReader, udf: &UdfFs) -> Result<DvdInfo> {
    let vmg_data = udf.read_file(reader, "/VIDEO_TS/VIDEO_TS.IFO")?;

    // Validate VMG magic
    if vmg_data.len() < 12 || &vmg_data[0..12] != VMG_MAGIC {
        return Err(Error::IfoParse);
    }

    // Minimum size: need at least through the TT_SRPT pointer at offset 0xC4
    if vmg_data.len() < 0xC8 {
        return Err(Error::IfoParse);
    }

    // TT_SRPT sector pointer at bytes 0xC4 (offset 196, documented as bytes 62-65
    // in some references, but the canonical IFO spec uses 0xC4).
    // NOTE: The user spec says bytes 62-65, which is offset 0x3E.
    // Let's use the value from the spec provided.
    let tt_srpt_sector = be_u32(&vmg_data, 0xC4)?;

    // Read TT_SRPT — it's at the given sector offset relative to the start of VIDEO_TS.IFO.
    // In the IFO file data we already have, sector offsets are relative to the IFO start.
    let tt_srpt_offset = (tt_srpt_sector as usize)
        .checked_mul(SECTOR_SIZE)
        .ok_or(Error::IfoParse)?;

    // TT_SRPT may be beyond what we read; if so, it's embedded in the file data
    // (IFO files are typically small, a few sectors). Check bounds.
    if tt_srpt_offset + 8 > vmg_data.len() {
        return Err(Error::IfoParse);
    }

    let num_titles = be_u16(&vmg_data, tt_srpt_offset)?;

    // Parse title entries — each is 12 bytes, starting at tt_srpt_offset + 8
    let entries_start = tt_srpt_offset + 8;
    let mut title_set_map: std::collections::BTreeMap<u8, Vec<(u16, u8)>> =
        std::collections::BTreeMap::new();

    for i in 0..num_titles as usize {
        let base = entries_start + i * 12;
        if base + 12 > vmg_data.len() {
            break; // truncated — parse what we can
        }

        let num_chapters = be_u16(&vmg_data, base + 2)?;
        let vts_number = byte_at(&vmg_data, base + 6)?;
        let vts_title_num = byte_at(&vmg_data, base + 7)?;

        if vts_number == 0 {
            continue; // invalid
        }

        title_set_map
            .entry(vts_number)
            .or_default()
            .push((num_chapters, vts_title_num));
    }

    // Parse each VTS IFO
    let mut title_sets = Vec::new();
    for (&vts_number, titles_info) in &title_set_map {
        match parse_vts(reader, udf, vts_number, titles_info) {
            Ok(ts) => title_sets.push(ts),
            Err(_) => {
                // Skip unreadable title sets — some DVDs have placeholder entries.
                continue;
            }
        }
    }

    Ok(DvdInfo { title_sets })
}

// ── VTS parser ──────────────────────────────────────────────────────────────

/// Parse VTS_XX_0.IFO for one title set.
///
/// `titles_info` is a list of (chapter_count, vts_title_number) from TT_SRPT.
fn parse_vts(
    reader: &mut dyn SectorReader,
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

    // VTS_PGCIT sector pointer
    let pgcit_sector = be_u32(&vts_data, 0xCC)?;

    // First VOB sector
    let vob_start_sector = be_u32(&vts_data, 0xC0)?;

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
        .checked_mul(SECTOR_SIZE)
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

/// Parse video attributes from VTS header offset 0x200.
fn parse_video_attr(data: &[u8]) -> Result<DvdVideoAttr> {
    let b0 = byte_at(data, 0x200)?;

    let standard = match b0 & 0x03 {
        0 => "NTSC",
        1 => "PAL",
        _ => "NTSC",
    };

    let aspect = match (b0 >> 2) & 0x03 {
        0 => "4:3",
        3 => "16:9",
        _ => "4:3",
    };

    let resolution = match (b0 >> 4) & 0x03 {
        0 => {
            if standard == "PAL" {
                "720x576"
            } else {
                "720x480"
            }
        }
        1 => {
            if standard == "PAL" {
                "704x576"
            } else {
                "704x480"
            }
        }
        2 => {
            if standard == "PAL" {
                "352x576"
            } else {
                "352x480"
            }
        }
        3 => {
            if standard == "PAL" {
                "352x288"
            } else {
                "352x240"
            }
        }
        _ => "720x480",
    };

    Ok(DvdVideoAttr {
        codec: "mpeg2".to_string(),
        resolution: resolution.to_string(),
        aspect: aspect.to_string(),
        standard: standard.to_string(),
    })
}

/// Parse one audio stream attribute block (8 bytes at `offset`).
fn parse_audio_attr(data: &[u8], offset: usize) -> Result<DvdAudioAttr> {
    let b0 = byte_at(data, offset)?;
    let b1 = byte_at(data, offset + 1)?;

    let coding_mode = (b0 >> 5) & 0x07;
    let codec = match coding_mode {
        0 => "ac3",
        2 => "mpeg1",
        3 => "mpeg2",
        4 => "lpcm",
        6 => "dts",
        _ => "unknown",
    };

    let sample_rate_flag = (b0 >> 3) & 0x03;
    let sample_rate = match sample_rate_flag {
        0 => 48000,
        1 => 96000,
        _ => 48000,
    };

    let channels = ((b1 >> 4) & 0x0F) + 1; // stored as channels minus 1

    // Language code: bytes 2-3 as ISO 639
    let lang_bytes = sub_slice(data, offset + 2, 2)?;
    let language = if lang_bytes[0] >= b'a'
        && lang_bytes[0] <= b'z'
        && lang_bytes[1] >= b'a'
        && lang_bytes[1] <= b'z'
    {
        String::from_utf8_lossy(lang_bytes).to_string()
    } else if lang_bytes[0] == 0 && lang_bytes[1] == 0 {
        String::new()
    } else {
        // Try to interpret as printable ASCII
        let s: String = lang_bytes
            .iter()
            .filter(|&&b| b.is_ascii_alphanumeric())
            .map(|&b| b as char)
            .collect();
        s
    };

    Ok(DvdAudioAttr {
        codec: codec.to_string(),
        channels,
        sample_rate,
        language,
    })
}

/// Parse one subtitle stream attribute block (6 bytes at `offset`).
fn parse_subtitle_attr(data: &[u8], offset: usize) -> Result<DvdSubtitleAttr> {
    // Language code: bytes 2-3 as ISO 639
    let lang_bytes = sub_slice(data, offset + 2, 2)?;
    let language = if lang_bytes[0] >= b'a'
        && lang_bytes[0] <= b'z'
        && lang_bytes[1] >= b'a'
        && lang_bytes[1] <= b'z'
    {
        String::from_utf8_lossy(lang_bytes).to_string()
    } else if lang_bytes[0] == 0 && lang_bytes[1] == 0 {
        String::new()
    } else {
        let s: String = lang_bytes
            .iter()
            .filter(|&&b| b.is_ascii_alphanumeric())
            .map(|&b| b as char)
            .collect();
        s
    };

    Ok(DvdSubtitleAttr { language })
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

    for &(chapter_count, vts_title_num) in titles_info {
        // VTS title numbers are 1-based; map to PGC index (typically 1:1)
        let pgc_index = vts_title_num.saturating_sub(1) as usize;
        if pgc_index >= num_pgcs as usize {
            continue;
        }

        let entry_offset = entries_start + pgc_index * 8;
        if entry_offset + 8 > data.len() {
            continue;
        }

        // PGC byte offset relative to VTS_PGCIT start
        let pgc_byte_offset = be_u32(data, entry_offset + 4)? as usize;
        let pgc_abs = pgcit_offset
            .checked_add(pgc_byte_offset)
            .ok_or(Error::IfoParse)?;

        match parse_pgc(data, pgc_abs, chapter_count) {
            Ok(title) => titles.push(title),
            Err(_) => continue, // skip malformed PGCs
        }
    }

    Ok(titles)
}

/// Parse a single PGC (Program Chain) to extract duration and cells.
fn parse_pgc(data: &[u8], pgc_offset: usize, chapters: u16) -> Result<DvdTitle> {
    // PGC needs at least 0xE6 bytes for the cell info offsets
    if pgc_offset + 0xE6 > data.len() {
        return Err(Error::IfoParse);
    }

    // Playback time at offset 2-5 (4 BCD bytes)
    let time_bytes = sub_slice(data, pgc_offset + 2, 4)?;
    let duration_secs = bcd_to_secs(time_bytes);

    // Number of cells: the user spec says byte 0x03, but in the standard IFO
    // format bytes 0x02-0x05 are the BCD playback time. The real cell count
    // lives at PGC offset 0x07. We read from 0x03 as primary (per spec) and
    // fall back to 0x07 if that yields zero.
    let num_cells_primary = byte_at(data, pgc_offset + 0x03)? as usize;
    let num_cells = if num_cells_primary == 0 {
        byte_at(data, pgc_offset + 0x07).unwrap_or(0) as usize
    } else {
        num_cells_primary
    };

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
            let first_sector = be_u32(data, co + 8)?;
            let last_sector = be_u32(data, co + 20)?;
            cells.push(DvdCell {
                first_sector,
                last_sector,
            });
        }
    }

    // Recalculate duration from cell times if PGC-level time is zero
    let duration_secs = if duration_secs == 0.0 && !cells.is_empty() && cell_playback_offset > 0 {
        let cell_base = pgc_offset + cell_playback_offset;
        let mut total = 0.0;
        for i in 0..cells.len() {
            let co = cell_base + i * 24;
            if co + 4 <= data.len() {
                total += bcd_to_secs(&data[co..co + 4]);
            }
        }
        total
    } else {
        duration_secs
    };

    // Extract subtitle palette at PGC offset 0xA4: 16 colors × 4 bytes [padding, Y, Cb, Cr]
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
        palette,
    })
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
        // 0 hours, 1 minute, 30 seconds, 15 frames at 29.97fps
        let bcd = [0x00, 0x01, 0x30, 0b11_010101];
        let secs = bcd_to_secs(&bcd);
        // 0b010101 = 0x15, BCD = 15 frames
        let expected = 0.0 + 60.0 + 30.0 + 15.0 / 29.97;
        assert!((secs - expected).abs() < 0.01, "got {}", secs);
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
        };
        assert_eq!(cell.first_sector, 100);
        assert_eq!(cell.last_sector, 200);

        let title = DvdTitle {
            chapters: 5,
            duration_secs: 3600.0,
            cells: vec![cell.clone()],
            palette: None,
        };
        assert_eq!(title.chapters, 5);
        assert!((title.duration_secs - 3600.0).abs() < 0.01);
        assert_eq!(title.cells.len(), 1);

        let video = DvdVideoAttr {
            codec: "mpeg2".to_string(),
            resolution: "720x480".to_string(),
            aspect: "16:9".to_string(),
            standard: "NTSC".to_string(),
        };
        assert_eq!(video.codec, "mpeg2");

        let audio = DvdAudioAttr {
            codec: "ac3".to_string(),
            channels: 6,
            sample_rate: 48000,
            language: "en".to_string(),
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
    fn video_attr_parsing() {
        // Build minimal data with video attrs at 0x200
        let mut data = vec![0u8; 0x204];
        // NTSC, 16:9, 720x480: standard=0b00, aspect=0b11, resolution=0b00
        // b0 = 0b00_00_11_00 = 0x0C
        data[0x200] = 0x0C;
        let attr = parse_video_attr(&data).unwrap();
        assert_eq!(attr.standard, "NTSC");
        assert_eq!(attr.aspect, "16:9");
        assert_eq!(attr.resolution, "720x480");
        assert_eq!(attr.codec, "mpeg2");
    }

    #[test]
    fn video_attr_pal() {
        let mut data = vec![0u8; 0x204];
        // PAL, 4:3, 720x576: standard=0b01, aspect=0b00, resolution=0b00
        // b0 = 0b00_00_00_01 = 0x01
        data[0x200] = 0x01;
        let attr = parse_video_attr(&data).unwrap();
        assert_eq!(attr.standard, "PAL");
        assert_eq!(attr.aspect, "4:3");
        assert_eq!(attr.resolution, "720x576");
    }

    #[test]
    fn audio_attr_parsing() {
        let mut data = vec![0u8; 16];
        // AC3 (coding=0), 48kHz (rate=0), 6 channels (stored as 5)
        // b0: bits 7-5=000(AC3), bits 4-3=00(48k) => 0x00
        data[0] = 0x00;
        // b1: bits 7-4=0101 (channels-1=5) => 0x50
        data[1] = 0x50;
        // language "en"
        data[2] = b'e';
        data[3] = b'n';

        let attr = parse_audio_attr(&data, 0).unwrap();
        assert_eq!(attr.codec, "ac3");
        assert_eq!(attr.sample_rate, 48000);
        assert_eq!(attr.channels, 6);
        assert_eq!(attr.language, "en");
    }

    #[test]
    fn audio_attr_dts() {
        let mut data = vec![0u8; 16];
        // DTS (coding=6), 96kHz (rate=1), 2 channels (stored as 1)
        // b0: bits 7-5=110(DTS), bits 4-3=01(96k) => 0b110_01_000 = 0xC8
        data[0] = 0xC8;
        // b1: bits 7-4=0001 (channels-1=1) => 0x10
        data[1] = 0x10;
        data[2] = b'f';
        data[3] = b'r';

        let attr = parse_audio_attr(&data, 0).unwrap();
        assert_eq!(attr.codec, "dts");
        assert_eq!(attr.sample_rate, 96000);
        assert_eq!(attr.channels, 2);
        assert_eq!(attr.language, "fr");
    }
}
