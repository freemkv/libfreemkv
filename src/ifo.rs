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
    pub codec: Codec,
    pub resolution: Resolution,
    pub aspect: String,
    pub standard: String,
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

    let resolution = if standard == "PAL" {
        Resolution::R576i
    } else {
        Resolution::R480i
    };

    Ok(DvdVideoAttr {
        codec: Codec::Mpeg2,
        resolution,
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
        0 => Codec::Ac3,
        2 => Codec::Mpeg1,
        3 => Codec::Mp2,
        4 => Codec::Lpcm,
        6 => Codec::Dts,
        _ => Codec::Unknown(coding_mode),
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
/// stream by per-codec ordinal, matching DVD authoring convention and the
/// muxer's `dvd_pid()` routing:
///   - AC-3  → `0x80 + n` (n = 0-based index among AC-3 streams)
///   - DTS   → `0x88 + n`
///   - LPCM  → `0xA0 + n`
///   - MP1/MP2 and anything else → `None` (regular MPEG-audio PES, not a
///     private-stream-1 sub-id).
///
/// Indices saturate at the codec range ceiling (8 AC-3/DTS, 8 LPCM) so a
/// malformed over-count never produces an out-of-range sub-id.
fn assign_audio_sub_stream_ids(streams: &mut [DvdAudioAttr]) {
    let mut n_ac3 = 0u8;
    let mut n_dts = 0u8;
    let mut n_lpcm = 0u8;
    for s in streams.iter_mut() {
        s.sub_stream_id = match s.codec {
            Codec::Ac3 => {
                let id = 0x80 + n_ac3.min(7);
                n_ac3 = n_ac3.saturating_add(1);
                Some(id)
            }
            Codec::Dts => {
                let id = 0x88 + n_dts.min(7);
                n_dts = n_dts.saturating_add(1);
                Some(id)
            }
            Codec::Lpcm => {
                let id = 0xA0 + n_lpcm.min(7);
                n_lpcm = n_lpcm.saturating_add(1);
                Some(id)
            }
            _ => None,
        };
    }
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
            // By design: a single unparseable PGC (truncated/corrupt entry,
            // authoring-tool quirk) must not lose the whole title list.
            // Skip it and keep collecting the titles that do parse.
            Err(_) => continue,
        }
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
            // Collect cell durations
            let mut cell_durations = Vec::with_capacity(num_cells);
            let cell_base = pgc_offset + cell_playback_offset;
            for i in 0..num_cells {
                let co = cell_base + i * 24;
                if co + 8 <= data.len() {
                    cell_durations.push(bcd_to_secs(&data[co + 4..co + 8]));
                } else {
                    cell_durations.push(0.0);
                }
            }
            // Program map: each byte is the first cell number (1-based) for that program
            for p in 0..nr_of_programs {
                if pgm_base + p >= data.len() {
                    break;
                }
                let first_cell = data[pgm_base + p] as usize;
                // Chapter time = sum of cell durations before this program's first cell.
                // Clamp to cell_durations.len(): a crafted/corrupt IFO can set first_cell
                // beyond the actual cell count, which would panic the slice index.
                let end = first_cell.saturating_sub(1).min(cell_durations.len());
                let time: f64 = cell_durations[..end].iter().sum();
                times.push(time);
            }
        }
        times
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
        chapter_times,
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
            chapter_times: Vec::new(),
            palette: None,
        };
        assert_eq!(title.chapters, 5);
        assert!((title.duration_secs - 3600.0).abs() < 0.01);
        assert_eq!(title.cells.len(), 1);

        let video = DvdVideoAttr {
            codec: Codec::Mpeg2,
            resolution: Resolution::R480i,
            aspect: "16:9".to_string(),
            standard: "NTSC".to_string(),
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
        let expected = 1.0 * 3600.0 + 59.0 * 60.0 + 30.0;
        assert!(
            (title.duration_secs - expected).abs() < 0.1,
            "expected ~{expected}s, got {}s",
            title.duration_secs
        );
        assert_eq!(title.chapters, 5);
        assert_eq!(title.cells.len(), 2);
        assert_eq!(title.cells[0].first_sector, 100);
        assert_eq!(title.cells[0].last_sector, 200);
        assert_eq!(title.cells[1].first_sector, 300);
        assert_eq!(title.cells[1].last_sector, 400);
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
        assert_eq!(attr.resolution, Resolution::R480i);
        assert_eq!(attr.codec, Codec::Mpeg2);
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
        assert_eq!(attr.resolution, Resolution::R576i);
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
        assert_eq!(attr.codec, Codec::Ac3);
        assert_eq!(attr.sample_rate, 48000);
        assert_eq!(attr.channels, 6);
        assert_eq!(attr.language, "en");
    }

    #[test]
    fn mixed_codec_sub_stream_ids_are_distinct() {
        // A title mixing AC-3, DTS and LPCM must get per-codec ordinal
        // sub-ids (0x80, 0x88, 0xA0...), all distinct — this is the
        // routing key that keeps mixed-codec audio from colliding.
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
        assert_eq!(streams[0].sub_stream_id, Some(0x80)); // AC-3 #0
        assert_eq!(streams[1].sub_stream_id, Some(0x88)); // DTS  #0
        assert_eq!(streams[2].sub_stream_id, Some(0xA0)); // LPCM #0
        assert_eq!(streams[3].sub_stream_id, Some(0x81)); // AC-3 #1
        // All sub-ids unique.
        let ids: Vec<u8> = streams.iter().filter_map(|s| s.sub_stream_id).collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(ids.len(), sorted.len(), "sub-stream ids must be unique");
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
        assert_eq!(attr.codec, Codec::Dts);
        assert_eq!(attr.sample_rate, 96000);
        assert_eq!(attr.channels, 2);
        assert_eq!(attr.language, "fr");
    }

    // ─────────────────────────────────────────────────────────────────────
    // Added hardening tests. Grounded in the DVD-Video IFO spec
    // (dvd_udf / libdvdread ifo_types.h; http://dvd.sourceforge.net).
    // ─────────────────────────────────────────────────────────────────────

    /// BCD frame-rate flag: bits 7-6 of byte[3]. 0b01 = 25fps (PAL),
    /// 0b11 = 29.97fps (NTSC). 0b00/0b10 are "unknown" → frames ignored.
    /// Verify the 25fps branch contributes frames correctly.
    #[test]
    fn bcd_25fps_frame_contribution() {
        // 0h 0m 0s, 12 frames at 25fps → 12/25 = 0.48s.
        let bcd = [0x00, 0x00, 0x00, 0b01_010010]; // frame BCD 0x12 = 12
        let secs = bcd_to_secs(&bcd);
        assert!((secs - 12.0 / 25.0).abs() < 0.001, "got {secs}");
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
        let secs = bcd_to_secs(&bcd);
        assert!((secs - 25.0 / 29.97).abs() < 0.001, "got {secs}");
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

    /// Video attr standard bits (b0 & 0x03): 0=NTSC, 1=PAL, else NTSC.
    /// Value 2 and 3 fall into the NTSC default. Verify the catch-all.
    #[test]
    fn video_attr_reserved_standard_defaults_ntsc() {
        let mut data = vec![0u8; 0x204];
        data[0x200] = 0x02; // standard bits = 0b10 → default NTSC
        let attr = parse_video_attr(&data).unwrap();
        assert_eq!(attr.standard, "NTSC");
        assert_eq!(attr.resolution, Resolution::R480i);
    }

    /// Video aspect bits ((b0>>2)&0x03): 0=4:3, 3=16:9, else 4:3.
    /// Value 1/2 fall into the 4:3 default (catch-all arm).
    #[test]
    fn video_attr_reserved_aspect_defaults_4_3() {
        let mut data = vec![0u8; 0x204];
        data[0x200] = 0b00_01_00_00; // aspect bits = 0b01 → default 4:3
        let attr = parse_video_attr(&data).unwrap();
        assert_eq!(attr.aspect, "4:3");
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
    /// language is the empty string (unspecified), per source.
    #[test]
    fn audio_attr_zero_language_is_empty() {
        let mut data = vec![0u8; 8];
        data[0] = 0x00;
        data[2] = 0x00;
        data[3] = 0x00;
        let attr = parse_audio_attr(&data, 0).unwrap();
        assert_eq!(attr.language, "");
    }

    /// Audio sample_rate flag (b0>>3 & 0x03): 0=48kHz, 1=96kHz, else 48kHz.
    /// Verify flag 2/3 fall back to 48kHz (catch-all).
    #[test]
    fn audio_attr_reserved_rate_defaults_48k() {
        let mut data = vec![0u8; 8];
        data[0] = 0b000_10_000; // rate flag = 0b10
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
        assert_eq!(attr.language, "de");

        let zero = vec![0u8; 6];
        let attr2 = parse_subtitle_attr(&zero, 0).unwrap();
        assert_eq!(attr2.language, "");
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
        assert_eq!(streams[1].sub_stream_id, Some(0x80)); // AC3 #0
    }

    /// assign_audio_sub_stream_ids saturates the per-codec ordinal at the
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

    /// parse_pgc palette: at PGC+0xA4, 16 colors × 4 bytes [pad, Y, Cb, Cr].
    /// A palette with at least one non-zero Y/Cb/Cr is returned as Some;
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

    /// parse_pgc palette layout: each color is [padding, Y, Cb, Cr] and the
    /// "non-empty" test ignores the padding byte (index 0). A palette whose
    /// ONLY non-zero bytes are padding must still be treated as empty (None).
    #[test]
    fn pgc_palette_padding_only_is_empty() {
        let mut pgc = vec![0u8; 0xEA];
        pgc[0x03] = 0;
        // Set padding byte (index 0) of color 0 non-zero, but Y/Cb/Cr zero.
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
}
