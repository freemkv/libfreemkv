//! MPLS playlist parser — Blu-ray movie playlists.
//!
//! Each .mpls file in BDMV/PLAYLIST/ defines a title.
//! Contains play items (clips) with in/out timestamps,
//! stream info (video, audio, subtitle tracks).
//!
//! Reference: https://github.com/lw/BluRay/wiki/MPLS

use crate::error::{Error, Result};

/// Parsed MPLS playlist.
#[derive(Debug)]
pub(crate) struct Playlist {
    /// MPLS version (e.g. "0200" or "0300"). Parsed for completeness;
    /// no production reader yet.
    #[allow(dead_code)]
    pub version: String,
    /// Play items in playback order
    pub play_items: Vec<PlayItem>,
    /// Streams from the first play item's STN table
    pub streams: Vec<StreamEntry>,
    /// Playlist marks (chapter points, etc.)
    pub marks: Vec<PlaylistMark>,
}

/// A playlist mark entry from the PlayListMark section.
#[derive(Debug, Clone)]
pub(crate) struct PlaylistMark {
    /// PlayListMark mark_type (BD-ROM PlayListMark spec):
    ///   0 = reserved, 1 = entry mark (chapter), 2 = link point.
    /// Chapter filters should test `== 1`, not `<= 1`.
    pub mark_type: u8,
    /// Which play item this mark belongs to. Carries the per-PlayItem
    /// timebase needed to place a mark in a multi-PlayItem playlist;
    /// the chapter builder does not consume it yet.
    #[allow(dead_code)]
    pub play_item_ref: u16,
    /// Timestamp in 45kHz PTS ticks
    pub timestamp: u32,
}

/// A play item — one clip reference with in/out times.
#[derive(Debug)]
pub(crate) struct PlayItem {
    /// Clip filename without extension (e.g. "00001")
    pub clip_id: String,
    /// In-time in 45kHz ticks
    pub in_time: u32,
    /// Out-time in 45kHz ticks
    pub out_time: u32,
    /// Connection condition (1=seamless, 5/6=non-seamless). Parsed for
    /// completeness; no production reader yet.
    #[allow(dead_code)]
    pub connection_condition: u8,
}

/// A stream entry from the STN table.
#[derive(Debug, Clone)]
pub struct StreamEntry {
    /// Stream category: 1=video, 2=audio, 3=PG subtitle, 5=secondary audio,
    /// 6=secondary video, 7=DV EL. IG (4) is consumed during parsing to keep
    /// the STN cursor aligned but is never retained as a StreamEntry.
    pub stream_type: u8,
    /// MPEG-TS PID
    pub pid: u16,
    /// Coding type (0x24=HEVC, 0x1B=H264, 0x83=TrueHD, etc.)
    pub coding_type: u8,
    /// Video format (1=480i, 4=1080i, 5=720p, 6=1080p, 8=2160p)
    pub video_format: u8,
    /// Video frame rate (1=23.976, 2=24, 3=25, 4=29.97, 6=50, 7=59.94)
    pub video_rate: u8,
    /// Audio channel layout (1=mono, 3=stereo, 6=5.1, 12=7.1)
    pub audio_format: u8,
    /// Audio sample rate (1=48kHz, 4=96kHz, 5=192kHz)
    pub audio_rate: u8,
    /// ISO 639-2 language code (e.g. "eng")
    pub language: String,
    /// HDR dynamic range (0=SDR, 1=HDR10, 2=Dolby Vision)
    pub dynamic_range: u8,
    /// Color space (0=unknown, 1=BT.709, 2=BT.2020)
    pub color_space: u8,
    /// Whether this is a secondary stream (commentary, PiP, DV EL)
    pub secondary: bool,
}

/// Parse an MPLS file from raw bytes.
///
/// `data` is the raw contents of a `BDMV/PLAYLIST/*.mpls` file. Returns
/// [`Error::MplsParse`] on malformed or truncated input.
///
/// Note: [`Playlist::streams`] is extracted ONLY from the first play
/// item's STN table. Multi-item playlists whose later items carry a
/// different codec/track set are not fully represented by `streams`;
/// callers selecting tracks for mux should account for this.
pub fn parse(data: &[u8]) -> Result<Playlist> {
    if data.len() < 40 {
        return Err(Error::MplsParse);
    }
    if &data[0..4] != b"MPLS" {
        return Err(Error::MplsParse);
    }

    let version = String::from_utf8_lossy(&data[4..8]).to_string();
    let playlist_start = u32::from_be_bytes([data[8], data[9], data[10], data[11]]) as usize;
    let mark_start = u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;

    if playlist_start + 10 > data.len() {
        return Err(Error::MplsParse);
    }

    let pl = &data[playlist_start..];
    let num_play_items = u16::from_be_bytes([pl[6], pl[7]]) as usize;

    // num_play_items is an untrusted u16 (max 65535); cap the pre-allocation
    // so a truncated/fuzz input can't force a large reservation that the
    // bounds-checked loop never fills. 256 covers any realistic playlist.
    let mut play_items = Vec::with_capacity(num_play_items.min(256));
    let mut streams = Vec::new();
    let mut pos = 10;

    for item_idx in 0..num_play_items {
        if pos + 2 > pl.len() {
            break;
        }
        let item_length = u16::from_be_bytes([pl[pos], pl[pos + 1]]) as usize;
        if pos + 2 + item_length > pl.len() {
            break;
        }

        let item = &pl[pos + 2..pos + 2 + item_length];
        if item.len() < 20 {
            pos += 2 + item_length;
            continue;
        }

        let clip_id = String::from_utf8_lossy(&item[0..5]).to_string();
        let connection_condition = item[9] & 0x0F;
        let in_time = u32::from_be_bytes([item[12], item[13], item[14], item[15]]);
        let out_time = u32::from_be_bytes([item[16], item[17], item[18], item[19]]);

        // Parse STN table from the first play item
        // PlayItem layout after out_time:
        //   [20:28] UO_mask_table (8 bytes)
        //   [28]    misc flags (1 byte)
        //   [29]    still_mode (1 byte)
        //   [30:32] still_time (2 bytes)
        //   [32:]   STN_table
        const STN_OFFSET: usize = 32;
        if item_idx == 0 && item.len() > STN_OFFSET + 16 {
            // STN header: length(2) + reserved(2) + counts(8) + reserved(4) = 16 bytes
            let n_video = item[STN_OFFSET + 4] as usize;
            let n_audio = item[STN_OFFSET + 5] as usize;
            let n_pg = item[STN_OFFSET + 6] as usize;
            let n_ig = item[STN_OFFSET + 7] as usize;
            let n_sec_audio = item[STN_OFFSET + 8] as usize;
            let n_sec_video = item[STN_OFFSET + 9] as usize;
            let n_pip_pg = item[STN_OFFSET + 10] as usize;
            let n_dv = item[STN_OFFSET + 11] as usize;

            let mut spos = STN_OFFSET + 16;

            // Primary video
            for _ in 0..n_video {
                if let Some((entry, next)) = parse_stream_entry(item, spos, STREAM_CATEGORY_VIDEO) {
                    streams.push(entry);
                    spos = next;
                } else {
                    break;
                }
            }
            // Primary audio
            for _ in 0..n_audio {
                if let Some((entry, next)) = parse_stream_entry(item, spos, STREAM_CATEGORY_AUDIO) {
                    streams.push(entry);
                    spos = next;
                } else {
                    break;
                }
            }
            // PG subtitles
            for _ in 0..n_pg {
                if let Some((entry, next)) =
                    parse_stream_entry(item, spos, STREAM_CATEGORY_PG_SUBTITLE)
                {
                    streams.push(entry);
                    spos = next;
                } else {
                    break;
                }
            }
            // IG (skip but advance)
            for _ in 0..n_ig {
                if let Some((_, next)) = parse_stream_entry(item, spos, STREAM_CATEGORY_IG) {
                    spos = next;
                } else {
                    break;
                }
            }
            // Secondary audio
            for _ in 0..n_sec_audio {
                if let Some((mut entry, next)) =
                    parse_stream_entry(item, spos, STREAM_CATEGORY_AUDIO)
                {
                    entry.stream_type = 5;
                    entry.secondary = true;
                    streams.push(entry);
                    // Skip extra ref bytes: num_refs(1) + reserved(1) + refs + padding
                    if next < item.len() {
                        let n_refs = item[next] as usize;
                        spos = next + 2 + n_refs + (n_refs % 2);
                    } else {
                        spos = next;
                    }
                } else {
                    break;
                }
            }
            // Secondary video (PiP)
            for _ in 0..n_sec_video {
                if let Some((mut entry, next)) =
                    parse_stream_entry(item, spos, STREAM_CATEGORY_VIDEO)
                {
                    entry.stream_type = 6;
                    entry.secondary = true;
                    streams.push(entry);
                    // Skip extra ref bytes (audio refs + PG refs).
                    // Use `next < item.len()` to match the sibling secondary
                    // blocks; the inner `after_arefs < item.len()` re-guards
                    // the second read, so the stricter `+2` only mis-aligned
                    // spos when the aref count sits in the last 1-2 bytes.
                    if next < item.len() {
                        let n_arefs = item[next] as usize;
                        let after_arefs = next + 2 + n_arefs + (n_arefs % 2);
                        if after_arefs < item.len() {
                            let n_prefs = item[after_arefs] as usize;
                            spos = after_arefs + 2 + n_prefs + (n_prefs % 2);
                        } else {
                            spos = after_arefs;
                        }
                    } else {
                        spos = next;
                    }
                } else {
                    break;
                }
            }
            // Secondary PG (PiP subtitles) — must consume to keep spos aligned
            for _ in 0..n_pip_pg {
                if let Some((mut entry, next)) =
                    parse_stream_entry(item, spos, STREAM_CATEGORY_PG_SUBTITLE)
                {
                    entry.secondary = true;
                    streams.push(entry);
                    // Skip reference data: num_refs(1) + reserved(1) + refs + padding
                    if next < item.len() {
                        let n_refs = item[next] as usize;
                        spos = next + 2 + n_refs + (n_refs % 2);
                    } else {
                        spos = next;
                    }
                } else {
                    break;
                }
            }
            // Dolby Vision enhancement layer
            for _ in 0..n_dv {
                if let Some((mut entry, next)) =
                    parse_stream_entry(item, spos, STREAM_CATEGORY_VIDEO)
                {
                    entry.stream_type = 7;
                    entry.secondary = true;
                    streams.push(entry);
                    spos = next;
                } else {
                    break;
                }
            }
        }

        play_items.push(PlayItem {
            clip_id,
            in_time,
            out_time,
            connection_condition,
        });

        pos += 2 + item_length;
    }

    // Parse PlayListMark section
    let mut marks = Vec::new();
    // The first real read is num_marks at ms[4..6], so the section needs
    // at least 6 bytes (length(4) + num_marks(2)).
    if mark_start > 0 && mark_start + 6 <= data.len() {
        let ms = &data[mark_start..];
        {
            let num_marks = u16::from_be_bytes([ms[4], ms[5]]) as usize;
            let mut mpos = 6;
            for _ in 0..num_marks {
                if mpos + 14 > ms.len() {
                    break;
                }
                // PlayListMark entry: reserved(1) + mark_type(1) +
                // ref_to_PlayItem_id(2) + mark_time_stamp(4) +
                // entry_ES_PID(2) + duration(4). mark_type is at +1, not +0.
                let mark_type = ms[mpos + 1];
                let play_item_ref = u16::from_be_bytes([ms[mpos + 2], ms[mpos + 3]]);
                let timestamp =
                    u32::from_be_bytes([ms[mpos + 4], ms[mpos + 5], ms[mpos + 6], ms[mpos + 7]]);
                marks.push(PlaylistMark {
                    mark_type,
                    play_item_ref,
                    timestamp,
                });
                mpos += 14;
            }
        }
    }

    Ok(Playlist {
        version,
        play_items,
        streams,
        marks,
    })
}

/// Parse one stream entry from the STN table.
/// Returns (StreamEntry, next position) or None.
/// BD `stream_entry()` type codes (the `stream_entry_type` field). Determine
/// where the PID sits within the entry — see `parse_stream_entry`.
const STREAM_ENTRY_PLAYITEM_CLIP: u8 = 0x01; // stream in the PlayItem's Clip
const STREAM_ENTRY_SUBPATH_SUBCLIP: u8 = 0x02; // stream in a SubPath SubClip
const STREAM_ENTRY_SUBPATH_CLIP: u8 = 0x03; // stream in a SubPath clip
const STREAM_ENTRY_SUBPATH_DV_EL: u8 = 0x04; // SubPath Dolby Vision enhancement layer

/// STN-table primary stream categories — the `stream_type` tag carried on each
/// [`StreamEntry`]. Secondary streams reuse the primary category and set the
/// `secondary` flag rather than carrying a distinct code.
const STREAM_CATEGORY_VIDEO: u8 = 1;
const STREAM_CATEGORY_AUDIO: u8 = 2;
const STREAM_CATEGORY_PG_SUBTITLE: u8 = 3;
const STREAM_CATEGORY_IG: u8 = 4;

fn parse_stream_entry(item: &[u8], pos: usize, stream_type: u8) -> Option<(StreamEntry, usize)> {
    use crate::consts::coding_type as c;
    if pos + 2 > item.len() {
        return None;
    }

    // Stream entry: length(1) + data
    let se_len = item[pos] as usize;
    let se_end = pos + 1 + se_len;
    if se_end > item.len() {
        return None;
    }

    // PID location depends on the stream-entry type (BD spec stream_entry()):
    //   type 1 (stream in the PlayItem's Clip):          PID at +2
    //   type 2 (stream in a SubPath SubClip):            +subpath_id(1)+subclip_id(1) → PID at +4
    //   type 3 / 4 (SubPath clip; type 4 = Dolby Vision  +subpath_id(1)              → PID at +3
    //              enhancement layer, e.g. PID 0x1015):
    // Previously only type 1 was handled, so the DV EL (type 4) and any
    // sub-path stream fell through to PID 0 and were dropped by the mux.
    let pid_off = match item[pos + 1] {
        STREAM_ENTRY_PLAYITEM_CLIP => 2,
        STREAM_ENTRY_SUBPATH_SUBCLIP => 4,
        STREAM_ENTRY_SUBPATH_CLIP | STREAM_ENTRY_SUBPATH_DV_EL => 3,
        _ => 0,
    };
    // Bound the PID read by the entry's declared end (se_end), not just by
    // item.len(): a short se_len must not let us read PID bytes out of the
    // following stream_attributes region.
    let pid = if pid_off != 0 && pos + pid_off + 2 <= se_end {
        u16::from_be_bytes([item[pos + pid_off], item[pos + pid_off + 1]])
    } else {
        0
    };

    // Stream attributes: length(1) + coding_type(1) + format-specific data
    if se_end + 2 > item.len() {
        return None;
    }
    let sa_len = item[se_end] as usize;
    let sa_end = se_end + 1 + sa_len;
    if sa_end > item.len() || sa_len < 1 {
        return None;
    }

    let sa = &item[se_end + 1..se_end + 1 + sa_len];
    let coding_type = sa[0];

    let mut video_format = 0u8;
    let mut video_rate = 0u8;
    let mut audio_format = 0u8;
    let mut audio_rate = 0u8;
    let mut dynamic_range = 0u8;
    let mut color_space_val = 0u8;
    let mut language = String::new();

    // `stream_type` here is the STN category passed by the caller, which is
    // only ever a primary category (VIDEO/AUDIO/PG_SUBTITLE/IG). Secondary
    // audio/video and the DV enhancement layer are parsed through their
    // matching primary category (identical attribute layout) and re-tagged by
    // the caller after this returns, so there are no secondary arms here.
    match stream_type {
        STREAM_CATEGORY_VIDEO => {
            // Video: coding_type(1) + format_rate(1) + [hdr_info(1) if HEVC]
            if sa.len() >= 2 {
                video_format = (sa[1] >> 4) & 0x0F;
                video_rate = sa[1] & 0x0F;
            }
            if coding_type == c::HEVC && sa.len() > 2 {
                dynamic_range = (sa[2] >> 4) & 0x0F;
                color_space_val = sa[2] & 0x0F;
            }
        }
        STREAM_CATEGORY_AUDIO => {
            // Audio: coding_type(1) + format_rate(1) + language(3)
            // Exception: PG/IG in an audio slot uses PG layout: coding_type(1) + language(3)
            if coding_type == c::PG || coding_type == c::IG {
                if sa.len() >= 4 {
                    language = String::from_utf8_lossy(&sa[1..4]).to_string();
                }
            } else {
                if sa.len() >= 2 {
                    audio_format = (sa[1] >> 4) & 0x0F;
                    audio_rate = sa[1] & 0x0F;
                }
                if sa.len() >= 5 {
                    language = String::from_utf8_lossy(&sa[2..5]).to_string();
                }
            }
        }
        STREAM_CATEGORY_PG_SUBTITLE => {
            // PG: coding_type(1) + language(3).
            // IG is parsed only to advance spos and is then discarded by the
            // caller, so it deliberately has no arm here.
            if sa.len() >= 4 {
                language = String::from_utf8_lossy(&sa[1..4]).to_string();
            }
        }
        _ => {}
    }

    Some((
        StreamEntry {
            stream_type,
            pid,
            coding_type,
            video_format,
            video_rate,
            audio_format,
            audio_rate,
            language,
            dynamic_range,
            color_space: color_space_val,
            secondary: false,
        },
        sa_end,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A mark entry for test MPLS building.
    struct TestMark {
        mark_type: u8,
        play_item_ref: u16,
        timestamp: u32,
    }

    /// Build a minimal MPLS binary with given play items and STN streams on the first item.
    /// STN counts: (n_video, n_audio, n_pg, n_ig, n_sec_audio, n_sec_video, n_pip_pg, n_dv)
    fn build_mpls(
        play_items_data: &[(
            /*clip_id*/ &[u8; 5],
            /*conn*/ u8,
            /*in_time*/ u32,
            /*out_time*/ u32,
        )],
        stn_counts: (u8, u8, u8, u8, u8, u8, u8, u8),
        stream_entries: &[Vec<u8>], // raw stream entry + attributes bytes for each stream
    ) -> Vec<u8> {
        build_mpls_with_marks(play_items_data, stn_counts, stream_entries, &[])
    }

    fn build_mpls_with_marks(
        play_items_data: &[(
            /*clip_id*/ &[u8; 5],
            /*conn*/ u8,
            /*in_time*/ u32,
            /*out_time*/ u32,
        )],
        stn_counts: (u8, u8, u8, u8, u8, u8, u8, u8),
        stream_entries: &[Vec<u8>],
        marks: &[TestMark],
    ) -> Vec<u8> {
        let playlist_start: u32 = 40; // right after the 40-byte header
        let mut buf = Vec::new();

        // File header: "MPLS" + version + playlist_start + mark_start placeholder
        buf.extend_from_slice(b"MPLS0200");
        buf.extend_from_slice(&playlist_start.to_be_bytes());
        // mark_start placeholder (will be patched), extension_start, padding to 40 bytes
        buf.extend_from_slice(&[0u8; 28]);

        // PlayList section starts here (offset 40)
        // PlayList: length(4) + reserved(2) + num_play_items(2) + num_sub_paths(2) = 10 header bytes
        let pl_start = buf.len();
        buf.extend_from_slice(&[0u8; 4]); // length placeholder
        buf.extend_from_slice(&[0u8; 2]); // reserved
        buf.extend_from_slice(&(play_items_data.len() as u16).to_be_bytes());
        buf.extend_from_slice(&[0u8; 2]); // num_sub_paths

        for (idx, (clip_id, conn, in_time, out_time)) in play_items_data.iter().enumerate() {
            // Build play item content
            let mut item = Vec::new();
            // [0..5] clip_id
            item.extend_from_slice(*clip_id);
            // [5..9] codec_id ("M2TS")
            item.extend_from_slice(b"M2TS");
            // [9] connection_condition in low nibble
            item.push(*conn & 0x0F);
            // [10..12] reserved
            item.extend_from_slice(&[0u8; 2]);
            // [12..16] in_time
            item.extend_from_slice(&in_time.to_be_bytes());
            // [16..20] out_time
            item.extend_from_slice(&out_time.to_be_bytes());
            // [20..28] UO_mask_table
            item.extend_from_slice(&[0u8; 8]);
            // [28] misc flags
            item.push(0);
            // [29] still_mode
            item.push(0);
            // [30..32] still_time
            item.extend_from_slice(&[0u8; 2]);

            // STN table (only for the first play item)
            if idx == 0 {
                // STN header: length(2) + reserved(2) + counts(8) + reserved(4) = 16 bytes
                let stn_header_start = item.len();
                item.extend_from_slice(&[0u8; 2]); // STN length placeholder
                item.extend_from_slice(&[0u8; 2]); // reserved
                item.push(stn_counts.0); // n_video
                item.push(stn_counts.1); // n_audio
                item.push(stn_counts.2); // n_pg
                item.push(stn_counts.3); // n_ig
                item.push(stn_counts.4); // n_sec_audio
                item.push(stn_counts.5); // n_sec_video
                item.push(stn_counts.6); // n_pip_pg
                item.push(stn_counts.7); // n_dv
                item.extend_from_slice(&[0u8; 4]); // reserved

                // Stream entries
                for se in stream_entries {
                    item.extend_from_slice(se);
                }

                // Patch STN length
                let stn_len = (item.len() - stn_header_start - 2) as u16;
                let stn_len_bytes = stn_len.to_be_bytes();
                item[stn_header_start] = stn_len_bytes[0];
                item[stn_header_start + 1] = stn_len_bytes[1];
            }

            // Write item_length(2) + item
            let item_length = item.len() as u16;
            buf.extend_from_slice(&item_length.to_be_bytes());
            buf.extend_from_slice(&item);
        }

        // Patch PlayList length
        let pl_len = (buf.len() - pl_start - 4) as u32;
        let pl_len_bytes = pl_len.to_be_bytes();
        buf[pl_start] = pl_len_bytes[0];
        buf[pl_start + 1] = pl_len_bytes[1];
        buf[pl_start + 2] = pl_len_bytes[2];
        buf[pl_start + 3] = pl_len_bytes[3];

        // Write PlayListMark section
        let mark_start = buf.len() as u32;
        // Patch mark_start offset in header (bytes 12-15)
        let ms_bytes = mark_start.to_be_bytes();
        buf[12] = ms_bytes[0];
        buf[13] = ms_bytes[1];
        buf[14] = ms_bytes[2];
        buf[15] = ms_bytes[3];

        // Mark section: length(4) + num_marks(2) + marks(14 each)
        let mark_section_len = 2 + marks.len() * 14;
        buf.extend_from_slice(&(mark_section_len as u32).to_be_bytes());
        buf.extend_from_slice(&(marks.len() as u16).to_be_bytes());
        for m in marks {
            buf.push(0); // [0] reserved
            buf.push(m.mark_type); // [1] mark_type
            buf.extend_from_slice(&m.play_item_ref.to_be_bytes()); // [2-3] play_item_ref
            buf.extend_from_slice(&m.timestamp.to_be_bytes()); // [4-7] timestamp
            buf.extend_from_slice(&[0u8; 6]); // [8-13] entry_ES_PID(2) + duration(4)
        }

        buf
    }

    /// Build a stream entry (stream_entry part + stream_attributes part).
    /// stream_entry: type=0x01 (PlayItem stream), PID given.
    /// For video: attrs = coding_type(1) + format_rate(1) [+ hdr_byte if HEVC]
    /// For audio: attrs = coding_type(1) + format_rate(1) + language(3)
    /// For PG:    attrs = coding_type(1) + language(3)
    fn build_stream_entry_video(
        pid: u16,
        coding_type: u8,
        format: u8,
        rate: u8,
        hdr: Option<u8>,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        // Stream entry: length(1) + sub_path_type(1) + pid(2)
        out.push(3); // se_len = 3 bytes (type + pid_hi + pid_lo)
        out.push(0x01); // type: PlayItem stream
        out.extend_from_slice(&pid.to_be_bytes());
        // Stream attributes
        let mut attrs = vec![coding_type, (format << 4) | rate];
        if let Some(h) = hdr {
            attrs.push(h);
        }
        out.push(attrs.len() as u8); // sa_len
        out.extend_from_slice(&attrs);
        out
    }

    fn build_stream_entry_audio(
        pid: u16,
        coding_type: u8,
        ch_layout: u8,
        sample_rate: u8,
        lang: &[u8; 3],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(3);
        out.push(STREAM_ENTRY_PLAYITEM_CLIP);
        out.extend_from_slice(&pid.to_be_bytes());
        // attrs: coding_type(1) + format_rate(1) + language(3)
        let attrs = vec![
            coding_type,
            (ch_layout << 4) | sample_rate,
            lang[0],
            lang[1],
            lang[2],
        ];
        out.push(attrs.len() as u8);
        out.extend_from_slice(&attrs);
        out
    }

    fn build_stream_entry_pg(pid: u16, coding_type: u8, lang: &[u8; 3]) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(3);
        out.push(STREAM_ENTRY_PLAYITEM_CLIP);
        out.extend_from_slice(&pid.to_be_bytes());
        // attrs: coding_type(1) + language(3)
        let attrs = vec![coding_type, lang[0], lang[1], lang[2]];
        out.push(attrs.len() as u8);
        out.extend_from_slice(&attrs);
        out
    }

    #[test]
    fn parse_valid_mpls() {
        let in_time: u32 = 90000; // 2 seconds at 45kHz
        let out_time: u32 = 4500000; // 100 seconds

        let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None); // H264, 1080p, 23.976
        let audio = build_stream_entry_audio(0x1100, 0x83, 6, 1, b"eng"); // TrueHD, 5.1, 48kHz
        let pg = build_stream_entry_pg(0x1200, 0x90, b"eng"); // PGS subtitle

        let data = build_mpls(
            &[(b"00001", 1, in_time, out_time)],
            (1, 1, 1, 0, 0, 0, 0, 0),
            &[video, audio, pg],
        );

        let playlist = parse(&data).expect("should parse valid MPLS");
        assert_eq!(playlist.version, "0200");
        assert_eq!(playlist.play_items.len(), 1);
        assert_eq!(playlist.play_items[0].clip_id, "00001");
        assert_eq!(playlist.play_items[0].in_time, in_time);
        assert_eq!(playlist.play_items[0].out_time, out_time);
        assert_eq!(playlist.play_items[0].connection_condition, 1);
    }

    #[test]
    fn parse_streams() {
        let video = build_stream_entry_video(0x1011, 0x24, 8, 1, Some(0x12)); // HEVC, 2160p, 23.976, HDR10+BT.2020
        let audio = build_stream_entry_audio(0x1100, 0x83, 6, 1, b"eng");
        let pg = build_stream_entry_pg(0x1200, 0x90, b"fra");

        let data = build_mpls(
            &[(b"00001", 1, 0, 9000000)],
            (1, 1, 1, 0, 0, 0, 0, 0),
            &[video, audio, pg],
        );

        let playlist = parse(&data).expect("should parse");
        assert_eq!(playlist.streams.len(), 3);

        // Video stream
        let v = &playlist.streams[0];
        assert_eq!(v.stream_type, 1);
        assert_eq!(v.pid, 0x1011);
        assert_eq!(v.coding_type, 0x24); // HEVC
        assert_eq!(v.video_format, 8); // 2160p
        assert_eq!(v.video_rate, 1); // 23.976
        assert_eq!(v.dynamic_range, 1); // HDR10
        assert_eq!(v.color_space, 2); // BT.2020
        assert!(!v.secondary);

        // Audio stream
        let a = &playlist.streams[1];
        assert_eq!(a.stream_type, 2);
        assert_eq!(a.pid, 0x1100);
        assert_eq!(a.coding_type, 0x83); // TrueHD
        assert_eq!(a.audio_format, 6); // 5.1
        assert_eq!(a.audio_rate, 1); // 48kHz
        assert_eq!(a.language, "eng");
        assert!(!a.secondary);

        // PG subtitle stream
        let s = &playlist.streams[2];
        assert_eq!(s.stream_type, 3);
        assert_eq!(s.pid, 0x1200);
        assert_eq!(s.coding_type, 0x90); // PGS
        assert_eq!(s.language, "fra");
        assert!(!s.secondary);
    }

    #[test]
    fn parse_invalid_magic() {
        let mut data = build_mpls(&[(b"00001", 1, 0, 9000000)], (0, 0, 0, 0, 0, 0, 0, 0), &[]);
        data[0] = b'X';
        data[1] = b'X';
        data[2] = b'X';
        data[3] = b'X';
        assert!(parse(&data).is_err());
    }

    #[test]
    fn parse_truncated() {
        // Less than 40 bytes
        assert!(parse(&[0u8; 10]).is_err());
        assert!(parse(b"MPLS0200").is_err());
        assert!(parse(&[0u8; 39]).is_err());
    }

    #[test]
    fn parse_multiple_play_items() {
        let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);

        let data = build_mpls(
            &[
                (b"00001", 1, 90000, 4500000),
                (b"00002", 5, 4500000, 9000000),
                (b"00003", 6, 9000000, 13500000),
            ],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[video],
        );

        let playlist = parse(&data).expect("should parse multiple play items");
        assert_eq!(playlist.play_items.len(), 3);
        assert_eq!(playlist.play_items[0].clip_id, "00001");
        assert_eq!(playlist.play_items[0].connection_condition, 1);
        assert_eq!(playlist.play_items[1].clip_id, "00002");
        assert_eq!(playlist.play_items[1].connection_condition, 5);
        assert_eq!(playlist.play_items[1].in_time, 4500000);
        assert_eq!(playlist.play_items[2].clip_id, "00003");
        assert_eq!(playlist.play_items[2].connection_condition, 6);
        assert_eq!(playlist.play_items[2].out_time, 13500000);
    }

    #[test]
    fn parse_secondary_streams() {
        // Primary video
        let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
        // Secondary audio (stream_type 5): build as audio, parser overrides type to 5
        let sec_audio_se = build_stream_entry_audio(0x1A00, 0x83, 3, 1, b"eng");
        // Need ref bytes after secondary audio: num_refs(1) + reserved(1) = 2 bytes min
        let mut sec_audio_with_refs = sec_audio_se;
        sec_audio_with_refs.push(0); // num_refs = 0
        sec_audio_with_refs.push(0); // reserved

        // Secondary video (stream_type 6): build as video, parser overrides type to 6
        let sec_video_se = build_stream_entry_video(0x1B00, 0x1B, 4, 1, None);
        // Need ref bytes: n_arefs(1) + reserved(1) + n_prefs(1) + reserved(1) = 4 bytes
        let mut sec_video_with_refs = sec_video_se;
        sec_video_with_refs.push(0); // n_arefs = 0
        sec_video_with_refs.push(0); // reserved
        sec_video_with_refs.push(0); // n_prefs = 0
        sec_video_with_refs.push(0); // reserved

        let data = build_mpls(
            &[(b"00001", 1, 0, 9000000)],
            (1, 0, 0, 0, 1, 1, 0, 0), // 1 video, 0 audio, 0 pg, 0 ig, 1 sec_audio, 1 sec_video
            &[video, sec_audio_with_refs, sec_video_with_refs],
        );

        let playlist = parse(&data).expect("should parse secondary streams");
        // Should have 3 streams: primary video, secondary audio, secondary video
        assert_eq!(playlist.streams.len(), 3);

        // Primary video
        assert_eq!(playlist.streams[0].stream_type, 1);
        assert!(!playlist.streams[0].secondary);

        // Secondary audio
        assert_eq!(playlist.streams[1].stream_type, 5);
        assert!(playlist.streams[1].secondary);
        assert_eq!(playlist.streams[1].pid, 0x1A00);

        // Secondary video
        assert_eq!(playlist.streams[2].stream_type, 6);
        assert!(playlist.streams[2].secondary);
        assert_eq!(playlist.streams[2].pid, 0x1B00);
    }

    #[test]
    fn parse_secondary_video_then_dv_alignment() {
        // Regression: the secondary-video ref-skip must use the same
        // `next < item.len()` guard as the sibling secondary blocks so spos
        // stays aligned for a following stream (here a Dolby Vision EL).
        let video = build_stream_entry_video(0x1011, 0x24, 8, 1, Some(0x12));

        // Secondary video with audio-ref + PG-ref blocks present.
        let mut sec_video_with_refs = build_stream_entry_video(0x1B00, 0x1B, 4, 1, None);
        sec_video_with_refs.push(0); // n_arefs = 0
        sec_video_with_refs.push(0); // reserved
        sec_video_with_refs.push(0); // n_prefs = 0
        sec_video_with_refs.push(0); // reserved

        // Dolby Vision enhancement layer immediately after.
        let dv_el = build_stream_entry_video(0x1015, 0x24, 8, 1, Some(0x12));

        let data = build_mpls(
            &[(b"00001", 1, 0, 9000000)],
            (1, 0, 0, 0, 0, 1, 0, 1), // 1 video, 1 sec_video, 1 dv
            &[video, sec_video_with_refs, dv_el],
        );

        let playlist = parse(&data).expect("should parse");
        assert_eq!(playlist.streams.len(), 3);
        // Secondary video
        assert_eq!(playlist.streams[1].stream_type, 6);
        assert_eq!(playlist.streams[1].pid, 0x1B00);
        // DV EL parsed at the correct offset → correct PID and type 7.
        assert_eq!(playlist.streams[2].stream_type, 7);
        assert_eq!(playlist.streams[2].pid, 0x1015);
        assert!(playlist.streams[2].secondary);
    }

    #[test]
    fn parse_marks_chapter_entries() {
        let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
        let marks = vec![
            TestMark {
                mark_type: 1,
                play_item_ref: 0,
                timestamp: 90000,
            },
            TestMark {
                mark_type: 1,
                play_item_ref: 0,
                timestamp: 4500000,
            },
            TestMark {
                mark_type: 1,
                play_item_ref: 0,
                timestamp: 9000000,
            },
        ];

        let data = build_mpls_with_marks(
            &[(b"00001", 1, 90000, 13500000)],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[video],
            &marks,
        );

        let playlist = parse(&data).expect("should parse marks");
        assert_eq!(playlist.marks.len(), 3);
        assert_eq!(playlist.marks[0].mark_type, 1);
        assert_eq!(playlist.marks[0].play_item_ref, 0);
        assert_eq!(playlist.marks[0].timestamp, 90000);
        assert_eq!(playlist.marks[1].timestamp, 4500000);
        assert_eq!(playlist.marks[2].timestamp, 9000000);
    }

    #[test]
    fn parse_marks_chapter_timestamps_correct() {
        let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
        let in_time: u32 = 90000;

        // Chapters at 0s, 100s, 200s relative to in_time
        let marks = vec![
            TestMark {
                mark_type: 1,
                play_item_ref: 0,
                timestamp: in_time,
            },
            TestMark {
                mark_type: 1,
                play_item_ref: 0,
                timestamp: in_time + 45000 * 100,
            },
            TestMark {
                mark_type: 1,
                play_item_ref: 0,
                timestamp: in_time + 45000 * 200,
            },
            TestMark {
                mark_type: 2,
                play_item_ref: 0,
                timestamp: in_time + 45000 * 50,
            }, // non-chapter mark
        ];

        let data = build_mpls_with_marks(
            &[(b"00001", 1, in_time, in_time + 45000 * 300)],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[video],
            &marks,
        );

        let playlist = parse(&data).expect("should parse");
        // All 4 marks should be parsed
        assert_eq!(playlist.marks.len(), 4);
        // Chapter marks (type 1) are 3 of them
        let chapter_marks: Vec<_> = playlist.marks.iter().filter(|m| m.mark_type == 1).collect();
        assert_eq!(chapter_marks.len(), 3);
        // Non-chapter mark (type 2)
        assert_eq!(playlist.marks[3].mark_type, 2);

        // Verify timestamp conversion: (timestamp - in_time) / 45000
        let ch0_secs = (chapter_marks[0].timestamp as f64 - in_time as f64) / 45000.0;
        let ch1_secs = (chapter_marks[1].timestamp as f64 - in_time as f64) / 45000.0;
        let ch2_secs = (chapter_marks[2].timestamp as f64 - in_time as f64) / 45000.0;
        assert!((ch0_secs - 0.0).abs() < 0.001);
        assert!((ch1_secs - 100.0).abs() < 0.001);
        assert!((ch2_secs - 200.0).abs() < 0.001);
    }

    #[test]
    fn mark_type_read_from_correct_offset() {
        // Regression for the mark_type off-by-one: each PlayListMark entry is
        // reserved(1) + mark_type(1) + .... The parser must read byte[1], not
        // byte[0]. build_mpls_with_marks writes reserved=0 at byte[0] and the
        // mark_type at byte[1], so a parser that read byte[0] would see 0 for
        // every mark. Use distinct non-zero, non-1 types to make the offset
        // error unmistakable.
        let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
        let marks = vec![
            TestMark {
                mark_type: 1, // entry mark (chapter)
                play_item_ref: 0,
                timestamp: 90000,
            },
            TestMark {
                mark_type: 2, // link point (not a chapter)
                play_item_ref: 0,
                timestamp: 180000,
            },
            TestMark {
                mark_type: 3, // arbitrary other type
                play_item_ref: 0,
                timestamp: 270000,
            },
        ];

        let data = build_mpls_with_marks(
            &[(b"00001", 1, 0, 9000000)],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[video],
            &marks,
        );

        let playlist = parse(&data).expect("should parse marks");
        assert_eq!(playlist.marks.len(), 3);
        // If the parser read the reserved byte (byte[0] == 0) these would all
        // be 0; reading byte[1] yields the real types.
        assert_eq!(playlist.marks[0].mark_type, 1);
        assert_eq!(playlist.marks[1].mark_type, 2);
        assert_eq!(playlist.marks[2].mark_type, 3);
        // Only the type-1 mark is a chapter under the corrected convention.
        let chapters = playlist.marks.iter().filter(|m| m.mark_type == 1).count();
        assert_eq!(chapters, 1);
    }

    #[test]
    fn parse_no_marks_section() {
        // When mark_start is 0, no marks should be returned
        let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
        let data = build_mpls(
            &[(b"00001", 1, 90000, 4500000)],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[video],
        );
        let playlist = parse(&data).expect("should parse without marks");
        // build_mpls writes an empty mark section (0 marks)
        assert_eq!(playlist.marks.len(), 0);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Added hardening tests below. Grounded in the BD-ROM MPLS spec
    // (https://github.com/lw/BluRay/wiki/MPLS) byte layout.
    // ─────────────────────────────────────────────────────────────────────

    /// Header guard: parse() requires `playlist_start + 10 <= data.len()`
    /// before reading the PlayList header (num_play_items at pl[6..8]).
    /// A playlist_start that points past EOF must be rejected with
    /// MplsParse, not panic.
    #[test]
    fn playlist_start_past_eof_errs() {
        let mut data = build_mpls(&[(b"00001", 1, 0, 9000000)], (0, 0, 0, 0, 0, 0, 0, 0), &[]);
        // Overwrite PlayList_start_address (bytes 8..12) with a huge offset.
        data[8..12].copy_from_slice(&0xFFFF_0000u32.to_be_bytes());
        assert!(parse(&data).is_err());
    }

    /// Spec: connection_condition is the LOW nibble of PlayItem byte[9]
    /// (high nibble is reserved/flags). A byte 0xF5 must yield 5, not 0xF5.
    #[test]
    fn connection_condition_is_low_nibble_only() {
        // Build a custom item where byte[9] = 0xF5 (high nibble set).
        // build_mpls masks with &0x0F when writing, so write raw to verify
        // the PARSER masks. We patch the item byte directly after building.
        let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
        let mut data = build_mpls(
            &[(b"00001", 0, 0, 9000000)],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[video],
        );
        // Locate PlayItem byte[9]: header(40) + pl_header(10) + item_len(2) + 9.
        let conn_idx = 40 + 10 + 2 + 9;
        data[conn_idx] = 0xF5;
        let pl = parse(&data).expect("should parse");
        assert_eq!(pl.play_items[0].connection_condition, 0x05);
    }

    /// stream_entry() PID location for type 0x02 (stream in a SubPath
    /// SubClip): subpath_id(1)+subclip_id(1) precede the PID, so PID is at
    /// +4 within the entry. A parser that read +2 (type-1 layout) would
    /// pick up the subpath/subclip bytes as the PID.
    #[test]
    fn stream_entry_type2_pid_at_offset_4() {
        // Build a primary-audio entry with stream_entry type 0x02.
        // se_len = 5: type(1) + subpath_id(1) + subclip_id(1) + pid(2)
        let mut se = vec![
            5,                            // se_len
            STREAM_ENTRY_SUBPATH_SUBCLIP, // type: SubPath SubClip
            0xAA,                         // subpath_id (must NOT be read as PID hi)
            0xBB,                         // subclip_id
        ];
        se.extend_from_slice(&0x1100u16.to_be_bytes()); // real PID at +4
        // stream_attributes: audio coding(1)+fmt(1)+lang(3)
        let attrs = vec![0x83u8, (6 << 4) | 1, b'e', b'n', b'g'];
        se.push(attrs.len() as u8);
        se.extend_from_slice(&attrs);

        let data = build_mpls(
            &[(b"00001", 1, 0, 9000000)],
            (0, 1, 0, 0, 0, 0, 0, 0),
            &[se],
        );
        let pl = parse(&data).expect("should parse");
        assert_eq!(pl.streams.len(), 1);
        assert_eq!(pl.streams[0].pid, 0x1100);
    }

    /// stream_entry() PID for type 0x03/0x04 (SubPath clip; 0x04 = DV EL):
    /// subpath_id(1) precedes PID, so PID is at +3. Cited in source:
    /// DV EL PID e.g. 0x1015.
    #[test]
    fn stream_entry_type4_pid_at_offset_3() {
        let mut se = Vec::new();
        se.push(4); // se_len: type(1)+subpath_id(1)+pid(2)
        se.push(STREAM_ENTRY_SUBPATH_DV_EL); // type 4 (DV EL)
        se.push(0x07); // subpath_id (not PID)
        se.extend_from_slice(&0x1015u16.to_be_bytes()); // PID at +3
        let attrs = vec![0x24u8, (8 << 4) | 1, 0x12]; // HEVC video attrs
        se.push(attrs.len() as u8);
        se.extend_from_slice(&attrs);

        // Put it in the primary-video slot so it's retained as a stream.
        let data = build_mpls(
            &[(b"00001", 1, 0, 9000000)],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[se],
        );
        let pl = parse(&data).expect("should parse");
        assert_eq!(pl.streams.len(), 1);
        assert_eq!(pl.streams[0].pid, 0x1015);
    }

    /// stream_entry() unknown type → pid_off match arm `_ => 0`, so PID is
    /// left 0. A type byte of 0x09 (not 1/2/3/4) must yield pid 0, never an
    /// out-of-spec read. Grounded in the explicit default arm in source.
    #[test]
    fn stream_entry_unknown_type_pid_zero() {
        let mut se = Vec::new();
        se.push(3);
        se.push(0x09); // unknown stream_entry type
        se.extend_from_slice(&0x1234u16.to_be_bytes());
        let attrs = vec![0x24u8, (8 << 4) | 1];
        se.push(attrs.len() as u8);
        se.extend_from_slice(&attrs);
        let data = build_mpls(
            &[(b"00001", 1, 0, 9000000)],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[se],
        );
        let pl = parse(&data).expect("should parse");
        assert_eq!(pl.streams.len(), 1);
        assert_eq!(pl.streams[0].pid, 0); // unknown type → PID not read
    }

    /// Video stream_attributes: byte[1] high nibble = video_format, low
    /// nibble = video_rate (BD spec format/frame_rate packing). Verify the
    /// split: 0x84 → format 8 (2160p), rate 4.
    #[test]
    fn video_attr_nibble_split() {
        let video = build_stream_entry_video(0x1011, 0x1B, 8, 4, None);
        let data = build_mpls(
            &[(b"00001", 1, 0, 9000000)],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[video],
        );
        let pl = parse(&data).expect("should parse");
        assert_eq!(pl.streams[0].video_format, 8);
        assert_eq!(pl.streams[0].video_rate, 4);
    }

    /// HDR byte (HEVC only, coding_type 0x24): sa[2] high nibble =
    /// dynamic_range, low nibble = color_space. For a non-HEVC video
    /// (e.g. H264 0x1B) the HDR byte must NOT be consumed even if present,
    /// per the `coding_type == 0x24` guard.
    #[test]
    fn hdr_byte_only_for_hevc() {
        // H264 video with a third attr byte present — must stay SDR/unknown.
        let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, Some(0x12));
        let data = build_mpls(
            &[(b"00001", 1, 0, 9000000)],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[video],
        );
        let pl = parse(&data).expect("should parse");
        assert_eq!(pl.streams[0].coding_type, 0x1B);
        assert_eq!(pl.streams[0].dynamic_range, 0); // not parsed for H264
        assert_eq!(pl.streams[0].color_space, 0);
    }

    /// Audio language is at sa[2..5] (after coding_type + format_rate),
    /// EXCEPT when the audio slot carries a PG coding_type (0x90/0x91),
    /// where the layout is coding_type(1)+language(3) → lang at sa[1..4].
    /// This branch is explicit in source. Verify the PG-in-audio path.
    #[test]
    fn pg_coding_in_audio_slot_uses_pg_lang_offset() {
        // Audio-slot entry but coding_type 0x90 (PGS): attrs = 0x90 + lang(3).
        let mut se = Vec::new();
        se.push(3);
        se.push(STREAM_ENTRY_PLAYITEM_CLIP);
        se.extend_from_slice(&0x1100u16.to_be_bytes());
        let attrs = vec![0x90u8, b'j', b'p', b'n']; // PG layout: coding + lang
        se.push(attrs.len() as u8);
        se.extend_from_slice(&attrs);
        let data = build_mpls(
            &[(b"00001", 1, 0, 9000000)],
            (0, 1, 0, 0, 0, 0, 0, 0),
            &[se],
        );
        let pl = parse(&data).expect("should parse");
        assert_eq!(pl.streams[0].coding_type, 0x90);
        assert_eq!(pl.streams[0].language, "jpn"); // read from sa[1..4]
        // audio_format/rate not parsed in PG branch.
        assert_eq!(pl.streams[0].audio_format, 0);
    }

    /// IG streams (count n_ig, stream_type 4) are consumed to keep the STN
    /// cursor aligned but NEVER retained as StreamEntry (doc'd in source).
    /// An STN with 1 video + 1 IG + 1 PG must report exactly the video and
    /// PG, and the PG must keep its correct PID (proving IG advanced spos).
    #[test]
    fn ig_consumed_but_not_retained_and_dv_after_aligned() {
        // STN parse order is video, audio, PG, IG, sec_audio, sec_video,
        // pip_pg, DV. The IG entry must be consumed (advancing spos) but
        // never retained. To PROVE IG advanced the cursor, place a Dolby
        // Vision EL after the IG: if IG didn't advance spos, the DV parse
        // would land on the IG bytes and read the wrong PID.
        let video = build_stream_entry_video(0x1011, 0x24, 8, 1, Some(0x12));
        let ig = build_stream_entry_pg(0x1400, 0x91, b"eng"); // IG entry bytes
        let dv = build_stream_entry_video(0x1015, 0x24, 8, 1, Some(0x12)); // DV EL
        let data = build_mpls(
            &[(b"00001", 1, 0, 9000000)],
            (1, 0, 0, 1, 0, 0, 0, 1), // 1 video, 1 ig, 1 dv
            &[video, ig, dv],
        );
        let pl = parse(&data).expect("should parse");
        // 2 retained streams: video + DV EL (IG dropped).
        assert_eq!(pl.streams.len(), 2);
        assert_eq!(pl.streams[0].stream_type, 1);
        assert_eq!(pl.streams[0].pid, 0x1011);
        // DV EL parsed at correct offset → IG advanced spos past 0x1400.
        assert_eq!(pl.streams[1].stream_type, 7);
        assert_eq!(pl.streams[1].pid, 0x1015);
        assert!(pl.streams.iter().all(|s| s.pid != 0x1400));
    }

    /// parse_stream_entry short-circuits when the declared stream_entry
    /// length runs past the item end (`se_end > item.len()` → None). The
    /// STN count loop then `break`s, so a truncated entry yields fewer
    /// streams without panicking. Build n_video=2 but only enough bytes
    /// for 1 full entry plus a too-long second.
    #[test]
    fn truncated_stream_entry_stops_without_panic() {
        let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
        // Second "entry" declares se_len=200 but supplies no body → None.
        let bad = vec![200u8, STREAM_ENTRY_PLAYITEM_CLIP];
        let data = build_mpls(
            &[(b"00001", 1, 0, 9000000)],
            (2, 0, 0, 0, 0, 0, 0, 0), // claims 2 video
            &[video, bad],
        );
        let pl = parse(&data).expect("should not panic on truncated entry");
        // Only the first parsed; second aborted the loop.
        assert_eq!(pl.streams.len(), 1);
        assert_eq!(pl.streams[0].pid, 0x1011);
    }

    /// PID must be bounded by the entry's declared se_end, not item.len():
    /// a short se_len must leave PID 0 rather than reading into the
    /// following stream_attributes region (explicit in source comment).
    /// se_len=1 (only the type byte) for a type-1 entry → PID read would
    /// need bytes at +2/+3 which are inside attrs, so PID must be 0.
    #[test]
    fn short_se_len_does_not_read_pid_from_attrs() {
        // se_len = 1: just the type byte, no PID bytes within the entry.
        let mut se = Vec::new();
        se.push(1); // se_len = 1
        se.push(0x01); // type 1; PID would be at +2 but that's past se_end
        // stream_attributes follow immediately.
        let attrs = vec![0x1Bu8, (6 << 4) | 1];
        se.push(attrs.len() as u8);
        se.extend_from_slice(&attrs);
        let data = build_mpls(
            &[(b"00001", 1, 0, 9000000)],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[se],
        );
        let pl = parse(&data).expect("should parse");
        assert_eq!(pl.streams.len(), 1);
        // PID bytes lie outside the declared entry → must be 0, not attrs.
        assert_eq!(pl.streams[0].pid, 0);
    }

    /// parse_stream_entry rejects sa_len == 0 (`sa_len < 1` → None). A
    /// zero-length stream_attributes block means the entry is unusable and
    /// the STN loop must break, not push a degenerate StreamEntry.
    #[test]
    fn zero_length_stream_attributes_yields_no_stream() {
        let mut se = Vec::new();
        se.push(3);
        se.push(STREAM_ENTRY_PLAYITEM_CLIP);
        se.extend_from_slice(&0x1011u16.to_be_bytes());
        se.push(0); // sa_len = 0 → parse_stream_entry returns None
        let data = build_mpls(
            &[(b"00001", 1, 0, 9000000)],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[se],
        );
        let pl = parse(&data).expect("should parse");
        assert_eq!(pl.streams.len(), 0);
    }

    /// PlayListMark timestamp is a big-endian u32 at entry offset +4..+8
    /// (after reserved(1)+mark_type(1)+ref(2)). Verify BE decode and that
    /// ref_to_PlayItem_id is read from +2..+4.
    #[test]
    fn mark_timestamp_and_ref_offsets() {
        let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
        let marks = vec![TestMark {
            mark_type: 1,
            play_item_ref: 0x0203,
            timestamp: 0x0A0B0C0D,
        }];
        let data = build_mpls_with_marks(
            &[(b"00001", 1, 0, 9000000)],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[video],
            &marks,
        );
        let pl = parse(&data).expect("should parse");
        assert_eq!(pl.marks.len(), 1);
        assert_eq!(pl.marks[0].play_item_ref, 0x0203);
        assert_eq!(pl.marks[0].timestamp, 0x0A0B0C0D);
    }

    /// num_marks is read from ms[4..6] (after length(4)). Each entry is
    /// strictly 14 bytes. The loop must stop when fewer than 14 bytes
    /// remain (`mpos + 14 > ms.len()` → break) rather than panic, so a
    /// num_marks that overshoots the actual byte count is safe.
    #[test]
    fn mark_count_overshoot_truncates_safely() {
        let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
        let marks = vec![
            TestMark {
                mark_type: 1,
                play_item_ref: 0,
                timestamp: 100,
            },
            TestMark {
                mark_type: 1,
                play_item_ref: 0,
                timestamp: 200,
            },
        ];
        let mut data = build_mpls_with_marks(
            &[(b"00001", 1, 0, 9000000)],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[video],
            &marks,
        );
        // Find mark_start (header bytes 12..16) and bump num_marks to 99.
        let mark_start = u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;
        // num_marks at ms[4..6].
        data[mark_start + 4] = 0;
        data[mark_start + 5] = 99;
        let pl = parse(&data).expect("should not panic on mark overshoot");
        // Only the 2 real marks fit; the loop broke at the 3rd.
        assert_eq!(pl.marks.len(), 2);
    }

    /// Mark section guard: `mark_start + 6 <= data.len()` is required before
    /// reading num_marks at ms[4..6]. A mark_start pointing within 5 bytes
    /// of EOF must yield zero marks, not panic.
    #[test]
    fn mark_start_near_eof_yields_no_marks() {
        let video = build_stream_entry_video(0x1011, 0x1B, 6, 1, None);
        let mut data = build_mpls(
            &[(b"00001", 1, 0, 9000000)],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[video],
        );
        // Point mark_start to len-3 (only 3 bytes remain < 6 needed).
        let near = (data.len() - 3) as u32;
        data[12..16].copy_from_slice(&near.to_be_bytes());
        let pl = parse(&data).expect("should parse");
        assert_eq!(pl.marks.len(), 0);
    }

    /// A PlayItem whose declared item_length leaves fewer than 20 bytes of
    /// body is skipped (`item.len() < 20` → continue) — its clip_id/times
    /// are not parsed, but the cursor advances and following items still
    /// parse. Grounded in the `if item.len() < 20` guard.
    #[test]
    fn short_play_item_skipped_cursor_advances() {
        // Construct two items manually: a short (10-byte) first item, then
        // a valid second item. We can't use build_mpls (it always writes
        // ≥32-byte items), so assemble directly.
        let playlist_start: u32 = 40;
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MPLS0200");
        buf.extend_from_slice(&playlist_start.to_be_bytes());
        buf.extend_from_slice(&[0u8; 28]); // mark_start=0 + padding

        let pl_start = buf.len();
        buf.extend_from_slice(&[0u8; 4]); // pl length placeholder
        buf.extend_from_slice(&[0u8; 2]); // reserved
        buf.extend_from_slice(&2u16.to_be_bytes()); // num_play_items = 2
        buf.extend_from_slice(&[0u8; 2]); // num_sub_paths

        // Item 0: length 10 (< 20) → skipped.
        let short = vec![0u8; 10];
        buf.extend_from_slice(&(short.len() as u16).to_be_bytes());
        buf.extend_from_slice(&short);

        // Item 1: a valid 32-byte item with clip_id "00009".
        let mut item = Vec::new();
        item.extend_from_slice(b"00009");
        item.extend_from_slice(b"M2TS");
        item.push(0x01); // connection_condition
        item.extend_from_slice(&[0u8; 2]);
        item.extend_from_slice(&90000u32.to_be_bytes()); // in_time
        item.extend_from_slice(&180000u32.to_be_bytes()); // out_time
        item.resize(32, 0); // pad through STN_OFFSET; item.len()==32 so no STN
        buf.extend_from_slice(&(item.len() as u16).to_be_bytes());
        buf.extend_from_slice(&item);

        let pl_len = (buf.len() - pl_start - 4) as u32;
        buf[pl_start..pl_start + 4].copy_from_slice(&pl_len.to_be_bytes());

        let pl = parse(&buf).expect("should parse with a short leading item");
        // Only the valid second item is retained.
        assert_eq!(pl.play_items.len(), 1);
        assert_eq!(pl.play_items[0].clip_id, "00009");
        assert_eq!(pl.play_items[0].in_time, 90000);
    }

    /// data.len() exactly 40 with valid magic but playlist_start past the
    /// header: parse() must hit the `playlist_start + 10 > data.len()`
    /// guard. A 40-byte buffer with playlist_start=40 has no PlayList body.
    #[test]
    fn exactly_40_bytes_no_playlist_body_errs() {
        let mut data = vec![0u8; 40];
        data[0..4].copy_from_slice(b"MPLS");
        data[4..8].copy_from_slice(b"0200");
        data[8..12].copy_from_slice(&40u32.to_be_bytes()); // playlist_start = 40 = len
        assert!(parse(&data).is_err());
    }
}
