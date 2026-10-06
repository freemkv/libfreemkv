//! Blu-ray MPLS playlists: clips, in/out timestamps and stream tables.
//!
//! The STN table begins at PlayItem offset 32 for single-angle items. Multi-angle
//! items insert a two-byte header and ten bytes per additional angle before STN,
//! so its offset is `32 + 2 + (number_of_angles - 1) * 10`. The primary angle's
//! clip is already present at the start of the PlayItem.

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
    /// timebase needed to place a mark in a multi-PlayItem playlist; the
    /// chapter builder resolves each mark against that PlayItem.
    pub play_item_ref: u16,
    /// Timestamp in 45kHz PTS ticks
    pub timestamp: u32,
}

impl PlaylistMark {
    // Only mark_type == 1 is a chapter (0 = reserved, 2 = link point).
    // Route all chapter filters through here; a hand-rolled `<= 1` copy
    // once drifted and silently counted reserved marks as chapters.
    pub(crate) fn is_chapter_mark(&self) -> bool {
        self.mark_type == 1
    }
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
    /// Connection condition (1=non-seamless, 5/6=seamless). Parsed for
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
    /// Audio channel layout (1=mono, 3=stereo, 6=5.1, 12=combo)
    pub audio_format: u8,
    /// Audio sample rate (1=48kHz, 4=96kHz, 5=192kHz)
    pub audio_rate: u8,
    /// ISO 639-2 language code (e.g. "eng")
    pub language: String,
    /// HDR dynamic range (0=SDR, 1=HDR10, 2=Dolby Vision)
    pub dynamic_range: u8,
    /// Color space (0=unknown, 1=BT.709, 2=BT.2020)
    pub color_space: u8,
    /// HEVC `hdr_plus_flag`: bit 6 of the byte after `color_space` (bit 7 is
    /// `cr_flag`), set when the stream carries HDR10+ (ST 2094-40) metadata.
    pub hdr_plus: bool,
    /// Whether this is a secondary stream (commentary, PiP, DV EL)
    pub secondary: bool,
}

/// Parse an MPLS file from raw bytes.
///
/// `data` is the raw contents of a `BDMV/PLAYLIST/*.mpls` file. Returns
/// [`Error::MplsParse`] on a malformed header, or when truncation leaves no play item;
/// a truncated item list keeps the items before the damage (warned).
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
    let mut truncated = false;

    for item_idx in 0..num_play_items {
        // A truncated item ends the list; the items parsed so far are kept.
        if pos + 2 > pl.len() {
            truncated = true;
            break;
        }
        let item_length = u16::from_be_bytes([pl[pos], pl[pos + 1]]) as usize;
        if pos + 2 + item_length > pl.len() {
            truncated = true;
            break;
        }

        let item = &pl[pos + 2..pos + 2 + item_length];
        if item.len() < 20 {
            tracing::warn!(
                target: "freemkv::mpls",
                item_idx,
                item_length,
                "play item too short to read; it is dropped from the playlist"
            );
            pos += 2 + item_length;
            continue;
        }

        let clip_id = String::from_utf8_lossy(&item[0..5]).to_string();
        // After clip_id[0..5] and codec_id[5..9], item[9] is fully reserved;
        // item[10] holds is_multi_angle (bit 4) plus connection_condition in its
        // low nibble. stc_id follows at item[11] and in_time at item[12..16].
        let connection_condition = item[10] & 0x0F;
        let in_time = u32::from_be_bytes([item[12], item[13], item[14], item[15]]);
        let out_time = u32::from_be_bytes([item[16], item[17], item[18], item[19]]);

        // STN table offset: base 32, shifted past the multi-angle angle block when
        // present (issue #45 — see the module `//!` header for the layout and why a
        // fixed 32 misparses). is_multi_angle is bit 4 of item[10].
        let is_multi_angle = (item[10] & 0x10) != 0;
        let stn_offset = if is_multi_angle && item.len() > 32 {
            let number_of_angles = item[32] as usize;
            32 + 2 + number_of_angles.saturating_sub(1) * 10
        } else {
            32
        };
        // `>=`: the 16-byte STN header spans item[stn_offset..stn_offset+16], so a
        // stream-less STN table ending exactly at the item boundary is still read.
        if item_idx == 0 && item.len() >= stn_offset + 16 {
            // STN header: length(2) + reserved(2) + counts(8) + reserved(4) = 16 bytes
            let n_video = item[stn_offset + 4] as usize;
            let n_audio = item[stn_offset + 5] as usize;
            let n_pg = item[stn_offset + 6] as usize;
            let n_ig = item[stn_offset + 7] as usize;
            let n_sec_audio = item[stn_offset + 8] as usize;
            let n_sec_video = item[stn_offset + 9] as usize;
            let n_pip_pg = item[stn_offset + 10] as usize;
            let n_dv = item[stn_offset + 11] as usize;

            let mut spos = stn_offset + 16;

            // Primary video
            for _ in 0..n_video {
                if let Some((entry, next)) = parse_stream_entry(item, spos, STREAM_CATEGORY_VIDEO) {
                    streams.push(entry);
                    spos = next;
                } else {
                    stn_entry_unreadable("primary video");
                    break;
                }
            }
            // Primary audio
            for _ in 0..n_audio {
                if let Some((entry, next)) = parse_stream_entry(item, spos, STREAM_CATEGORY_AUDIO) {
                    streams.push(entry);
                    spos = next;
                } else {
                    stn_entry_unreadable("primary audio");
                    break;
                }
            }
            // PG/TextST then PiP PG in one loop, before IG, no ref block (libbluray
            // _parse_stn, reverse-engineered player layout).
            for i in 0..n_pg + n_pip_pg {
                if let Some((mut entry, next)) =
                    parse_stream_entry(item, spos, STREAM_CATEGORY_PG_SUBTITLE)
                {
                    entry.secondary = i >= n_pg;
                    streams.push(entry);
                    spos = next;
                } else {
                    stn_entry_unreadable("PG subtitle");
                    break;
                }
            }
            // IG (skip but advance)
            for _ in 0..n_ig {
                if let Some((_, next)) = parse_stream_entry(item, spos, STREAM_CATEGORY_IG) {
                    spos = next;
                } else {
                    stn_entry_unreadable("IG");
                    break;
                }
            }
            // Secondary audio
            for _ in 0..n_sec_audio {
                if let Some((mut entry, next)) =
                    parse_stream_entry(item, spos, STREAM_CATEGORY_AUDIO)
                {
                    entry.stream_type = STREAM_CATEGORY_SECONDARY_AUDIO;
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
                    stn_entry_unreadable("secondary audio");
                    break;
                }
            }
            // Secondary video (PiP)
            for _ in 0..n_sec_video {
                if let Some((mut entry, next)) =
                    parse_stream_entry(item, spos, STREAM_CATEGORY_VIDEO)
                {
                    entry.stream_type = STREAM_CATEGORY_SECONDARY_VIDEO;
                    entry.secondary = true;
                    streams.push(entry);
                    // Skip extra ref bytes (audio refs + PG refs). `next < item.len()`
                    // matches the sibling secondary blocks; the inner
                    // `after_arefs < item.len()` re-guards the second read near the end.
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
                    stn_entry_unreadable("secondary video");
                    break;
                }
            }
            // Dolby Vision enhancement layer
            for _ in 0..n_dv {
                if let Some((mut entry, next)) =
                    parse_stream_entry(item, spos, STREAM_CATEGORY_VIDEO)
                {
                    entry.stream_type = STREAM_CATEGORY_DV_EL;
                    entry.secondary = true;
                    streams.push(entry);
                    spos = next;
                } else {
                    stn_entry_unreadable("Dolby Vision enhancement layer");
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

    if truncated {
        if play_items.is_empty() {
            return Err(Error::MplsParse);
        }
        tracing::warn!(
            target: "freemkv::mpls",
            kept = play_items.len(),
            declared = num_play_items,
            "truncated playlist: keeping the play items parsed so far"
        );
    }

    // Parse PlayListMark section
    let mut marks = Vec::new();
    // The first real read is num_marks at ms[4..6], so the section needs
    // at least 6 bytes (length(4) + num_marks(2)).
    if mark_start > 0 && mark_start + 6 <= data.len() {
        let ms = &data[mark_start..];
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

    Ok(Playlist {
        version,
        play_items,
        streams,
        marks,
    })
}

// An unreadable stream entry ends its STN category and every one after it.
fn stn_entry_unreadable(kind: &str) {
    tracing::warn!(
        target: "freemkv::mpls",
        kind,
        "unreadable STN stream entry; this and the later stream entries are dropped"
    );
}

impl Playlist {
    /// Total play-item running time in 45 kHz ticks (out_time - in_time, summed).
    pub fn duration_ticks(&self) -> u64 {
        self.play_items
            .iter()
            .map(|pi| pi.out_time.saturating_sub(pi.in_time) as u64)
            .sum()
    }
}

// Parse one stream entry from the STN table.
// Returns (StreamEntry, next position) or None. BD `stream_entry()` type
// codes (`stream_entry_type` field); determine where the PID sits — see `parse_stream_entry`.
const STREAM_ENTRY_PLAYITEM_CLIP: u8 = 0x01; // stream in the PlayItem's Clip
const STREAM_ENTRY_SUBPATH_SUBCLIP: u8 = 0x02; // stream in a SubPath SubClip
const STREAM_ENTRY_SUBPATH_CLIP: u8 = 0x03; // stream in a SubPath clip
const STREAM_ENTRY_SUBPATH_DV_EL: u8 = 0x04; // SubPath Dolby Vision enhancement layer

/// STN-table stream categories — the `stream_type` tag carried on each
/// [`StreamEntry`]. `parse` re-tags secondary audio/video and the DV
/// enhancement layer with the SECONDARY_*/DV_EL codes and sets `secondary`.
pub(crate) const STREAM_CATEGORY_VIDEO: u8 = 1;
pub(crate) const STREAM_CATEGORY_AUDIO: u8 = 2;
const STREAM_CATEGORY_PG_SUBTITLE: u8 = 3;
const STREAM_CATEGORY_IG: u8 = 4;
const STREAM_CATEGORY_SECONDARY_AUDIO: u8 = 5;
const STREAM_CATEGORY_SECONDARY_VIDEO: u8 = 6;
const STREAM_CATEGORY_DV_EL: u8 = 7;

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

    // PID location depends on stream-entry type (BD spec stream_entry()): type 1
    // (PlayItem's Clip) → +2; type 2 (SubPath SubClip) → +4; type 3/4 (SubPath
    // clip / DV enhancement layer) → +3.
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
    let mut hdr_plus = false;
    let mut language = String::new();

    // `stream_type` is the STN category from the caller — always a primary category
    // (VIDEO/AUDIO/PG_SUBTITLE/IG). Secondary audio/video and the DV enhancement layer
    // share the primary's attribute layout and are re-tagged by the caller afterward.
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
                hdr_plus = sa.get(3).is_some_and(|b| b & 0x40 != 0);
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
            // PG: coding_type(1) + language(3); TextST adds character_code(1) first.
            // IG is parsed only to advance spos and is then discarded by the
            // caller, so it deliberately has no arm here.
            let lang_at = if coding_type == c::TEXT_SUBTITLE {
                2
            } else {
                1
            };
            if let Some(lang) = sa.get(lang_at..lang_at + 3) {
                language = String::from_utf8_lossy(lang).to_string();
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
            hdr_plus,
            secondary: false,
        },
        sa_end,
    ))
}

#[cfg(test)]
#[path = "mpls_tests.rs"]
mod tests;
