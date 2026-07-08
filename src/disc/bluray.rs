//! Blu-ray title scanning — MPLS playlist parsing, CLPI clip info, BD metadata.

use super::*;
use crate::clpi;
use crate::mpls;
use crate::sector::SectorSource;
use crate::udf;

/// Stream-file extensions probed for a BD-family playlist clip, in priority
/// order. A clip is normally `.m2ts`; AACS 2.1 (FMTS) discs name the main feature
/// `.fmts` (an M2TS transport stream plus forensic variant segments) and 3D discs
/// use `.ssif`. `.m2ts` is tried first, so a normal clip is unaffected — the
/// fallback only runs when `.m2ts` is absent (exactly when `file_extents` errors).
///
/// Scope: these are all variants that live in `BDMV/STREAM/` and are reached
/// through an MPLS playlist. HD-DVD's `.evo` does NOT belong here — HD-DVD is a
/// different tree (`HVDVD_TS/`) with `.XPL` playlists and needs its own
/// enumerator (a peer to `parse_playlist`), not another extension in this list.
const CLIP_STREAM_EXTS: [&str; 3] = ["m2ts", "fmts", "ssif"];

impl Disc {
    /// Scan Blu-ray titles from MPLS playlists.
    pub(super) fn scan_bluray_titles(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
    ) -> Vec<DiscTitle> {
        let mut titles = Vec::new();
        if let Some(playlist_dir) = udf_fs.find_dir("/BDMV/PLAYLIST") {
            for entry in &playlist_dir.entries {
                if !entry.is_dir && entry.name.to_lowercase().ends_with(".mpls") {
                    let path = format!("/BDMV/PLAYLIST/{}", entry.name);
                    if let Ok(mpls_data) = udf_fs.read_file(reader, &path) {
                        if let Some(title) =
                            Self::parse_playlist(reader, udf_fs, &entry.name, &mpls_data)
                        {
                            titles.push(title);
                        }
                    }
                }
            }
        }
        titles
    }

    /// Parse one MPLS playlist into a [`DiscTitle`].
    ///
    /// Sums PlayItem durations; returns `None` if the playlist is under
    /// 30 seconds (skips menu / clip-info stub playlists) or fails to
    /// parse. Physical sector extents are pulled from the UDF allocation
    /// descriptors of each referenced `.m2ts` (deduplicated by clip_id).
    pub(super) fn parse_playlist(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
        filename: &str,
        data: &[u8],
    ) -> Option<DiscTitle> {
        let parsed = mpls::parse(data).ok()?;

        // Calculate duration from play items
        let duration_ticks: u64 = parsed
            .play_items
            .iter()
            .map(|pi| (pi.out_time.saturating_sub(pi.in_time)) as u64)
            .sum();
        let duration_secs = duration_ticks as f64 / 45000.0;

        // Skip very short playlists (< 30 seconds)
        if duration_secs < 30.0 {
            return None;
        }

        // Parse each clip for size, duration, and sector extents
        let mut extents = Vec::new();
        let mut total_size: u64 = 0;
        let mut clips = Vec::with_capacity(parsed.play_items.len());
        // BD playlists legally reference the same .m2ts clip_id from
        // multiple PlayItems (multi-angle, seamless splits, looped
        // segments). The physical extents and packet count must be
        // counted ONCE per unique clip — mux reads extents in order, so
        // a duplicate would mux the A/V twice and inflate size_bytes.
        // Per-PlayItem Clip entries (differing in/out times) still get
        // recorded.
        let mut seen_clips: std::collections::HashSet<String> = std::collections::HashSet::new();

        for play_item in &parsed.play_items {
            let clip_dur = play_item.out_time.saturating_sub(play_item.in_time) as f64 / 45000.0;
            let mut pkt_count: u32 = 0;

            let clpi_path = format!("/BDMV/CLIPINF/{}.clpi", play_item.clip_id);
            if let Ok(clpi_data) = udf_fs.read_file(reader, &clpi_path) {
                if let Ok(clip_info) = clpi::parse(&clpi_data) {
                    pkt_count = clip_info.source_packet_count;

                    // Mark the clip seen ONLY after its .clpi parses — a transient
                    // read/parse failure on the first PlayItem referencing a clip
                    // must not permanently suppress its extents/size for a later
                    // PlayItem referencing the same clip that succeeds.
                    let first_ref = seen_clips.insert(play_item.clip_id.clone());

                    // Only fetch/push the physical extents and add to the
                    // total size the first time this clip_id is seen.
                    if first_ref {
                        total_size += pkt_count as u64 * 192;

                        // Get stream file extents from UDF allocation descriptors.
                        // Dual-layer discs split files across layers — UDF knows the real layout.
                        //
                        // The clip's stream file is normally `.m2ts`, but AACS 2.1
                        // (FMTS) discs name the main feature `.fmts` and 3D discs
                        // use `.ssif` (see [`CLIP_STREAM_EXTS`]). A normal `.m2ts`
                        // clip is unchanged — the fallback only runs when `.m2ts`
                        // is absent, which is exactly when `file_extents` errors.
                        let file_exts = CLIP_STREAM_EXTS.iter().find_map(|ext| {
                            let path = format!("/BDMV/STREAM/{}.{}", play_item.clip_id, ext);
                            udf_fs.file_extents(reader, &path).ok()
                        });
                        if let Some(file_exts) = file_exts {
                            for (lba, sectors) in file_exts {
                                if sectors > 0 && lba > 0 {
                                    extents.push(Extent {
                                        start_lba: lba,
                                        sector_count: sectors,
                                    });
                                }
                            }
                        }
                    }
                }
            }

            clips.push(Clip {
                clip_id: play_item.clip_id.clone(),
                in_time: play_item.in_time,
                out_time: play_item.out_time,
                duration_secs: clip_dur,
                source_packets: pkt_count,
            });
        }

        // Build streams from STN table
        let streams: Vec<Stream> = parsed
            .streams
            .iter()
            .filter_map(|s| {
                // Skip empty/padding entries (coding_type 0x00)
                if s.coding_type == 0 {
                    return None;
                }
                let codec = Codec::from_coding_type(s.coding_type);
                match s.stream_type {
                    1 | 6 | 7 => Some(Stream::Video(VideoStream {
                        pid: s.pid,
                        codec,
                        resolution: Resolution::from_video_format(s.video_format),
                        frame_rate: FrameRate::from_video_rate(s.video_rate),
                        hdr: match s.dynamic_range {
                            1 => HdrFormat::Hdr10,
                            2 => HdrFormat::DolbyVision,
                            _ => HdrFormat::Sdr,
                        },
                        color_space: match s.color_space {
                            1 => ColorSpace::Bt709,
                            2 => ColorSpace::Bt2020,
                            _ => ColorSpace::Unknown,
                        },
                        // Blu-ray HD/UHD video is square-pixel; display aspect
                        // equals the pixel grid (16:9). Anamorphic SD-on-BD is
                        // not special-cased here.
                        display_aspect: None,
                        secondary: s.secondary,
                        // No user-facing English in the library (numeric-code
                        // rule): the Dolby Vision enhancement layer is signalled
                        // structurally (secondary video + DolbyVision hdr) and
                        // the CLI/UI render the localized descriptor. `label`
                        // stays empty for disc video streams.
                        label: String::new(),
                        // TODO(spec): for 1080i HEVC/H.264/VC-1 titles, surface
                        // the measured field order (H.264/HEVC pic_struct, VC-1
                        // pulldown) from the codec parser instead of the TFF
                        // fallback; needs the parser→title channel (see dvd.rs).
                        // TODO(spec): prefer the HEVC/H.264 VUI colour_description
                        // (measured CICP) over this MPLS playlist-nibble guess
                        // once the parser surfaces it through the output title.
                        // `None` keeps the enum fallback. (HDR MaxCLL/Mastering
                        // metadata is a separate task and intentionally not here.)
                        measured_cicp: None,
                    })),
                    2 | 5 => {
                        // Guard: if coding_type is a subtitle codec (PGS 0x90/0x91),
                        // this is a misaligned stream -- treat as subtitle, not audio
                        if matches!(codec, Codec::Pgs) {
                            Some(Stream::Subtitle(SubtitleStream {
                                pid: s.pid,
                                codec,
                                language: s.language.clone(),
                                forced: false,
                                qualifier: crate::disc::LabelQualifier::None,
                                codec_data: None,
                            }))
                        } else {
                            Some(Stream::Audio(AudioStream {
                                pid: s.pid,
                                codec,
                                channels: AudioChannels::from_audio_format(s.audio_format),
                                language: s.language.clone(),
                                sample_rate: SampleRate::from_audio_rate(s.audio_rate),
                                secondary: s.stream_type == 5,
                                purpose: crate::disc::LabelPurpose::Normal,
                                label: String::new(),
                            }))
                        }
                    }
                    3 => Some(Stream::Subtitle(SubtitleStream {
                        pid: s.pid,
                        codec,
                        language: s.language.clone(),
                        forced: false,
                        qualifier: crate::disc::LabelQualifier::None,
                        codec_data: None,
                    })),
                    // Stream type 4 = IG, unknown types -- skip.
                    other => {
                        tracing::warn!(
                            "dropping STN stream entry: unhandled stream_type {} (PID {:#06x}, coding_type {:#04x})",
                            other,
                            s.pid,
                            s.coding_type,
                        );
                        None
                    }
                }
            })
            .collect();

        // Convert marks to chapters. mark_type == 1 is an entry-mark
        // (chapter); type 2 is a link point and type 0 is reserved, so
        // neither is a chapter.
        //
        // Each mark's timestamp is in the timebase of the PlayItem it
        // references (play_item_ref). The chapter's position on the
        // muxed timeline is the summed duration of every preceding
        // PlayItem plus the mark's offset within its own PlayItem. Using
        // play_items[0].in_time for every mark would misplace chapters in
        // multi-PlayItem playlists.
        let chapters: Vec<Chapter> = parsed
            .marks
            .iter()
            .filter(|m| m.mark_type == 1)
            .filter_map(|m| {
                let pi_idx = m.play_item_ref as usize;
                let pi = parsed.play_items.get(pi_idx)?;
                let preceding: f64 = parsed.play_items[..pi_idx]
                    .iter()
                    .map(|p| p.out_time.saturating_sub(p.in_time) as f64 / 45000.0)
                    .sum();
                let within = (m.timestamp as f64 - pi.in_time as f64) / 45000.0;
                let time_secs = preceding + within;
                Some(Chapter {
                    time_secs: if time_secs < 0.0 { 0.0 } else { time_secs },
                    name: String::new(), // filled with the ordinal below
                })
            })
            .enumerate()
            .map(|(i, mut ch)| {
                ch.name = super::chapter_name(i);
                ch
            })
            .collect();

        // Strip the .mpls suffix case-insensitively before parsing the
        // numeric playlist id (the dir scan accepts any-case .mpls).
        let playlist_num = filename
            .get(..filename.len().saturating_sub(5))
            .filter(|_| {
                filename.len() >= 5 && filename[filename.len() - 5..].eq_ignore_ascii_case(".mpls")
            })
            .unwrap_or(filename);
        let playlist_id = playlist_num.parse::<u16>().unwrap_or(0);

        Some(DiscTitle {
            playlist: filename.to_string(),
            playlist_id,
            duration_secs,
            size_bytes: total_size,
            clips,
            streams,
            chapters,
            extents,
            content_format: ContentFormat::BdTs,
            codec_privates: Vec::new(),
        })
    }

    /// Read disc title from META/DL/bdmt_eng.xml (Blu-ray Disc Meta Table).
    /// Prefers English, falls back to first available language.
    /// Returns None if META directory is empty or XML has no usable title.
    pub(super) fn read_meta_title(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
    ) -> Option<String> {
        let meta_dir = udf_fs.find_dir("/BDMV/META")?;
        for sub in &meta_dir.entries {
            if !sub.is_dir {
                continue;
            }
            let dl_path = format!("/BDMV/META/{}", sub.name);
            if let Some(dl_dir) = udf_fs.find_dir(&dl_path) {
                let xml_files: Vec<_> = dl_dir
                    .entries
                    .iter()
                    .filter(|e| !e.is_dir && e.name.to_lowercase().ends_with(".xml"))
                    .collect();

                let eng = xml_files
                    .iter()
                    .find(|e| e.name.to_lowercase().contains("eng"));
                let target = eng.or_else(|| xml_files.first());

                if let Some(entry) = target {
                    let path = format!("{}/{}", dl_path, entry.name);
                    if let Ok(data) = udf_fs.read_file(reader, &path) {
                        let xml = String::from_utf8_lossy(&data);
                        if let Some(start) = xml.find("<di:name>") {
                            let s = start + "<di:name>".len();
                            if let Some(end) = xml[s..].find("</di:name>") {
                                let title = xml[s..s + end].trim().to_string();
                                if !title.is_empty() && title != "Blu-ray" {
                                    return Some(title);
                                }
                            }
                        }
                    }
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::udf::fixture::*;
    // ---------------------------------------------------------------
    // MPLS builder (BD-ROM PlayList spec). Mirrors the layout the
    // `mpls::parse` consumer reads (header@0, PlayList@playlist_start,
    // PlayListMark@mark_start). Offsets cited against mpls.rs.
    // ---------------------------------------------------------------

    struct PiSpec {
        clip_id: [u8; 5],
        in_time: u32,
        out_time: u32,
    }

    struct MarkSpec {
        mark_type: u8,
        play_item_ref: u16,
        timestamp: u32,
    }

    /// One STN stream entry: stream_entry (len(1)=3, type(1)=0x01, pid(2))
    /// plus stream_attributes (len(1) + coding_type(1) + payload). Matches
    /// the mpls.rs test builders.
    fn se_video(pid: u16, coding_type: u8) -> Vec<u8> {
        let mut out = vec![3u8, 0x01];
        out.extend_from_slice(&pid.to_be_bytes());
        let attrs = vec![coding_type, 0x10]; // format/rate nibbles
        out.push(attrs.len() as u8);
        out.extend_from_slice(&attrs);
        out
    }
    fn se_audio(pid: u16, coding_type: u8, lang: &[u8; 3]) -> Vec<u8> {
        let mut out = vec![3u8, 0x01];
        out.extend_from_slice(&pid.to_be_bytes());
        // PGS in an audio slot uses PG layout (coding_type + lang(3)); the
        // builder only needs the non-PGS audio layout here.
        let attrs = vec![coding_type, 0x21, lang[0], lang[1], lang[2]];
        out.push(attrs.len() as u8);
        out.extend_from_slice(&attrs);
        out
    }
    fn se_pg(pid: u16, coding_type: u8, lang: &[u8; 3]) -> Vec<u8> {
        let mut out = vec![3u8, 0x01];
        out.extend_from_slice(&pid.to_be_bytes());
        let attrs = vec![coding_type, lang[0], lang[1], lang[2]];
        out.push(attrs.len() as u8);
        out.extend_from_slice(&attrs);
        out
    }

    /// Build an MPLS playlist. `stn_counts` = (video, audio, pg, ig,
    /// sec_audio, sec_video, pip_pg, dv); `stream_entries` are appended on
    /// the FIRST play item in that order.
    fn build_mpls(
        items: &[PiSpec],
        stn_counts: (u8, u8, u8, u8, u8, u8, u8, u8),
        stream_entries: &[Vec<u8>],
        marks: &[MarkSpec],
    ) -> Vec<u8> {
        let playlist_start: u32 = 40;
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MPLS0200"); // type+version
        buf.extend_from_slice(&playlist_start.to_be_bytes()); // [8..12]
        buf.extend_from_slice(&[0u8; 28]); // mark_start placeholder + pad to 40

        // PlayList section: length(4) + reserved(2) + num_play_items(2)
        // + num_sub_paths(2) header.
        let pl_start = buf.len();
        buf.extend_from_slice(&[0u8; 4]); // length placeholder
        buf.extend_from_slice(&[0u8; 2]); // reserved
        buf.extend_from_slice(&(items.len() as u16).to_be_bytes());
        buf.extend_from_slice(&[0u8; 2]); // num_sub_paths

        for (idx, pi) in items.iter().enumerate() {
            let mut item = Vec::new();
            item.extend_from_slice(&pi.clip_id); // [0..5]
            item.extend_from_slice(b"M2TS"); // [5..9] codec_id
            item.push(0); // [9] connection_condition
            item.extend_from_slice(&[0u8; 2]); // [10..12] reserved
            item.extend_from_slice(&pi.in_time.to_be_bytes()); // [12..16]
            item.extend_from_slice(&pi.out_time.to_be_bytes()); // [16..20]
            item.extend_from_slice(&[0u8; 8]); // [20..28] UO_mask
            item.push(0); // [28] misc
            item.push(0); // [29] still_mode
            item.extend_from_slice(&[0u8; 2]); // [30..32] still_time
            if idx == 0 {
                // STN table: length(2)+reserved(2)+counts(8)+reserved(4).
                let stn_start = item.len();
                item.extend_from_slice(&[0u8; 2]); // length placeholder
                item.extend_from_slice(&[0u8; 2]); // reserved
                item.push(stn_counts.0);
                item.push(stn_counts.1);
                item.push(stn_counts.2);
                item.push(stn_counts.3);
                item.push(stn_counts.4);
                item.push(stn_counts.5);
                item.push(stn_counts.6);
                item.push(stn_counts.7);
                item.extend_from_slice(&[0u8; 4]); // reserved
                for se in stream_entries {
                    item.extend_from_slice(se);
                }
                let stn_len = (item.len() - stn_start - 2) as u16;
                item[stn_start..stn_start + 2].copy_from_slice(&stn_len.to_be_bytes());
            }
            buf.extend_from_slice(&(item.len() as u16).to_be_bytes());
            buf.extend_from_slice(&item);
        }

        let pl_len = (buf.len() - pl_start - 4) as u32;
        buf[pl_start..pl_start + 4].copy_from_slice(&pl_len.to_be_bytes());

        // PlayListMark section.
        let mark_start = buf.len() as u32;
        buf[12..16].copy_from_slice(&mark_start.to_be_bytes());
        let mark_section_len = 2 + marks.len() * 14;
        buf.extend_from_slice(&(mark_section_len as u32).to_be_bytes());
        buf.extend_from_slice(&(marks.len() as u16).to_be_bytes());
        for m in marks {
            buf.push(0); // [0] reserved
            buf.push(m.mark_type); // [1] mark_type
            buf.extend_from_slice(&m.play_item_ref.to_be_bytes()); // [2..4]
            buf.extend_from_slice(&m.timestamp.to_be_bytes()); // [4..8]
            buf.extend_from_slice(&[0u8; 6]); // [8..14] PID + duration
        }
        buf
    }

    // ---------------------------------------------------------------
    // CLPI builder. `clpi::parse` reads "HDMV" magic, prog_info_start@12,
    // cpi_start@16, source_packet_count@56. Zeroing prog_info/cpi starts
    // disables those sections cleanly.
    // ---------------------------------------------------------------

    fn build_clpi(source_packet_count: u32) -> Vec<u8> {
        let mut d = vec![0u8; 60];
        d[0..4].copy_from_slice(b"HDMV");
        d[4..8].copy_from_slice(b"0200");
        // seq_info_start/prog_info_start/cpi_start all 0 → skipped.
        d[56..60].copy_from_slice(&source_packet_count.to_be_bytes());
        d
    }

    // ---------------------------------------------------------------
    // Tests: parse_playlist
    // ---------------------------------------------------------------

    /// A playlist whose summed PlayItem duration is < 30 s is a menu /
    /// clip-info stub and must be dropped (bluray.rs: `duration_secs <
    /// 30.0 → None`). 45000 ticks/s timebase: 29 s = 1_305_000 ticks.
    #[test]
    fn parse_playlist_drops_under_30_seconds() {
        let mut disc = MemDisc::new();
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 29 * 45000, // 29 s < 30 s threshold
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        let udf = make_min_fs(&mut disc);
        assert!(
            Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).is_none(),
            "playlists shorter than 30s must be skipped"
        );
    }

    /// At exactly 30 s the playlist is kept (`< 30.0` is strict).
    #[test]
    fn parse_playlist_keeps_exactly_30_seconds() {
        let mut disc = MemDisc::new();
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 30 * 45000,
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        let udf = make_min_fs(&mut disc);
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("30s playlist must be kept");
        assert!((t.duration_secs - 30.0).abs() < 1e-6);
    }

    /// Garbage that isn't an MPLS must yield None (parse error path), not
    /// panic. mpls::parse rejects on missing "MPLS" magic.
    #[test]
    fn parse_playlist_rejects_non_mpls() {
        let mut disc = MemDisc::new();
        let udf = make_min_fs(&mut disc);
        let junk = vec![0u8; 100];
        assert!(Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &junk).is_none());
    }

    /// Build a full BDMV tree with one STREAM/.m2ts (Long-AD ICB) and one
    /// CLPINF/.clpi, returning the navigable UdfFs plus a populated disc.
    /// This is the canonical 0.31.0 extent-assembly fixture.
    fn make_min_fs(disc: &mut MemDisc) -> udf::UdfFs {
        // Empty BDMV/PLAYLIST so directory navigation in parse_playlist's
        // clip lookups still works even when no clip files exist.
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![DirSpec {
                name: "BDMV".to_string(),
                icb_lba: 12,
                dir_data_lba: 13,
                files: Vec::new(),
                subdirs: vec![],
            }],
        };
        build_udf_skeleton(disc, 10);
        lay_dir(disc, &root);
        udf::read_filesystem(disc).expect("fs")
    }

    /// Full BDMV with STREAM + CLPINF for the listed clip ids. Each clip's
    /// .m2ts gets a Long-AD ICB with `sectors` sectors at a distinct LBA;
    /// each .clpi declares `packets` source packets. Returns the UdfFs.
    fn make_bdmv_fs(
        disc: &mut MemDisc,
        clips: &[(
            &str,
            u32, /*sectors*/
            u32, /*packets*/
            u32, /*data_lba*/
        )],
    ) -> udf::UdfFs {
        make_bdmv_fs_ext(disc, clips, "m2ts")
    }

    /// As [`make_bdmv_fs`] but the STREAM file carries `stream_ext` instead of
    /// `.m2ts` (e.g. "fmts" for an AACS 2.1 feature clip, "ssif" for 3D) — drives
    /// the [`CLIP_STREAM_EXTS`] fallback in `parse_playlist`.
    fn make_bdmv_fs_ext(
        disc: &mut MemDisc,
        clips: &[(
            &str,
            u32, /*sectors*/
            u32, /*packets*/
            u32, /*data_lba*/
        )],
        stream_ext: &str,
    ) -> udf::UdfFs {
        // Layout LBAs: pick widely separated values to avoid collisions.
        let mut stream_files = Vec::new();
        let mut clipinf_files = Vec::new();
        let mut icb = 100u32;
        for (name, sectors, packets, data_lba) in clips {
            let m2ts = format!("{name}.{stream_ext}");
            // Size in bytes — file_extents derives sectors via div_ceil(2048).
            let size = sectors * 2048;
            stream_files.push(file(&m2ts, icb, *data_lba, size, true));
            icb += 1;
            let clpi = format!("{name}.clpi");
            clipinf_files.push(file_with(
                &clpi,
                icb,
                *data_lba + 1000,
                build_clpi(*packets),
                false,
            ));
            icb += 1;
        }
        let bdmv = DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![
                DirSpec {
                    name: "STREAM".to_string(),
                    icb_lba: 22,
                    dir_data_lba: 23,
                    files: stream_files,
                    subdirs: vec![],
                },
                DirSpec {
                    name: "CLIPINF".to_string(),
                    icb_lba: 24,
                    dir_data_lba: 25,
                    files: clipinf_files,
                    subdirs: vec![],
                },
            ],
        };
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![bdmv],
        };
        build_udf_skeleton(disc, 10);
        lay_dir(disc, &root);
        udf::read_filesystem(disc).expect("fs")
    }

    /// Single-clip playlist: size_bytes = source_packets * 192 and the
    /// physical extent is pulled from the m2ts Long-AD ICB. Per bluray.rs:
    /// `total_size += pkt_count * 192`; extents from file_extents.
    #[test]
    fn parse_playlist_single_clip_size_and_extent() {
        let mut disc = MemDisc::new();
        // 1000 sectors of m2ts at LBA 5000 (data_lba arg); 4000 packets.
        let udf = make_bdmv_fs(&mut disc, &[("00001", 1000, 4000, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000, // 60 s
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("title");
        // BD source packet = 192 bytes (188 TS + 4-byte timestamp header).
        assert_eq!(t.size_bytes, 4000 * 192);
        assert_eq!(t.extents.len(), 1, "one m2ts → one extent");
        // file_extents absolute LBA = partition_start + data_lba.
        assert_eq!(t.extents[0].start_lba, PART_START + 5000);
        assert_eq!(t.extents[0].sector_count, 1000);
        assert_eq!(t.clips.len(), 1);
        assert_eq!(t.clips[0].source_packets, 4000);
    }

    /// AACS 2.1: the feature clip is `00001.fmts`, NOT `.m2ts`. The
    /// [`CLIP_STREAM_EXTS`] fallback in `parse_playlist` must still resolve the
    /// physical extent — before the fix the hard-coded `.m2ts` path errored,
    /// yielding empty extents (a silent empty rip and 0 encrypted samples for key
    /// resolution). Size still comes from the `.clpi`, which parses regardless.
    #[test]
    fn parse_playlist_fmts_clip_resolves_extent() {
        let mut disc = MemDisc::new();
        // Only a .fmts stream exists for clip 00001 (no .m2ts on disc).
        let udf = make_bdmv_fs_ext(&mut disc, &[("00001", 1000, 4000, 5000)], "fmts");
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("title");
        assert_eq!(t.size_bytes, 4000 * 192, "size from .clpi source packets");
        assert_eq!(
            t.extents.len(),
            1,
            "the .fmts extent must be resolved via fallback"
        );
        assert_eq!(t.extents[0].start_lba, PART_START + 5000);
        assert_eq!(t.extents[0].sector_count, 1000);
    }

    /// THE 0.31.0 DEDUP PATH. A playlist that references the SAME clip_id
    /// from multiple PlayItems (seamless split / looped segment) must count
    /// the physical extents and packet bytes EXACTLY ONCE — mux reads
    /// extents in order, so a duplicate would mux the A/V twice and inflate
    /// size_bytes (bluray.rs: `first_ref = seen_clips.insert(...)` gates
    /// both `total_size +=` and the `extents.push`). Per-PlayItem Clip
    /// entries are still recorded for both.
    #[test]
    fn parse_playlist_dedups_repeated_clip_extents_and_size() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 1000, 4000, 5000)]);
        let mpls = build_mpls(
            &[
                PiSpec {
                    clip_id: *b"00001",
                    in_time: 0,
                    out_time: 60 * 45000,
                },
                PiSpec {
                    clip_id: *b"00001", // SAME clip — second reference
                    in_time: 60 * 45000,
                    out_time: 120 * 45000,
                },
            ],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("title");
        // Extent and size counted ONCE despite two PlayItems.
        assert_eq!(
            t.extents.len(),
            1,
            "repeated clip must not duplicate extent"
        );
        assert_eq!(
            t.size_bytes,
            4000 * 192,
            "size counted once per unique clip"
        );
        // But BOTH PlayItems are recorded as Clip entries (differing times).
        assert_eq!(t.clips.len(), 2, "each PlayItem still gets a Clip entry");
        assert_eq!(t.clips[0].clip_id, "00001");
        assert_eq!(t.clips[1].clip_id, "00001");
    }

    /// Distinct clips each contribute their own extent and bytes, in
    /// PlayItem order (mux relies on extent order).
    #[test]
    fn parse_playlist_distinct_clips_accumulate_in_order() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(
            &mut disc,
            &[("00001", 1000, 4000, 5000), ("00002", 500, 2000, 9000)],
        );
        let mpls = build_mpls(
            &[
                PiSpec {
                    clip_id: *b"00001",
                    in_time: 0,
                    out_time: 60 * 45000,
                },
                PiSpec {
                    clip_id: *b"00002",
                    in_time: 0,
                    out_time: 30 * 45000,
                },
            ],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("title");
        assert_eq!(t.extents.len(), 2);
        assert_eq!(t.extents[0].start_lba, PART_START + 5000);
        assert_eq!(t.extents[1].start_lba, PART_START + 9000);
        assert_eq!(t.size_bytes, (4000 + 2000) * 192);
    }

    /// A clip whose .clpi is missing contributes NO size and NO extent
    /// (bluray.rs only fetches extents inside the `if let Ok(clpi_data)`
    /// and `if let Ok(clip_info)` blocks), but the Clip entry is still
    /// recorded with packet count 0. Never panics on the missing read.
    #[test]
    fn parse_playlist_missing_clpi_yields_no_extent_no_size() {
        let mut disc = MemDisc::new();
        // STREAM has the m2ts but CLIPINF is empty for this clip.
        let udf = make_bdmv_fs(&mut disc, &[]); // no clips wired
        // Re-lay a STREAM-only tree: put an m2ts but no clpi.
        let udf = {
            let _ = udf;
            let bdmv = DirSpec {
                name: "BDMV".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: Vec::new(),
                subdirs: vec![
                    DirSpec {
                        name: "STREAM".to_string(),
                        icb_lba: 22,
                        dir_data_lba: 23,
                        files: vec![file("00009.m2ts", 100, 5000, 1000 * 2048, true)],
                        subdirs: vec![],
                    },
                    DirSpec {
                        name: "CLIPINF".to_string(),
                        icb_lba: 24,
                        dir_data_lba: 25,
                        files: Vec::new(), // no .clpi
                        subdirs: vec![],
                    },
                ],
            };
            let root = DirSpec {
                name: String::new(),
                icb_lba: 10,
                dir_data_lba: 11,
                files: Vec::new(),
                subdirs: vec![bdmv],
            };
            let mut d2 = MemDisc::new();
            build_udf_skeleton(&mut d2, 10);
            lay_dir(&mut d2, &root);
            disc = d2;
            udf::read_filesystem(&mut disc).expect("fs")
        };
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00009",
                in_time: 0,
                out_time: 60 * 45000,
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00009.mpls", &mpls).expect("title");
        assert_eq!(t.size_bytes, 0, "no clpi → no size contribution");
        assert!(t.extents.is_empty(), "no clpi → no extent fetched");
        assert_eq!(t.clips.len(), 1);
        assert_eq!(t.clips[0].source_packets, 0);
    }

    /// `file_extents` filters extents with `lba == 0` or `sectors == 0`
    /// (bluray.rs: `if sectors > 0 && lba > 0`). A clip whose data lands at
    /// partition-relative LBA 0 would produce abs LBA == PART_START (> 0),
    /// so to exercise the lba==0 guard we'd need partition_start 0; instead
    /// verify a zero-length declared file produces no extent. A 0-byte
    /// m2ts → sectors == 0 → dropped.
    #[test]
    fn parse_playlist_zero_length_extent_is_filtered() {
        let mut disc = MemDisc::new();
        // m2ts declared 0 bytes → file_extents sectors = div_ceil(0,2048)=0.
        let udf = {
            let bdmv = DirSpec {
                name: "BDMV".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: Vec::new(),
                subdirs: vec![
                    DirSpec {
                        name: "STREAM".to_string(),
                        icb_lba: 22,
                        dir_data_lba: 23,
                        files: vec![file("00001.m2ts", 100, 5000, 0, true)],
                        subdirs: vec![],
                    },
                    DirSpec {
                        name: "CLIPINF".to_string(),
                        icb_lba: 24,
                        dir_data_lba: 25,
                        files: vec![file_with("00001.clpi", 102, 8000, build_clpi(4000), false)],
                        subdirs: vec![],
                    },
                ],
            };
            let root = DirSpec {
                name: String::new(),
                icb_lba: 10,
                dir_data_lba: 11,
                files: Vec::new(),
                subdirs: vec![bdmv],
            };
            build_udf_skeleton(&mut disc, 10);
            lay_dir(&mut disc, &root);
            udf::read_filesystem(&mut disc).expect("fs")
        };
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("title");
        // size still counted (from clpi packets) but the empty extent dropped.
        assert_eq!(t.size_bytes, 4000 * 192);
        assert!(t.extents.is_empty(), "zero-sector extent must be filtered");
    }

    // ---------------------------------------------------------------
    // Tests: STN stream mapping
    // ---------------------------------------------------------------

    /// stream_type 1 video (HEVC 0x24) → Stream::Video with the parsed PID
    /// and codec. coding_type 0x24 maps to HEVC (Codec::from_coding_type).
    #[test]
    fn parse_playlist_maps_video_stream() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            }],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[se_video(0x1011, 0x24)],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("title");
        let videos: Vec<_> = t
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Video(v) => Some(v),
                _ => None,
            })
            .collect();
        assert_eq!(videos.len(), 1);
        assert_eq!(videos[0].pid, 0x1011);
        assert_eq!(videos[0].codec, Codec::Hevc);
    }

    /// A PGS coding_type (0x90) sitting in the AUDIO STN slot is a
    /// misaligned-stream guard case: bluray.rs routes it to Subtitle, not
    /// Audio (`if matches!(codec, Codec::Pgs)`). Wrong-title regression
    /// guard: ensures audio slot data never silently becomes a fake audio
    /// track when it is really PGS.
    #[test]
    fn parse_playlist_pgs_in_audio_slot_becomes_subtitle() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            }],
            // 1 audio entry, but its coding_type is PGS (0x90).
            (0, 1, 0, 0, 0, 0, 0, 0),
            &[se_pg(0x1100, 0x90, b"eng")],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("title");
        assert!(
            t.streams.iter().all(|s| !matches!(s, Stream::Audio(_))),
            "PGS in audio slot must NOT become an audio stream"
        );
        assert!(
            t.streams
                .iter()
                .any(|s| matches!(s, Stream::Subtitle(sub) if sub.codec == Codec::Pgs)),
            "PGS in audio slot must become a PGS subtitle"
        );
    }

    /// A real audio entry (AC-3 0x81) in the audio slot → Stream::Audio.
    #[test]
    fn parse_playlist_maps_audio_stream() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            }],
            (0, 1, 0, 0, 0, 0, 0, 0),
            &[se_audio(0x1100, 0x81, b"eng")],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("title");
        let audios: Vec<_> = t
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Audio(a) => Some(a),
                _ => None,
            })
            .collect();
        assert_eq!(audios.len(), 1);
        assert_eq!(audios[0].codec, Codec::Ac3);
        assert_eq!(audios[0].language, "eng");
    }

    /// stream_type 3 PG (PGS 0x90) → Stream::Subtitle with language.
    #[test]
    fn parse_playlist_maps_pg_subtitle() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            }],
            (0, 0, 1, 0, 0, 0, 0, 0),
            &[se_pg(0x1200, 0x90, b"fra")],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("title");
        let subs: Vec<_> = t
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Subtitle(sub) => Some(sub),
                _ => None,
            })
            .collect();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].codec, Codec::Pgs);
        assert_eq!(subs[0].language, "fra");
    }

    // ---------------------------------------------------------------
    // Tests: chapters
    // ---------------------------------------------------------------

    /// Only mark_type 1 (entry-mark) becomes a chapter; type 2 (link
    /// point) and type 0 (reserved) are dropped (bluray.rs filter
    /// `m.mark_type == 1`).
    #[test]
    fn parse_playlist_only_entry_marks_become_chapters() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 120 * 45000,
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[
                MarkSpec {
                    mark_type: 1,
                    play_item_ref: 0,
                    timestamp: 0,
                },
                MarkSpec {
                    mark_type: 2,
                    play_item_ref: 0,
                    timestamp: 30 * 45000,
                }, // link → drop
                MarkSpec {
                    mark_type: 1,
                    play_item_ref: 0,
                    timestamp: 60 * 45000,
                },
                MarkSpec {
                    mark_type: 0,
                    play_item_ref: 0,
                    timestamp: 90 * 45000,
                }, // reserved → drop
            ],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("title");
        assert_eq!(
            t.chapters.len(),
            2,
            "only the two type-1 marks are chapters"
        );
    }

    /// In a multi-PlayItem playlist, a mark referencing PlayItem 1 is
    /// placed at (sum of preceding PlayItem durations) + (mark offset
    /// within its own PlayItem). Using play_items[0].in_time for every
    /// mark would misplace it (bluray.rs `preceding + within`). PI0 = 60s,
    /// mark in PI1 at its in_time → chapter at exactly 60 s.
    #[test]
    fn parse_playlist_chapter_time_accounts_for_preceding_play_items() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let pi1_in = 10 * 45000u32;
        let mpls = build_mpls(
            &[
                PiSpec {
                    clip_id: *b"00001",
                    in_time: 0,
                    out_time: 60 * 45000, // PI0 lasts 60 s
                },
                PiSpec {
                    clip_id: *b"00001",
                    in_time: pi1_in,
                    out_time: pi1_in + 60 * 45000,
                },
            ],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[MarkSpec {
                mark_type: 1,
                play_item_ref: 1,
                timestamp: pi1_in, // at the very start of PI1
            }],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("title");
        assert_eq!(t.chapters.len(), 1);
        // preceding (PI0 = 60s) + within (timestamp - pi1.in_time = 0) = 60s.
        assert!(
            (t.chapters[0].time_secs - 60.0).abs() < 1e-6,
            "chapter must sit at 60s, got {}",
            t.chapters[0].time_secs
        );
    }

    /// A mark whose timestamp precedes its PlayItem's in_time would yield a
    /// negative within-offset; bluray.rs clamps the chapter to 0.0 (`if
    /// time_secs < 0.0 { 0.0 }`). Never emits a negative chapter time.
    #[test]
    fn parse_playlist_negative_chapter_time_clamped_to_zero() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 50 * 45000,
                out_time: 110 * 45000,
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[MarkSpec {
                mark_type: 1,
                play_item_ref: 0,
                timestamp: 0, // before in_time → would be negative
            }],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("title");
        assert_eq!(t.chapters.len(), 1);
        assert_eq!(t.chapters[0].time_secs, 0.0);
    }

    /// A mark referencing a non-existent PlayItem index is dropped via the
    /// `?` on `play_items.get(pi_idx)` — must not panic or index OOB.
    #[test]
    fn parse_playlist_mark_with_bad_play_item_ref_dropped() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[MarkSpec {
                mark_type: 1,
                play_item_ref: 99, // no such PlayItem
                timestamp: 0,
            }],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("title");
        assert!(
            t.chapters.is_empty(),
            "out-of-range mark ref must be dropped"
        );
    }

    // ---------------------------------------------------------------
    // Tests: playlist id parsing
    // ---------------------------------------------------------------

    /// playlist_id is the numeric stem of the filename with the .mpls
    /// suffix stripped case-insensitively (bluray.rs `playlist_num`).
    #[test]
    fn parse_playlist_id_strips_suffix_case_insensitive() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        // Uppercase suffix must still parse the numeric stem.
        let t = Disc::parse_playlist(&mut disc, &udf, "00800.MPLS", &mpls).expect("title");
        assert_eq!(t.playlist_id, 800);
        assert_eq!(
            t.playlist, "00800.MPLS",
            "playlist field keeps original name"
        );
    }

    /// A non-numeric stem falls back to playlist_id 0 (`parse::<u16>()
    /// .unwrap_or(0)`), never panics.
    #[test]
    fn parse_playlist_id_non_numeric_defaults_zero() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "MENU.mpls", &mpls).expect("title");
        assert_eq!(t.playlist_id, 0);
    }

    // ---------------------------------------------------------------
    // Tests: scan_bluray_titles
    // ---------------------------------------------------------------

    /// scan_bluray_titles enumerates BDMV/PLAYLIST/*.mpls and keeps only
    /// playlists that parse to a >= 30s title. A short one is dropped.
    #[test]
    fn scan_bluray_titles_keeps_long_drops_short() {
        let mut disc = MemDisc::new();
        // Build full tree with PLAYLIST holding two .mpls + STREAM/CLIPINF.
        let long_mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 7200 * 45000, // 2 h
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        let short_mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 5 * 45000, // 5 s menu
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        // m2ts (Long-AD) + clpi for clip 00001.
        let stream = DirSpec {
            name: "STREAM".to_string(),
            icb_lba: 22,
            dir_data_lba: 23,
            files: vec![file("00001.m2ts", 100, 5000, 1000 * 2048, true)],
            subdirs: vec![],
        };
        let clipinf = DirSpec {
            name: "CLIPINF".to_string(),
            icb_lba: 24,
            dir_data_lba: 25,
            files: vec![file_with("00001.clpi", 102, 8000, build_clpi(4000), false)],
            subdirs: vec![],
        };
        let playlist = DirSpec {
            name: "PLAYLIST".to_string(),
            icb_lba: 26,
            dir_data_lba: 27,
            files: vec![
                file_with("00800.mpls", 104, 30000, long_mpls, false),
                file_with("00801.mpls", 110, 40000, short_mpls, false),
            ],
            subdirs: vec![],
        };
        let bdmv = DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![stream, clipinf, playlist],
        };
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![bdmv],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = udf::read_filesystem(&mut disc).expect("fs");

        let titles = Disc::scan_bluray_titles(&mut disc, &udf);
        assert_eq!(titles.len(), 1, "only the 2h playlist should survive");
        assert_eq!(titles[0].playlist_id, 800);
    }

    /// With no PLAYLIST directory, scan_bluray_titles returns an empty
    /// vec (the `find_dir` is None) — never panics.
    #[test]
    fn scan_bluray_titles_no_playlist_dir_is_empty() {
        let mut disc = MemDisc::new();
        let udf = make_min_fs(&mut disc); // BDMV exists, no PLAYLIST
        assert!(Disc::scan_bluray_titles(&mut disc, &udf).is_empty());
    }

    // ---------------------------------------------------------------
    // Tests: read_meta_title
    // ---------------------------------------------------------------

    /// read_meta_title extracts <di:name> from BDMV/META/DL/*eng*.xml and
    /// prefers the English file (bluray.rs `eng.or_else(first)`).
    #[test]
    fn read_meta_title_extracts_english_di_name() {
        let mut disc = MemDisc::new();
        let xml = b"<x><di:name>My Movie</di:name></x>".to_vec();
        let dl = DirSpec {
            name: "DL".to_string(),
            icb_lba: 30,
            dir_data_lba: 31,
            files: vec![file_with("bdmt_eng.xml", 104, 50000, xml, false)],
            subdirs: vec![],
        };
        let meta = DirSpec {
            name: "META".to_string(),
            icb_lba: 28,
            dir_data_lba: 29,
            files: Vec::new(),
            subdirs: vec![dl],
        };
        let bdmv = DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![meta],
        };
        let root = DirSpec {
            name: String::new(),
            icb_lba: 10,
            dir_data_lba: 11,
            files: Vec::new(),
            subdirs: vec![bdmv],
        };
        build_udf_skeleton(&mut disc, 10);
        lay_dir(&mut disc, &root);
        let udf = udf::read_filesystem(&mut disc).expect("fs");
        assert_eq!(
            Disc::read_meta_title(&mut disc, &udf),
            Some("My Movie".to_string())
        );
    }

    /// The placeholder title "Blu-ray" and empty titles are rejected
    /// (bluray.rs `!title.is_empty() && title != "Blu-ray"`).
    #[test]
    fn read_meta_title_rejects_placeholder_and_empty() {
        for body in ["<di:name>Blu-ray</di:name>", "<di:name>   </di:name>"] {
            let mut disc = MemDisc::new();
            let dl = DirSpec {
                name: "DL".to_string(),
                icb_lba: 30,
                dir_data_lba: 31,
                files: vec![file_with(
                    "bdmt_eng.xml",
                    104,
                    50000,
                    body.as_bytes().to_vec(),
                    false,
                )],
                subdirs: vec![],
            };
            let meta = DirSpec {
                name: "META".to_string(),
                icb_lba: 28,
                dir_data_lba: 29,
                files: Vec::new(),
                subdirs: vec![dl],
            };
            let bdmv = DirSpec {
                name: "BDMV".to_string(),
                icb_lba: 20,
                dir_data_lba: 21,
                files: Vec::new(),
                subdirs: vec![meta],
            };
            let root = DirSpec {
                name: String::new(),
                icb_lba: 10,
                dir_data_lba: 11,
                files: Vec::new(),
                subdirs: vec![bdmv],
            };
            build_udf_skeleton(&mut disc, 10);
            lay_dir(&mut disc, &root);
            let udf = udf::read_filesystem(&mut disc).expect("fs");
            assert_eq!(
                Disc::read_meta_title(&mut disc, &udf),
                None,
                "placeholder/empty title must be rejected for body {body:?}"
            );
        }
    }

    /// No META directory → None.
    #[test]
    fn read_meta_title_no_meta_dir_is_none() {
        let mut disc = MemDisc::new();
        let udf = make_min_fs(&mut disc);
        assert_eq!(Disc::read_meta_title(&mut disc, &udf), None);
    }
}
