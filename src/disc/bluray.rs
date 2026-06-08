//! Blu-ray title scanning — MPLS playlist parsing, CLPI clip info, BD metadata.

use super::*;
use crate::clpi;
use crate::mpls;
use crate::sector::SectorSource;
use crate::udf;

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
            let first_ref = seen_clips.insert(play_item.clip_id.clone());

            let clpi_path = format!("/BDMV/CLIPINF/{}.clpi", play_item.clip_id);
            if let Ok(clpi_data) = udf_fs.read_file(reader, &clpi_path) {
                if let Ok(clip_info) = clpi::parse(&clpi_data) {
                    pkt_count = clip_info.source_packet_count;

                    // Only fetch/push the physical extents and add to the
                    // total size the first time this clip_id is seen.
                    if first_ref {
                        total_size += pkt_count as u64 * 192;

                        // Get m2ts file extents from UDF allocation descriptors.
                        // Dual-layer discs split files across layers — UDF knows the real layout.
                        let m2ts_path = format!("/BDMV/STREAM/{}.m2ts", play_item.clip_id);
                        if let Ok(file_exts) = udf_fs.file_extents(reader, &m2ts_path) {
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
                        secondary: s.secondary,
                        label: match s.stream_type {
                            7 => "Dolby Vision EL".to_string(),
                            _ => String::new(),
                        },
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
