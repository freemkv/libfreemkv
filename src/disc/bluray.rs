//! Blu-ray title scanning — MPLS playlist parsing, CLPI clip info, BD metadata.

use super::*;
use crate::clpi;
use crate::mpls;
use crate::sector::SectorSource;
use crate::udf;

// Stream extensions probed for a BD clip, in priority order: `.m2ts` normally, AACS 2.1 uses
// `.fmts`, 3D uses `.ssif`. HD-DVD's `.evo` is a different tree and does NOT belong here.
const CLIP_STREAM_EXTS: [&str; 3] = ["m2ts", "fmts", "ssif"];

// MPLS/CLPI timestamps tick at 45 kHz; playlists shorter than this are menus/stubs.
const MPLS_TICKS_PER_SEC: f64 = 45000.0;
const MIN_TITLE_SECS: f64 = 30.0;

impl Disc {
    // Scan Blu-ray titles from MPLS playlists. `halt` is polled between playlists and once more
    // after the loop; a Halted read propagates instead of being swallowed.
    pub(super) fn scan_bluray_titles(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
        halt: Option<&crate::halt::Halt>,
    ) -> Result<Vec<DiscTitle>> {
        let mut titles = Vec::new();
        // clip_id -> packet count, shared across playlists so each .clpi is read once per scan.
        let mut clip_pkts: std::collections::HashMap<String, u32> =
            std::collections::HashMap::new();
        if let Some(playlist_dir) = udf_fs.find_dir("/BDMV/PLAYLIST") {
            for entry in &playlist_dir.entries {
                if halt.is_some_and(|h| h.is_cancelled()) {
                    return Err(Error::Halted);
                }
                if !entry.is_dir
                    && entry.name.len() >= 5
                    && entry.name.as_bytes()[entry.name.len() - 5..].eq_ignore_ascii_case(b".mpls")
                {
                    let path = format!("/BDMV/PLAYLIST/{}", entry.name);
                    let mpls_data = match udf_fs.read_file(reader, &path) {
                        Ok(data) => data,
                        // A cancel that lands on the LAST `.mpls` used to end
                        // the loop with nothing to show for it and still
                        // return `Ok`, so the truncation was invisible.
                        Err(Error::Halted) => return Err(Error::Halted),
                        // Every other read failure keeps the pre-existing
                        // best-effort skip: one unreadable playlist is not a
                        // reason to abandon the disc.
                        Err(e) => {
                            tracing::warn!(
                                target: "freemkv::disc",
                                playlist = ?entry.name,
                                "E{}", e.code()
                            );
                            continue;
                        }
                    };
                    if let Some(title) = Self::parse_playlist_memo(
                        reader,
                        udf_fs,
                        &entry.name,
                        &mpls_data,
                        &mut clip_pkts,
                    )? {
                        titles.push(title);
                    }
                }
            }
        }
        // Polled again AFTER the loop: a cancel raised during the final
        // iteration's reads has nothing left to poll, so without this the
        // last playlist could still slip a truncated list through as success.
        if halt.is_some_and(|h| h.is_cancelled()) {
            return Err(Error::Halted);
        }
        super::selection_hdmv::annotate(reader, udf_fs, &mut titles);
        super::selection_onq::annotate(reader, udf_fs, halt, &mut titles)?;
        Ok(titles)
    }

    // Parse one MPLS playlist into a DiscTitle. `Ok(None)` covers both benign misses
    // (unparseable, sub-30s) and deliberate drops; `Err` (only Halted) means the scan is over.
    #[cfg(test)]
    pub(super) fn parse_playlist(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
        filename: &str,
        data: &[u8],
    ) -> Result<Option<DiscTitle>> {
        let mut clip_pkts = std::collections::HashMap::new();
        Self::parse_playlist_memo(reader, udf_fs, filename, data, &mut clip_pkts)
    }

    // As `parse_playlist`, with a caller-owned clip_id -> packet-count memo. Failures are never
    // inserted, so a transient .clpi error is retried by the next playlist.
    pub(super) fn parse_playlist_memo(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
        filename: &str,
        data: &[u8],
        clip_pkts: &mut std::collections::HashMap<String, u32>,
    ) -> Result<Option<DiscTitle>> {
        let parsed = match mpls::parse(data) {
            Ok(p) => p,
            Err(e) => {
                // Real code, matching the CLPI path: a malformed playlist is
                // dropped, but never silently.
                tracing::warn!(target: "freemkv::disc", playlist = ?filename, "E{}", e.code());
                return Ok(None);
            }
        };

        // Calculate duration from play items
        let duration_ticks: u64 = parsed
            .play_items
            .iter()
            .map(|pi| (pi.out_time.saturating_sub(pi.in_time)) as u64)
            .sum();
        let duration_secs = duration_ticks as f64 / MPLS_TICKS_PER_SEC;

        // Skip very short playlists
        if duration_secs < MIN_TITLE_SECS {
            return Ok(None);
        }

        // Parse each clip for size, duration, and sector extents
        let mut extents = Vec::new();
        let mut total_size: u64 = 0;
        // Set when any clip resolves to a STREAM/SSIF/<clip>.ssif — a Blu-ray 3D
        // interleaved stream carrying both base and MVC dependent views. Drives
        // reading the SSIF for both eyes and adding the dependent-view below.
        let mut is_3d = false;
        let mut clips = Vec::with_capacity(parsed.play_items.len());
        // BD playlists legally reference the same .m2ts clip_id from multiple
        // PlayItems; extents/packet count must be counted ONCE per unique clip
        // (mux reads in order) or a duplicate mux's the A/V twice, inflating size.
        let mut seen_clips: std::collections::HashSet<String> = std::collections::HashSet::new();
        // Byte offset of the next extent within the TITLE'S FEED (the concatenation
        // of `extents` in read order). Each clip's span is recorded so a frame's
        // source offset identifies its clip by lookup, not by ambiguous timestamps.
        let mut feed_pos: u64 = 0;
        let mut spans: std::collections::HashMap<String, (u64, u64)> =
            std::collections::HashMap::new();
        // `clip_pkts` memoizes the packet count per clip_id: neither a repeated PlayItem nor
        // another playlist may re-read/re-parse a .clpi (hostile MPLS floods would never finish).

        for play_item in &parsed.play_items {
            let clip_dur =
                play_item.out_time.saturating_sub(play_item.in_time) as f64 / MPLS_TICKS_PER_SEC;

            let pkt_count: u32 = if let Some(&n) = clip_pkts.get(&play_item.clip_id) {
                n
            } else {
                let clpi_path = format!("/BDMV/CLIPINF/{}.clpi", play_item.clip_id);
                // A `.clpi` that cannot be read or parsed is NOT a benign miss:
                // `duration_ticks` already claims the full runtime, so drop the
                // title instead of silently shipping it missing bytes.
                let clip_info = match udf_fs
                    .read_file(reader, &clpi_path)
                    .and_then(|clpi_data| clpi::parse(&clpi_data))
                {
                    Ok(info) => info,
                    // The operator's Stop, not a disc defect: every remaining command
                    // fails the same way once the drive's flag is set, so classifying
                    // it as unresolvable would truncate the title list at success.
                    Err(Error::Halted) => return Err(Error::Halted),
                    Err(e) => {
                        // The REAL code, not a fixed one: DiscRead, UdfNotFound and
                        // ClpiParse are different populations, and flattening them
                        // would send anyone triaging the first after the third.
                        tracing::warn!(
                            target: "freemkv::disc",
                            playlist = ?filename,
                            clip = ?play_item.clip_id,
                            "E{}", e.code()
                        );
                        return Ok(None);
                    }
                };
                let n = clip_info.source_packet_count;
                clip_pkts.insert(play_item.clip_id.clone(), n);
                n
            };

            // The clip is marked seen only after its .clpi parses. That ordering
            // used to matter because a transient failure must not permanently
            // suppress extents for a later PlayItem; kept as still-correct.
            let first_ref = seen_clips.insert(play_item.clip_id.clone());

            // Only fetch/push the physical extents and add to the
            // total size the first time this clip_id is seen.
            if first_ref {
                // WHOLE-clip size/extents on purpose (no EP-map seek): out-of-mark
                // bytes are dropped downstream by PTS at the SeamPlan — see
                // `SeamPlan::place` (src/mux/timeline.rs) `raw_ns >= in_ns && <= out_ns`.
                let clpi_bytes = pkt_count as u64 * crate::consts::BD_SOURCE_PACKET_BYTES as u64;
                // The .ssif interleaves the dependent view, so the CLPI count (base
                // view only) under-sizes what the extents deliver.
                let mut from_ssif = false;

                // Get stream file extents from UDF allocation descriptors (dual-
                // layer discs split files across layers). Normally `.m2ts`; AACS 2.1
                // uses `.fmts`, 3D interleaves both eyes in STREAM/SSIF/<clip>.ssif.
                let ssif = format!("/BDMV/STREAM/SSIF/{}.ssif", play_item.clip_id);
                // ABSENCE (`UdfNotFound`) is the only benign failure — the fallback
                // exists for it. DiscRead/UdfAdChainTooLong/UdfEmbeddedData mean
                // bytes are unresolved; `Halted` is the operator cancelling, propagated.
                let mut unresolved: Option<u16> = None;
                let mut halted = false;
                let mut note = |e: &Error| {
                    if matches!(e, Error::Halted) {
                        halted = true;
                    } else if !matches!(e, Error::UdfNotFound { .. }) {
                        unresolved.get_or_insert(e.code());
                    }
                };
                let file_exts = match udf_fs.file_extents(reader, &ssif) {
                    Ok(exts) => {
                        is_3d = true;
                        from_ssif = true;
                        Some(exts)
                    }
                    Err(e) => {
                        note(&e);
                        CLIP_STREAM_EXTS.iter().find_map(|ext| {
                            let path = format!("/BDMV/STREAM/{}.{}", play_item.clip_id, ext);
                            match udf_fs.file_extents(reader, &path) {
                                Ok(exts) => Some(exts),
                                Err(e) => {
                                    note(&e);
                                    None
                                }
                            }
                        })
                    }
                };
                // Propagate the cancel BEFORE the classification below, or the
                // title is dropped (or emitted short) and the scan carries on
                // as if the disc were at fault.
                if halted {
                    return Err(Error::Halted);
                }
                // Nothing resolved AND a hole was the reason: drop the whole
                // title, or the clip contributes no extents while durations/size
                // still count it — data loss wearing the shape of a normal rip.
                match (&file_exts, unresolved) {
                    (None, Some(code)) => {
                        // The REAL code, not a fixed one: e.g. E6000 vs E6016
                        // logged as E6017 would hide the population that exists.
                        tracing::warn!(
                            target: "freemkv::disc",
                            playlist = ?filename,
                            clip = ?play_item.clip_id,
                            "E{}", code
                        );
                        return Ok(None);
                    }
                    // A non-absence failure the FALLBACK papered over: `.ssif` failed
                    // non-benignly but `.m2ts` resolved. LOGGED, not refused — the
                    // fallback IS a truthful 2D read plan; dropping trades it for none.
                    (Some(_), Some(code)) => {
                        tracing::warn!(
                            target: "freemkv::disc",
                            playlist = ?filename,
                            clip = ?play_item.clip_id,
                            fell_back = true,
                            code = code,
                            "E{}", code
                        );
                    }
                    // Every candidate was merely absent: the clip's bytes are gone, same
                    // as a missing .clpi, so refuse the title rather than ship it short.
                    (None, None) => {
                        tracing::warn!(
                            target: "freemkv::disc",
                            playlist = ?filename,
                            clip = ?play_item.clip_id,
                            "E{}", crate::error::E_UDF_NOT_FOUND
                        );
                        return Ok(None);
                    }
                    (Some(_), None) => {}
                }
                // KNOWN GAP, deliberately left open: an empty-but-Ok `file_extents`
                // leaves the clip with no extents while size/timing still count it —
                // not closed since it's unclear that's always a defect (see hddvd.rs).
                if let Some(file_exts) = file_exts {
                    let span_start = feed_pos;
                    push_extents(file_exts, &mut extents, &mut feed_pos);
                    if feed_pos > span_start {
                        spans.insert(play_item.clip_id.clone(), (span_start, feed_pos));
                    }
                    // A damaged CLPI reports no packet count: size the clip by its extents.
                    total_size += if pkt_count == 0 || from_ssif {
                        feed_pos - span_start
                    } else {
                        clpi_bytes
                    };
                }
            }

            clips.push(Clip {
                feed_span: spans.get(&play_item.clip_id).copied(),
                clip_id: play_item.clip_id.clone(),
                in_time: play_item.in_time,
                out_time: play_item.out_time,
                duration_secs: clip_dur,
                source_packets: pkt_count,
            });
        }

        // Build streams from STN table
        let mut streams: Vec<Stream> = parsed
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
                        // Dolby Vision is its own (EL) entry; HDR10+ rides the HDR10 base.
                        hdr: match s.dynamic_range {
                            1 if s.hdr_plus => HdrFormat::Hdr10Plus,
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
                        // No user-facing English (numeric-code rule): Dolby Vision
                        // is signalled structurally (secondary + DolbyVision hdr);
                        // `label` stays empty for disc video streams.
                        label: String::new(),
                        // TODO(spec): surface measured field order instead of the TFF
                        // fallback (needs a parser→title channel, see dvd.rs); prefer
                        // measured VUI CICP over this MPLS nibble guess once surfaced.
                        measured_cicp: None,
                    })),
                    2 | 5 => {
                        // Guard: if coding_type is the PGS subtitle codec (0x90), this
                        // is a misaligned stream -- treat as subtitle, not audio
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
                                channels: playlist_channels(s.audio_format, codec),
                                language: s.language.clone(),
                                sample_rate: SampleRate::from_audio_rate(s.audio_rate),
                                secondary: s.stream_type == 5,
                                purpose: crate::disc::LabelPurpose::Normal,
                                label: String::new(),
                            }))
                        }
                    }
                    // PiP PG belongs to the secondary-video overlay, not the feature.
                    3 if s.secondary => None,
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
                            target: "freemkv::disc",
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

        // 3D: add the MVC dependent (right-eye) stream, PID = base PID + 1 (base
        // STN omits it; lives in MPLS STN_table_SS). `is_3d` latches per
        // PLAYLIST, so a mixed 3D/2D playlist over-claims 3D for 2D frames.
        if is_3d
            && let Some(base) = streams.iter().find_map(|s| match s {
                Stream::Video(v) => Some(v.clone()),
                _ => None,
            })
        {
            let dep_pid = base.pid.wrapping_add(1);
            let have_dep = streams
                .iter()
                .any(|s| matches!(s, Stream::Video(v) if v.pid == dep_pid));
            if !have_dep {
                streams.push(Stream::Video(VideoStream {
                    pid: dep_pid,
                    secondary: true,
                    label: crate::disc::MVC_DEPENDENT_LABEL.to_string(),
                    ..base
                }));
            }
        }

        // Convert marks to chapters: mark_type 1 is an entry-mark; 2/0 aren't.
        // Each mark's timestamp is in its own PlayItem's timebase, so position
        // sums preceding durations plus offset (not play_items[0].in_time).
        let chapters: Vec<Chapter> = parsed
            .marks
            .iter()
            .filter(|m| m.is_chapter_mark())
            .filter_map(|m| {
                let pi_idx = m.play_item_ref as usize;
                let pi = parsed.play_items.get(pi_idx)?;
                let preceding: f64 = parsed.play_items[..pi_idx]
                    .iter()
                    .map(|p| p.out_time.saturating_sub(p.in_time) as f64 / MPLS_TICKS_PER_SEC)
                    .sum();
                let within = (m.timestamp as f64 - pi.in_time as f64) / MPLS_TICKS_PER_SEC;
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

        Ok(Some(DiscTitle {
            selection_evidence: Default::default(),
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
        }))
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
                    .filter(|e| {
                        !e.is_dir
                            && e.name.len() >= 4
                            && e.name.as_bytes()[e.name.len() - 4..].eq_ignore_ascii_case(b".xml")
                    })
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
                                // Disc-authored text reaches terminals and file names:
                                // control characters (decoded from `&#27;` too) are dropped.
                                let title = crate::labels::display_text(&xml_text_decode(
                                    xml[s..s + end].trim(),
                                ));
                                if !title.is_empty() && !crate::labels::is_placeholder_title(&title)
                                {
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

/// Append each usable `(lba, sectors)` to `extents`, advancing `feed_pos` by its bytes. An extent
/// with no sectors or at LBA 0 occupies no readable bytes and is skipped without moving `feed_pos`.
fn push_extents(file_exts: Vec<(u32, u32)>, extents: &mut Vec<Extent>, feed_pos: &mut u64) {
    for (lba, sectors) in file_exts {
        if sectors > 0 && lba > 0 {
            extents.push(Extent {
                start_lba: lba,
                sector_count: sectors,
            });
            *feed_pos =
                feed_pos.saturating_add(sectors as u64 * crate::consts::SECTOR_BYTES as u64);
        }
    }
}

// Decode the five predefined XML entities and numeric char refs in element text, and unwrap a
// CDATA section; unknown or malformed references are kept literally.
fn xml_text_decode(raw: &str) -> String {
    if let Some(inner) = raw
        .strip_prefix("<![CDATA[")
        .and_then(|r| r.strip_suffix("]]>"))
    {
        return inner.trim().to_string();
    }
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        rest = &rest[amp..];
        let decoded = rest.find(';').filter(|&semi| semi <= 10).and_then(|semi| {
            let ent = &rest[1..semi];
            let ch = match ent {
                "amp" => Some('&'),
                "lt" => Some('<'),
                "gt" => Some('>'),
                "quot" => Some('"'),
                "apos" => Some('\''),
                _ => ent.strip_prefix('#').and_then(|n| {
                    match n.strip_prefix(['x', 'X']) {
                        Some(h) => u32::from_str_radix(h, 16).ok(),
                        None => n.parse::<u32>().ok(),
                    }
                    .and_then(char::from_u32)
                }),
            };
            ch.map(|c| (c, semi))
        });
        match decoded {
            Some((c, semi)) => {
                out.push(c);
                rest = &rest[semi + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

// Playlist audio_format 6 is "multi-channel" with no count. DTS-HD and DD+ reach 7.1 and
// nothing here re-reads their layout, so 6 stays Unknown rather than a claimed 5.1; AC-3 and
// DTS core top out at 5.1, and TrueHD and LPCM are completed from the stream later.
fn playlist_channels(audio_format: u8, codec: Codec) -> AudioChannels {
    match (audio_format, codec) {
        (6, Codec::DtsHdMa | Codec::DtsHdHr | Codec::Ac3Plus) => AudioChannels::Unknown,
        _ => AudioChannels::from_audio_format(audio_format),
    }
}

#[cfg(test)]
#[path = "bluray_tests.rs"]
mod tests;
