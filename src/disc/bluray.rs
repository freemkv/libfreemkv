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
    ///
    /// Cancellation: `halt` is polled between playlists and once more after
    /// the loop, and a read that fails with [`Error::Halted`] — how a live
    /// drive reports a Stop, since `Drive::checked_exec` fails EVERY command
    /// once its flag is set and `Drive::read` preserves the variant — is
    /// propagated rather than swallowed. `Halted` is the only error this
    /// returns from the enumeration itself; an unreadable or unparseable
    /// playlist keeps its best-effort skip.
    ///
    /// It has to be an error and not a short title list, for the same reason
    /// spelled out on [`Disc::scan_hddvd_titles`]: a cancelled enumeration
    /// that returned `Ok` would be indistinguishable from a disc that
    /// genuinely holds fewer titles, and the caller would cache and act on
    /// it. Before this, a Stop pressed mid-scan failed every REMAINING
    /// `.mpls` read in turn, each one silently skipped, and the scan returned
    /// `Ok(truncated)` at rc=0.
    pub(super) fn scan_bluray_titles(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
        halt: Option<&crate::halt::Halt>,
    ) -> Result<Vec<DiscTitle>> {
        let mut titles = Vec::new();
        if let Some(playlist_dir) = udf_fs.find_dir("/BDMV/PLAYLIST") {
            for entry in &playlist_dir.entries {
                if halt.is_some_and(|h| h.is_cancelled()) {
                    return Err(Error::Halted);
                }
                if !entry.is_dir && entry.name.to_lowercase().ends_with(".mpls") {
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
                        Err(_) => continue,
                    };
                    if let Some(title) =
                        Self::parse_playlist(reader, udf_fs, &entry.name, &mpls_data)?
                    {
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
        Ok(titles)
    }

    /// Parse one MPLS playlist into a [`DiscTitle`].
    ///
    /// Sums PlayItem durations; returns `Ok(None)` if the playlist is under
    /// 30 seconds (skips menu / clip-info stub playlists), fails to
    /// parse, or names a clip that cannot be resolved. Physical sector
    /// extents are pulled from the UDF allocation descriptors of each
    /// referenced `.m2ts` (deduplicated by clip_id).
    ///
    /// `Ok(None)` keeps its two benign meanings (unparseable MPLS, sub-30 s
    /// playlist) plus the deliberate "drop this title" outcomes below. `Err`
    /// means the SCAN is over: today that is only [`Error::Halted`], the
    /// operator's Stop, which must not be reported as a disc that simply
    /// holds fewer titles.
    pub(super) fn parse_playlist(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
        filename: &str,
        data: &[u8],
    ) -> Result<Option<DiscTitle>> {
        let Ok(parsed) = mpls::parse(data) else {
            return Ok(None);
        };

        // Calculate duration from play items
        let duration_ticks: u64 = parsed
            .play_items
            .iter()
            .map(|pi| (pi.out_time.saturating_sub(pi.in_time)) as u64)
            .sum();
        let duration_secs = duration_ticks as f64 / 45000.0;

        // Skip very short playlists (< 30 seconds)
        if duration_secs < 30.0 {
            return Ok(None);
        }

        // Parse each clip for size, duration, and sector extents
        let mut extents = Vec::new();
        let mut total_size: u64 = 0;
        // Set when any clip resolves to a STREAM/SSIF/<clip>.ssif — a Blu-ray 3D
        // interleaved stream carrying both the base (left) and MVC dependent
        // (right) views. Drives reading the SSIF for both eyes and adding the
        // dependent-view stream below.
        let mut is_3d = false;
        let mut clips = Vec::with_capacity(parsed.play_items.len());
        // BD playlists legally reference the same .m2ts clip_id from
        // multiple PlayItems (multi-angle, seamless splits, looped
        // segments). The physical extents and packet count must be
        // counted ONCE per unique clip — mux reads extents in order, so
        // a duplicate would mux the A/V twice and inflate size_bytes.
        // Per-PlayItem Clip entries (differing in/out times) still get
        // recorded.
        let mut seen_clips: std::collections::HashSet<String> = std::collections::HashSet::new();
        // Byte offset of the next extent within the TITLE'S FEED — the
        // concatenation of `extents` in the order the mux reads them. Each
        // clip's span is recorded so a frame's source offset identifies its
        // clip by lookup rather than by guessing from timestamps, which is
        // ambiguous inside an overlap. A clip referenced a SECOND time pushes
        // no extents (see `first_ref`), so it reuses the span of its first
        // reference — the same bytes, read once.
        let mut feed_pos: u64 = 0;
        let mut spans: std::collections::HashMap<String, (u64, u64)> =
            std::collections::HashMap::new();

        for play_item in &parsed.play_items {
            let clip_dur = play_item.out_time.saturating_sub(play_item.in_time) as f64 / 45000.0;

            let clpi_path = format!("/BDMV/CLIPINF/{}.clpi", play_item.clip_id);
            // A `.clpi` that cannot be read or parsed is NOT a benign miss.
            // `duration_ticks` was already summed from the PlayItems above, so
            // the title keeps claiming its full runtime; skipping this block
            // (what the old `if let Ok(..) && let Ok(..)` did) pushed no
            // extents and added nothing to `total_size`, so the title shipped
            // with the clip's bytes silently absent — and, because the `if let`
            // DISCARDED the error, without a single log line to say so. Same
            // classification as the extent resolver below: drop the title, and
            // log the error's OWN code.
            let clip_info = match udf_fs
                .read_file(reader, &clpi_path)
                .and_then(|clpi_data| clpi::parse(&clpi_data))
            {
                Ok(info) => info,
                // The operator's Stop, not a disc defect. Every remaining
                // command fails the same way once the drive's flag is set, so
                // classifying it as an unresolvable clip would drop each
                // remaining playlist in turn and hand back a truncated title
                // list at success.
                Err(Error::Halted) => return Err(Error::Halted),
                Err(e) => {
                    // The REAL code, not a fixed one: a scratched CLIPINF
                    // sector (DiscRead), a missing `.clpi` (UdfNotFound) and a
                    // malformed one (ClpiParse) are different populations, and
                    // flattening them would send anyone triaging the first
                    // after the third.
                    tracing::warn!(
                        target: "freemkv::disc",
                        playlist = ?filename,
                        clip = ?play_item.clip_id,
                        "E{}", e.code()
                    );
                    return Ok(None);
                }
            };
            let pkt_count: u32 = clip_info.source_packet_count;

            // The clip is marked seen only after its .clpi parses. That
            // ordering used to matter because a transient failure on the first
            // PlayItem referencing a clip must not permanently suppress its
            // extents/size for a later PlayItem referencing the same clip;
            // a failure now drops the whole title, so the ordering is kept
            // simply because it is still the correct place for it.
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
                // 3D discs interleave the left (base) and right (MVC
                // dependent) views in STREAM/SSIF/<clip>.ssif — note the
                // SSIF/ subdir. Prefer it when present: the SSIF is one
                // transport stream carrying BOTH eyes on distinct PIDs,
                // so muxing it captures the full 3D. 2D clips fall back to
                // the base .m2ts / .fmts as before.
                let ssif = format!("/BDMV/STREAM/SSIF/{}.ssif", play_item.clip_id);
                // A clip stream that carries an unrecorded (never-written)
                // extent cannot be turned into a truthful read plan — see
                // `UdfFs::file_extents`. Track it separately from an
                // ordinary "file absent" error: absence is what the
                // extension fallback exists for, whereas a hole means the
                // bytes this title needs do not exist on the disc.
                //
                // ABSENCE is the only benign failure. A missing `.ssif` is
                // the ordinary case (every 2D disc), and the extension
                // fallback below exists precisely for it. Every OTHER
                // error means the bytes this title needs could not be
                // resolved: a scratched sector under the clip's ICB
                // (DiscRead), an allocation-descriptor chain that never
                // terminated (UdfAdChainTooLong), a file whose data is
                // embedded rather than extent-mapped (UdfEmbeddedData).
                // Those used to fall through to the not-found path, so the
                // clip contributed zero extents while `total_size` and the
                // play-item timing still counted it — a title advertising
                // its full runtime with a piece silently missing, and no
                // log line anywhere.
                //
                // `Halted` is NOT a disc defect and must not be treated as
                // one either. It is the operator cancelling: once the flag is
                // set, EVERY drive command returns it, so classifying it here
                // would drop each remaining playlist in turn and hand back a
                // truncated title list at success — the same shape this
                // refusal exists to prevent, wearing a cancel. It used to be
                // merely EXEMPTED (neither classified nor propagated, because
                // this function returned `Option`); exempting it is no longer
                // enough now that there is a channel. A cancel landing on
                // `file_extents` would otherwise return a title with this
                // clip's bytes silently missing — the flagship defect shape,
                // wearing a cancel. It is propagated below instead.
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
                // Propagate the cancel BEFORE the classification below: a
                // halted `file_extents` resolves nothing, so the title would
                // otherwise be dropped (or, with a partial resolve, emitted
                // short) and the scan would carry on as if the disc were at
                // fault.
                if halted {
                    return Err(Error::Halted);
                }
                // Nothing resolved AND a hole was the reason: drop the
                // whole title. Letting the clip contribute no extents
                // (the ordinary not-found path) would emit a title whose
                // feed is silently missing this clip's runtime while its
                // durations, spans and size still count it — data loss
                // wearing the shape of a normal rip.
                if let (None, Some(code)) = (&file_exts, unresolved) {
                    // The REAL code, not a fixed one. A scratched sector
                    // (E6000) and an over-long AD chain (E6016) logged as
                    // E6017 would send anyone triaging them after authoring
                    // holes and hide the population that actually exists.
                    tracing::warn!(
                        target: "freemkv::disc",
                        playlist = ?filename,
                        clip = ?play_item.clip_id,
                        "E{}", code
                    );
                    return Ok(None);
                }
                // KNOWN GAP, deliberately left open: `file_extents` can also
                // return `Ok(vec![])`, or a vector every entry of which the
                // `sectors > 0 && lba > 0` filter below discards (zero-length
                // placeholder ADs — see `UdfFs::file_extents`). That clip then
                // contributes no extents and no span while `total_size` and
                // the play-item timing still count it: the same shape the
                // refusal above exists to prevent, reached by a different
                // route. It is NOT closed with a post-loop "every unique
                // clip_id must appear in `spans`" invariant because it is not
                // settled that an empty-but-Ok resolve is always a defect
                // rather than a legitimate healthy-disc state, and dropping
                // healthy titles is a worse failure than the residual gap.
                // Recorded here so the next audit finds the decision instead
                // of re-deriving it.
                if let Some(file_exts) = file_exts {
                    let span_start = feed_pos;
                    for (lba, sectors) in file_exts {
                        if sectors > 0 && lba > 0 {
                            extents.push(Extent {
                                start_lba: lba,
                                sector_count: sectors,
                            });
                            feed_pos = feed_pos.saturating_add(
                                sectors as u64 * crate::consts::SECTOR_BYTES as u64,
                            );
                        }
                    }
                    if feed_pos > span_start {
                        spans.insert(play_item.clip_id.clone(), (span_start, feed_pos));
                    }
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

        // 3D: add the MVC dependent (right-eye) video stream. The base STN table
        // lists only the left-eye video; the dependent view is a second video
        // PID (stream_type 0x20) carried in the SSIF. The on-disc PAT/PMT are
        // AACS-encrypted (unreadable pre-key) and the base STN omits the
        // dependent view (it lives in the MPLS STN_table_SS), so we use the
        // BD-3D PID convention: dependent = base-view video PID + 1
        // (e.g. 0x1011 -> 0x1012). Reading the SSIF (above) provides its packets.
        //
        // Limitation: `is_3d` latches per PLAYLIST, not per clip. A playlist that
        // mixed a 3D clip (has an SSIF) with a 2D clip (no SSIF) would tag the
        // whole title 3D; the 2D clip's frames then mux as plain Blocks (no
        // dependent PID → no BlockAdditional) under a track that still advertises
        // the mvcC mapping. That output is valid (per-frame BlockAdditional is
        // optional) but over-claims 3D for those frames. Real 3D main-feature
        // playlists are single-clip or uniformly 3D, so this is not exercised;
        // per-clip 3D would need per-clip stream sets (a larger change).
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
            .filter(|m| m.is_chapter_mark())
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

        Ok(Some(DiscTitle {
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
    /// HEVC video stream entry carrying the third (HDR) attribute byte:
    /// high nibble = dynamic_range, low nibble = color_space (mpls.rs only
    /// parses this byte for coding_type == HEVC and sa.len() > 2).
    fn se_video_hevc(pid: u16, dynamic_range: u8, color_space: u8) -> Vec<u8> {
        let mut out = vec![3u8, 0x01];
        out.extend_from_slice(&pid.to_be_bytes());
        let hdr_byte = (dynamic_range << 4) | color_space;
        let attrs = vec![0x24u8, 0x10, hdr_byte]; // coding_type = HEVC
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
            Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
                .expect("scan")
                .is_none(),
            "playlists shorter than 30s must be skipped"
        );
    }

    /// At exactly 30 s the playlist is kept (`< 30.0` is strict).
    ///
    /// The fixture is a FULLY WIRED BDMV (`make_bdmv_fs`), not the bare
    /// `make_min_fs` this used to use. `make_min_fs` lays no STREAM/CLIPINF,
    /// so the play item's `.clpi` read now fails and the title drops — which
    /// is correct behaviour for an unresolvable clip but has nothing to do
    /// with the 30-second boundary this test exists to pin. Wiring the clip
    /// keeps the test measuring exactly one thing.
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
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
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
        assert!(
            Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &junk)
                .expect("scan")
                .is_none()
        );
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
            stream_files.push(file(&m2ts, icb, *data_lba, size as u64, true));
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

    /// Full BDMV with a real Blu-ray 3D layout: `.ssif` files under
    /// `BDMV/STREAM/SSIF/<clip>.ssif` (note the SSIF subdirectory, unlike
    /// [`make_bdmv_fs_ext`]) plus a matching `.clpi` in CLIPINF. Resolving
    /// the SSIF is what latches `is_3d = true` in `parse_playlist`.
    fn make_bdmv_fs_ssif(
        disc: &mut MemDisc,
        clips: &[(
            &str,
            u32, /*sectors*/
            u32, /*packets*/
            u32, /*data_lba*/
        )],
    ) -> udf::UdfFs {
        let mut ssif_files = Vec::new();
        let mut clipinf_files = Vec::new();
        let mut icb = 200u32;
        for (name, sectors, packets, data_lba) in clips {
            let ssif = format!("{name}.ssif");
            let size = sectors * 2048;
            ssif_files.push(file(&ssif, icb, *data_lba, size as u64, true));
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
            icb_lba: 40,
            dir_data_lba: 41,
            files: Vec::new(),
            subdirs: vec![
                DirSpec {
                    name: "STREAM".to_string(),
                    icb_lba: 42,
                    dir_data_lba: 43,
                    files: Vec::new(),
                    subdirs: vec![DirSpec {
                        name: "SSIF".to_string(),
                        icb_lba: 44,
                        dir_data_lba: 45,
                        files: ssif_files,
                        subdirs: vec![],
                    }],
                },
                DirSpec {
                    name: "CLIPINF".to_string(),
                    icb_lba: 46,
                    dir_data_lba: 47,
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
        // BD source packet = 192 bytes (188 TS + 4-byte timestamp header).
        assert_eq!(t.size_bytes, 4000 * 192);
        assert_eq!(t.extents.len(), 1, "one m2ts → one extent");
        // file_extents absolute LBA = partition_start + data_lba.
        assert_eq!(t.extents[0].start_lba, PART_START + 5000);
        assert_eq!(t.extents[0].sector_count, 1000);
        assert_eq!(t.clips.len(), 1);
        assert_eq!(t.clips[0].source_packets, 4000);
    }

    /// Each Clip's `duration_secs` is `(out_time - in_time) / 45000` (the BD
    /// 45kHz playback clock). Uses a duration (75s) whose ticks are not a
    /// multiple of any small constant, so a `*` or `%` in place of `/` would
    /// produce a wildly different (or non-matching) value instead of 75.0.
    #[test]
    fn parse_playlist_clip_duration_secs_computed_from_ticks() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 45000,
                out_time: 45000 + 75 * 45000, // 75s clip
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
        assert_eq!(t.clips.len(), 1);
        assert!(
            (t.clips[0].duration_secs - 75.0).abs() < 1e-6,
            "clip duration_secs must be ticks/45000 seconds, got {}",
            t.clips[0].duration_secs
        );
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
        assert_eq!(t.extents.len(), 2);
        assert_eq!(t.extents[0].start_lba, PART_START + 5000);
        assert_eq!(t.extents[1].start_lba, PART_START + 9000);
        assert_eq!(t.size_bytes, (4000 + 2000) * 192);
    }

    /// A clip whose `.clpi` is missing must yield NO TITLE.
    ///
    /// This test used to assert the opposite — a title with `size_bytes == 0`
    /// and empty extents — and its docstring quoted the buggy control flow
    /// ("bluray.rs only fetches extents inside the `if let Ok(clpi_data)`")
    /// as if it were the specification. It was blessing the defect: the
    /// title's `duration_secs` is summed from the PlayItems BEFORE the clip is
    /// resolved, so the returned title advertised the movie's full runtime
    /// while carrying none of its bytes, and the discarded error meant not one
    /// log line said so. That is the flagship failure class of this crate — a
    /// failure that looks like success — reached through an ordinary missing
    /// or scratched CLIPINF file.
    ///
    /// The correct behaviour is the same as for a clip whose extents cannot be
    /// resolved (see `parse_playlist_unreadable_clip_icb_yields_no_title`):
    /// drop the title and log the read's OWN error code.
    #[test]
    fn parse_playlist_missing_clpi_yields_no_title() {
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00009.mpls", &mpls).expect("scan");
        assert!(
            t.is_none(),
            "a clip with no .clpi cannot be sized or resolved, so offering the \
             title would advertise the full play-item runtime with none of the \
             clip's bytes behind it; got {:?}",
            t.map(|t| (t.size_bytes, t.extents))
        );
    }

    /// A clip stream whose ICB declares an UNRECORDED (ECMA-167 4/14.14.1.1
    /// type-1) extent must not yield a title at all.
    ///
    /// The extent is allocated to the file but was never written, so the
    /// file's content there is zeros while the media holds whatever was left
    /// at those sectors. Neither answer a `(lba, sector_count)` read plan can
    /// give is true — reading it splices undefined sectors into the rip as
    /// content, dropping it slides every later extent's byte space — so the
    /// title is refused rather than mis-ripped. This fixture is the shape a
    /// crafted disc uses to get such a range into a title's extent list.
    ///
    /// (The `sectors > 0 && lba > 0` filter below the resolver stays as
    /// defence in depth; a zero-length AD is only reachable as an unrecorded
    /// descriptor, since a zero-length TYPE 0 one terminates the AD list.)
    #[test]
    fn parse_playlist_unrecorded_extent_yields_no_title() {
        let mut disc = MemDisc::new();
        // The m2ts ICB is rewritten below to carry TWO short ADs: a
        // zero-length one (0 sectors) followed by a real 4096-byte one.
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
                        files: vec![file("00001.m2ts", 100, 5000, 4096, false)],
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
            // Rewrite the .m2ts ICB (laid at PART_START + 100 by `lay_dir`)
            // with a two-descriptor short-AD list:
            //   AD0: ECMA-167 4/14.14.1.1 type 1 (allocated, NOT recorded),
            //        length 0, at LBA 4999 — a zero-length descriptor that
            //        SURVIVES `read_icb_extents` (only a zero-length TYPE 0
            //        descriptor is the AD-list terminator), so it reaches
            //        `file_extents` as an extent of div_ceil(0, 2048) = 0
            //        sectors. This is the shape a crafted disc uses to put a
            //        readable-looking but empty range into a title's extent
            //        list.
            //   AD1: type 0, 4096 bytes at LBA 5000 — the real content.
            let mut icb = build_file_icb(4096, 5000, false);
            icb[212..216].copy_from_slice(&16u32.to_le_bytes()); // l_ad: two short ADs
            icb[216..220].copy_from_slice(&0x4000_0800u32.to_le_bytes()); // type 1, 2048 bytes
            icb[220..224].copy_from_slice(&4999u32.to_le_bytes());
            icb[224..228].copy_from_slice(&4096u32.to_le_bytes()); // type 0, 4096 bytes
            icb[228..232].copy_from_slice(&5000u32.to_le_bytes());
            disc.put_bytes(PART_START + 100, &icb);
            udf::read_filesystem(&mut disc).expect("fs")
        };
        // The fixture must really carry the unrecorded descriptor, or the
        // behaviour under test is never reached. `file_extents_addressing`
        // shows what is there: the hole in its byte-space position, followed
        // by the real content.
        assert_eq!(
            udf.file_extents_addressing(&mut disc, "/BDMV/STREAM/00001.m2ts")
                .expect("extents"),
            vec![(PART_START + 4999, 1), (PART_START + 5000, 2)],
            "fixture must present one unrecorded extent that OCCUPIES byte \
             space, and one real one"
        );
        assert!(
            matches!(
                udf.file_extents(&mut disc, "/BDMV/STREAM/00001.m2ts"),
                Err(Error::UdfUnrecordedExtent { .. })
            ),
            "a read plan over an unrecorded extent must be refused"
        );
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("scan");
        assert!(
            t.is_none(),
            "the only clip has no truthful read plan, so offering the title \
             would mean ripping undefined sectors as content; got {:?}",
            t.map(|t| t.extents)
        );
    }

    /// An unrecorded extent was never the only way a clip fails to resolve.
    ///
    /// RED BEFORE GREEN: this fixture gives the .m2ts an ICB whose descriptor
    /// tag is neither 261 nor 266, so `file_extents` returns `DiscRead` — the
    /// same variant a SCRATCHED SECTOR under a real clip's ICB produces, which
    /// is the ordinary way this happens on real media. Before the fix only
    /// `UdfUnrecordedExtent` set the drop flag, so this fell through to the
    /// "file absent" path and `parse_playlist` returned a title: full declared
    /// duration from the play item, `total_size` already counted from the
    /// .clpi, and ZERO extents — a movie advertising its runtime with the
    /// content missing, and not one log line. The clip must drop the title
    /// exactly as an unrecorded extent does.
    #[test]
    fn parse_playlist_unreadable_clip_icb_yields_no_title() {
        let mut disc = MemDisc::new();
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
                        files: vec![file("00001.m2ts", 100, 5000, 4096, false)],
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
            // Corrupt ONLY the descriptor tag, leaving a structurally valid
            // ICB behind it. That is what an unreadable/garbled sector looks
            // like to the parser, and it is deliberately NOT an unrecorded
            // extent — the point is the error class the old code ignored.
            let mut icb = build_file_icb(4096, 5000, false);
            icb[0..2].copy_from_slice(&999u16.to_le_bytes());
            disc.put_bytes(PART_START + 100, &icb);
            udf::read_filesystem(&mut disc).expect("fs")
        };
        // The fixture must really produce a non-unrecorded error, or the
        // behaviour under test is never reached.
        assert!(
            matches!(
                udf.file_extents(&mut disc, "/BDMV/STREAM/00001.m2ts"),
                Err(Error::DiscRead { .. })
            ),
            "fixture must fail with DiscRead, not UdfUnrecordedExtent"
        );
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls).expect("scan");
        assert!(
            t.is_none(),
            "a clip whose extents could not be resolved must drop the title, \
             not yield one that counts the clip's runtime and ships none of \
             its bytes; got {:?}",
            t.map(|t| (t.size_bytes, t.extents))
        );
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
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

    /// HEVC HDR byte (sa[2]): high nibble = dynamic_range, low nibble =
    /// color_space. dynamic_range 1 -> HDR10, color_space 2 -> BT.2020
    /// (bluray.rs `match s.dynamic_range { 1 => Hdr10, ... }` /
    /// `match s.color_space { 2 => Bt2020, ... }`).
    #[test]
    fn parse_playlist_maps_hdr10_bt2020_from_hevc_nibbles() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            }],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[se_video_hevc(0x1011, 1, 2)],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
        let v = t
            .streams
            .iter()
            .find_map(|s| match s {
                Stream::Video(v) => Some(v),
                _ => None,
            })
            .expect("video stream");
        assert_eq!(v.hdr, HdrFormat::Hdr10);
        assert_eq!(v.color_space, ColorSpace::Bt2020);
    }

    /// dynamic_range 2 -> DolbyVision, color_space 1 -> BT.709: the other
    /// pair of named arms in the same two match expressions.
    #[test]
    fn parse_playlist_maps_dolby_vision_bt709_from_hevc_nibbles() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            }],
            (1, 0, 0, 0, 0, 0, 0, 0),
            &[se_video_hevc(0x1011, 2, 1)],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
        let v = t
            .streams
            .iter()
            .find_map(|s| match s {
                Stream::Video(v) => Some(v),
                _ => None,
            })
            .expect("video stream");
        assert_eq!(v.hdr, HdrFormat::DolbyVision);
        assert_eq!(v.color_space, ColorSpace::Bt709);
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
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
        assert!(
            !audios[0].secondary,
            "a primary (stream_type 2) audio entry must not be marked secondary"
        );
    }

    /// A secondary-audio STN entry (stream_type 5, e.g. a director's
    /// commentary track) must set `AudioStream::secondary` (bluray.rs
    /// `secondary: s.stream_type == 5`).
    #[test]
    fn parse_playlist_secondary_audio_flag_set_for_stream_type_5() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            }],
            (0, 0, 0, 0, 1, 0, 0, 0), // one secondary-audio (stream_type 5) entry
            &[se_audio(0x1a00, 0x83, b"eng")],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
        let audios: Vec<_> = t
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Audio(a) => Some(a),
                _ => None,
            })
            .collect();
        assert_eq!(audios.len(), 1);
        assert!(
            audios[0].secondary,
            "stream_type 5 (secondary audio) must set AudioStream::secondary"
        );
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
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
    // Tests: Blu-ray 3D dependent-view stream
    // ---------------------------------------------------------------

    /// When a clip resolves via `STREAM/SSIF/<clip>.ssif`, `is_3d` latches
    /// and a synthetic MVC dependent-view video stream is added at
    /// `base_pid + 1` (bluray.rs's 3D block). Verifies all three fields set
    /// on the synthesized `VideoStream`: `pid`, `secondary`, `label`.
    #[test]
    fn parse_playlist_3d_adds_dependent_view_stream() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs_ssif(&mut disc, &[("00001", 1000, 4000, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            }],
            (1, 0, 0, 0, 0, 0, 0, 0),
            // Base (left-eye) view only -- STN table omits the dependent view.
            &[se_video(0x1011, 0x1B)],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
        let videos: Vec<_> = t
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Video(v) => Some(v),
                _ => None,
            })
            .collect();
        assert_eq!(
            videos.len(),
            2,
            "a 3D title must add one dependent-view video stream"
        );
        let dep = videos
            .iter()
            .find(|v| v.pid == 0x1012)
            .expect("dependent-view stream at base_pid + 1");
        assert!(dep.secondary, "dependent view must be marked secondary");
        assert_eq!(
            dep.label,
            crate::disc::MVC_DEPENDENT_LABEL,
            "dependent view must carry the MVC dependent-view label"
        );
    }

    /// If the STN table already lists a video stream at `base_pid + 1`
    /// (e.g. an authoring tool that populated STN_table_SS), the synthetic
    /// push must be skipped -- never duplicate an existing dependent-view
    /// entry (bluray.rs `if !have_dep`).
    #[test]
    fn parse_playlist_3d_does_not_duplicate_existing_dependent_stream() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs_ssif(&mut disc, &[("00001", 1000, 4000, 5000)]);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 60 * 45000,
            }],
            (1, 0, 0, 0, 0, 1, 0, 0), // primary video + secondary (PiP) video
            &[se_video(0x1011, 0x1B), se_video(0x1012, 0x1B)],
            &[],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
        let dep_count = t
            .streams
            .iter()
            .filter(|s| matches!(s, Stream::Video(v) if v.pid == 0x1012))
            .count();
        assert_eq!(
            dep_count, 1,
            "an already-present stream at base_pid + 1 must not be duplicated"
        );
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
        assert_eq!(t.chapters.len(), 1);
        // preceding (PI0 = 60s) + within (timestamp - pi1.in_time = 0) = 60s.
        assert!(
            (t.chapters[0].time_secs - 60.0).abs() < 1e-6,
            "chapter must sit at 60s, got {}",
            t.chapters[0].time_secs
        );
    }

    /// The within-PlayItem offset is `(timestamp - pi.in_time) / 45000`
    /// ticks-to-seconds. Uses a non-zero, non-round offset (5s) added to a
    /// non-zero `preceding` (60s) so a `*` or `%` in place of `/` would not
    /// coincidentally produce the same total (bluray.rs `within = ... /
    /// 45000.0`).
    #[test]
    fn parse_playlist_chapter_within_offset_divides_ticks_to_seconds() {
        let mut disc = MemDisc::new();
        let udf = make_bdmv_fs(&mut disc, &[("00001", 100, 400, 5000)]);
        let pi1_in = 10 * 45000u32;
        let within_ticks = 5 * 45000u32; // 5s into PI1
        let mpls = build_mpls(
            &[
                PiSpec {
                    clip_id: *b"00001",
                    in_time: 0,
                    out_time: 60 * 45000, // PI0 lasts 60s
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
                timestamp: pi1_in + within_ticks,
            }],
        );
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
        assert_eq!(t.chapters.len(), 1);
        assert!(
            (t.chapters[0].time_secs - 65.0).abs() < 1e-6,
            "chapter time must be preceding(60s) + within(5s) = 65s, got {}",
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00001.mpls", &mpls)
            .expect("scan")
            .expect("title");
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
        let t = Disc::parse_playlist(&mut disc, &udf, "00800.MPLS", &mpls)
            .expect("scan")
            .expect("title");
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
        let t = Disc::parse_playlist(&mut disc, &udf, "MENU.mpls", &mpls)
            .expect("scan")
            .expect("title");
        assert_eq!(t.playlist_id, 0);
    }

    /// A filename that is long enough (>= 5 bytes) but does NOT end in
    /// ".mpls" must NOT have its last 5 bytes stripped -- the whole string
    /// is handed to the numeric parse instead, which fails and falls back
    /// to playlist_id 0 (bluray.rs `filename.len() >= 5 &&
    /// filename[len-5..].eq_ignore_ascii_case(".mpls")`).
    #[test]
    fn parse_playlist_id_falls_back_to_zero_when_suffix_is_not_mpls() {
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
        // "00800zzzzz": stripping the last 5 bytes would leave "00800" (a
        // valid u16), but the suffix isn't ".mpls" so nothing may be
        // stripped -- the whole (non-numeric) string must fail to parse.
        let t = Disc::parse_playlist(&mut disc, &udf, "00800zzzzz", &mpls)
            .expect("scan")
            .expect("title");
        assert_eq!(
            t.playlist_id, 0,
            "a filename not ending in .mpls must not have its last 5 bytes stripped"
        );
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

        let titles = Disc::scan_bluray_titles(&mut disc, &udf, None).expect("scan");
        assert_eq!(titles.len(), 1, "only the 2h playlist should survive");
        assert_eq!(titles[0].playlist_id, 800);
    }

    /// A non-directory PLAYLIST entry whose name does NOT end in ".mpls"
    /// must be skipped even though its content parses as a perfectly good
    /// (long) MPLS playlist -- extension gating, not content sniffing,
    /// decides eligibility (bluray.rs `!entry.is_dir &&
    /// entry.name...ends_with(".mpls")`).
    #[test]
    fn scan_bluray_titles_skips_non_mpls_extension_file() {
        let mut disc = MemDisc::new();
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 7200 * 45000, // 2h -- easily long enough to be kept
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        let playlist = DirSpec {
            name: "PLAYLIST".to_string(),
            icb_lba: 26,
            dir_data_lba: 27,
            files: vec![file_with("00800.dat", 104, 30000, mpls, false)],
            subdirs: vec![],
        };
        let bdmv = DirSpec {
            name: "BDMV".to_string(),
            icb_lba: 20,
            dir_data_lba: 21,
            files: Vec::new(),
            subdirs: vec![playlist],
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

        let titles = Disc::scan_bluray_titles(&mut disc, &udf, None).expect("scan");
        assert!(
            titles.is_empty(),
            "a PLAYLIST entry not ending in .mpls must be skipped regardless of content"
        );
    }

    /// With no PLAYLIST directory, scan_bluray_titles returns an empty
    /// vec (the `find_dir` is None) — never panics.
    #[test]
    fn scan_bluray_titles_no_playlist_dir_is_empty() {
        let mut disc = MemDisc::new();
        let udf = make_min_fs(&mut disc); // BDMV exists, no PLAYLIST
        assert!(
            Disc::scan_bluray_titles(&mut disc, &udf, None)
                .expect("scan")
                .is_empty()
        );
    }

    /// A `SectorSource` that fails every read in `halt_range` with
    /// [`Error::Halted`] — exactly how a LIVE DRIVE behaves once the operator
    /// presses Stop: `Drive::checked_exec` fails every SCSI command with
    /// `Halted` from then on, and `Drive::read` deliberately preserves the
    /// variant. Reads outside the range still succeed, so a test can aim the
    /// cancel at ONE structure and leave the scan far enough along to have
    /// something to truncate.
    struct HaltingReader<'a> {
        inner: &'a mut MemDisc,
        halt_range: std::ops::Range<u32>,
    }
    impl SectorSource for HaltingReader<'_> {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> Result<usize> {
            if self.halt_range.contains(&lba) {
                return Err(Error::Halted);
            }
            self.inner.read_sectors(lba, count, buf, recovery)
        }
    }

    /// Lay a BDMV holding TWO 2-hour playlists (00800.mpls at data LBA 30000,
    /// 00801.mpls at 40000) over one fully wired clip. Both playlists are
    /// keepable, so a scan that returns fewer than two titles has LOST one.
    fn two_playlist_bd_fs(disc: &mut MemDisc) -> udf::UdfFs {
        let long_mpls = || {
            build_mpls(
                &[PiSpec {
                    clip_id: *b"00001",
                    in_time: 0,
                    out_time: 7200 * 45000, // 2 h
                }],
                (0, 0, 0, 0, 0, 0, 0, 0),
                &[],
                &[],
            )
        };
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
                file_with("00800.mpls", 104, 30000, long_mpls(), false),
                file_with("00801.mpls", 110, 40000, long_mpls(), false),
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
        build_udf_skeleton(disc, 10);
        lay_dir(disc, &root);
        udf::read_filesystem(disc).expect("fs")
    }

    /// A Stop on a LIVE DRIVE never touches `ScanOptions::halt`: `Drive` has
    /// its own flag and `checked_exec` fails every SCSI command with
    /// [`Error::Halted`] once it is set. The Blu-ray enumerator must not
    /// swallow that into a successful scan.
    ///
    /// RED BEFORE GREEN: with the propagation reverted this returned
    /// `Ok([00800])` — the `if let Ok(mpls_data)` skipped the cancelled read
    /// of 00801.mpls, the loop ended, and a HALF-ENUMERATED disc came back at
    /// success. One title from a two-title disc is indistinguishable from a
    /// disc that genuinely holds one title, and the caller caches and rips
    /// from it.
    ///
    /// The halt lands on the SECOND playlist deliberately: it is the last
    /// iteration, so nothing after it would poll a flag — only propagating
    /// the read's own error catches it.
    #[test]
    fn halted_playlist_read_is_not_reported_as_a_shorter_disc() {
        let mut disc = MemDisc::new();
        let udf = two_playlist_bd_fs(&mut disc);
        // Sanity: both playlists enumerate when nothing is cancelled, so a
        // truncated result below can only be the cancel.
        assert_eq!(
            Disc::scan_bluray_titles(&mut disc, &udf, None)
                .expect("scan")
                .len(),
            2,
            "fixture must offer two keepable playlists"
        );
        let mut reader = HaltingReader {
            inner: &mut disc,
            halt_range: PART_START + 40000..u32::MAX, // 00801.mpls's data extent
        };
        let res = Disc::scan_bluray_titles(&mut reader, &udf, None);
        assert!(
            matches!(res, Err(Error::Halted)),
            "a read cancelled by the drive's own halt flag must surface as a \
             cancelled scan, not as a shorter title list; got {:?}",
            res.map(|ts| ts.iter().map(|t| t.playlist.clone()).collect::<Vec<_>>())
        );
    }

    /// The same cancel landing on a `.clpi` read must not be classified as an
    /// unresolvable clip either.
    ///
    /// RED BEFORE GREEN: with the `Err(Error::Halted)` arm removed from the
    /// CLIPINF match, the cancel fell into the generic "clip could not be
    /// resolved" arm, which logs a disc-defect code and drops the title —
    /// accounting an operator Stop as a scratched disc, and (in the scan loop)
    /// dropping every remaining playlist in turn for a truncated `Ok`. With
    /// the fix the cancel is propagated with its own variant intact.
    #[test]
    fn halted_clpi_read_is_not_accounted_as_an_unresolvable_clip() {
        let mut disc = MemDisc::new();
        let udf = two_playlist_bd_fs(&mut disc);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 7200 * 45000,
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        // Only the CLIPINF data extent is cancelled; the MPLS bytes are
        // already in hand and every other structure reads normally.
        let mut reader = HaltingReader {
            inner: &mut disc,
            halt_range: PART_START + 8000..PART_START + 8001,
        };
        let res = Disc::parse_playlist(&mut reader, &udf, "00800.mpls", &mpls);
        assert!(
            matches!(res, Err(Error::Halted)),
            "a cancelled .clpi read must propagate, not drop the title as a \
             disc defect; got {:?}",
            res.map(|t| t.map(|t| (t.size_bytes, t.extents)))
        );
    }

    /// And the same cancel landing on `file_extents` — the clip's ICB, not its
    /// CLIPINF — must propagate too.
    ///
    /// RED BEFORE GREEN: `note` used to EXEMPT `Halted` from the unresolved
    /// classification (correctly — a cancel is not an authoring hole) but had
    /// no way to propagate it, so the resolver simply produced no extents and
    /// `parse_playlist` returned a title with the clip's runtime counted, its
    /// `size_bytes` counted from the .clpi, and ZERO bytes behind it. Measured
    /// with the propagation reverted: `Ok(Some((768000, [])))` — the flagship
    /// defect shape, wearing a cancel.
    #[test]
    fn halted_extent_resolve_is_not_a_title_missing_its_clip() {
        let mut disc = MemDisc::new();
        let udf = two_playlist_bd_fs(&mut disc);
        let mpls = build_mpls(
            &[PiSpec {
                clip_id: *b"00001",
                in_time: 0,
                out_time: 7200 * 45000,
            }],
            (0, 0, 0, 0, 0, 0, 0, 0),
            &[],
            &[],
        );
        // ONE sector is cancelled: the .m2ts ICB (metadata LBA 100), which is
        // precisely and only where `file_extents` looks. The .clpi (ICB 102,
        // data 8000) still reads, so the clip is sized and the earlier
        // CLIPINF arm is not the thing under test here.
        let mut reader = HaltingReader {
            inner: &mut disc,
            halt_range: PART_START + 100..PART_START + 101,
        };
        let res = Disc::parse_playlist(&mut reader, &udf, "00800.mpls", &mpls);
        assert!(
            matches!(res, Err(Error::Halted)),
            "a cancelled extent resolve must propagate, not yield a title \
             claiming its full runtime with none of the clip's bytes; got {:?}",
            res.map(|t| t.map(|t| (t.size_bytes, t.extents)))
        );
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

    /// A non-.xml file must be ignored even if its content looks like a
    /// valid meta XML (contains a `<di:name>`) -- extension gating, not
    /// content sniffing, decides eligibility (bluray.rs `!e.is_dir &&
    /// e.name...ends_with(".xml")`).
    #[test]
    fn read_meta_title_ignores_non_xml_file_regardless_of_content() {
        let mut disc = MemDisc::new();
        let bogus = b"<x><di:name>Should Not Be Used</di:name></x>".to_vec();
        let dl = DirSpec {
            name: "DL".to_string(),
            icb_lba: 30,
            dir_data_lba: 31,
            files: vec![file_with("bdmt_eng.txt", 104, 50000, bogus, false)],
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
            "a non-.xml file must be ignored even if its content looks like valid meta XML"
        );
    }

    /// No META directory → None.
    #[test]
    fn read_meta_title_no_meta_dir_is_none() {
        let mut disc = MemDisc::new();
        let udf = make_min_fs(&mut disc);
        assert_eq!(Disc::read_meta_title(&mut disc, &udf), None);
    }
}
