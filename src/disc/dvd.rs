//! DVD title scanning — IFO parsing, stream mapping, VOB extent building.

use super::*;
use crate::ifo;
use crate::sector::SectorSource;
use crate::udf;

// Reads and parses a VMG copy (VIDEO_TS.IFO or .BUP), returning its bytes for dvdnav reuse.
fn load_vmg(
    reader: &mut dyn SectorSource,
    udf_fs: &udf::UdfFs,
    path: &str,
) -> Result<(Vec<u8>, ifo::DvdInfo)> {
    let bytes = udf_fs.read_file(reader, path)?;
    let info = ifo::parse_vmg_with(reader, udf_fs, Some(&bytes))?;
    Ok((bytes, info))
}

// Navigation pack layout: the DSI packet starts at byte 0x400 of the pack, its data after the
// 6-byte PES header and the 0x01 substream byte; SML_PBI follows the 32-byte DSI_GI.
const DSI_DATA: usize = 0x407;
const DSI_ILVU_EA: usize = DSI_DATA + 32 + 2;
const DSI_NEXT_ILVU_SA: usize = DSI_DATA + 32 + 6;
// SML_PBI's "no next unit": 0xFFFFFFFF by the book, and discs also write 0x7FFFFFFF.
const NO_NEXT_ILVU: u32 = 0x7FFF_FFFF;

// An interleaved cell's own units, following each unit's nav pack, clipped to the cell (at most
// one read per cell sector, `halt` polled before each). `Ok(None)` when a pack does not read
// whole or parse, which leaves the caller the cell's range; a Stop is `Err(Halted)`.
fn interleaved_units(
    reader: &mut dyn SectorSource,
    vob_start: u32,
    cell: &ifo::DvdCell,
    halt: Option<&crate::halt::Halt>,
) -> Result<Option<Vec<Extent>>> {
    let mut buf = vec![0u8; crate::consts::SECTOR_BYTES];
    let mut at = cell.first_sector;
    let mut units = Vec::new();
    while at <= cell.last_sector {
        if halt.is_some_and(|h| h.is_cancelled()) {
            return Err(Error::Halted);
        }
        let Some(lba) = vob_start.checked_add(at) else {
            return Ok(None);
        };
        match reader.read_sectors(lba, 1, &mut buf, false) {
            Ok(n) if n >= buf.len() => {}
            Err(Error::Halted) => return Err(Error::Halted),
            Ok(_) | Err(_) => return Ok(None),
        }
        let is_nav = buf[..4] == [0, 0, 1, 0xBA]
            && buf[0x400..0x404] == [0, 0, 1, 0xBF]
            && buf[0x406] == 0x01;
        if !is_nav {
            return Ok(None);
        }
        let word = |o: usize| u32::from_be_bytes([buf[o], buf[o + 1], buf[o + 2], buf[o + 3]]);
        let (end_rel, next_rel) = (word(DSI_ILVU_EA), word(DSI_NEXT_ILVU_SA));
        let end = at.saturating_add(end_rel).min(cell.last_sector);
        let Some(sector_count) = (end - at).checked_add(1).filter(|_| end_rel != 0) else {
            return Ok(None);
        };
        units.push(Extent {
            start_lba: lba,
            sector_count,
        });
        if next_rel >= NO_NEXT_ILVU || next_rel == 0 {
            break;
        }
        let Some(next) = at.checked_add(next_rel) else {
            break;
        };
        at = next;
    }
    Ok((!units.is_empty()).then_some(units))
}

// Chapter (part of title) names moved onto the `programs` program marks: each part names the
// program it starts at. `None` when that is not one name per program at most (a part in
// another PGC, two parts on one program, a part past the marks).
fn program_names(
    names: &[Option<String>],
    title: &ifo::DvdTitle,
    programs: usize,
) -> Option<Vec<Option<String>>> {
    let Some(parts) = title.ptt_programs.as_deref() else {
        // No part table: parts are taken as programs 1, 2, ...
        return Some(names.to_vec());
    };
    if parts.len() != names.len() {
        return None;
    }
    let mut out: Vec<Option<Option<String>>> = vec![None; programs];
    for (&pgn, name) in parts.iter().zip(names) {
        let slot = out.get_mut(usize::from(pgn).checked_sub(1)?)?;
        if slot.replace(name.clone()).is_some() {
            return None;
        }
    }
    Some(out.into_iter().map(Option::flatten).collect())
}

impl Disc {
    // Scan DVD titles; a cancelled read is Err(Error::Halted), never an empty list. Also
    // returns the nav-resolved main feature (or None) and the VMG's region coding.
    pub(super) fn scan_dvd_titles(
        reader: &mut dyn SectorSource,
        udf_fs: &udf::UdfFs,
        halt: Option<&crate::halt::Halt>,
    ) -> Result<(Vec<DiscTitle>, Option<u16>, DiscRegion)> {
        if halt.is_some_and(|h| h.is_cancelled()) {
            return Err(Error::Halted);
        }
        // Read VIDEO_TS.IFO once and reuse for both the VMG parse and the nav
        // resolver below; otherwise the VTS reads in parse_vmg evict the single
        // sector-cache window, forcing a duplicate physical read of the same file.
        let (vmg_bytes, dvd_info) = match load_vmg(reader, udf_fs, "/VIDEO_TS/VIDEO_TS.IFO") {
            Ok(v) => v,
            Err(Error::Halted) => return Err(Error::Halted),
            Err(ifo_err) => match load_vmg(reader, udf_fs, "/VIDEO_TS/VIDEO_TS.BUP") {
                Ok(v) => {
                    tracing::warn!(target: "freemkv::scan", code = ifo_err.code(), "dvd: VIDEO_TS.IFO bad; using BUP");
                    v
                }
                Err(Error::Halted) => return Err(Error::Halted),
                Err(bup_err) => {
                    // Neither copy usable: nothing to enumerate (a missing pair is not a fault).
                    let missing = |e: &Error| matches!(e, Error::UdfNotFound { .. });
                    if !(missing(&ifo_err) && missing(&bup_err)) {
                        tracing::warn!(target: "freemkv::scan", ifo = ifo_err.code(), bup = bup_err.code(), "dvd: VIDEO_TS.IFO and BUP unusable");
                    }
                    return Ok((Vec::new(), None, DiscRegion::Unknown));
                }
            },
        };

        // Follow the disc's First-Play navigation like a player would, to find the
        // nav-chosen feature title (issue #40). Read-only; any failure/non-convergence
        // yields `None`, so this only re-ranks, never removes, a title.
        let nav_target = crate::dvdnav::resolve_main_title(reader, udf_fs, Some(&vmg_bytes));
        let mut nav_feature: Option<u16> = None;

        let mut titles = Vec::new();
        let mut title_addresses = Vec::new();
        let mut title_number: u16 = 0;

        for ts in &dvd_info.title_sets {
            // Diagnostic dump (--log-level 3): IFO video/audio attrs for this
            // title set. No-op unless the freemkv::diag target is enabled.
            crate::diag::dump_dvd_attrs(ts);

            let video_stream = Stream::Video(VideoStream {
                pid: 0xE0, // DVD video PID (standard MPEG PS video stream)
                codec: ts.video.codec,
                resolution: ts.video.resolution,
                frame_rate: match ts.video.standard {
                    crate::ifo::TvSystem::Pal => FrameRate::F25,
                    crate::ifo::TvSystem::Ntsc => FrameRate::F29_97,
                },
                hdr: HdrFormat::Sdr,
                // DVD is SD, not HD: PAL is BT.470BG, NTSC is SMPTE-170M.
                // Stamping BT.709 (HD) mis-tags the colour primaries/transfer.
                color_space: match ts.video.standard {
                    crate::ifo::TvSystem::Pal => ColorSpace::Bt470bg,
                    crate::ifo::TvSystem::Ntsc => ColorSpace::Smpte170m,
                },
                // DVD pixels are anamorphic 720x480/576; real display shape comes from the
                // IFO aspect flag, not the pixel grid. Carry it so the MKV muxer emits correct
                // 16:9/4:3 DisplayWidth/Height instead of the wrong square-pixel 3:2/5:4.
                display_aspect: Some(match ts.video.aspect {
                    crate::ifo::DvdAspect::R16x9 => (16, 9),
                    crate::ifo::DvdAspect::R4x3 => (4, 3),
                }),
                secondary: false,
                label: String::new(),
                // TODO(spec): measured colour/TFF await a CodecParser->title channel.
                measured_cicp: None,
            });
            // The frame rate is the standard's; an MKV corrects it from the measured cadence.

            for (vts_title_idx, dvd_title) in ts.titles.iter().enumerate() {
                title_number += 1;

                // Match the nav target by VTS number + the REAL vts_title_num, not
                // by position (a dropped sibling PGC or non-monotonic TT_SRPT
                // desyncs position from vts_ttn); remember its playlist for ranking.
                if let Some(rt) = nav_target
                    && rt.vtsn == ts.vts_number
                    && rt.vts_ttn == dvd_title.vts_title_num
                {
                    nav_feature = Some(title_number);
                }

                // Diagnostic dump (--log-level 3): per-cell category table +
                // chapter map for this title, BEFORE lowering drops the
                // per-cell IFO detail. No-op unless freemkv::diag is enabled.
                crate::diag::dump_dvd_cells(ts.vts_number, title_number, dvd_title);

                // Feature-start nav resolution is PARKED (#40): the "menu at start" symptom
                // was actually a sector-mapping fault in `ifo::parse_vts`'s VOB rebase, not
                // navigation. Bypassed via `USE_NAV_RESOLVER = false`; fallback is the filter.
                const USE_NAV_RESOLVER: bool = false;
                let feature_start = if USE_NAV_RESOLVER {
                    crate::dvdnav::resolve_feature_start(
                        reader,
                        udf_fs,
                        ts.vts_number as u16,
                        (vts_title_idx + 1) as u16,
                    )
                    .unwrap_or_else(|| dvd_title.feature_start_cell())
                } else {
                    dvd_title.feature_start_cell()
                }
                .min(dvd_title.cells.len());
                let dropped_secs: f64 = dvd_title.cells[..feature_start]
                    .iter()
                    .map(|c| c.duration_secs)
                    .sum();
                if feature_start > 0 {
                    tracing::debug!(
                        target: "freemkv::scan",
                        vts = ts.vts_number,
                        title = title_number,
                        dropped_cells = feature_start,
                        dropped_secs,
                        "dvd: dropped leading non-feature cell(s)"
                    );
                }

                // Extents are cell ranges (vob_start + offset) from the feature-start cell. An angle
                // block reads its first angle only; an interleaved cell reads its own unit chain,
                // since its range also holds another program's units.
                let mut extents: Vec<Extent> = Vec::new();
                for cell in &dvd_title.cells[feature_start..] {
                    let category = ifo::CellCategory::decode(cell.category);
                    if category.is_secondary_block_piece() {
                        continue;
                    }
                    let whole = Extent {
                        start_lba: ts.vob_start_sector.saturating_add(cell.first_sector),
                        sector_count: cell
                            .last_sector
                            .saturating_sub(cell.first_sector)
                            .saturating_add(1),
                    };
                    if !category.interleaved {
                        extents.push(whole);
                        continue;
                    }
                    match interleaved_units(reader, ts.vob_start_sector, cell, halt)? {
                        Some(units) => extents.extend(units),
                        None => {
                            tracing::warn!(
                                target: "freemkv::scan",
                                vts = ts.vts_number,
                                title = title_number,
                                first = cell.first_sector,
                                "dvd: interleaved cell's unit chain unreadable; reading its whole range"
                            );
                            extents.push(whole);
                        }
                    }
                }

                let size_bytes: u64 = extents.iter().map(|e| e.sector_count as u64 * 2048).sum();

                // Build pre-formatted VobSub `.idx` codec_data. The `size:` line carries the
                // coded video frame (720x480 NTSC / 720x576 PAL) so players place/scale the
                // subpicture correctly; format_palette omits it on unresolved (0,0).
                let (vid_w, vid_h) = ts.video.resolution.pixels().unwrap_or((0, 0));
                let codec_data = dvd_title
                    .palette
                    .as_ref()
                    .map(|pal| crate::mux::codec::dvdsub::format_palette(pal, vid_w, vid_h));

                // Logical stream i routes to the physical id this PGC's SPST_CTL names (not
                // 0x20+i: anamorphic discs interleave wide/letterbox/pan-scan variants).
                // Absent streams get no track; a repeated physical id keeps the first.
                let mut subtitle_streams: Vec<Stream> = Vec::new();
                let mut kept_langs: [Option<&str>; 32] = [None; 32];
                for (s, ctl) in ts.subtitle_streams.iter().zip(dvd_title.spst_ctl) {
                    let Some(sub_id) = crate::ifo::subpicture_stream_id(ctl, ts.video.aspect)
                    else {
                        continue;
                    };
                    let slot = &mut kept_langs[(sub_id & 0x1F) as usize];
                    if let Some(kept) = slot {
                        crate::diag::dvd_ctl_duplicate(
                            "spst",
                            ts.vts_number,
                            title_number,
                            sub_id.into(),
                            kept,
                            &s.language,
                        );
                        continue;
                    }
                    *slot = Some(&s.language);
                    let (forced, qualifier) = subpicture_label(s.code_extension);
                    let pid = crate::mux::ps::dvd_subtitle_pid(sub_id).unwrap_or(sub_id as u16);
                    subtitle_streams.push(Stream::Subtitle(SubtitleStream {
                        pid,
                        codec: Codec::DvdSub,
                        language: s.language.clone(),
                        forced,
                        qualifier,
                        codec_data: codec_data.clone(),
                    }));
                }
                if subtitle_streams.is_empty() && !ts.subtitle_streams.is_empty() {
                    crate::diag::dvd_ctl_none_present(
                        "spst",
                        ts.vts_number,
                        title_number,
                        ts.subtitle_streams.len(),
                        "no subtitle tracks",
                    );
                }

                let mut streams = vec![video_stream.clone()];
                streams.extend(title_audio_streams(ts, dvd_title, title_number));
                streams.extend(subtitle_streams);

                // The dropped head is angle pieces, which chapter times already leave out (an
                // angle block plays one angle); marks inside it collapse to one at 0.0.
                let shifted: Vec<f64> = dvd_title
                    .chapter_times
                    .iter()
                    .map(|&t| t.max(0.0))
                    .collect();
                let in_head = shifted.iter().filter(|&&t| t <= 0.0).count();
                let skipped = in_head.saturating_sub(1);
                // Disc text names each chapter (part of title); a mark is a program, indexed
                // from before the head was dropped.
                let names = dvd_info
                    .chapter_names
                    .get(&(ts.vts_number, dvd_title.vts_title_num))
                    .and_then(|n| program_names(n, dvd_title, shifted.len()));
                let chapters: Vec<Chapter> = shifted
                    .into_iter()
                    .skip(skipped)
                    .enumerate()
                    .map(|(i, time_secs)| Chapter {
                        time_secs,
                        name: names
                            .as_ref()
                            .and_then(|n| n.get(skipped + i).cloned().flatten())
                            .unwrap_or_else(|| chapter_name(i)),
                    })
                    .collect();

                title_addresses.push((ts.vts_number, dvd_title.vts_title_num));
                titles.push(DiscTitle {
                    selection_evidence: Default::default(),
                    playlist: format!("VTS_{:02}_{}.VOB", ts.vts_number, title_number),
                    playlist_id: title_number,
                    // The dropped head is angle pieces, which the title time already leaves out.
                    duration_secs: dvd_title.duration_secs,
                    size_bytes,
                    clips: Vec::new(),
                    streams,
                    chapters,
                    extents,
                    content_format: ContentFormat::DvdPs,
                    codec_privates: Vec::new(),
                });
            }
        }

        // Polled again AFTER the loop: a cancel raised during per-title-set IFO reads in
        // `parse_vmg` has nothing left to poll otherwise, so without this a partially
        // enumerated disc could still be handed back as success.
        if halt.is_some_and(|h| h.is_cancelled()) {
            return Err(Error::Halted);
        }
        warn_multi_pgc_titles(&vmg_bytes, &dvd_info);
        super::dvd_menu::annotate(
            reader,
            udf_fs,
            &vmg_bytes,
            &title_addresses,
            &mut titles,
            halt,
        )?;
        Ok((titles, nav_feature, dvd_region(dvd_info.region_mask)))
    }
}

/// The region a VMG region mask (VMGI_MAT 0x23) allows: bit n set means region n+1 is
/// prohibited, so the playable regions are the clear bits. 0x00 is region-free; 0xFF
/// leaves no region at all, an empty [`DiscRegion::Dvd`].
pub(super) fn dvd_region(mask: u8) -> DiscRegion {
    if mask == 0 {
        return DiscRegion::Free;
    }
    DiscRegion::Dvd(
        (0..8u8)
            .filter(|n| mask & (1 << n) == 0)
            .map(|n| n + 1)
            .collect(),
    )
}

// An audio stream's purpose from its IFO code extension (libdvdread dvd_audio_code_ext_t):
// 2 visually impaired is descriptive audio, 3 and 4 are director's comments.
fn audio_purpose(code_extension: u8) -> LabelPurpose {
    match code_extension {
        2 => LabelPurpose::Descriptive,
        3 | 4 => LabelPurpose::Commentary,
        _ => LabelPurpose::Normal,
    }
}

/// A subpicture stream's `(forced, qualifier)` from its IFO code extension (libdvdread
/// dvd_subp_code_ext_t). Captions (5-7, any size) are the closed captions BD calls SDH;
/// 9 is forced. Large (2), children (3) and director's comments (13-15) have no slot in
/// the subtitle label model and stay unlabelled.
fn subpicture_label(code_extension: u8) -> (bool, LabelQualifier) {
    match code_extension {
        5..=7 => (false, LabelQualifier::Sdh),
        9 => (true, LabelQualifier::Forced),
        _ => (false, LabelQualifier::None),
    }
}

// One warning per disc per process for titles split across PGCs (only the first is read):
// a rip scans its disc more than once, and each scan would repeat it. Later scans log at
// debug. The disc is told apart by its VIDEO_TS.IFO.
fn warn_multi_pgc_titles(vmg: &[u8], info: &ifo::DvdInfo) {
    use std::hash::{Hash, Hasher};
    static WARNED: std::sync::Mutex<Vec<u64>> = std::sync::Mutex::new(Vec::new());
    let split: Vec<String> = info
        .title_sets
        .iter()
        .flat_map(|ts| {
            ts.titles.iter().filter(|t| t.pgcs > 1).map(move |t| {
                format!(
                    "VTS {} title {} ({} PGCs)",
                    ts.vts_number, t.vts_title_num, t.pgcs
                )
            })
        })
        .collect();
    if split.is_empty() {
        return;
    }
    let mut h = std::hash::DefaultHasher::new();
    vmg.hash(&mut h);
    let key = h.finish();
    let first = {
        let mut warned = WARNED.lock().unwrap_or_else(|e| e.into_inner());
        let first = !warned.contains(&key);
        if first {
            warned.push(key);
        }
        first
    };
    let titles = split.join(", ");
    if first {
        tracing::warn!(target: "freemkv::scan", titles = %titles, "titles span several PGCs; only the first PGC of each is read, the rest of those titles is missing");
    } else {
        tracing::debug!(target: "freemkv::scan", titles = %titles, "titles span several PGCs; only the first PGC of each is read");
    }
}

// This title's audio tracks: logical stream i plays the physical stream its PGC_AST_CTL
// names (libdvdnav semantics), absent streams get no track, a repeated physical id keeps
// the first. A PGC marking nothing present keeps the positional ids rather than go silent.
fn title_audio_streams(ts: &ifo::DvdTitleSet, t: &ifo::DvdTitle, title: u16) -> Vec<Stream> {
    let declared = &ts.audio_streams;
    let any_present = declared
        .iter()
        .zip(t.ast_ctl)
        .any(|(_, c)| ifo::audio_stream_number(c).is_some());
    if !any_present && !declared.is_empty() {
        let outcome = "positional routing (codec base | position)";
        crate::diag::dvd_ctl_none_present("ast", ts.vts_number, title, declared.len(), outcome);
    }
    let mut kept: Vec<(u16, &str)> = Vec::new();
    let mut out = Vec::new();
    for (i, (a, ctl)) in declared.iter().zip(t.ast_ctl).enumerate() {
        let n = if any_present {
            match ifo::audio_stream_number(ctl) {
                Some(n) => n,
                None => {
                    crate::diag::dvd_audio_route(ts.vts_number, title, i, a, ctl, None);
                    continue;
                }
            }
        } else {
            i as u8
        };
        // No route (an unknown coding mode): a unique placeholder PID nothing feeds.
        let pid = ifo::audio_pid(a.codec, n).unwrap_or(0xBD00 + i as u16);
        crate::diag::dvd_audio_route(ts.vts_number, title, i, a, ctl, Some(pid));
        if let Some((_, first)) = kept.iter().find(|(p, _)| *p == pid) {
            crate::diag::dvd_ctl_duplicate("ast", ts.vts_number, title, pid, first, &a.language);
            continue;
        }
        kept.push((pid, &a.language));
        out.push(Stream::Audio(AudioStream {
            pid,
            codec: a.codec,
            channels: AudioChannels::from_count(a.channels),
            language: a.language.clone(),
            sample_rate: SampleRate::from_hz(a.sample_rate),
            secondary: false,
            purpose: audio_purpose(a.code_extension),
            label: String::new(),
        }));
        // Coding mode 3, "MPEG-2 with extension bitstream" (EP0867877A2): its extension stream
        // `0xD0|n` becomes a dependent track right after the base (pairing inferred, see
        // ps::dvd_mpeg_audio_extension_pid).
        if a.mpeg_ext && a.codec == Codec::Mp2 {
            let Some(ext_pid) = crate::mux::ps::dvd_mpeg_audio_extension_pid(0xD0 | n) else {
                continue;
            };
            out.push(Stream::Audio(AudioStream {
                pid: ext_pid,
                codec: Codec::Mp2,
                channels: AudioChannels::Unknown,
                language: a.language.clone(),
                sample_rate: SampleRate::from_hz(a.sample_rate),
                secondary: false,
                purpose: audio_purpose(a.code_extension),
                label: crate::disc::MP2_EXTENSION_LABEL.to_string(),
            }));
        }
    }
    out
}

#[cfg(test)]
#[path = "dvd_tests.rs"]
mod tests;
