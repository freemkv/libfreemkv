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

// The sector ranges an interleaved cell plays: its own interleaved units, found by following
// each unit's navigation pack. The chain runs per VOB, past the cell, so it is clipped to the
// cell. `None` when a pack does not read or parse, which leaves the cell's range to the caller.
fn interleaved_units(
    reader: &mut dyn SectorSource,
    vob_start: u32,
    cell: &ifo::DvdCell,
) -> Option<Vec<Extent>> {
    let mut buf = vec![0u8; 2048];
    let mut at = cell.first_sector;
    let mut units = Vec::new();
    while at <= cell.last_sector {
        reader
            .read_sectors(vob_start.checked_add(at)?, 1, &mut buf, false)
            .ok()?;
        let is_nav = buf[..4] == [0, 0, 1, 0xBA]
            && buf[0x400..0x404] == [0, 0, 1, 0xBF]
            && buf[0x406] == 0x01;
        if !is_nav {
            return None;
        }
        let word = |o: usize| u32::from_be_bytes([buf[o], buf[o + 1], buf[o + 2], buf[o + 3]]);
        let (end_rel, next_rel) = (word(DSI_ILVU_EA), word(DSI_NEXT_ILVU_SA));
        if end_rel == 0 {
            return None;
        }
        let end = at.checked_add(end_rel)?.min(cell.last_sector);
        units.push(Extent {
            start_lba: vob_start.checked_add(at)?,
            sector_count: end - at + 1,
        });
        if next_rel >= NO_NEXT_ILVU || next_rel == 0 {
            break;
        }
        at = at.checked_add(next_rel)?;
    }
    (!units.is_empty()).then_some(units)
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
                    if halt.is_some_and(|h| h.is_cancelled()) {
                        return Err(Error::Halted);
                    }
                    match interleaved_units(reader, ts.vob_start_sector, cell) {
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
                // Disc text names index the chapter before the head was dropped.
                let names = dvd_info
                    .chapter_names
                    .get(&(ts.vts_number, dvd_title.vts_title_num));
                let chapters: Vec<Chapter> = shifted
                    .into_iter()
                    .skip(skipped)
                    .enumerate()
                    .map(|(i, time_secs)| Chapter {
                        time_secs,
                        name: names
                            .and_then(|n| n.get(skipped + i).cloned().flatten())
                            .unwrap_or_else(|| chapter_name(i)),
                    })
                    .collect();

                titles.push(DiscTitle {
                    playlist: format!("VTS_{:02}_{}.VOB", ts.vts_number, title_number),
                    playlist_id: title_number,
                    duration_secs: (dvd_title.duration_secs - dropped_secs).max(0.0),
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
mod tests {
    use super::*;
    use crate::sector::SectorSource;
    use std::collections::HashMap;

    // In-memory disc + minimal UDF image (single physical partition,
    // metadata_start == partition_start). Offsets cited against
    // udf.rs::read_filesystem / ECMA-167.

    const PART_START: u32 = 3000;

    struct MemDisc {
        sectors: HashMap<u32, [u8; 2048]>,
    }
    impl MemDisc {
        fn new() -> Self {
            Self {
                sectors: HashMap::new(),
            }
        }
        fn put(&mut self, lba: u32, data: [u8; 2048]) {
            self.sectors.insert(lba, data);
        }
        fn put_bytes(&mut self, lba: u32, bytes: &[u8]) {
            for (i, chunk) in bytes.chunks(2048).enumerate() {
                let mut s = [0u8; 2048];
                s[..chunk.len()].copy_from_slice(chunk);
                self.put(lba + i as u32, s);
            }
        }
    }
    impl SectorSource for MemDisc {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            let need = count as usize * 2048;
            for i in 0..count as u32 {
                let off = i as usize * 2048;
                let s = self.sectors.get(&(lba + i)).copied().unwrap_or([0u8; 2048]);
                buf[off..off + 2048].copy_from_slice(&s);
            }
            Ok(need)
        }
    }

    /// Extended File Entry ICB (tag 266) with one Short AD. info_length@56,
    /// l_ea@208, l_ad@212, AD len(4)@216 | lba(4)@220.
    fn build_file_icb(size: u32, data_lba: u32) -> [u8; 2048] {
        let mut s = [0u8; 2048];
        s[0..2].copy_from_slice(&266u16.to_le_bytes());
        s[56..64].copy_from_slice(&(size as u64).to_le_bytes());
        s[208..212].copy_from_slice(&0u32.to_le_bytes());
        s[212..216].copy_from_slice(&8u32.to_le_bytes());
        s[216..220].copy_from_slice(&(size & 0x3FFF_FFFF).to_le_bytes());
        s[220..224].copy_from_slice(&data_lba.to_le_bytes());
        s
    }

    /// One FID (tag 257). file_chars@18, l_fi@19, ICB LBA@24, l_iu@36,
    /// name@(38). Name compression-id 8 (ASCII).
    fn push_fid(buf: &mut Vec<u8>, name: &str, icb_lba: u32, is_dir: bool, is_parent: bool) {
        let start = buf.len();
        let name_field: Vec<u8> = if is_parent {
            Vec::new()
        } else {
            let mut v = vec![0x08u8];
            v.extend_from_slice(name.as_bytes());
            v
        };
        let mut fid = vec![0u8; 38];
        fid[0..2].copy_from_slice(&257u16.to_le_bytes());
        let mut fc = 0u8;
        if is_dir {
            fc |= 0x02;
        }
        if is_parent {
            fc |= 0x08;
        }
        fid[18] = fc;
        fid[19] = name_field.len() as u8;
        fid[24..28].copy_from_slice(&icb_lba.to_le_bytes());
        fid[36..38].copy_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&fid);
        buf.extend_from_slice(&name_field);
        let used = buf.len() - start;
        buf.resize(start + ((used + 3) & !3), 0);
    }

    struct FileSpec {
        name: String,
        icb_lba: u32,
        data_lba: u32,
        contents: Vec<u8>,
    }

    fn build_udf_skeleton(disc: &mut MemDisc, root_icb_lba: u32) {
        let mut avdp = [0u8; 2048];
        avdp[0..2].copy_from_slice(&2u16.to_le_bytes());
        disc.put(256, avdp);
        let mut pd = [0u8; 2048];
        pd[0..2].copy_from_slice(&5u16.to_le_bytes());
        pd[188..192].copy_from_slice(&PART_START.to_le_bytes());
        disc.put(32, pd);
        let mut lvd = [0u8; 2048];
        lvd[0..2].copy_from_slice(&6u16.to_le_bytes());
        lvd[268..272].copy_from_slice(&1u32.to_le_bytes());
        disc.put(33, lvd);
        let mut td = [0u8; 2048];
        td[0..2].copy_from_slice(&8u16.to_le_bytes());
        disc.put(34, td);
        let mut fsd = [0u8; 2048];
        fsd[0..2].copy_from_slice(&256u16.to_le_bytes());
        fsd[404..408].copy_from_slice(&root_icb_lba.to_le_bytes());
        disc.put(PART_START, fsd);
    }

    /// Build a UDF tree with a single VIDEO_TS directory holding the given
    /// files, and return the navigable UdfFs over `disc`.
    fn build_video_ts_fs(disc: &mut MemDisc, files: &[FileSpec]) -> crate::udf::UdfFs {
        let mut fids = Vec::new();
        push_fid(&mut fids, "", 50, true, true);
        for f in files {
            push_fid(&mut fids, &f.name, f.icb_lba, false, false);
            disc.put(
                PART_START + f.icb_lba,
                build_file_icb(f.contents.len() as u32, f.data_lba),
            );
            disc.put_bytes(PART_START + f.data_lba, &f.contents);
        }
        // VIDEO_TS dir ICB + data.
        disc.put(PART_START + 50, build_file_icb(fids.len() as u32, 51));
        disc.put_bytes(PART_START + 51, &fids);
        // Root dir referencing VIDEO_TS.
        let mut root_fids = Vec::new();
        push_fid(&mut root_fids, "", 10, true, true);
        push_fid(&mut root_fids, "VIDEO_TS", 50, true, false);
        disc.put(PART_START + 10, build_file_icb(root_fids.len() as u32, 11));
        disc.put_bytes(PART_START + 11, &root_fids);
        build_udf_skeleton(disc, 10);
        crate::udf::read_filesystem(disc).expect("fs")
    }

    // IFO builders (DVD-Video spec, offsets per ifo.rs). VIDEO_TS.IFO layout.
    fn build_vmg(
        titles: &[(
            u16, /*chapters*/
            u8,  /*vts*/
            u8,  /*vts_title*/
        )],
    ) -> Vec<u8> {
        // Put TT_SRPT at sector 1 (offset 2048).
        let tt_srpt_sector = 1u32;
        let mut d = vec![0u8; 2 * 2048];
        d[0..12].copy_from_slice(b"DVDVIDEO-VMG");
        d[0xC4..0xC8].copy_from_slice(&tt_srpt_sector.to_be_bytes());
        let base = tt_srpt_sector as usize * 2048;
        d[base..base + 2].copy_from_slice(&(titles.len() as u16).to_be_bytes());
        for (i, (chapters, vts, vts_title)) in titles.iter().enumerate() {
            let e = base + 8 + i * 12;
            d[e + 2..e + 4].copy_from_slice(&chapters.to_be_bytes());
            d[e + 6] = *vts;
            d[e + 7] = *vts_title;
        }
        d
    }

    /// Cell playback info entry (24 bytes): category byte@0, BCD time@4..8,
    /// first_sector(u32 BE)@8, last_sector(u32 BE)@20.
    fn write_cell(buf: &mut [u8], off: usize, first_sector: u32, last_sector: u32) {
        buf[off + 8..off + 12].copy_from_slice(&first_sector.to_be_bytes());
        buf[off + 20..off + 24].copy_from_slice(&last_sector.to_be_bytes());
    }

    /// Like [`write_cell`] but also stamps the cell-category byte (`+0`) so a
    /// test can build a leading scene-index / interleaved-angle sub-block cell.
    fn write_cell_cat(buf: &mut [u8], off: usize, first: u32, last: u32, category: u8) {
        write_cell(buf, off, first, last);
        buf[off] = category;
    }

    // VTS_XX_0.IFO + PGCIT + PGC layout (offsets, vtstt_vobs vs vtsm_vobs).
    #[allow(clippy::too_many_arguments)]
    fn build_vts(
        vob_start: u32,
        video_b0: u8,
        audio: &[(
            u8,      /*b0 coding/sr*/
            u8,      /*b1 channels*/
            [u8; 2], /*lang*/
        )],
        subs: &[[u8; 2]],
        cells: &[(u32, u32)],
        palette_nonzero: bool,
    ) -> Vec<u8> {
        // Total file: header sector(s) + PGCIT at sector 2.
        let pgcit_sector = 2u32;
        let mut d = vec![0u8; 4 * 2048];
        d[0..12].copy_from_slice(b"DVDVIDEO-VTS");
        d[0xC4..0xC8].copy_from_slice(&vob_start.to_be_bytes()); // vtstt_vobs (Title VOBS)
        d[0xCC..0xD0].copy_from_slice(&pgcit_sector.to_be_bytes());
        d[0x200] = video_b0;
        d[0x202..0x204].copy_from_slice(&(audio.len() as u16).to_be_bytes());
        for (i, (b0, b1, lang)) in audio.iter().enumerate() {
            let a = 0x204 + i * 8;
            d[a] = *b0;
            d[a + 1] = *b1;
            d[a + 2] = lang[0];
            d[a + 3] = lang[1];
        }
        d[0x254..0x256].copy_from_slice(&(subs.len() as u16).to_be_bytes());
        for (i, lang) in subs.iter().enumerate() {
            let s = 0x256 + i * 6;
            d[s + 2] = lang[0];
            d[s + 3] = lang[1];
        }

        // PGCIT: one PGC.
        let pg = pgcit_sector as usize * 2048;
        d[pg..pg + 2].copy_from_slice(&1u16.to_be_bytes()); // num_pgcs = 1
        // PGC info entry 0 at pg+8; PGC byte offset (rel to PGCIT) at +4.
        let pgc_rel: u32 = 0x100; // PGC body 256 bytes into the PGCIT
        d[pg + 8 + 4..pg + 8 + 8].copy_from_slice(&pgc_rel.to_be_bytes());
        let pgc = pg + pgc_rel as usize;
        // Ensure room for PGC (needs >= 0xEA past pgc, plus cell table).
        d[pgc + 0x02] = 1; // nr_of_programs
        d[pgc + 0x03] = cells.len() as u8; // nr_of_cells
        // BCD playback time 00:00:30:00 → 30 s, frame-rate bits 0b01 (25fps)
        // not needed; keep simple 30s. BCD: hh,mm,ss,frame|rate.
        d[pgc + 0x04] = 0x00;
        d[pgc + 0x05] = 0x00;
        d[pgc + 0x06] = 0x30; // 30 seconds BCD
        d[pgc + 0x07] = 0b0100_0000; // rate bits = 01 (25fps); 0 frames
        // pgm map ptr @0xE6, cell playback ptr @0xE8 (rel to PGC start).
        let cell_tbl_rel: u16 = 0xF0;
        let pgm_map_rel: u16 = 0xEC;
        d[pgc + 0xE6..pgc + 0xE8].copy_from_slice(&pgm_map_rel.to_be_bytes());
        d[pgc + 0xE8..pgc + 0xEA].copy_from_slice(&cell_tbl_rel.to_be_bytes());
        // Program map: program 0 → first cell 1.
        d[pgc + pgm_map_rel as usize] = 1;
        // Cell playback table.
        let cell_base = pgc + cell_tbl_rel as usize;
        for (i, (first, last)) in cells.iter().enumerate() {
            write_cell(&mut d, cell_base + i * 24, *first, *last);
        }
        // Palette at PGC+0xA4: 16 × [pad,Y,Cb,Cr]. Non-zero if requested.
        if palette_nonzero {
            d[pgc + 0xA4 + 1] = 0x40; // Y of color 0
        }
        // SPST_CTL: every declared subpicture present, all four ids = its ordinal.
        let ordinal: Vec<u32> = (0..subs.len() as u32)
            .map(|i| 0x8000_0000 | (i << 24) | (i << 16) | (i << 8) | i)
            .collect();
        set_spst(&mut d, &ordinal);
        let ast: Vec<u16> = (0..audio.len() as u16).map(|i| 0x8000 | (i << 8)).collect();
        set_ast(&mut d, &ast);
        d
    }

    // Overwrites build_vts's PGC_AST_CTL (PGC+0x0C, 8 x u16 BE).
    fn set_ast(vts: &mut [u8], ctl: &[u16]) {
        let at = 2 * 2048 + 0x100 + 0x0C;
        for (i, c) in ctl.iter().enumerate() {
            vts[at + i * 2..at + i * 2 + 2].copy_from_slice(&c.to_be_bytes());
        }
    }

    // Scans a one-VTS disc and returns its first title's (pid, language) audio tracks.
    fn scan_audio(vts: Vec<u8>) -> (Vec<(u16, String)>, Vec<String>) {
        let mut disc = MemDisc::new();
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: build_vmg(&[(1, 1, 1)]),
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let (t, ev) = crate::testlog::capture(|| {
            Disc::scan_dvd_titles(&mut disc, &udf, None)
                .expect("scan")
                .0
        });
        let audio = t[0]
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Audio(a) => Some((a.pid, a.language.clone())),
                _ => None,
            })
            .collect();
        (audio, diag_lines(&ev))
    }

    const AC3_6CH: (u8, u8) = (0x00, 0x05);
    const DTS_6CH: (u8, u8) = (0xC0, 0x05);
    const MP2_2CH: (u8, u8) = (0x40, 0x01);

    fn aud(c: (u8, u8), lang: &[u8; 2]) -> (u8, u8, [u8; 2]) {
        (c.0, c.1, *lang)
    }

    /// L084b: the physical audio stream number comes from PGC_AST_CTL, not the logical
    /// position. This is per spec; do not change without a spec citation proving otherwise.
    #[test]
    fn scan_dvd_titles_audio_uses_ast_ctl_stream_number() {
        let mut vts = build_vts(0, 0x00, &[aud(AC3_6CH, b"en")], &[], &[(0, 9)], false);
        set_ast(&mut vts, &[0x8100]);
        // vm_get_audio_stream: "streamN = (...audio_control[audioN] >> 8) & 0x07;" = 1 -> 0x81.
        assert_eq!(scan_audio(vts).0, vec![(0xBD81, "eng".to_string())]);
    }

    /// Codec base + AST_CTL number per libdvdnav; reserved bits 14-11 are masked off.
    #[test]
    fn scan_dvd_titles_audio_ast_ctl_codec_base_and_mask() {
        let audio = [aud(DTS_6CH, b"en"), aud(AC3_6CH, b"fr")];
        let mut vts = build_vts(0, 0x00, &audio, &[], &[(0, 9)], false);
        set_ast(&mut vts, &[0xFA00, 0x8300]);
        // "& 0x07" drops bits 14-11: DTS 2 -> VLC "0x88 -> 0x8f"; AC-3 3 -> "0x80 -> 0x87".
        assert_eq!(
            scan_audio(vts).0,
            vec![(0xBD8A, "eng".to_string()), (0xBD83, "fra".to_string())]
        );
    }

    /// A stream AST_CTL marks absent gets no track (spec); a repeated physical id keeps the
    /// first (freemkv policy, not a spec rule: two tracks cannot share one PID).
    #[test]
    fn scan_dvd_titles_audio_skips_absent_and_duplicate() {
        let audio = [
            aud(AC3_6CH, b"en"),
            aud(AC3_6CH, b"fr"),
            aud(AC3_6CH, b"de"),
        ];
        let mut vts = build_vts(0, 0x00, &audio, &[], &[(0, 9)], false);
        set_ast(&mut vts, &[0x0000, 0x8200, 0x8200]);
        let (tracks, diag) = scan_audio(vts);
        // mpucoder PGC_AST_CTL: "1 = stream available" is clear for entry 0.
        assert_eq!(tracks, vec![(0xBD82, "fra".to_string())]);
        assert!(
            diag.iter().any(|m| m.contains("tag=dvd.astctl")
                && m.contains("id=0xBD82 kept=\"fra\" dropped=\"deu\"")),
            "{diag:?}"
        );
    }

    /// No AST_CTL entry present at all: keep every declared stream on its positional id
    /// rather than rip silently, and trace it. freemkv policy, not a spec rule: libdvdnav's
    /// vm_get_audio_stream returns "streamN = -1" (no stream) here.
    #[test]
    fn scan_dvd_titles_audio_all_absent_falls_back_to_position() {
        let audio = [aud(AC3_6CH, b"en"), aud(DTS_6CH, b"fr")];
        let mut vts = build_vts(0, 0x00, &audio, &[], &[(0, 9)], false);
        set_ast(&mut vts, &[0, 0]);
        let (tracks, diag) = scan_audio(vts);
        assert_eq!(
            tracks,
            vec![(0xBD80, "eng".to_string()), (0xBD89, "fra".to_string())]
        );
        assert!(
            diag.iter().any(|m| m.contains("tag=dvd.astctl")
                && m.contains("declared=2 present=0 -> positional routing")),
            "{diag:?}"
        );
    }

    /// MPEG audio rides its own PES id 0xC0|n, with n from AST_CTL like every other codec.
    /// This is per spec; do not change without a spec citation proving otherwise.
    #[test]
    fn scan_dvd_titles_mp2_audio_routes_by_ast_to_pes_id() {
        let audio = [aud(MP2_2CH, b"en"), aud(MP2_2CH, b"fr")];
        let mut vts = build_vts(0, 0x00, &audio, &[], &[(0, 9)], false);
        set_ast(&mut vts, &[0x0000, 0x8300]);
        // mpucoder PGC_AST_CTL: "Stream number (MPEG audio) or Substream number (all others)";
        // PES: "0xC0 - 0xDF MPEG-1 or MPEG-2 audio stream number x xxxx".
        assert_eq!(scan_audio(vts).0, vec![(0x00C3, "fra".to_string())]);
    }

    // Scans a one-title disc whose IFO declares one MP2 stream (b0, b1), muxes `frame` as its
    // first audio frame, and returns the MKV Tracks/Audio/Channels value.
    fn scanned_mp2_mkv_channels(b0: u8, b1: u8, frame: &[u8]) -> u8 {
        use crate::mux::mkv::{MkvMuxer, MkvTrack};
        let t = &scan_title(build_vts(
            0,
            0x00,
            &[aud((b0, b1), b"en")],
            &[],
            &[(0, 9)],
            false,
        ));
        let tracks: Vec<MkvTrack> = t
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Video(v) => Some(MkvTrack::video(v)),
                Stream::Audio(a) => Some(MkvTrack::audio(a)),
                _ => None,
            })
            .collect();
        let mut out = std::io::Cursor::new(Vec::new());
        let mut m = MkvMuxer::new(&mut out, &tracks, None, 0.0, &[]).unwrap();
        m.write_frame(0, 0, true, &[1, 2], None, None).unwrap();
        // A count above nch commits after RUN_FRAMES CRC-valid frames in a row.
        for _ in 0..crate::mux::codec::mp2_channels::RUN_FRAMES {
            m.write_frame(1, 0, false, frame, None, None).unwrap();
        }
        m.finish().unwrap();
        let d = out.into_inner();
        let at = d
            .windows(4)
            .position(|w| w == [0x16, 0x54, 0xAE, 0x6B])
            .expect("Tracks");
        let ch = at
            + d[at..]
                .windows(2)
                .position(|w| w == [0x9F, 0x81])
                .expect("Channels");
        d[ch + 2]
    }

    fn scan_title(vts: Vec<u8>) -> DiscTitle {
        let mut disc = MemDisc::new();
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: build_vmg(&[(1, 1, 1)]),
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0
            .remove(0)
    }

    /// Scan to MKV: IFO coding mode 3 declares 6 channels ("channels-1" = 5); the stored 0xC0
    /// base has an extension stream, so the track holds the stereo base only.
    #[test]
    fn scan_to_mkv_mp2_mode3_with_extension_stream_is_stereo() {
        use crate::mux::codec::mp2_channels::tests::{MC_3_2_LFE, Mc, STEREO_256, Spec, write};
        let f = write(Spec {
            mc: Some(Mc {
                ext: true,
                ..MC_3_2_LFE
            }),
            ..STEREO_256
        })
        .0;
        // mpucoder IFO: coding mode "3 Mpeg-2ext", byte 1 bits 2-0 "channels-1".
        assert_eq!(scanned_mp2_mkv_channels(0x60, 0x05, &f), 2);
    }

    /// Scan to MKV with the multichannel data stored in the base frame.
    /// This is per spec; do not change without a spec citation proving otherwise.
    #[test]
    fn scan_to_mkv_mp2_mode3_without_extension_stream_keeps_5_1() {
        use crate::mux::codec::mp2_channels::tests::{MC_3_2_LFE, STEREO_256, Spec, write};
        let f = write(Spec {
            mc: Some(MC_3_2_LFE),
            ..STEREO_256
        })
        .0;
        // 13818-3 §2.5.2.13: "'0' no extension stream present" with 3/2 + LFE.
        assert_eq!(scanned_mp2_mkv_channels(0x60, 0x05, &f), 6);
    }

    // (pid, language, is_mp2_extension) of every audio track of a one-title disc.
    fn scanned_audio(vts: Vec<u8>) -> Vec<(u16, String, bool)> {
        scan_title(vts)
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Audio(a) => Some((a.pid, a.language.clone(), a.is_mp2_extension())),
                _ => None,
            })
            .collect()
    }

    const MP2_EXT_51: (u8, u8) = (0x60, 0x05); // coding mode 3, 6 channels declared

    /// IFO coding mode 3 ("3 Mpeg-2ext"; EP0867877A2 "011b MPEG-2 with extension bitstream")
    /// declares the extension stream `0xD0|n` right after its base `0xC0|n`, n from AST_CTL.
    /// This is per spec; do not change without a spec citation proving otherwise.
    #[test]
    fn mode_3_mp2_declares_its_extension_track_after_the_base() {
        let audio = [aud(MP2_EXT_51, b"en"), aud(MP2_2CH, b"fr")];
        let mut vts = build_vts(0, 0x00, &audio, &[], &[(0, 9)], false);
        set_ast(&mut vts, &[0x8300, 0x8100]);
        assert_eq!(
            scanned_audio(vts),
            vec![
                (0x00C3, "eng".to_string(), false),
                (0x00D3, "eng".to_string(), true),
                // "010b MPEG-1 or MPEG-2 without extension bit stream": no extension track.
                (0x00C1, "fra".to_string(), false),
            ]
        );
    }

    /// A stream AST_CTL marks absent declares neither its base nor its extension.
    #[test]
    fn absent_mode_3_stream_declares_no_extension_either() {
        let audio = [aud(MP2_EXT_51, b"en"), aud(MP2_2CH, b"fr")];
        let mut vts = build_vts(0, 0x00, &audio, &[], &[(0, 9)], false);
        set_ast(&mut vts, &[0x0000, 0x8000]);
        assert_eq!(scanned_audio(vts), vec![(0x00C0, "fra".to_string(), false)]);
    }

    /// An unknown coding mode has no route: a unique placeholder PID 0xBD00 + i, never
    /// colliding with a sibling.
    #[test]
    fn scan_dvd_titles_unknown_audio_codec_gets_distinct_placeholder() {
        let unknown = (0x20u8, 0x01u8); // coding_mode 1: reserved
        let audio = [aud(unknown, b"en"), aud(unknown, b"fr")];
        let vts = build_vts(0, 0x00, &audio, &[], &[(0, 9)], false);
        let pids: Vec<u16> = scan_audio(vts).0.iter().map(|t| t.0).collect();
        assert_eq!(pids, vec![0xBD00, 0xBD01]);
    }

    /// Per-title diag line with the ROUTED id, not the VTS-level positional one.
    #[test]
    fn scan_dvd_titles_logs_routed_audio_id_per_title() {
        let mut vts = build_vts(0, 0x00, &[aud(AC3_6CH, b"en")], &[], &[(0, 9)], false);
        set_ast(&mut vts, &[0x8100]);
        let diag = scan_audio(vts).1;
        assert!(
            diag.iter().any(|m| m.contains("tag=dvd.aroute")
                && m.contains("title=1 idx=0")
                && m.contains("ast=0x8100 pid=0xBD81")),
            "{diag:?}"
        );
    }

    // Overwrites build_vts's PGC_SPST_CTL (PGC+0x1C, 32 x u32 BE).
    fn set_spst(vts: &mut [u8], ctl: &[u32]) {
        let at = 2 * 2048 + 0x100 + 0x1C;
        for (i, c) in ctl.iter().enumerate() {
            vts[at + i * 4..at + i * 4 + 4].copy_from_slice(&c.to_be_bytes());
        }
    }

    // Scans a one-VTS disc and returns its first title's (pid, language) subtitles.
    fn scan_subs(vts: Vec<u8>) -> Vec<(u16, String)> {
        let mut disc = MemDisc::new();
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: build_vmg(&[(1, 1, 1)]),
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0;
        t[0].streams
            .iter()
            .filter_map(|s| match s {
                Stream::Subtitle(s) => Some((s.pid, s.language.clone())),
                _ => None,
            })
            .collect()
    }

    const NTSC_16X9: u8 = 0x0C;

    /// L084: an anamorphic title carries wide/letterbox/pan-scan variants per language, so the
    /// wide sub-stream id comes from SPST_CTL, not the ordinal. This is per spec; do not
    /// change without a spec citation proving otherwise.
    #[test]
    fn scan_dvd_titles_anamorphic_subtitles_use_spst_wide_id() {
        let mut vts = build_vts(0, NTSC_16X9, &[], &[*b"en", *b"fr"], &[(0, 9)], true);
        // mpucoder PGC_SPST_CTL bytes: "Stream number for 4:3", "for wide", "for letterbox",
        // "for pan&scan".
        set_spst(&mut vts, &[0x8000_0102, 0x8003_0405]);
        // vm_get_subp_stream "mode == 0 - widescreen": "subp_control[subpN] >> 16) & 0x1f".
        assert_eq!(
            scan_subs(vts),
            vec![(0x20, "eng".to_string()), (0x23, "fra".to_string())],
            "French must route to its wide sub-stream 0x23, not ordinal 0x21"
        );
    }

    /// A 4:3 title uses SPST_CTL's 4:3 field (byte 0 bits 4-0).
    #[test]
    fn scan_dvd_titles_4x3_subtitles_use_spst_4x3_id() {
        let mut vts = build_vts(0, 0x00, &[], &[*b"en", *b"de"], &[(0, 9)], true);
        set_spst(&mut vts, &[0x8500_0000, 0x8200_0000]);
        // vm_get_subp_stream "if(source_aspect == 0) /* 4:3 */": "subp_control[subpN] >> 24".
        assert_eq!(
            scan_subs(vts),
            vec![(0x25, "eng".to_string()), (0x22, "deu".to_string())]
        );
    }

    /// A subpicture stream SPST_CTL marks absent gets no track (spec); two logical streams on
    /// one physical id keep the first (freemkv policy, not a spec rule).
    #[test]
    fn scan_dvd_titles_skips_absent_and_duplicate_subpictures() {
        let subs = [*b"en", *b"es", *b"it"];
        let mut vts = build_vts(0, NTSC_16X9, &[], &subs, &[(0, 9)], true);
        set_spst(&mut vts, &[0x0000_0000, 0x8001_0100, 0x8001_0100]);
        let (subs, ev) = crate::testlog::capture(|| scan_subs(vts));
        // ifo_print: "subp_control[i] & 0x80000000) { /* The 'is present' bit */" is clear.
        assert_eq!(subs, vec![(0x21, "spa".to_string())]);
        assert!(
            diag_lines(&ev).iter().any(|m| m.contains("tag=dvd.spstctl")
                && m.contains("id=0x21 kept=\"spa\" dropped=\"ita\"")),
            "{:?}",
            diag_lines(&ev)
        );
    }

    fn diag_lines(ev: &[crate::testlog::CapturedEvent]) -> Vec<String> {
        ev.iter()
            .filter(|e| e.target == "freemkv::diag")
            .map(|e| e.message().to_string())
            .collect()
    }

    /// Every declared subpicture absent from the PGC: no tracks, and a diag line says so.
    #[test]
    fn scan_dvd_titles_all_subpictures_absent_is_traced() {
        let mut vts = build_vts(0, NTSC_16X9, &[], &[*b"en", *b"fr"], &[(0, 9)], true);
        set_spst(&mut vts, &[0, 0]);
        let (subs, ev) = crate::testlog::capture(|| scan_subs(vts));
        assert!(subs.is_empty());
        assert!(
            diag_lines(&ev)
                .iter()
                .any(|m| m.contains("tag=dvd.spstctl") && m.contains("declared=2 present=0")),
            "{:?}",
            diag_lines(&ev)
        );
    }

    // Tests. HaltingReader fails every read at/above halt_at with Error::Halted, mimicking a
    // live drive after a Stop.
    struct HaltingReader<'a> {
        inner: &'a mut MemDisc,
        halt_at: u32,
    }
    impl SectorSource for HaltingReader<'_> {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            recovery: bool,
        ) -> crate::error::Result<usize> {
            if lba >= self.halt_at {
                return Err(crate::error::Error::Halted);
            }
            self.inner.read_sectors(lba, count, buf, recovery)
        }
    }

    // A drive Stop must surface as Err(Error::Halted), not a truncated/empty scan. Regression:
    // two prior swallow sites (parse_vmg placeholder entry; scan_dvd_titles's Err(_) =>
    // Vec::new()).
    #[test]
    fn halted_ifo_read_is_not_reported_as_a_shorter_disc() {
        // Two title sets; only the second title set's CONTENT read is halted.
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1), (1, 2, 1)]);
        let vts1 = build_vts(100, 0x00, &[], &[], &[(0, 9)], false);
        let vts2 = build_vts(200, 0x00, &[], &[], &[(0, 19)], false);
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts1,
                },
                FileSpec {
                    name: "VTS_02_0.IFO".into(),
                    icb_lba: 64,
                    data_lba: 7000,
                    contents: vts2,
                },
            ],
        );
        // Sanity: both title sets enumerate when nothing is cancelled, so a
        // short list below can only be the cancel.
        assert_eq!(
            Disc::scan_dvd_titles(&mut disc, &udf, None)
                .expect("scan")
                .0
                .len(),
            2,
            "fixture must offer two title sets"
        );

        let mut reader = HaltingReader {
            inner: &mut disc,
            halt_at: PART_START + 7000, // VTS_02_0.IFO's data extent
        };
        let res = Disc::scan_dvd_titles(&mut reader, &udf, None);
        assert!(
            matches!(res, Err(crate::error::Error::Halted)),
            "a cancelled title-set read must surface as a cancelled scan, not \
             as a disc with fewer titles; got {:?}",
            res.map(|(ts, _, _)| ts.iter().map(|t| t.playlist.clone()).collect::<Vec<_>>())
        );

        // The same cancel one level up: VIDEO_TS.IFO itself. This is the
        // `Err(_) => Vec::new()` path — a cancel that used to report a DVD as
        // carrying no titles whatsoever.
        let mut reader = HaltingReader {
            inner: &mut disc,
            halt_at: PART_START + 5000,
        };
        let res = Disc::scan_dvd_titles(&mut reader, &udf, None);
        assert!(
            matches!(res, Err(crate::error::Error::Halted)),
            "a cancelled VMG read must surface as a cancelled scan, not as an \
             empty disc; got {:?}",
            res.map(|(ts, _, _)| ts.len())
        );
    }

    /// scan_dvd_titles returns empty when VIDEO_TS.IFO can't be parsed
    /// (dvd.rs: `parse_vmg(...) Err → return Vec::new()`). Never panics.
    #[test]
    fn scan_dvd_titles_no_ifo_is_empty() {
        let mut disc = MemDisc::new();
        // VIDEO_TS exists but VIDEO_TS.IFO is missing.
        let udf = build_video_ts_fs(&mut disc, &[]);
        assert!(
            Disc::scan_dvd_titles(&mut disc, &udf, None)
                .expect("scan")
                .0
                .is_empty()
        );
    }

    /// Single VTS, single title, one cell. Extent absolute LBA =
    /// vob_start + cell.first_sector (dvd.rs); sector_count = last - first
    /// + 1 (inclusive range); size_bytes = sectors * 2048 (DVD sector).
    #[test]
    fn scan_dvd_titles_single_cell_extent_math() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]); // 1 chapter, VTS 1, title 1
        // vob_start 1000; one cell sectors [10..=109] → 100 sectors.
        let vts = build_vts(
            1000,
            0x00, // NTSC, 4:3
            &[],
            &[],
            &[(10, 109)],
            false,
        );
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let titles = Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0;
        assert_eq!(titles.len(), 1);
        let t = &titles[0];
        assert_eq!(t.extents.len(), 1);
        // absolute start = ifo_lba + vtstt_vobs(1000) + first_sector(10).
        // The IFO file sits at PART_START(3000) + data_lba(6000) = 9000, so
        // 9000 + 1000 + 10 = 10010.
        assert_eq!(t.extents[0].start_lba, 10010);
        // inclusive: 109 - 10 + 1 = 100 sectors.
        assert_eq!(t.extents[0].sector_count, 100);
        // DVD sector = 2048 bytes.
        assert_eq!(t.size_bytes, 100 * 2048);
        // playlist field format VTS_XX_title.VOB; title_number is 1.
        assert_eq!(t.playlist, "VTS_01_1.VOB");
        assert_eq!(t.playlist_id, 1);
        assert_eq!(t.content_format, ContentFormat::DvdPs);
    }

    // Regression: vob_start must come from Title VOBS (0xC4), not menu VOBS (0xC0) -- else a
    // per-title menu prepends and the rip opens on the wrong frame.
    #[test]
    fn scan_dvd_titles_uses_title_vobs_not_menu_vobs() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]);
        // vtstt_vobs (title) = 3640; cell 0 first_sector = 0.
        let mut vts = build_vts(3640, 0x00, &[], &[], &[(0, 99)], false);
        // Stamp a bogus vtsm_vobs (menu) at 0xC0 — the wrong pointer the bug
        // used. It must be ignored.
        vts[0xC0..0xC4].copy_from_slice(&44u32.to_be_bytes());
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let titles = Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0;
        assert_eq!(titles.len(), 1);
        let t = &titles[0];
        assert_eq!(t.extents.len(), 1);
        // ifo_lba(9000) + vtstt_vobs(3640) + first_sector(0) = 12640 — built
        // from the Title VOBS (0xC4), NOT the menu VOBS (0xC0). The IFO file is
        // at PART_START(3000) + data_lba(6000) = 9000.
        assert_eq!(
            t.extents[0].start_lba, 12640,
            "extent must start at ifo_lba + vtstt_vobs (0xC4), not vtsm_vobs (0xC0)"
        );
        // Must not resolve from the menu VOBS (would be 9000 + 44 = 9044), nor
        // use the raw IFO-relative vtstt_vobs (3640) without the absolute base.
        assert_ne!(t.extents[0].start_lba, 9044, "must not use the menu VOBS");
        assert_ne!(
            t.extents[0].start_lba, 3640,
            "must add the IFO's absolute disc LBA, not use the raw relative value"
        );
    }

    // ABSOLUTE-REBASE regression: start_lba must sum THREE distinct, non- overlapping terms
    // (ifo_lba + vtstt_vobs + cell first_sector), not just two.
    #[test]
    fn scan_dvd_titles_extent_is_absolute_three_term_sum() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]);
        // vtstt_vobs (Title VOBS, 0xC4) = 700; one cell first_sector = 33.
        let vts = build_vts(700, 0x00, &[], &[], &[(33, 132)], false);
        // IFO data at data_lba 6000 → absolute ifo_lba = PART_START(3000) + 6000.
        let ifo_lba = PART_START + 6000; // 9000
        let vtstt_vobs = 700u32;
        let first_sector = 33u32;
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0[0];
        assert_eq!(t.extents.len(), 1);
        let got = t.extents[0].start_lba;
        // The one correct answer: all three terms summed (9000 + 700 + 33).
        assert_eq!(
            got,
            ifo_lba + vtstt_vobs + first_sector,
            "extent start must be file_start_lba(IFO) + vtstt_vobs + cell.first_sector"
        );
        // Each wrong two-term combination must be rejected:
        assert_ne!(
            got,
            vtstt_vobs + first_sector,
            "must not use the bare relative vtstt_vobs (missing the IFO's absolute LBA)"
        );
        assert_ne!(
            got,
            ifo_lba + first_sector,
            "must not drop vtstt_vobs (the Title VOBS pointer)"
        );
        assert_ne!(
            got,
            ifo_lba + vtstt_vobs,
            "must not drop the cell's first_sector offset"
        );
    }

    /// Multi-cell title: extents preserve cell order and each maps to its
    /// own (vob_start + first .. last) range. mux reads cells in order.
    #[test]
    fn scan_dvd_titles_multi_cell_extents_in_order() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(2, 1, 1)]);
        let vts = build_vts(
            500,
            0x00,
            &[],
            &[],
            &[(0, 99), (200, 299)], // two cells
            false,
        );
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0[0];
        assert_eq!(t.extents.len(), 2);
        assert_eq!(t.extents[0].start_lba, 9500); // ifo_lba(9000) + 500 + 0
        assert_eq!(t.extents[0].sector_count, 100);
        assert_eq!(t.extents[1].start_lba, 9700); // ifo_lba(9000) + 500 + 200
        assert_eq!(t.extents[1].sector_count, 100);
        assert_eq!(t.size_bytes, 200 * 2048);
    }

    /// PAL video standard (b0 low bits == 1) sets FrameRate::F25; NTSC sets
    /// F29_97 (dvd.rs match on ts.video.standard). The video PID is the
    /// fixed DVD MPEG-PS video stream id 0xE0.
    #[test]
    fn scan_dvd_titles_pal_frame_rate_and_video_pid() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]);
        let vts = build_vts(
            0,
            crate::ifo::v_atr_byte(crate::ifo::VIDEO_FORMAT_PAL, crate::ifo::ASPECT_16X9),
            &[],
            &[],
            &[(0, 9)],
            false,
        );
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0[0];
        let v = t
            .streams
            .iter()
            .find_map(|s| match s {
                Stream::Video(v) => Some(v),
                _ => None,
            })
            .expect("video stream");
        assert_eq!(v.pid, 0xE0, "DVD video PID is fixed 0xE0");
        assert_eq!(v.frame_rate, FrameRate::F25, "PAL → 25 fps");
        assert_eq!(v.resolution, Resolution::R576i, "PAL → 576i");
        assert_eq!(
            v.color_space,
            ColorSpace::Bt470bg,
            "PAL DVD is SD BT.470BG, not BT.709"
        );
        assert_eq!(
            v.display_aspect,
            Some((16, 9)),
            "ASPECT_16X9 IFO byte must map to a 16:9 display aspect"
        );
    }

    /// NTSC DVD video is SD SMPTE-170M colorimetry (not BT.709). Mirror of the
    /// PAL test with `VIDEO_FORMAT_NTSC` → 480i / 29.97 / SMPTE-170M.
    #[test]
    fn scan_dvd_titles_ntsc_color_is_smpte170m() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]);
        let vts = build_vts(
            0,
            crate::ifo::v_atr_byte(crate::ifo::VIDEO_FORMAT_NTSC, crate::ifo::ASPECT_4X3),
            &[],
            &[],
            &[(0, 9)],
            false,
        );
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0[0];
        let v = t
            .streams
            .iter()
            .find_map(|s| match s {
                Stream::Video(v) => Some(v),
                _ => None,
            })
            .expect("video stream");
        assert_eq!(v.frame_rate, FrameRate::F29_97, "NTSC → 29.97 fps");
        assert_eq!(v.resolution, Resolution::R480i, "NTSC → 480i");
        assert_eq!(
            v.color_space,
            ColorSpace::Smpte170m,
            "NTSC DVD is SD SMPTE-170M, not BT.709"
        );
        assert_eq!(
            v.display_aspect,
            Some((4, 3)),
            "ASPECT_4X3 IFO byte must map to a 4:3 display aspect"
        );
    }

    /// AC-3 audio gets sub_stream_id 0x80 → PID routed via dvd_audio_pid
    /// (dvd.rs uses `a.sub_stream_id.and_then(dvd_audio_pid)`). A mixed
    /// AC-3 + DTS title must NOT collide: AC-3 → 0x80 base, DTS → 0x88 base.
    #[test]
    fn scan_dvd_titles_mixed_audio_codecs_distinct_pids() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]);
        // audio b0: coding_mode = (b0>>5)&7 (AC-3=0->0x00, DTS=6->0xC0). b1 channels nibble
        // = (channels-1) in bits 2-0 (5.1=0x05, 2.0=0x01). Old fixture used 0x10/0x50
        // (both -> 1ch), a placeholder that passed even with broken channel handling.
        let vts = build_vts(
            0,
            0x00,
            &[(0x00, 0x05, *b"en"), (0xC0, 0x01, *b"fr")], // AC-3 5.1 eng, DTS 2.0 fra
            &[],
            &[(0, 9)],
            false,
        );
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0[0];
        let audios: Vec<_> = t
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Audio(a) => Some(a),
                _ => None,
            })
            .collect();
        assert_eq!(audios.len(), 2);
        assert_eq!(audios[0].codec, Codec::Ac3);
        assert_eq!(audios[0].language, "eng");
        assert_eq!(audios[1].codec, Codec::Dts);
        // Real channel layouts survive the scan (not a 1ch placeholder): the
        // AC-3 is 5.1 (6ch), the DTS is 2.0 (2ch).
        assert_eq!(
            audios[0].channels.count(),
            6,
            "AC-3 5.1 nibble must decode to 6 channels"
        );
        assert_eq!(
            audios[1].channels.count(),
            2,
            "DTS 2.0 nibble must decode to 2 channels"
        );
        // PIDs route via the positional sub-id table: AC-3 @ pos 0 -> 0x80 -> 0xBD80, DTS @
        // pos 1 -> 0x89 -> 0xBD89 (shared audio-stream number in the low nibble, NOT a
        // per-codec ordinal). Distinct AND the exact canonical wire PIDs the demux routes on.
        assert_eq!(audios[0].pid, 0xBD80, "AC-3 @ pos 0 → 0xBD80");
        assert_eq!(audios[1].pid, 0xBD89, "DTS @ pos 1 → 0xBD89");
        assert_ne!(audios[0].pid, audios[1].pid);
    }

    // LPCM (coding_mode 4) at audio pos 1 must route to 0xBDA1, not the AC-3 space.
    #[test]
    fn scan_dvd_titles_lpcm_routes_to_a0_pid_range() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]);
        // b0 coding_mode = (b0 >> 5) & 7. LPCM = 4 → b0 = 4<<5 = 0x80.
        // b1 channels nibble: 2.0 stereo LPCM → (2-1)=1 → 0x01. Plus an AC-3 5.1
        // so we prove the two land in disjoint PID spaces (0xBD8x vs 0xBDAx).
        let vts = build_vts(
            0,
            0x00,
            &[(0x00, 0x05, *b"en"), (0x80, 0x01, *b"fr")], // AC-3 5.1 eng, LPCM 2.0 fra
            &[],
            &[(0, 9)],
            false,
        );
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0[0];
        let audios: Vec<_> = t
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Audio(a) => Some(a),
                _ => None,
            })
            .collect();
        assert_eq!(audios.len(), 2);
        assert_eq!(audios[0].codec, Codec::Ac3);
        assert_eq!(audios[1].codec, Codec::Lpcm, "coding_mode 4 → LPCM");
        assert_eq!(audios[0].pid, 0xBD80, "AC-3 @ pos 0 → 0xBD80");
        assert_eq!(
            audios[1].pid, 0xBDA1,
            "LPCM @ pos 1 → 0xBDA1 (the 0xA0 sub-id range | position), NOT the AC-3 space"
        );
        assert_eq!(
            audios[1].channels.count(),
            2,
            "LPCM 2.0 nibble must decode to 2 channels"
        );
    }

    // A multi-subtitle VTS must emit one Subtitle per entry, distinct PIDs.
    #[test]
    fn scan_dvd_titles_multiple_vobsub_tracks_distinct_pids() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]);
        let vts = build_vts(
            0,
            0x00,
            &[],
            &[*b"en", *b"fr", *b"de"], // three VobSub tracks
            &[(0, 9)],
            true, // non-zero palette → codec_data on every track
        );
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0[0];
        let subs: Vec<_> = t
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Subtitle(s) => Some(s),
                _ => None,
            })
            .collect();
        assert_eq!(subs.len(), 3, "three VobSub tracks must all surface");
        // Languages preserved in order.
        assert_eq!(
            subs.iter().map(|s| s.language.as_str()).collect::<Vec<_>>(),
            vec!["eng", "fra", "deu"]
        );
        // PIDs are 0x20 + ordinal, all distinct.
        let pids: Vec<u16> = subs.iter().map(|s| s.pid).collect();
        assert_eq!(pids, vec![0x20, 0x21, 0x22], "VobSub PID = 0x20 + ordinal");
        // Every track carries the palette codec_data.
        for s in &subs {
            assert_eq!(s.codec, Codec::DvdSub);
            assert!(
                s.codec_data.is_some(),
                "each VobSub track shares the PGC palette codec_data"
            );
        }
    }

    /// Subtitle streams map to Codec::DvdSub with palette codec_data when a
    /// non-zero palette is present (dvd.rs builds codec_data from
    /// dvd_title.palette). VobSub sub-id 0x20+i.
    #[test]
    fn scan_dvd_titles_subtitle_palette_codec_data() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]);
        let vts = build_vts(
            0,
            0x00,
            &[],
            &[*b"en"],
            &[(0, 9)],
            true, // non-zero palette
        );
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0[0];
        let sub = t
            .streams
            .iter()
            .find_map(|s| match s {
                Stream::Subtitle(s) => Some(s),
                _ => None,
            })
            .expect("subtitle stream");
        assert_eq!(sub.codec, Codec::DvdSub);
        assert_eq!(sub.language, "eng");
        assert!(
            sub.codec_data.is_some(),
            "non-zero palette must yield codec_data"
        );
        let idx = String::from_utf8(sub.codec_data.clone().expect("codec_data")).expect("utf8");
        assert!(
            idx.starts_with("size: 720x480\npalette: "),
            "the .idx carries the NTSC coded frame size, then the palette: {idx:?}"
        );
    }

    /// Multiple titles in one VTS each become their own DiscTitle with a
    /// monotonically increasing title_number / playlist_id (dvd.rs
    /// `title_number += 1` per dvd_title). Both share the VTS streams.
    #[test]
    fn scan_dvd_titles_numbering_increments_per_title() {
        let mut disc = MemDisc::new();
        // Two titles in VTS 1 (nums 1,2): pgc_index = vts_title-1 needs >=2 PGC entries, but
        // build_vts only emits 1, so the second title's PGC index (1) exceeds num_pgcs (1)
        // and is skipped. Use two separate VTS sets instead to exercise numbering.
        let vmg = build_vmg(&[(1, 1, 1), (1, 2, 1)]);
        let vts1 = build_vts(100, 0x00, &[], &[], &[(0, 9)], false);
        let vts2 = build_vts(200, 0x00, &[], &[], &[(0, 19)], false);
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts1,
                },
                FileSpec {
                    name: "VTS_02_0.IFO".into(),
                    icb_lba: 64,
                    data_lba: 7000,
                    contents: vts2,
                },
            ],
        );
        let titles = Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0;
        assert_eq!(titles.len(), 2);
        // title_number is a running counter across all title sets.
        assert_eq!(titles[0].playlist_id, 1);
        assert_eq!(titles[1].playlist_id, 2);
        assert_eq!(titles[0].playlist, "VTS_01_1.VOB");
        assert_eq!(titles[1].playlist, "VTS_02_2.VOB");
        // Distinct vob_start → distinct extents.
        // VTS_01 IFO @ PART_START(3000)+6000=9000; VTS_02 IFO @ 3000+7000=10000.
        assert_eq!(titles[0].extents[0].start_lba, 9100); // 9000 + 100
        assert_eq!(titles[1].extents[0].start_lba, 10200); // 10000 + 200
    }

    // Stamps a First-Play PGC with an unconditional JumpTT ttn pre-command.
    fn stamp_first_play_jumptt(vmg: &mut [u8], ttn: u8) {
        const FP_PGC_PTR: usize = 0x84; // u32 byte offset of the First-Play PGC
        const CMD_TBL_PTR: usize = 0xE4; // u16 cmd-table offset (rel to PGC)
        let fp_pgc: u32 = 0x200;
        let cmd_tbl_rel: u16 = 0x100;
        vmg[FP_PGC_PTR..FP_PGC_PTR + 4].copy_from_slice(&fp_pgc.to_be_bytes());
        let pgc = fp_pgc as usize;
        vmg[pgc + CMD_TBL_PTR..pgc + CMD_TBL_PTR + 2].copy_from_slice(&cmd_tbl_rel.to_be_bytes());
        let tbl = pgc + cmd_tbl_rel as usize;
        vmg[tbl..tbl + 2].copy_from_slice(&1u16.to_be_bytes()); // nr_of_pre = 1
        // JumpTT command (type 1, direct=1, cmd=2): ttn in byte 5.
        vmg[tbl + 8..tbl + 16].copy_from_slice(&[0x30, 0x02, 0, 0, 0, ttn, 0, 0]);
    }

    // First-Play nav promotion (issue #40): resolve_main_title's (vtsn, vts_ttn) must join to
    // the running title_number and surface as nav_feature.
    #[test]
    fn scan_dvd_titles_nav_promotes_first_play_target() {
        let mut disc = MemDisc::new();
        // TT_SRPT: title 1 → VTS 1 (in-set title 1); title 2 → VTS 2 (in-set 1).
        let mut vmg = build_vmg(&[(1, 1, 1), (1, 2, 1)]);
        stamp_first_play_jumptt(&mut vmg, 2); // First-Play → JumpTT title 2
        let vts1 = build_vts(100, 0x00, &[], &[], &[(0, 9)], false);
        let vts2 = build_vts(200, 0x00, &[], &[], &[(0, 19)], false);
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts1,
                },
                FileSpec {
                    name: "VTS_02_0.IFO".into(),
                    icb_lba: 64,
                    data_lba: 7000,
                    contents: vts2,
                },
            ],
        );
        let (titles, nav_feature, _) = Disc::scan_dvd_titles(&mut disc, &udf, None).expect("scan");
        assert_eq!(titles.len(), 2);
        // Sanity: the resolver reached the branch with the VTS-2 target.
        assert_eq!(
            crate::dvdnav::resolve_main_title(&mut disc, &udf, None),
            Some(crate::dvdnav::nav::ResolvedTitle {
                title: 2,
                vtsn: 2,
                vts_ttn: 1,
            })
        );
        // The promotion mapped that target to the second scanned title.
        assert_eq!(nav_feature, Some(2));
        assert_eq!(titles[1].playlist_id, 2);
    }

    // The nav target joins on the VTS number AND the in-set title number: VTS 2 also has an
    // in-set title 1, which must not take the promotion from VTS 1's.
    #[test]
    fn scan_dvd_titles_nav_target_is_matched_by_vts_number() {
        let mut disc = MemDisc::new();
        let mut vmg = build_vmg(&[(1, 1, 1), (1, 2, 1)]);
        stamp_first_play_jumptt(&mut vmg, 1);
        let vts1 = build_vts(100, 0x00, &[], &[], &[(0, 9)], false);
        let vts2 = build_vts(200, 0x00, &[], &[], &[(0, 19)], false);
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts1,
                },
                FileSpec {
                    name: "VTS_02_0.IFO".into(),
                    icb_lba: 64,
                    data_lba: 7000,
                    contents: vts2,
                },
            ],
        );
        let (titles, nav_feature, _) = Disc::scan_dvd_titles(&mut disc, &udf, None).expect("scan");
        assert_eq!(titles.len(), 2);
        assert_eq!(nav_feature, Some(1));
    }

    fn one_title_dvd(disc: &mut MemDisc) -> udf::UdfFs {
        build_video_ts_fs(
            disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: build_vmg(&[(1, 1, 1)]),
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: build_vts(0, 0x00, &[], &[], &[(0, 9)], false),
                },
            ],
        )
    }

    // A Stop raised before the scan, or during its last reads, is `Halted`, never a list.
    #[test]
    fn scan_dvd_titles_honours_the_halt_token() {
        struct CancellingReader<'a> {
            inner: &'a mut MemDisc,
            halt: crate::halt::Halt,
            reads: u32,
        }
        impl SectorSource for CancellingReader<'_> {
            fn read_sectors(
                &mut self,
                lba: u32,
                count: u16,
                buf: &mut [u8],
                recovery: bool,
            ) -> crate::error::Result<usize> {
                self.reads += 1;
                if lba >= PART_START + 6000 {
                    self.halt.cancel();
                }
                self.inner.read_sectors(lba, count, buf, recovery)
            }
        }
        let mut disc = MemDisc::new();
        let udf = one_title_dvd(&mut disc);
        let halt = crate::halt::Halt::new();
        halt.cancel();
        let mut reader = CancellingReader {
            inner: &mut disc,
            halt: halt.clone(),
            reads: 0,
        };
        let res = Disc::scan_dvd_titles(&mut reader, &udf, Some(&halt));
        assert!(matches!(res, Err(crate::error::Error::Halted)), "{res:?}");
        assert_eq!(reader.reads, 0, "a cancelled scan reads nothing");

        let halt = crate::halt::Halt::new();
        let mut reader = CancellingReader {
            inner: &mut disc,
            halt: halt.clone(),
            reads: 0,
        };
        let res = Disc::scan_dvd_titles(&mut reader, &udf, Some(&halt));
        assert!(matches!(res, Err(crate::error::Error::Halted)), "{res:?}");
    }

    /// chapter_times from the IFO become Chapter entries with ordinal
    /// names (dvd.rs maps chapter_times → Chapter{time_secs, chapter_name}).
    #[test]
    fn scan_dvd_titles_chapters_present() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]);
        let vts = build_vts(0, 0x00, &[], &[], &[(0, 9)], false);
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0[0];
        // One program in the program map → one chapter time (0.0 for the
        // first program). Name is the ordinal from chapter_name(0).
        assert_eq!(t.chapters.len(), 1);
        assert_eq!(t.chapters[0].name, chapter_name(0));
    }

    // A navigation pack at `lba` whose interleaved unit ends `end_rel` sectors on and whose
    // next unit starts `next_rel` on.
    fn nav_pack(disc: &mut MemDisc, lba: u32, end_rel: u32, next_rel: u32) {
        let mut s = [0u8; 2048];
        s[..4].copy_from_slice(&[0, 0, 1, 0xBA]);
        s[0x400..0x404].copy_from_slice(&[0, 0, 1, 0xBF]);
        s[0x406] = 0x01;
        s[super::DSI_ILVU_EA..super::DSI_ILVU_EA + 4].copy_from_slice(&end_rel.to_be_bytes());
        s[super::DSI_NEXT_ILVU_SA..super::DSI_NEXT_ILVU_SA + 4]
            .copy_from_slice(&next_rel.to_be_bytes());
        disc.put(lba, s);
    }

    fn icell(first: u32, last: u32) -> ifo::DvdCell {
        ifo::DvdCell {
            first_sector: first,
            last_sector: last,
            category: 0x04,
            duration_secs: 0.0,
        }
    }

    // An interleaved cell reads only its own units, following each unit's pack to the next;
    // both end-of-chain spellings stop the walk.
    #[test]
    fn an_interleaved_cell_reads_only_its_own_units() {
        for end_marker in [0x7FFF_FFFF, 0xFFFF_FFFF] {
            let mut disc = MemDisc::new();
            let vob = 1000;
            nav_pack(&mut disc, vob + 100, 9, 30); // unit 100..=109, next at 130
            nav_pack(&mut disc, vob + 130, 4, 20); // unit 130..=134, next at 150
            nav_pack(&mut disc, vob + 150, 9, end_marker); // unit 150..=159, last
            let units = super::interleaved_units(&mut disc, vob, &icell(100, 159)).unwrap();
            let spans: Vec<(u32, u32)> = units
                .iter()
                .map(|e| (e.start_lba, e.sector_count))
                .collect();
            assert_eq!(spans, vec![(1100, 10), (1130, 5), (1150, 10)]);
        }
    }

    // A unit that runs past the cell is cut at the cell's end; a sector that is no navigation
    // pack leaves the caller the whole range.
    #[test]
    fn an_interleaved_cell_unit_walk_stays_inside_the_cell() {
        let mut disc = MemDisc::new();
        nav_pack(&mut disc, 100, 50, 0x7FFF_FFFF);
        let units = super::interleaved_units(&mut disc, 0, &icell(100, 120)).unwrap();
        assert_eq!((units[0].start_lba, units[0].sector_count), (100, 21));
        assert!(super::interleaved_units(&mut disc, 0, &icell(500, 600)).is_none());
    }

    /// A chapter named in the VMG text data reaches Chapter.name in place of
    /// the ordinal.
    #[test]
    fn scan_dvd_titles_chapter_names_from_text_data() {
        let mut disc = MemDisc::new();
        let txtdt = ifo::build_txtdt_mg(0x11, &[(0x02, b"feature"), (0x04, b"Opening")]);
        let vmg = ifo::with_txtdt(build_vmg(&[(1, 1, 1)]), &txtdt);
        let vts = build_vts(0, 0x00, &[], &[], &[(0, 9)], false);
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0[0];
        assert_eq!(t.chapters.len(), 1);
        assert_eq!(t.chapters[0].name, "Opening");
    }

    /// Build a VTS with explicit per-cell category bytes and an N-program map.
    /// Returns the IFO bytes. Cells: `(first, last, category, dur_secs)`.
    fn build_vts_cells(
        vob_start: u32,
        video_b0: u8,
        cells: &[(u32, u32, u8, u8 /*BCD seconds*/)],
        program_first_cells: &[u8],
    ) -> Vec<u8> {
        let pgcit_sector = 2u32;
        let mut d = vec![0u8; 4 * 2048];
        d[0..12].copy_from_slice(b"DVDVIDEO-VTS");
        d[0xC4..0xC8].copy_from_slice(&vob_start.to_be_bytes()); // vtstt_vobs (Title VOBS)
        d[0xCC..0xD0].copy_from_slice(&pgcit_sector.to_be_bytes());
        d[0x200] = video_b0;
        // no audio / subs
        let pg = pgcit_sector as usize * 2048;
        d[pg..pg + 2].copy_from_slice(&1u16.to_be_bytes());
        let pgc_rel: u32 = 0x100;
        d[pg + 8 + 4..pg + 8 + 8].copy_from_slice(&pgc_rel.to_be_bytes());
        let pgc = pg + pgc_rel as usize;
        d[pgc + 0x02] = program_first_cells.len() as u8; // nr_of_programs
        d[pgc + 0x03] = cells.len() as u8; // nr_of_cells
        // Leave PGC-level BCD time zero → duration recomputed from cells.
        let cell_tbl_rel: u16 = 0xF0;
        let pgm_map_rel: u16 = 0xEC;
        d[pgc + 0xE6..pgc + 0xE8].copy_from_slice(&pgm_map_rel.to_be_bytes());
        d[pgc + 0xE8..pgc + 0xEA].copy_from_slice(&cell_tbl_rel.to_be_bytes());
        for (i, &fc) in program_first_cells.iter().enumerate() {
            d[pgc + pgm_map_rel as usize + i] = fc;
        }
        let cell_base = pgc + cell_tbl_rel as usize;
        for (i, (first, last, cat, secs)) in cells.iter().enumerate() {
            let off = cell_base + i * 24;
            write_cell_cat(&mut d, off, *first, *last, *cat);
            d[off + 6] = *secs; // BCD seconds in the cell time field
        }
        d
    }

    // A leading interleaved-angle sub-block cell (category 0x90) must be dropped from muxed
    // extents; chapters shift earlier by its duration.
    #[test]
    fn scan_dvd_titles_drops_leading_scene_index_cell() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(2, 1, 1)]);
        // Cell 0: leading scene-index/angle sub-block (cat 0x90), 5s, sectors 0..9.
        // Cell 1: feature start (cat 0x00), 59s, 100..199. Cell 2: feature, 59s, 300..399.
        // Programs: prog0 -> cell 1 (feature start), prog1 -> cell 3.
        let vts = build_vts_cells(
            1000,
            0x00,
            &[
                (0, 9, 0x90, 0x05),
                (100, 199, 0x00, 0x59),
                (300, 399, 0x00, 0x59),
            ],
            &[1, 3],
        );
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0[0];
        // The leading 0x90 cell is dropped: 2 feature extents, not 3.
        assert_eq!(t.extents.len(), 2, "leading angle sub-block cell dropped");
        // First extent starts at the feature cell (vob 1000 + 100), not at 1000+0.
        assert_eq!(t.extents[0].start_lba, 9000 + 1000 + 100); // ifo_lba + vtstt + first
        assert_eq!(t.extents[1].start_lba, 9000 + 1000 + 300);
        // Chapter times shift earlier by the dropped 5s. Program 0 was at the
        // dropped head (clamped to 0); program 1 was at cell 3 =
        // dur(cell0)+dur(cell1) = 5 + 59 = 64s, now 59s after the 5s shift.
        assert_eq!(t.chapters.len(), 2);
        assert!(
            (t.chapters[0].time_secs - 0.0).abs() < 0.01,
            "ch0 clamped to 0, got {}",
            t.chapters[0].time_secs
        );
        assert!(
            (t.chapters[1].time_secs - 59.0).abs() < 0.01,
            "ch1 shifted by dropped 5s → 59s, got {}",
            t.chapters[1].time_secs
        );
    }

    /// Conservative guard end-to-end: a normal feature (every cell category
    /// 0x00) is muxed in full — the filter drops nothing and chapters are
    /// unshifted. This is the plain-single-angle-feature case.
    #[test]
    fn scan_dvd_titles_plain_feature_untouched() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(2, 1, 1)]);
        let vts = build_vts_cells(
            1000,
            crate::ifo::v_atr_byte(crate::ifo::VIDEO_FORMAT_PAL, crate::ifo::ASPECT_16X9),
            &[(0, 99, 0x00, 0x30), (200, 299, 0x00, 0x30)],
            &[1, 2],
        );
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let t = &Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0[0];
        // Nothing dropped: both cells become extents, starting at the very head.
        assert_eq!(t.extents.len(), 2);
        assert_eq!(t.extents[0].start_lba, 9000 + 1000); // ifo_lba + vtstt + 0, head intact
        assert_eq!(t.extents[1].start_lba, 9000 + 1200);
        // Chapter 0 stays at 0.0 (no shift).
        assert!((t.chapters[0].time_secs - 0.0).abs() < 0.01);
    }

    // Positional MP2 routes to PES ids 0xC0|i (not the old never-fed 0xBD00 + i).
    #[test]
    fn scan_dvd_titles_mp2_audio_routes_to_pes_ids() {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]);
        // coding_mode bits are b0>>5 & 0x7; mode 2 = MPEG-1 Layer II (Mp2).
        // b0 = 0b010_00000 = 0x40. b1 = 0 (mono, sample rate 48k).
        let audio = [(0x40u8, 0x00u8, [0u8, 0u8]), (0x40u8, 0x00u8, [0u8, 0u8])];
        let vts = build_vts(1000, 0x00, &audio, &[], &[(10, 109)], false);
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let titles = Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0;
        let t = &titles[0];
        let audio_pids: Vec<u16> = t
            .streams
            .iter()
            .filter_map(|s| match s {
                Stream::Audio(a) => Some(a.pid),
                _ => None,
            })
            .collect();
        assert_eq!(audio_pids, vec![0x00C0u16, 0x00C1u16]);
    }

    fn scan_cells(cells: &[(u32, u32, u8, u8)], programs: &[u8], nchap: u16) -> DiscTitle {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(nchap, 1, 1)]);
        let vts = build_vts_cells(1000, 0x00, cells, programs);
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        Disc::scan_dvd_titles(&mut disc, &udf, None)
            .expect("scan")
            .0
            .remove(0)
    }

    // Dropped head time comes off the duration, and head chapters collapse to one mark at 0.
    #[test]
    fn scan_dvd_titles_dropped_head_adjusts_duration_and_chapters() {
        let t = scan_cells(
            &[
                (0, 9, 0x90, 0x05),
                (10, 19, 0x90, 0x05),
                (100, 199, 0x00, 0x20),
                (300, 399, 0x00, 0x20),
            ],
            &[1, 2, 3, 4],
            4,
        );
        assert!(
            (t.duration_secs - 40.0).abs() < 0.01,
            "got {}",
            t.duration_secs
        );
        let times: Vec<f64> = t.chapters.iter().map(|c| c.time_secs).collect();
        assert_eq!(times.iter().filter(|&&x| x <= 0.0).count(), 1, "{times:?}");
        assert_eq!(t.chapters[0].name, "1");
    }

    // VIDEO_TS / VTS IFOs are garbage or real, BUPs are garbage or real.
    fn scan_ifo_bup(ifo_ok: bool, bup_ok: bool) -> Result<Vec<DiscTitle>> {
        let mut disc = MemDisc::new();
        let vmg = build_vmg(&[(1, 1, 1)]);
        let vts = build_vts_cells(1000, 0x00, &[(0, 99, 0x00, 0x10)], &[1]);
        let pick = |ok: bool, good: &Vec<u8>| if ok { good.clone() } else { vec![0x55; 4096] };
        let spec = |name: &str, icb_lba, data_lba, contents| FileSpec {
            name: name.into(),
            icb_lba,
            data_lba,
            contents,
        };
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                spec("VIDEO_TS.IFO", 60, 5000, pick(ifo_ok, &vmg)),
                spec("VTS_01_0.IFO", 62, 6000, pick(ifo_ok, &vts)),
                spec("VIDEO_TS.BUP", 64, 7000, pick(bup_ok, &vmg)),
                spec("VTS_01_0.BUP", 66, 8000, pick(bup_ok, &vts)),
            ],
        );
        Disc::scan_dvd_titles(&mut disc, &udf, None).map(|r| r.0)
    }

    // A bad IFO falls back to its BUP; VOBs stay anchored at the IFO's own LBA.
    #[test]
    fn scan_dvd_titles_bad_ifo_uses_bup() {
        let good = scan_ifo_bup(true, false).expect("good ifo");
        let bup = scan_ifo_bup(false, true).expect("bup");
        assert_eq!(bup.len(), 1);
        assert_eq!(bup[0].extents[0].start_lba, good[0].extents[0].start_lba);
    }

    // IFO and BUP both bad: no titles, not a scan error (base behaviour).
    #[test]
    fn scan_dvd_titles_bad_ifo_and_bup_is_empty() {
        assert_eq!(scan_ifo_bup(false, false).expect("no error").len(), 0);
    }

    fn title(vts_title_num: u8, pgcs: usize) -> ifo::DvdTitle {
        ifo::DvdTitle {
            chapters: 1,
            duration_secs: 1.0,
            cells: Vec::new(),
            chapter_times: Vec::new(),
            palette: None,
            ast_ctl: [0; 8],
            spst_ctl: [0; 32],
            vts_title_num,
            pgcs,
        }
    }

    // A rip scans its disc more than once: the split-title warning is one line per disc,
    // naming every split title, and a rescan of the same disc logs it at debug only.
    #[test]
    fn multi_pgc_titles_warn_once_per_disc() {
        let set = |n: u8, titles: Vec<ifo::DvdTitle>| ifo::DvdTitleSet {
            vts_number: n,
            vob_start_sector: 0,
            video: ifo::DvdVideoAttr {
                codec: Codec::Mpeg2,
                resolution: Resolution::R480i,
                aspect: ifo::DvdAspect::R16x9,
                standard: ifo::TvSystem::Ntsc,
            },
            audio_streams: Vec::new(),
            subtitle_streams: Vec::new(),
            titles,
        };
        let info = ifo::DvdInfo {
            title_sets: vec![
                set(3, vec![title(1, 8)]),
                set(4, vec![title(1, 5), title(2, 1)]),
                set(11, vec![title(1, 1)]),
            ],
            chapter_names: Default::default(),
            region_mask: 0,
        };
        let vmg = b"multi_pgc_titles_warn_once_per_disc".to_vec();
        let ((), ev) = crate::testlog::capture(|| {
            warn_multi_pgc_titles(&vmg, &info);
            warn_multi_pgc_titles(&vmg, &info);
            warn_multi_pgc_titles(&vmg, &info);
        });
        let warns: Vec<_> = ev
            .iter()
            .filter(|e| e.level == tracing::Level::WARN)
            .collect();
        assert_eq!(warns.len(), 1, "{ev:?}");
        assert_eq!(
            warns[0].field("titles"),
            Some("VTS 3 title 1 (8 PGCs), VTS 4 title 1 (5 PGCs)")
        );
        // A disc with no split title logs nothing.
        let clear = ifo::DvdInfo {
            title_sets: vec![set(1, vec![title(1, 1)])],
            chapter_names: Default::default(),
            region_mask: 0,
        };
        let ((), ev) = crate::testlog::capture(|| warn_multi_pgc_titles(b"no split", &clear));
        assert!(ev.is_empty(), "{ev:?}");
    }

    #[test]
    fn dvd_region_decodes_the_prohibited_region_mask() {
        assert_eq!(dvd_region(0x00), DiscRegion::Free);
        // Only bit 0 clear: playable in region 1 alone.
        assert_eq!(dvd_region(0xFE), DiscRegion::Dvd(vec![1]));
        assert_eq!(dvd_region(0xFD), DiscRegion::Dvd(vec![2]));
        assert_eq!(dvd_region(0xF2), DiscRegion::Dvd(vec![1, 3, 4]));
        assert_eq!(dvd_region(0x01), DiscRegion::Dvd(vec![2, 3, 4, 5, 6, 7, 8]));
        assert_eq!(dvd_region(0xFF), DiscRegion::Dvd(vec![]));
    }

    #[test]
    fn audio_code_extension_maps_to_purpose() {
        assert_eq!(audio_purpose(0), LabelPurpose::Normal);
        assert_eq!(audio_purpose(1), LabelPurpose::Normal);
        assert_eq!(audio_purpose(2), LabelPurpose::Descriptive);
        assert_eq!(audio_purpose(3), LabelPurpose::Commentary);
        assert_eq!(audio_purpose(4), LabelPurpose::Commentary);
        assert_eq!(audio_purpose(5), LabelPurpose::Normal);
    }

    #[test]
    fn subpicture_code_extension_maps_to_forced_and_qualifier() {
        let none = (false, LabelQualifier::None);
        let sdh = (false, LabelQualifier::Sdh);
        for (ext, want) in [
            (0, none),
            (1, none),
            (2, none),
            (3, none),
            (5, sdh),
            (6, sdh),
            (7, sdh),
            (9, (true, LabelQualifier::Forced)),
            (13, none),
            (14, none),
            (15, none),
        ] {
            assert_eq!(subpicture_label(ext), want, "code extension {ext}");
        }
    }

    // Scans a one-VTS disc built from `vmg` and `vts`, keeping every result.
    fn scan_disc(vmg: Vec<u8>, vts: Vec<u8>) -> (Vec<DiscTitle>, DiscRegion) {
        let mut disc = MemDisc::new();
        let udf = build_video_ts_fs(
            &mut disc,
            &[
                FileSpec {
                    name: "VIDEO_TS.IFO".into(),
                    icb_lba: 60,
                    data_lba: 5000,
                    contents: vmg,
                },
                FileSpec {
                    name: "VTS_01_0.IFO".into(),
                    icb_lba: 62,
                    data_lba: 6000,
                    contents: vts,
                },
            ],
        );
        let (titles, _, region) = Disc::scan_dvd_titles(&mut disc, &udf, None).expect("scan");
        (titles, region)
    }

    #[test]
    fn scan_dvd_titles_reports_the_vmg_region() {
        let vts = || build_vts(0, 0x00, &[], &[], &[(0, 9)], false);
        assert_eq!(
            scan_disc(build_vmg(&[(1, 1, 1)]), vts()).1,
            DiscRegion::Free
        );
        let mut vmg = build_vmg(&[(1, 1, 1)]);
        vmg[0x23] = 0xFD;
        assert_eq!(scan_disc(vmg, vts()).1, DiscRegion::Dvd(vec![2]));
    }

    // The code extension belongs to the LOGICAL stream, so its label must follow the
    // AST_CTL / SPST_CTL routing to whichever physical stream that logical stream plays.
    #[test]
    fn scan_dvd_titles_code_extension_labels_follow_the_routed_stream() {
        let audio = [aud(AC3_6CH, b"en"), aud(AC3_6CH, b"en")];
        let mut vts = build_vts(0, 0x00, &audio, &[*b"de", *b"de"], &[(0, 9)], false);
        vts[0x204 + 8 + 5] = 3; // logical audio 1: director's comments
        vts[0x256 + 6 + 5] = 9; // logical subpicture 1: forced
        set_ast(&mut vts, &[0x8100, 0x8000]);
        set_spst(&mut vts, &[0x8100_0000, 0x8000_0000]);
        let (titles, _) = scan_disc(build_vmg(&[(1, 1, 1)]), vts);
        let mut audio = Vec::new();
        let mut subs = Vec::new();
        for s in &titles[0].streams {
            match s {
                Stream::Audio(a) => audio.push((a.pid, a.purpose)),
                Stream::Subtitle(s) => subs.push((s.pid, s.forced, s.qualifier)),
                _ => {}
            }
        }
        assert_eq!(
            audio,
            vec![
                (0xBD81, LabelPurpose::Normal),
                (0xBD80, LabelPurpose::Commentary)
            ]
        );
        assert_eq!(
            subs,
            vec![
                (0x21, false, LabelQualifier::None),
                (0x20, true, LabelQualifier::Forced)
            ]
        );
    }
}
