//! Structured scan diagnostics — the `--log-level 3` self-diagnosing dump.
//!
//! A bug report log must be self-diagnosing: everything needed to explain
//! *why* freemkv made the choices it did at scan must be in the log, in a
//! compact, machine-parseable form. This module emits one terse line per row
//! (title, cell, stream, decision) under the `tracing` target
//! `freemkv::diag`, which the CLI routes to `log.txt` when `--log-level 3`
//! (debug) is set.
//!
//! Format conventions (stable, greppable):
//!   - Every line is prefixed by a `tag=` so a log scraper can filter
//!     (`disc`, `title`, `dvd.cell`, `dvd.vattr`, `dvd.aattr`, `bd.clip`,
//!     `bd.mark`, `aacs`, `stream`, `decision`).
//!   - Raw bytes are shown as `0xNN` next to their decode so a wrong decode
//!     is obvious against the raw value.
//!   - This module only READS already-parsed scan state — it never re-reads
//!     the disc and never mutates anything.
//!
//! The DVD per-cell table (with the raw cell-category byte) is emitted from
//! the IFO scan itself ([`dump_dvd_cells`]), because the per-cell
//! `ifo::DvdCell` detail is lowered away before the `Disc` is built. The
//! `Disc`-level dump ([`dump_disc`]) covers everything that survives
//! lowering: titles, streams, the picked main feature, and AACS state.

use crate::disc::{
    AudioChannels, ColorSpace, Disc, DiscTitle, FrameRate, HdrFormat, Resolution, SampleRate,
    Stream,
};
use crate::ifo::{CellCategory, DvdTitle};

const DIAG: &str = "freemkv::diag";

// ── small format helpers (pure, unit-testable) ──────────────────────────────

/// Compact name for a [`Resolution`] with the interlace marker preserved.
pub fn res_str(r: Resolution) -> &'static str {
    match r {
        Resolution::R480i => "480i",
        Resolution::R480p => "480p",
        Resolution::R576i => "576i",
        Resolution::R576p => "576p",
        Resolution::R720p => "720p",
        Resolution::R1080i => "1080i",
        Resolution::R1080p => "1080p",
        Resolution::R2160p => "2160p",
        Resolution::R4320p => "4320p",
        Resolution::Unknown => "res?",
    }
}

/// Frames-per-second string for a [`FrameRate`].
pub fn fps_str(f: FrameRate) -> &'static str {
    match f {
        FrameRate::F23_976 => "23.976",
        FrameRate::F24 => "24",
        FrameRate::F25 => "25",
        FrameRate::F29_97 => "29.97",
        FrameRate::F30 => "30",
        FrameRate::F50 => "50",
        FrameRate::F59_94 => "59.94",
        FrameRate::F60 => "60",
        FrameRate::Unknown => "fps?",
    }
}

/// PAL/NTSC field-rate family inferred from the frame rate (DVD has no
/// explicit field, so this is the colour/standard the muxer stamps).
pub fn tv_system_str(f: FrameRate) -> &'static str {
    match f {
        FrameRate::F25 | FrameRate::F50 => "PAL",
        FrameRate::F23_976 | FrameRate::F29_97 | FrameRate::F59_94 => "NTSC",
        _ => "—",
    }
}

/// CICP-ish short name for a [`ColorSpace`].
pub fn color_str(c: ColorSpace) -> &'static str {
    match c {
        ColorSpace::Bt709 => "BT.709",
        ColorSpace::Bt2020 => "BT.2020",
        ColorSpace::Bt470bg => "BT.470BG",
        ColorSpace::Smpte170m => "SMPTE-170M",
        ColorSpace::Unknown => "color?",
    }
}

/// HDR format short name.
pub fn hdr_str(h: HdrFormat) -> &'static str {
    match h {
        HdrFormat::Sdr => "SDR",
        HdrFormat::Hdr10 => "HDR10",
        HdrFormat::Hdr10Plus => "HDR10+",
        HdrFormat::DolbyVision => "DoVi",
        HdrFormat::Hlg => "HLG",
    }
}

/// Channel count from an [`AudioChannels`] layout (what lands in the MKV
/// `Channels` element).
pub fn channel_count(ch: AudioChannels) -> u8 {
    match ch {
        AudioChannels::Mono => 1,
        AudioChannels::Stereo => 2,
        AudioChannels::Stereo21 => 3,
        AudioChannels::Quad => 4,
        AudioChannels::Surround50 => 5,
        AudioChannels::Surround51 => 6,
        AudioChannels::Surround61 => 7,
        AudioChannels::Surround71 => 8,
        AudioChannels::Unknown => 0,
    }
}

/// Sample-rate in Hz for a [`SampleRate`].
pub fn sample_rate_hz(s: SampleRate) -> u32 {
    match s {
        SampleRate::S44_1 => 44100,
        SampleRate::S48 => 48000,
        SampleRate::S96 => 96000,
        SampleRate::S192 => 192000,
        SampleRate::S48_96 => 96000,
        SampleRate::S48_192 => 192000,
        SampleRate::Unknown => 0,
    }
}

// ── DVD cell-category dump (from the IFO scan, pre-lowering) ─────────────────

/// One formatted cell row for the DVD per-PGC cell table. Returned as a
/// string so it can be unit-tested without a logger.
///
/// Columns: `idx`, raw category (`cat=0xNN`) + decoded fields, first/last
/// sector, duration, and the keep/drop verdict from the bug-4 leading-cell
/// filter.
pub fn dvd_cell_row(idx: usize, cell: &crate::ifo::DvdCell, dropped: bool) -> String {
    let c = CellCategory::decode(cell.category);
    format!(
        "tag=dvd.cell idx={idx} cat=0x{:02X} type={} block_mode={} block_type={} \
seamless={} ilv={} plain={} first={} last={} dur={:.1}s {}",
        cell.category,
        c.cell_type,
        c.block_mode,
        c.block_type,
        c.seamless_play as u8,
        c.interleaved as u8,
        c.is_plain_feature() as u8,
        cell.first_sector,
        cell.last_sector,
        cell.duration_secs,
        if dropped { "DROP(non-feature)" } else { "keep" },
    )
}

/// Emit the per-PGC cell table for one DVD title during the IFO scan.
///
/// `vts`/`title` identify the row group; `title` is the `DvdTitle` whose
/// cells (and bug-4 leading-cell verdict) are dumped. Called from
/// `scan_dvd_titles` while the `DvdTitle` is still in scope (the per-cell
/// category byte is lowered away before the `Disc` exists).
pub fn dump_dvd_cells(vts: u8, title_num: u16, title: &DvdTitle) {
    if !tracing::enabled!(target: DIAG, tracing::Level::DEBUG) {
        return;
    }
    let feature_start = title.feature_start_cell();
    tracing::debug!(
        target: DIAG,
        "tag=dvd.pgc vts={vts} title={title_num} cells={} chapters={} \
    dur={:.1}s feature_start_cell={feature_start}",
        title.cells.len(),
        title.chapters,
        title.duration_secs,
    );
    for (i, cell) in title.cells.iter().enumerate() {
        tracing::debug!(target: DIAG, "{}", dvd_cell_row(i, cell, i < feature_start));
    }
    // Chapter/PTT map (program → cumulative start time).
    for (i, &t) in title.chapter_times.iter().enumerate() {
        tracing::debug!(
            target: DIAG,
            "tag=dvd.chap vts={vts} title={title_num} ch={} time={:.1}s",
            i + 1,
            t,
        );
    }
}

/// Emit the IFO `video_attr` / `audio_attr` decode for one DVD title set,
/// showing the raw bytes next to their decoded meaning. Called from the IFO
/// scan with the still-parsed `ifo::DvdTitleSet` view.
pub fn dump_dvd_attrs(ts: &crate::ifo::DvdTitleSet) {
    if !tracing::enabled!(target: DIAG, tracing::Level::DEBUG) {
        return;
    }
    tracing::debug!(
        target: DIAG,
        "tag=dvd.vobs vts={} vob_start_sector={}",
        ts.vts_number,
        ts.vob_start_sector,
    );
    let v = &ts.video;
    tracing::debug!(
        target: DIAG,
        "tag=dvd.vattr vts={} codec={:?} res={} aspect={:?} std={:?}",
        ts.vts_number,
        v.codec,
        res_str(v.resolution),
        v.aspect,
        v.standard,
    );
    for (i, a) in ts.audio_streams.iter().enumerate() {
        tracing::debug!(
            target: DIAG,
            "tag=dvd.aattr vts={} idx={i} codec={:?} ch={} sr={}Hz lang={:?} sub_id={:?}",
            ts.vts_number,
            a.codec,
            a.channels,
            a.sample_rate,
            a.language,
            a.sub_stream_id.map(|x| format!("0x{x:02X}")),
        );
    }
    for (i, s) in ts.subtitle_streams.iter().enumerate() {
        tracing::debug!(
            target: DIAG,
            "tag=dvd.sattr vts={} idx={i} lang={:?}",
            ts.vts_number,
            s.language,
        );
    }
}

// ── Disc-level dump (post-lowering: titles, streams, decisions, AACS) ────────

/// Emit the full scan diagnostic block for a built [`Disc`]. Terse, one line
/// per row, under target `freemkv::diag` at DEBUG. No-op unless that target
/// is enabled, so it costs nothing when `--log-level 3` is off.
pub fn dump_disc(disc: &Disc) {
    if !tracing::enabled!(target: DIAG, tracing::Level::DEBUG) {
        return;
    }

    tracing::debug!(
        target: DIAG,
        "tag=disc vol={:?} format={:?} content={:?} cap_sectors={} layers={} titles={} encrypted={}",
        disc.volume_id,
        disc.format,
        disc.content_format,
        disc.capacity_sectors,
        disc.layers,
        disc.titles.len(),
        disc.encrypted,
    );

    dump_aacs(disc);

    for (ti, title) in disc.titles.iter().enumerate() {
        dump_title(ti, title);
    }

    // freemkv's top-level DECISION: which title is the main feature.
    if let Some(main) = disc.titles.first() {
        tracing::debug!(
            target: DIAG,
            "tag=decision pick=main_feature title_idx=0 playlist={:?} dur={:.1}s \
        size={}B clips={} reason=canonical_title_order(fits-disc, fewest-clips, longest, richest-audio)",
            main.playlist,
            main.duration_secs,
            main.size_bytes,
            main.clips.len(),
        );
    }
}

fn dump_aacs(disc: &Disc) {
    let Some(a) = disc.aacs.as_ref() else {
        if disc.css.is_some() {
            tracing::debug!(target: DIAG, "tag=aacs none crypto=CSS(DVD)");
        } else if disc.encrypted {
            tracing::debug!(target: DIAG, "tag=aacs none crypto=encrypted-no-keys");
        } else {
            tracing::debug!(target: DIAG, "tag=aacs none crypto=clear");
        }
        return;
    };
    // CPS-unit / unit-key counts: at scan `unit_keys` is empty (keys are
    // resolved later); the unit-key count is the BE16 in the raw
    // Unit_Key_RO.inf if captured. Report both: resolved count and raw len.
    tracing::debug!(
        target: DIAG,
        "tag=aacs version={} bus_enc={} mkb_version={:?} disc_hash={} key_source={:?} \
    vuk={} unit_keys_resolved={} uk_ro_bytes={} mkb_bytes={}",
        a.version,
        a.bus_encryption,
        a.mkb_version,
        a.disc_hash,
        a.key_source.name(),
        a.vuk.is_some(),
        a.unit_keys.len(),
        a.uk_ro.len(),
        a.mkb.len(),
    );
}

fn dump_title(ti: usize, title: &DiscTitle) {
    let (mut nv, mut na, mut ns) = (0u32, 0u32, 0u32);
    for s in &title.streams {
        match s {
            Stream::Video(_) => nv += 1,
            Stream::Audio(_) => na += 1,
            Stream::Subtitle(_) => ns += 1,
        }
    }
    tracing::debug!(
        target: DIAG,
        "tag=title idx={ti} playlist={:?} id={} dur={:.1}s size={}B clips={} \
    extents={} chapters={} v={nv} a={na} s={ns} fmt={:?}",
        title.playlist,
        title.playlist_id,
        title.duration_secs,
        title.size_bytes,
        title.clips.len(),
        title.extents.len(),
        title.chapters.len(),
        title.content_format,
    );

    // Per-clip rows (BD: PlayItem/CLPI; DVD has none).
    for (ci, c) in title.clips.iter().enumerate() {
        tracing::debug!(
            target: DIAG,
            "tag=clip title={ti} idx={ci} id={:?} in={} out={} dur={:.1}s src_packets={}",
            c.clip_id,
            c.in_time,
            c.out_time,
            c.duration_secs,
            c.source_packets,
        );
    }

    // Per-extent rows (the sectors freemkv will actually rip — the bug-4
    // decision is visible here: leading non-feature cells are already gone).
    for (ei, e) in title.extents.iter().enumerate() {
        tracing::debug!(
            target: DIAG,
            "tag=extent title={ti} idx={ei} start_lba={} sectors={}",
            e.start_lba,
            e.sector_count,
        );
    }

    // freemkv's per-stream DECISIONS (what the muxer will write).
    for (si, s) in title.streams.iter().enumerate() {
        match s {
            Stream::Video(v) => tracing::debug!(
                target: DIAG,
                "tag=stream title={ti} idx={si} kind=video pid=0x{:04X} codec={:?} \
            res={} interlaced={} fps={} std={} color={} hdr={} aspect={:?} secondary={}",
                v.pid,
                v.codec,
                res_str(v.resolution),
                v.resolution.is_interlaced(),
                fps_str(v.frame_rate),
                tv_system_str(v.frame_rate),
                color_str(v.color_space),
                hdr_str(v.hdr),
                v.display_aspect,
                v.secondary,
            ),
            Stream::Audio(a) => tracing::debug!(
                target: DIAG,
                "tag=stream title={ti} idx={si} kind=audio pid=0x{:04X} codec={:?} \
            channels={}({}) sr={}Hz lang={:?} secondary={}",
                a.pid,
                a.codec,
                a.channels,
                channel_count(a.channels),
                sample_rate_hz(a.sample_rate),
                a.language,
                a.secondary,
            ),
            Stream::Subtitle(sub) => tracing::debug!(
                target: DIAG,
                "tag=stream title={ti} idx={si} kind=subtitle pid=0x{:04X} codec={:?} \
            lang={:?} forced={}",
                sub.pid,
                sub.codec,
                sub.language,
                sub.forced,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn res_str_keeps_interlace_marker() {
        assert_eq!(res_str(Resolution::R576i), "576i");
        assert_eq!(res_str(Resolution::R480i), "480i");
        assert_eq!(res_str(Resolution::R2160p), "2160p");
    }

    #[test]
    fn fps_and_tv_system() {
        assert_eq!(fps_str(FrameRate::F25), "25");
        assert_eq!(tv_system_str(FrameRate::F25), "PAL");
        assert_eq!(fps_str(FrameRate::F29_97), "29.97");
        assert_eq!(tv_system_str(FrameRate::F29_97), "NTSC");
    }

    #[test]
    fn color_and_hdr() {
        assert_eq!(color_str(ColorSpace::Bt470bg), "BT.470BG");
        assert_eq!(color_str(ColorSpace::Bt2020), "BT.2020");
        assert_eq!(hdr_str(HdrFormat::Hdr10), "HDR10");
        assert_eq!(hdr_str(HdrFormat::DolbyVision), "DoVi");
        assert_eq!(hdr_str(HdrFormat::Sdr), "SDR");
    }

    #[test]
    fn channel_count_matches_layout() {
        assert_eq!(channel_count(AudioChannels::Mono), 1);
        assert_eq!(channel_count(AudioChannels::Stereo), 2);
        assert_eq!(channel_count(AudioChannels::Surround51), 6);
        assert_eq!(channel_count(AudioChannels::Surround71), 8);
    }

    #[test]
    fn sample_rate_hz_values() {
        assert_eq!(sample_rate_hz(SampleRate::S48), 48000);
        assert_eq!(sample_rate_hz(SampleRate::S96), 96000);
    }

    /// The cell row shows the raw category byte (0xNN) beside the decode, and
    /// the keep/drop verdict. A plain feature cell (0x00) is "keep"; a leading
    /// secondary-block cell flagged dropped reads "DROP".
    #[test]
    fn cell_row_shows_raw_byte_and_verdict() {
        let plain = crate::ifo::DvdCell {
            first_sector: 100,
            last_sector: 199,
            category: 0x00,
            duration_secs: 12.5,
        };
        let row = dvd_cell_row(0, &plain, false);
        assert!(row.contains("cat=0x00"), "{row}");
        assert!(row.contains("type=0"), "{row}");
        assert!(row.contains("first=100"), "{row}");
        assert!(row.contains("last=199"), "{row}");
        assert!(row.contains("dur=12.5s"), "{row}");
        assert!(row.contains("keep"), "{row}");
        assert!(!row.contains("DROP"), "{row}");

        // 0x80 = middle-of-angle-block (cell_type=2), shown dropped.
        let sec = crate::ifo::DvdCell {
            first_sector: 0,
            last_sector: 9,
            category: 0x80,
            duration_secs: 1.0,
        };
        let row = dvd_cell_row(0, &sec, true);
        assert!(row.contains("cat=0x80"), "{row}");
        assert!(row.contains("type=2"), "{row}");
        assert!(row.contains("DROP(non-feature)"), "{row}");
    }
}
