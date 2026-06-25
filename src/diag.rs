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
    // Per-cell keep/skip REASON (self-sufficient bug log): a dropped cell is a
    // leading secondary angle/interleave block piece; a kept cell is either the
    // first feature cell or genuine feature content. This makes the
    // leading-cell-filter decision auditable from the log without the disc.
    let verdict = if dropped {
        "DROP(leading-secondary-block-piece)"
    } else if c.is_secondary_block_piece() {
        // Kept despite being a secondary piece — only happens past the leading
        // run (the filter stops at the first plain feature cell).
        "keep(feature-body)"
    } else {
        "keep(plain-feature)"
    };
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
        verdict,
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

/// Emit the ACTUAL per-physical-sub-stream AC-3 channel counts read off the VOB
/// during the mux-time sub-stream probe (the Silence-of-the-Lambs wrong-stream
/// fix). This is the ground truth the IFO nibble is compared against: each row
/// is `sub_id=0x8x channels=N` for a physical `private_stream_1` AC-3 sub-stream
/// whose first frame was decoded. An empty probe (scrambled / unreadable / short
/// VOB) logs a single `probed=0` line so the absence is explicit in a bug log.
///
/// Self-sufficiency: with `tag=dvd.aattr` (the IFO's declared sub_id + claimed
/// channels) and these `tag=dvd.substream` rows (the physical reality), a bug
/// log alone shows whether the ordinal `0x80` actually carries the declared
/// channel layout — no disc needed to diagnose a wrong-substream rip.
pub fn dump_dvd_substream_probe(title_id: u16, probed: &std::collections::BTreeMap<u8, u8>) {
    if !tracing::enabled!(target: DIAG, tracing::Level::DEBUG) {
        return;
    }
    if probed.is_empty() {
        tracing::debug!(
            target: DIAG,
            "tag=dvd.substream title={title_id} probed=0 (no AC-3 sync in feature head — scrambled/unreadable/none)",
        );
        return;
    }
    for (sub, ch) in probed {
        tracing::debug!(
            target: DIAG,
            "tag=dvd.substream title={title_id} sub_id=0x{sub:02X} channels={ch} (physical acmod read from VOB)",
        );
    }
}

// ── MKV TrackEntry dump (the ACTUAL container elements written) ──────────────

/// `true` when the `--log-level 3` diagnostic target is enabled. Hot-path
/// callers (the opening-frame capture) check this once and skip all work when
/// off, so a normal run pays nothing.
pub fn diag_enabled() -> bool {
    tracing::enabled!(target: DIAG, tracing::Level::DEBUG)
}

/// Cap on the number of codecPrivate bytes rendered to hex in a `tag=mkv.track`
/// line. The sequence header / avcC / hvcC prefix that matters for diagnosis
/// (resolution, frame rate, profile) is at the front; a multi-KB blob past this
/// is summarised as `..(+NB)` rather than flooding the log.
const CODEC_PRIVATE_HEX_CAP: usize = 64;

/// Render a track's codecPrivate as an uppercase-hex string for the diagnostic
/// line, capped at [`CODEC_PRIVATE_HEX_CAP`] bytes (`..(+NB)` suffix beyond).
/// `None` / empty → `"none"`. Pure (no logging) so it is directly unit-testable.
fn codec_private_hex(cp: Option<&[u8]>) -> String {
    match cp {
        Some(b) if !b.is_empty() => {
            use std::fmt::Write;
            let shown = b.len().min(CODEC_PRIVATE_HEX_CAP);
            let mut s = String::with_capacity(shown * 2 + 8);
            for byte in &b[..shown] {
                let _ = write!(s, "{byte:02X}");
            }
            if b.len() > CODEC_PRIVATE_HEX_CAP {
                let _ = write!(s, "..(+{}B)", b.len() - CODEC_PRIVATE_HEX_CAP);
            }
            s
        }
        _ => "none".to_string(),
    }
}

/// Frame the raw bytes of one captured opening frame for the `.opening.bin` side
/// file: `[track:u8][keyframe:u8][pts_ns:i64 LE][len:u32 LE][raw bytes]`. Pure
/// (no I/O) so the record layout is directly unit-testable; `record` appends the
/// returned bytes to the side file.
fn frame_record(track_idx: usize, pts_ns: i64, keyframe: bool, data: &[u8]) -> Vec<u8> {
    let mut rec = Vec::with_capacity(14 + data.len());
    rec.push(track_idx as u8);
    rec.push(keyframe as u8);
    rec.extend_from_slice(&pts_ns.to_le_bytes());
    rec.extend_from_slice(&(data.len() as u32).to_le_bytes());
    rec.extend_from_slice(data);
    rec
}

/// Emit the MKV `TrackEntry` elements the muxer is about to WRITE for one
/// track — the Windows-fps-class metadata (FlagInterlaced, FieldOrder,
/// DefaultDuration, DefaultDecodedFieldDuration, Display dims) plus the
/// codecPrivate as hex. With this row a bug log alone is enough to verify why
/// Windows Explorer reports a given frame rate for an interlaced SD track: the
/// container values that drive its fps derivation are all present, no disc and
/// no MediaInfo needed.
///
/// `track_number` is the 1-based MKV track number; `track` is the built
/// [`crate::mux::mkv::MkvTrack`] whose fields map one-to-one onto the emitted
/// elements (see `MkvMuxer::new`). No-op unless the diag target is on.
pub fn dump_mkv_track(track_number: u64, track: &crate::mux::mkv::MkvTrack) {
    if !diag_enabled() {
        return;
    }
    // codecPrivate as hex (capped so a multi-KB hvcC doesn't flood the log; the
    // sequence header / avcC prefix that matters for diagnosis is at the front).
    let cp = codec_private_hex(track.codec_private.as_deref());
    let field_order = match track.field_order {
        crate::mux::ebml::FIELD_ORDER_TFF => "TFF",
        crate::mux::ebml::FIELD_ORDER_BFF => "BFF",
        _ => "—",
    };
    // FlagInterlaced is only written for video tracks (1=interlaced/2=progressive);
    // report what the muxer will emit, or "—" for non-video tracks where the
    // element is omitted entirely.
    let interlaced = if track.track_type == crate::mux::ebml::TRACK_TYPE_VIDEO {
        if track.interlaced {
            "1(interlaced)"
        } else {
            "2(progressive)"
        }
    } else {
        "—"
    };
    tracing::debug!(
        target: DIAG,
        "tag=mkv.track num={track_number} type={} codec={} flag_interlaced={interlaced} \
    field_order={field_order} default_duration_ns={} field_duration_ns={} \
    pixel={}x{} display={}x{} cp_len={} cp_hex={cp}",
        track.track_type,
        track.codec_id,
        track.default_duration_ns,
        track.field_duration_ns,
        track.pixel_width,
        track.pixel_height,
        track.display_width,
        track.display_height,
        track.codec_private.as_ref().map_or(0, |b| b.len()),
    );
}

// ── Opening-frame capture (first ~N coded frames per track → side file) ──────

/// Number of coded frames captured PER TRACK before the capture goes dormant.
/// ~100 frames covers a DVD's first few seconds of every track (the
/// opening-GOP / still-frame / menu window where mid-GOP open or PTS-floor bugs
/// show up) while bounding the side file to a few MB even for HD I-frames.
const OPENING_FRAMES_PER_TRACK: usize = 100;

/// Captures the first [`OPENING_FRAMES_PER_TRACK`] coded frames of EACH track to
/// a side file (`<output>.opening.bin`) and logs a per-frame summary line, so an
/// opening-GOP / menu / mid-GOP-open issue is diagnosable from a future log +
/// side file WITHOUT the disc. Gated to `--log-level 3`: constructed only when
/// the diag target is on, so a normal run never opens the file or records a byte.
///
/// Side-file record framing (so a reader can split it back into frames):
/// `[track:u8][keyframe:u8][pts_ns:i64 LE][len:u32 LE][raw frame bytes]`.
pub struct OpeningCapture {
    file: std::fs::File,
    /// Frames captured so far, per track index. Capture for a track stops once
    /// its counter reaches [`OPENING_FRAMES_PER_TRACK`].
    counts: Vec<usize>,
}

impl OpeningCapture {
    /// Open `<output>.opening.bin` next to the MKV output. Returns `None` (no
    /// capture) when the diag target is off OR the side file can't be created —
    /// a diagnostic must never fail the rip. `track_count` sizes the per-track
    /// counters.
    pub fn new(output_path: &std::path::Path, track_count: usize) -> Option<Self> {
        if !diag_enabled() {
            return None;
        }
        let mut name = output_path.as_os_str().to_os_string();
        name.push(".opening.bin");
        match std::fs::File::create(&name) {
            Ok(file) => {
                tracing::debug!(
                    target: DIAG,
                    "tag=mkv.opening.open path={:?} per_track_cap={OPENING_FRAMES_PER_TRACK}",
                    std::path::Path::new(&name),
                );
                Some(Self {
                    file,
                    counts: vec![0; track_count],
                })
            }
            Err(e) => {
                tracing::debug!(
                    target: DIAG,
                    "tag=mkv.opening.open path={:?} failed={e} (capture disabled, rip unaffected)",
                    std::path::Path::new(&name),
                );
                None
            }
        }
    }

    /// Record one coded frame for `track_idx` if that track is still under its
    /// per-track cap. Writes the framed raw bytes to the side file and logs a
    /// one-line summary. A write error disables further capture for the track
    /// (counter pinned to the cap) but never propagates — the rip is unaffected.
    pub fn record(&mut self, track_idx: usize, pts_ns: i64, keyframe: bool, data: &[u8]) {
        let Some(count) = self.counts.get_mut(track_idx) else {
            return;
        };
        if *count >= OPENING_FRAMES_PER_TRACK {
            return;
        }
        use std::io::Write;
        let rec = frame_record(track_idx, pts_ns, keyframe, data);
        if let Err(e) = self.file.write_all(&rec) {
            // Stop trying on this track; a broken side file must not stall mux.
            *count = OPENING_FRAMES_PER_TRACK;
            tracing::debug!(
                target: DIAG,
                "tag=mkv.opening.frame track={track_idx} write_failed={e} (capture stopped for track)",
            );
            return;
        }
        *count += 1;
        tracing::debug!(
            target: DIAG,
            "tag=mkv.opening.frame track={track_idx} n={count} type={} size={} pts_ns={pts_ns}",
            if keyframe { "key" } else { "delta" },
            data.len(),
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

    #[test]
    fn codec_private_hex_renders_caps_and_handles_empty() {
        // None / empty → "none" (no hex). The Windows-fps diagnosis only needs
        // the seq-header prefix, so render it but cap long blobs.
        assert_eq!(codec_private_hex(None), "none");
        assert_eq!(codec_private_hex(Some(&[])), "none");
        // Short blob: full uppercase hex, no suffix. An MPEG-2 seq header starts
        // 00 00 01 B3 — exactly what a reader greps for in a bug log.
        assert_eq!(
            codec_private_hex(Some(&[0x00, 0x00, 0x01, 0xB3])),
            "000001B3"
        );
        // Over the cap: first CODEC_PRIVATE_HEX_CAP bytes + a "..(+NB)" summary.
        let big = vec![0xABu8; CODEC_PRIVATE_HEX_CAP + 5];
        let s = codec_private_hex(Some(&big));
        assert!(s.starts_with(&"AB".repeat(CODEC_PRIVATE_HEX_CAP)), "{s}");
        assert!(s.ends_with("..(+5B)"), "{s}");
    }

    #[test]
    fn frame_record_layout_is_parseable() {
        // The .opening.bin record framing must round-trip so a future tool can
        // split the side file back into frames without the disc:
        // [track:u8][keyframe:u8][pts_ns:i64 LE][len:u32 LE][raw bytes].
        let data = [0xDEu8, 0xAD, 0xBE, 0xEF];
        let rec = frame_record(2, -40_000_000, true, &data);
        assert_eq!(rec.len(), 14 + data.len());
        assert_eq!(rec[0], 2, "track index");
        assert_eq!(rec[1], 1, "keyframe flag");
        assert_eq!(
            i64::from_le_bytes(rec[2..10].try_into().unwrap()),
            -40_000_000,
            "pts_ns survives (signed — opening back-anchor can be negative)"
        );
        assert_eq!(
            u32::from_le_bytes(rec[10..14].try_into().unwrap()),
            4,
            "len"
        );
        assert_eq!(&rec[14..], &data, "raw frame bytes follow");
        // A non-keyframe records the flag as 0.
        let delta = frame_record(0, 0, false, &[]);
        assert_eq!(delta[1], 0);
        assert_eq!(u32::from_le_bytes(delta[10..14].try_into().unwrap()), 0);
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
        assert!(row.contains("keep(plain-feature)"), "{row}");
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
        assert!(row.contains("DROP(leading-secondary-block-piece)"), "{row}");
    }
}
