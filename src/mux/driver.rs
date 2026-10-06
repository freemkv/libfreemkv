//! High-level mux driver: runs the `construct → headers gate → open sink →
//! pump → finish` pipeline the consumers (CLI `pipe`/`pipe_disc`, autorip
//! `run_mux`) each used to hand-roll.
//!
//! [`mux_with_keys`] DRIVES the existing pipeline via the same ISO pipeline builders
//! and a [`WRITE_PIPELINE_DEPTH`]-deep write [`Pipeline`]; it does not replace either.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::ctx::Ctx;
use crate::decrypt::DecryptKeys;
use crate::disc::DiscTitle;
use crate::error::Error;
use crate::event::Event;
use crate::halt::Halt;
use crate::io::FlushProgress;
use crate::io::pipeline::{Flow, Pipeline, Sink, WRITE_PIPELINE_DEPTH};
use crate::pes::{CountingStream, PesFrame, PesSink, PesSource};
use crate::sector::{FileSectorSource, SectorSource};
use crate::session::DiscSession;

use super::resolve::{
    InputOptions, SinkCaps, StreamUrl, build_iso_pipeline, output, output_with, parse_url,
};
#[cfg(test)]
use super::source::ScannedTitle;
use super::source::{Origin, Source};
use super::videomap::{Medium, SourceInfo};

// The source medium a parsed input URL denotes, used only for provenance. Exhaustive, so a
// new scheme must be placed; sink-only schemes never reach here and take the `File` default.
pub(crate) fn url_medium(parsed: &StreamUrl) -> Medium {
    match parsed {
        StreamUrl::Disc { .. } => Medium::Disc,
        // `dir://` is an image-level source: a UDF volume synthesized over the folder.
        StreamUrl::Iso { .. } | StreamUrl::Dir { .. } => Medium::Iso,
        StreamUrl::Mkv { .. }
        | StreamUrl::M2ts { .. }
        | StreamUrl::Mp4 { .. }
        | StreamUrl::Mpg { .. } => Medium::File,
        StreamUrl::Network { .. } | StreamUrl::Stdio => Medium::Stream,
        StreamUrl::Null
        | StreamUrl::Demux { .. }
        | StreamUrl::Video { .. }
        | StreamUrl::Audio { .. }
        | StreamUrl::Sub { .. }
        | StreamUrl::Fvi { .. }
        | StreamUrl::Chapters { .. }
        | StreamUrl::Json { .. }
        | StreamUrl::Unknown { .. } => Medium::File,
    }
}

// Ceiling on bytes buffered while waiting for `headers_ready()`, so a damaged title whose
// `codec_private` never resolves fails fast instead of OOM-killing the process.
pub(crate) const HEADER_BUFFER_CAP_BYTES: usize = 512 * 1024 * 1024;

/// Tuning / behaviour knobs for a mux run.
///
/// `Default` = keep-everything, no-skip, decrypt — the archival default. Added
/// so callers set only the fields they care about (and so an additive field
/// doesn't churn every constructor).
#[derive(Default, Clone)]
pub struct MuxOptions {
    /// Skip past read errors (zero-fill + continue) on the live-drive path
    /// instead of aborting. Applied by the live drive's Read policy.
    pub skip_errors: bool,
    /// Read batch size in logical (2048-byte) sectors.
    pub batch_sectors: u16,
    /// Ciphertext passthrough — skip decryption / CSS self-crack.
    pub raw: bool,
    /// Which audio/subtitle streams to keep in the muxed title. Default keeps
    /// every stream (video is always kept). Applied to the title before the
    /// demux pipeline is built, so track headers, `codec_privates`, and frame
    /// routing all follow the pruned list. See [`crate::StreamSelection`].
    ///
    /// Applies to every [`Source`].
    pub selection: crate::StreamSelection,
    /// 0-based index of the title to mux in the source's layout (a container source has
    /// one title, 0). Out of range is [`Error::MuxTrackRange`].
    pub title_index: usize,
}

/// The result of a [`mux_with_keys`] run.
#[derive(Debug, Clone)]
pub struct MuxOutcome {
    /// The mux drained to a natural EOF, finalised cleanly, and produced real
    /// output. `false` on interrupt (halt), or a wedged/failed finalise.
    pub completed: bool,
    /// The run's halt stopped it: during the open or the pump alike (`completed` is
    /// `false`). A wedged finalise with no Stop is `completed = false, halted = false`.
    pub halted: bool,
    /// The output sink was created (`output()` succeeded). `false` if the mux
    /// bailed before opening the sink (header gate, halt during header read).
    pub output_opened: bool,
    /// Total PES frame-payload bytes written to the sink (matches the CLI's
    /// `CountingStream::bytes_written`).
    pub bytes_written: u64,
    /// Cumulative read-error skip *events* (`Stream::errors`).
    pub errors: u64,
    /// Cumulative bytes zero-filled past read errors (`Stream::lost_bytes`).
    pub lost_bytes: u64,
    /// Number of streams in the muxed title.
    pub streams: usize,
    /// `title.streams` indices the output sink accepted frames for but could not
    /// put in the finished container (`Stream::undelivered_streams`): `mp4://` audio
    /// with no parseable sample entry, and `m2ts://` LPCM that BD LPCM can't carry.
    ///
    /// Non-empty means the file does NOT match the pre-mux plan even with `completed = true`. A
    /// caller reporting a successful export must report these too — a lossy outcome is never
    /// silent.
    pub undelivered_streams: Vec<usize>,
}

impl MuxOutcome {
    // A run that `ctx.halt` stopped before any frame reached a sink.
    fn stopped_before_output(errors: u64, lost_bytes: u64) -> Self {
        MuxOutcome {
            completed: false,
            halted: true,
            output_opened: false,
            bytes_written: 0,
            errors,
            lost_bytes,
            streams: 0,
            undelivered_streams: Vec::new(),
        }
    }
}

/// Mux `source` with the rip's up-front key set (KU §3.1): AACS reads go through the set's
/// map and on-arrival proof, with no key lookup. With no AACS set (or `opts.raw`) CSS and
/// clear content decrypt as before and a BD-TS disc refuses its first flagged unit (E7022).
/// E7013 when the set is not for the disc or title; E7026 for Pending forensic keys.
/// Unresolved codec headers are [`Error::MkvInvalid`], a zero-output drain
/// [`Error::NoStreams`]. A Stop of `ctx.halt`, during the open or the pump alike, yields
/// `completed = false, halted = true`, not an error.
pub fn mux_with_keys(
    source: Source,
    keys: Option<&crate::keys::KeyRing>,
    dest_url: &str,
    opts: &MuxOptions,
    ctx: &Ctx,
) -> std::io::Result<MuxOutcome> {
    match mux_routed(source, keys, dest_url, opts, ctx) {
        Err(e) if crate::error::is_halt(&e) && ctx.halt.is_cancelled() => {
            Ok(MuxOutcome::stopped_before_output(0, 0))
        }
        r => r,
    }
}

/// [`open_source`](super::source::open_source) then [`mux_with_keys`]: mux title
/// `opts.title_index` of the input `url`. A Stop during the open is a stopped outcome, as
/// during the pump.
pub fn mux_url(
    url: &str,
    keys: Option<&crate::keys::KeyRing>,
    dest_url: &str,
    opts: &MuxOptions,
    ctx: &Ctx,
) -> std::io::Result<MuxOutcome> {
    let source = match super::source::open_source(url, crate::disc::ScanOptions::default(), ctx) {
        Ok(s) => s,
        Err(e) => return open_failed(e, ctx),
    };
    mux_with_keys(source, keys, dest_url, opts, ctx)
}

// A failed open: the stopped outcome when it is a Stop, the error otherwise.
fn open_failed(e: Error, ctx: &Ctx) -> std::io::Result<MuxOutcome> {
    match e {
        Error::Halted if ctx.halt.is_cancelled() => Ok(MuxOutcome::stopped_before_output(0, 0)),
        e => Err(e.into()),
    }
}

// `mux_with_keys` before its one halt rule.
fn mux_routed(
    source: Source,
    keys: Option<&crate::keys::KeyRing>,
    dest_url: &str,
    opts: &MuxOptions,
    ctx: &Ctx,
) -> std::io::Result<MuxOutcome> {
    let name = source.title_name();
    let mut info = source.provenance(opts.title_index);
    let from_stream = matches!(source.origin, Origin::Stream { .. });
    let stream = open_pes(source, keys, opts, ctx)?;
    if from_stream {
        info.playlist = stream.info().playlist.clone();
    }
    if let Some(n) = &name {
        info.playlist = n.clone();
    }
    drive_mux(stream, dest_url, ctx, name.as_deref(), Some(&info))
}

// The PES stream of `opts.title_index` read from `source`: the one place that turns any
// opened input into frames (decrypting with `keys` unless `opts.raw`).
pub(crate) fn open_pes(
    source: Source,
    keys: Option<&crate::keys::KeyRing>,
    opts: &MuxOptions,
    ctx: &Ctx,
) -> std::io::Result<Box<dyn PesSource>> {
    let set = keys.filter(|s| s.is_aacs() && !opts.raw);
    let idx = opts.title_index;
    let input_opts = || InputOptions {
        title_index: Some(idx),
        raw: opts.raw,
        selection: opts.selection.clone(),
        keys: keys.cloned(),
    };
    match source.origin {
        Origin::Stream { url } => super::resolve::open_stream_url(&url, &input_opts(), ctx),
        Origin::Image { reader, disc, .. } => Ok(Box::new(super::resolve::image_input_scanned(
            reader,
            disc,
            &input_opts(),
            ctx,
        )?)),
        Origin::Prescanned { path, title: t } => {
            let (title, format) = (t.title, t.format);
            let keyless = keyless_ring(&title, format, set, opts);
            let set = set.or(keyless.as_ref());
            let mut title = title;
            opts.selection
                .apply(&mut title)
                .map_err(std::io::Error::from)?;
            let reader = FileSectorSource::open(&path)?;
            match set {
                Some(set) => Ok(Box::new(super::resolve::build_iso_pipeline_keyed(
                    reader,
                    title,
                    set,
                    opts.batch_sectors,
                    ctx,
                )?)),
                None => Ok(Box::new(build_iso_pipeline(
                    reader,
                    title,
                    DecryptKeys::None,
                    opts.batch_sectors,
                    format,
                    opts.raw,
                    ctx,
                )?)),
            }
        }
        Origin::Live { reader, title: t } => {
            let (title, format) = (t.title, t.format);
            let keyless = keyless_ring(&title, format, set, opts);
            match set.or(keyless.as_ref()) {
                Some(set) => {
                    let title = live_keyed_title(&*reader, title, set, opts)?;
                    live_keyed(reader, title, set, opts, ctx)
                }
                None => {
                    let mut title = title;
                    opts.selection
                        .apply(&mut title)
                        .map_err(std::io::Error::from)?;
                    live_unkeyed(reader, title, format, DecryptKeys::None, opts, ctx)
                }
            }
        }
        Origin::Session(session) => session_pes(session, set, opts, ctx),
        Origin::Drive(mut session) => session_pes(&mut session, set, opts, ctx),
    }
}

// Title `idx` of a scanned layout, and the layout's container format.
fn title_of(
    disc: &crate::disc::Disc,
    idx: usize,
) -> std::io::Result<(DiscTitle, crate::disc::ContentFormat)> {
    let title = disc.titles.get(idx).cloned().ok_or(Error::MuxTrackRange {
        track: idx,
        tracks: disc.titles.len(),
    })?;
    Ok((title, disc.content_format))
}

// KU §3.1: "`keys` must be `Some` for AACS". A BD-TS read with no AACS set reads through a
// keyless set: the first AACS-flagged unit is E7022, never muxed as content. MPEG-PS keeps
// its own path (a DVD cracks its CSS key in the stream).
fn keyless_ring(
    title: &DiscTitle,
    format: crate::disc::ContentFormat,
    set: Option<&crate::keys::KeyRing>,
    opts: &MuxOptions,
) -> Option<crate::keys::KeyRing> {
    (set.is_none() && !opts.raw && format == crate::disc::ContentFormat::BdTs)
        .then(|| crate::keys::KeyRing::keyless_for(title, format))
}

// A live mux off a drive session: the set's readers when keyed; else the session's own
// CSS/clear keys, and a keyless set for an AACS disc (HD DVD refuses up front).
fn session_pes(
    session: &mut DiscSession,
    set: Option<&crate::keys::KeyRing>,
    opts: &MuxOptions,
    ctx: &Ctx,
) -> std::io::Result<Box<dyn PesSource>> {
    let idx = opts.title_index;
    let path = session.device_path().to_string();
    let not_ready = || Error::DeviceNotReady { path: path.clone() };
    let disc = session.disc().ok_or_else(not_ready)?;
    let (title, format) = title_of(disc, idx)?;
    let keyless = match set {
        None if !opts.raw && disc.aacs.is_some() => {
            if disc.content_format != crate::disc::ContentFormat::BdTs {
                return Err(Error::NoDiscKey {
                    disc_hash: disc.aacs_disc_hash(),
                }
                .into());
            }
            crate::keys::KeyRing::keyless_for_disc(disc, idx)
        }
        _ => None,
    };
    let keys = session_mux_keys(disc);
    let set = set.or(keyless.as_ref());
    if let Some(set) = set {
        let scope = crate::keys::KeyScope::Titles(vec![idx]);
        if !set.is_for(&disc.media_id()) || !set.covers(&scope) {
            tracing::error!(target: "freemkv::keys", "key set is not for this session's title");
            return Err(Error::DecryptFailed.into());
        }
    }
    let batch = match opts.batch_sectors {
        0 => crate::disc::detect_max_batch_sectors(&path),
        n => n,
    };
    let opts = &MuxOptions {
        batch_sectors: batch,
        ..opts.clone()
    };
    if session.staged_reader().is_none() {
        session.stage_drive_as_reader();
    }
    match set {
        Some(set) => {
            // Refuse the selection / the set's gate BEFORE taking the reader, so a refused
            // mux leaves the session retryable.
            let staged = session.staged_reader().ok_or_else(not_ready)?;
            let title = live_keyed_title(staged, title, set, opts)?;
            let reader = session.take_reader().ok_or_else(not_ready)?;
            live_keyed(reader, title, set, opts, ctx)
        }
        None => {
            let mut title = title;
            opts.selection
                .apply(&mut title)
                .map_err(std::io::Error::from)?;
            let reader = session.take_reader().ok_or_else(not_ready)?;
            live_unkeyed(reader, title, format, keys, opts, ctx)
        }
    }
}

// A live drive's title with no AACS set: `keys` (CSS cracks per title when `None` on a
// DVD), or ciphertext under `opts.raw`, read under the drive's Read policy.
fn live_unkeyed(
    reader: Box<dyn SectorSource>,
    title: DiscTitle,
    format: crate::disc::ContentFormat,
    keys: DecryptKeys,
    opts: &MuxOptions,
    ctx: &Ctx,
) -> std::io::Result<Box<dyn PesSource>> {
    let keys = if opts.raw { DecryptKeys::None } else { keys };
    Ok(Box::new(super::resolve::build_sector_pipeline(
        reader,
        title,
        keys,
        live_policy(opts),
        format,
        opts.raw,
        ctx,
    )?))
}

// A live drive's Read policy: adaptive batches from `opts.batch_sectors`, then skip or fail.
fn live_policy(opts: &MuxOptions) -> crate::sector::read_stage::ReadPolicy {
    crate::sector::read_stage::ReadPolicy::Live {
        batch: opts.batch_sectors,
        skip_errors: opts.skip_errors,
    }
}

// The live title after the selection, once the set's gate admits it over `reader`.
fn live_keyed_title(
    reader: &dyn SectorSource,
    mut title: DiscTitle,
    set: &crate::keys::KeyRing,
    opts: &MuxOptions,
) -> std::io::Result<DiscTitle> {
    opts.selection
        .apply(&mut title)
        .map_err(std::io::Error::from)?;
    let ranges: Vec<(u32, u32)> = title
        .extents
        .iter()
        .map(|e| (e.start_lba, e.start_lba.saturating_add(e.sector_count)))
        .collect();
    set.gate(reader.random_access(), Some(&ranges), false)?;
    Ok(title)
}

// A live drive's title over the set (`title` from `live_keyed_title`): its keys, map and
// on-arrival proof, read under the drive's Read policy.
fn live_keyed(
    reader: Box<dyn SectorSource>,
    title: DiscTitle,
    set: &crate::keys::KeyRing,
    opts: &MuxOptions,
    ctx: &Ctx,
) -> std::io::Result<Box<dyn PesSource>> {
    Ok(Box::new(super::resolve::build_keyed_pipeline(
        reader,
        title,
        set,
        live_policy(opts),
        ctx,
    )?))
}

// Decrypt keys for the live `Session` mux of `disc`. A DVD is handed `DecryptKeys::None` so
// the title's pipeline cracks the CORRECT per-title CSS key rather than the whole-disc
// (largest-title) VTS key.
fn session_mux_keys(disc: &crate::disc::Disc) -> DecryptKeys {
    if matches!(disc.format, crate::disc::DiscFormat::Dvd) {
        DecryptKeys::None
    } else {
        disc.decrypt_keys()
    }
}

// Join the write consumer after the pump. A send that hit its deadline means the
// consumer is wedged, so the join gets only the short grace, not JOIN_TIMEOUT.
// While it waits, each increase of the output's durable bytes is forwarded (§4.5, LP20).
fn finish_pumped<I: Send + 'static, R: Send + 'static>(
    pipe: Pipeline<I, R>,
    halt: &Halt,
    send_timed_out: bool,
    flush: &FlushProgress,
    ctx: &Ctx,
) -> Result<R, Error> {
    let mut fwd = FlushForwarder {
        flush,
        ctx,
        last: flush.bytes_durable(),
        last_call: None,
    };
    let timing = crate::io::pipeline::JoinTiming::default();
    let wedged = Halt::new();
    let halt = if send_timed_out {
        wedged.cancel();
        &wedged
    } else {
        halt
    };
    let joined = pipe.finish_with_halt_observed(Some(halt), timing, &mut || fwd.poll(false));
    fwd.poll(true);
    joined
}

// At most one `BytesDurable` per this long (4 Hz, §4.5).
const FLUSH_PROGRESS_EVERY: Duration = Duration::from_millis(250);

// Forwards increases of the output's durable bytes as `Event::BytesDurable`:
// one event per increase, rate-limited, none while nothing moves.
struct FlushForwarder<'a> {
    flush: &'a FlushProgress,
    ctx: &'a Ctx,
    last: u64,
    last_call: Option<std::time::Instant>,
}

impl FlushForwarder<'_> {
    // `last_word`: the wait is over, so a pending increase goes out now.
    fn poll(&mut self, last_word: bool) {
        let done = self.flush.bytes_durable();
        let due = self
            .last_call
            .is_none_or(|t| t.elapsed() >= FLUSH_PROGRESS_EVERY);
        if done > self.last && (due || last_word) {
            self.ctx.emit(Event::BytesDurable {
                bytes: done,
                total: self.flush.bytes_total(),
            });
            self.last = done;
            self.last_call = Some(std::time::Instant::now());
        }
    }
}

// Whether a finished mux counts as COMPLETED: interrupted, finalize_failed, or halt_cancelled
// each force `false`. Pure fn so this mapping is unit-tested directly.
fn mux_run_completed(interrupted: bool, finalize_failed: bool, halt_cancelled: bool) -> bool {
    !(interrupted || finalize_failed || halt_cancelled)
}

// The reader-agnostic driver body: headers → gate → sink → pump → finish.
// Split out so it can be unit-tested against a synthetic Stream (the
// injection seam), independent of which constructor built `stream`.
fn drive_mux(
    mut stream: Box<dyn PesSource>,
    dest_url: &str,
    ctx: &Ctx,
    playlist_name: Option<&str>,
    source: Option<&SourceInfo>,
) -> std::io::Result<MuxOutcome> {
    let halt = &ctx.halt;
    // Title assembled from the scanned metadata; the playlist name (disc name)
    // overrides `info().playlist` where the consumer supplied one.
    let mut out_title = stream.info().clone();
    if let Some(name) = playlist_name {
        out_title.playlist = name.to_string();
    }

    let caps = SinkCaps::of(&parse_url(dest_url));
    let mut buffered: Vec<PesFrame> = Vec::new();
    let mut buffered_bytes: usize = 0;

    // ── Metadata sinks (`needs_frames = false`) — BEFORE the header pump/gate ──
    // They write their whole file at `output()` and take no frames, so the header gate
    // could false-fail them; the title's TrueHD labels are still completed first.
    if !caps.needs_frames {
        let done = complete_truehd(
            &mut *stream,
            &mut buffered,
            &mut buffered_bytes,
            &mut out_title.streams,
            halt,
        )?;
        if !done {
            return Ok(MuxOutcome::stopped_before_output(
                stream.errors(),
                stream.lost_bytes(),
            ));
        }
        let mut sink = CountingStream::new(output(dest_url, &out_title, source)?);
        ctx.emit(Event::OutputOpened { title: &out_title });
        sink.finish()?;
        return Ok(MuxOutcome {
            completed: true,
            halted: false,
            output_opened: true,
            bytes_written: sink.bytes_written(),
            errors: stream.errors(),
            lost_bytes: stream.lost_bytes(),
            streams: out_title.streams.len(),
            undelivered_streams: sink.undelivered_streams(),
        });
    }

    // ── Header pump ── Buffer frames until every video (and AAC) track's
    // codec_private has resolved; MKV can't write a track header without codec init data.
    // The loop breaks on EOF/None too, so the gate below re-checks.
    while !stream.headers_ready() {
        if halt.is_cancelled() {
            return Ok(MuxOutcome::stopped_before_output(
                stream.errors(),
                stream.lost_bytes(),
            ));
        }
        let read = match stream.read() {
            Ok(r) => r,
            // A halt landing DURING a blocking read surfaces as `Error::Halted`
            // (reads dominate wall-clock, so a stop usually lands here). Not a
            // failure — yield `completed = false` so stop-preserves-staging runs.
            Err(e) if crate::error::is_halt(&e) => {
                return Ok(MuxOutcome::stopped_before_output(
                    stream.errors(),
                    stream.lost_bytes(),
                ));
            }
            Err(e) => return Err(e),
        };
        match read {
            Some(frame) => {
                buffered_bytes = buffered_bytes.saturating_add(frame.data.len());
                buffered.push(frame);
                // Bounded header buffer: a title whose codec_private never resolves
                // would otherwise buffer the whole stream into RAM until OOM. NOT
                // `Error::MkvInvalid` — that's skippable-stub, and this must not be.
                if buffered_bytes > HEADER_BUFFER_CAP_BYTES {
                    tracing::error!(
                        target: "mux",
                        buffered_bytes,
                        cap = HEADER_BUFFER_CAP_BYTES,
                        "header buffer cap exceeded: the title keeps yielding frames but a \
                         required codec_private (video, AAC or BD LPCM) never resolved; refusing \
                         rather than buffering the whole stream into RAM"
                    );
                    return Err(Error::MuxHeaderBufferExceeded {
                        bytes: buffered_bytes as u64,
                    }
                    .into());
                }
            }
            None => break,
        }
    }

    // ── Header gate ── Halt first, whatever headers_ready() says: on the
    // highway path a halt can end the stream as `Ok(None)`, and that EOF also
    // releases the in-band config wait, so a ready gate does not mean the pump finished.
    if halt.is_cancelled() {
        return Ok(MuxOutcome::stopped_before_output(
            stream.errors(),
            stream.lost_bytes(),
        ));
    }
    // The pump can break on EOF without headers resolving. Finalising then would
    // write a track header with no CODEC_PRIVATE — a structurally-invalid MKV the
    // zero-output guard does not catch. Refuse.
    if !stream.headers_ready() {
        return Err(Error::MkvInvalid.into());
    }

    // Assemble the output title now that codec_privates have resolved, its TrueHD
    // labels completed from the stream (BUG-6).
    let info = stream.info().clone();
    out_title.streams = info.streams.clone();
    let done = complete_truehd(
        &mut *stream,
        &mut buffered,
        &mut buffered_bytes,
        &mut out_title.streams,
        halt,
    )?;
    if !done {
        return Ok(MuxOutcome::stopped_before_output(
            stream.errors(),
            stream.lost_bytes(),
        ));
    }
    out_title.size_bytes = info.size_bytes;
    out_title.codec_privates = (0..info.streams.len())
        .map(|i| stream.codec_private(i))
        .collect();
    crate::mux::codec::lpcm::correct_title_layout(&mut out_title);
    // AAC tracks whose ASC was unknown at header time: handed to the sink when
    // it resolves (a seekable MKV backpatches it).
    let mut late_aac: Vec<usize> = info
        .streams
        .iter()
        .enumerate()
        .filter(|(track, s)| {
            matches!(s, crate::disc::Stream::Audio(a) if matches!(a.codec, crate::disc::Codec::Aac))
                && out_title
                    .codec_privates
                    .get(*track)
                    .is_none_or(Option::is_none)
        })
        .map(|(track, _)| track)
        .collect();
    let late_configs: LateConfigs = Arc::default();
    let total_bytes = info.size_bytes;
    let num_streams = info.streams.len();

    // ── Open the sink, wrap in a byte counter, hand it to the write pipeline ──
    // The output file's flush counters share the consumer's progress (§2.10 item 3).
    let flush = FlushProgress::new(crate::halt::Liveness::new());
    let out_flush = super::resolve::OutputFlush {
        progress: &flush,
        halt,
    };
    let mut output_stream = open_output(dest_url, &out_title, source, out_flush)?;
    for track in 0..num_streams {
        output_stream.set_track_timing(track, stream.track_timing(track))?;
    }
    // A sink may refuse a stream at open (m2ts: LPCM BD LPCM can't carry), and a sink with no
    // mapping never writes an MPEG-2 extension track (reported lost only once its packets
    // arrive); the opened title lists only what will be written.
    let mut refused = output_stream.undelivered_streams();
    if !caps.carries_mp2_extensions {
        refused.extend((out_title.streams.iter().enumerate()).filter_map(|(i, s)| {
            matches!(s, crate::disc::Stream::Audio(a) if a.is_mp2_extension()).then_some(i)
        }));
    }
    if refused.is_empty() {
        ctx.emit(Event::OutputOpened { title: &out_title });
    } else {
        let mut opened = out_title.clone();
        let keep = |i: &usize| !refused.contains(i);
        opened.streams = (0..opened.streams.len())
            .filter(keep)
            .map(|i| out_title.streams[i].clone())
            .collect();
        opened.codec_privates = (0..out_title.codec_privates.len())
            .filter(keep)
            .map(|i| out_title.codec_privates[i].clone())
            .collect();
        ctx.emit(Event::OutputOpened { title: &opened });
    }
    let output_stream = CountingStream::new(output_stream);

    // The write consumer runs on its own thread so the latency-bound sink write
    // overlaps the next `stream.read()`. `bytes` mirrors the consumer's running
    // written-byte count out to the driving thread for `BytesWritten`.
    let bytes = Arc::new(AtomicU64::new(0));
    let read_failed = Arc::new(AtomicBool::new(false));
    let sink = WriteSink {
        output: output_stream,
        bytes: bytes.clone(),
        late_configs: late_configs.clone(),
        read_failed: read_failed.clone(),
    };
    let consumer_progress = flush.progress().clone();
    let pipe = Pipeline::spawn_named_with_progress(
        "freemkv-mux-consumer",
        WRITE_PIPELINE_DEPTH,
        sink,
        consumer_progress,
    )
    .map_err(std::io::Error::from)?;
    // The op's token, not the wedge token `finish_pumped` may pass (§2.5 `.partial` rule).
    pipe.set_op_token(halt);

    // ── Frame pump ── No per-frame deadline (T27): a slow-but-alive sink blocks and
    // Stop is the bound; `send_with_halt` re-checks halt every `WAIT_SLICE`.
    let deadline = Duration::MAX;
    let mut interrupted = false;
    // A send refused with no halt and a healthy consumer ran out its deadline.
    let mut send_timed_out = false;

    // Buffered header frames first, in order.
    for frame in buffered {
        if pipe.send_with_halt(frame, halt, deadline).is_err() {
            interrupted = true;
            send_timed_out = !halt.is_cancelled() && !pipe.consumer_failed();
            break;
        }
        // Feed write-side progress during the drain exactly as the steady-state
        // loop below does — the watchdog is fed only from `BytesWritten`,
        // and no `stream.read()` runs here to do it for us.
        ctx.emit(Event::BytesWritten {
            bytes: bytes.load(Ordering::Relaxed),
            total: total_bytes,
        });
    }

    // Then the remainder of the stream.
    if !interrupted {
        loop {
            if halt.is_cancelled() {
                interrupted = true;
                break;
            }
            match stream.read() {
                Ok(Some(frame)) => {
                    // Queued before the frame is sent, so the sink applies it first.
                    if !late_aac.is_empty() {
                        collect_late_configs(&*stream, &mut late_aac, &late_configs);
                    }
                    if pipe.send_with_halt(frame, halt, deadline).is_err() {
                        interrupted = true;
                        send_timed_out = !halt.is_cancelled() && !pipe.consumer_failed();
                        break;
                    }
                    ctx.emit(Event::BytesWritten {
                        bytes: bytes.load(Ordering::Relaxed),
                        total: total_bytes,
                    });
                }
                Ok(None) => break,
                // A halt landing mid-read is a clean operator stop, not a read
                // failure: fall through to the `completed = false` interrupt path,
                // matching the header pump, finish stage, and highway.
                Err(e) if crate::error::is_halt(&e) => {
                    interrupted = true;
                    break;
                }
                Err(e) => {
                    // Drain + join the consumer, then report the ROOT cause: a
                    // write failure (e.g. volume full) precedes and explains the
                    // read error, so prefer it — except Halt/join-timeout, not root causes.
                    read_failed.store(true, Ordering::Relaxed);
                    match pipe.finish_with_halt(Some(halt)) {
                        Err(w @ (Error::Halted | Error::PipelineJoinTimeout)) => {
                            tracing::debug!(
                                target: "mux",
                                write_side = %w,
                                read_side = %e,
                                "read failed; consumer stopped for a non-root-cause reason — reporting the read error"
                            );
                            return Err(e);
                        }
                        Err(w) => {
                            tracing::error!(
                                target: "mux",
                                write_side = %w,
                                read_side = %e,
                                "read failed, but the write side had already failed — reporting the write failure as the root cause"
                            );
                            return Err(w.into());
                        }
                        Ok(_) => return Err(e),
                    }
                }
            }
        }
    }

    if !late_aac.is_empty() {
        collect_late_configs(&*stream, &mut late_aac, &late_configs);
        if !late_aac.is_empty() {
            tracing::debug!(target: "mux", tracks = ?late_aac, "AAC tracks never yielded an AudioSpecificConfig");
        }
    }

    // ── Finish ── Drop the producer, join the consumer; `close()` finalises
    // the container. On halt/wedge this returns an error variant, translated
    // to `completed = false` rather than a hard failure.
    let (bytes_written, undelivered_streams, finalize_failed) =
        match finish_pumped(pipe, halt, send_timed_out, &flush, ctx) {
            Ok(c) => (c.bytes, c.undelivered, false),
            Err(Error::Halted | Error::PipelineJoinTimeout) => {
                (bytes.load(Ordering::Relaxed), Vec::new(), true)
            }
            Err(e) => return Err(e.into()),
        };
    if !undelivered_streams.is_empty() {
        tracing::warn!(
            target: "mux",
            streams = ?undelivered_streams,
            "output sink could not deliver every planned stream; the file does not match \
             the pre-mux plan (surfaced as MuxOutcome::undelivered_streams)"
        );
    }

    for (track, frames) in stream.config_changes() {
        tracing::warn!(
            target: "mux",
            track,
            frames,
            "in-band codec config changed mid-track; those frames keep the first config"
        );
    }

    if !mux_run_completed(interrupted, finalize_failed, halt.is_cancelled()) {
        return Ok(MuxOutcome {
            completed: false,
            halted: halt.is_cancelled(),
            output_opened: true,
            bytes_written,
            errors: stream.errors(),
            lost_bytes: stream.lost_bytes(),
            streams: num_streams,
            undelivered_streams,
        });
    }

    // ── Zero-output / NoStreams gate ── A natural drain that wrote no streams
    // or not a single payload byte is the empty/undecryptable-input silent
    // failure — refuse to report it complete.
    if num_streams == 0 || bytes_written == 0 {
        return Err(Error::NoStreams.into());
    }

    Ok(MuxOutcome {
        completed: true,
        halted: false,
        output_opened: true,
        bytes_written,
        errors: stream.errors(),
        lost_bytes: stream.lost_bytes(),
        streams: num_streams,
        undelivered_streams,
    })
}

// Late `(track, codec_private)` pairs from the reader, drained by the sink.
type LateConfigs = Arc<std::sync::Mutex<Vec<(usize, Vec<u8>)>>>;

fn lock_late(q: &LateConfigs) -> std::sync::MutexGuard<'_, Vec<(usize, Vec<u8>)>> {
    q.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

// Move tracks whose codec_private has now resolved from `pending` to the queue.
fn collect_late_configs(stream: &dyn PesSource, pending: &mut Vec<usize>, out: &LateConfigs) {
    pending.retain(|&track| match stream.codec_private(track) {
        Some(cp) => {
            lock_late(out).push((track, cp));
            false
        }
        None => true,
    });
}

// Bytes of frames read past the header gate while looking for TrueHD major syncs: about
// the 8 MiB of title start the playlist-era probe read.
const TRUEHD_PROBE_BYTES: usize = 8 * 1024 * 1024;
// Bytes of one TrueHD track kept for the major-sync parse.
const TRUEHD_PROBE_TRACK_BYTES: usize = 1024 * 1024;

// Header completion (BUG-6): a playlist labels a 7.1/Atmos TrueHD track 5.1, so read on into
// `buffered` until each TrueHD track shows a major sync (at most `TRUEHD_PROBE_BYTES`) and
// label it from that. `Ok(false)` when a Stop ended the read.
fn complete_truehd(
    stream: &mut dyn PesSource,
    buffered: &mut Vec<PesFrame>,
    buffered_bytes: &mut usize,
    streams: &mut [crate::disc::Stream],
    halt: &Halt,
) -> std::io::Result<bool> {
    use crate::disc::{Codec, Stream as Track};
    let mut pending: Vec<usize> = (0..streams.len())
        .filter(|&i| matches!(&streams[i], Track::Audio(a) if matches!(a.codec, Codec::TrueHd)))
        .collect();
    let mut payload: std::collections::HashMap<usize, Vec<u8>> = Default::default();
    let start = *buffered_bytes;
    let mut seen = 0;
    while !pending.is_empty() {
        while seen < buffered.len() && !pending.is_empty() {
            let f = &buffered[seen];
            seen += 1;
            let Some(pos) = pending.iter().position(|&t| t == f.track) else {
                continue;
            };
            let buf = payload.entry(f.track).or_default();
            if buf.len() >= TRUEHD_PROBE_TRACK_BYTES {
                continue;
            }
            buf.extend_from_slice(&f.data);
            let has_sync = f.data.windows(4).any(|w| w == [0xF8, 0x72, 0x6F, 0xBA]);
            if let (true, Track::Audio(a)) = (has_sync, &mut streams[f.track])
                && crate::disc::correct_truehd_stream(a, buf)
            {
                pending.remove(pos);
            }
        }
        if pending.is_empty() || *buffered_bytes - start > TRUEHD_PROBE_BYTES {
            break;
        }
        if halt.is_cancelled() {
            return Ok(false);
        }
        match stream.read() {
            Ok(Some(f)) => {
                *buffered_bytes = buffered_bytes.saturating_add(f.data.len());
                buffered.push(f);
            }
            Ok(None) => break,
            Err(e) if crate::error::is_halt(&e) => return Ok(false),
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

// The sink for `dest_url`; a test may substitute its own (thread-local seam).
fn open_output(
    dest_url: &str,
    title: &DiscTitle,
    source: Option<&SourceInfo>,
    flush: super::resolve::OutputFlush<'_>,
) -> std::io::Result<Box<dyn PesSink>> {
    #[cfg(test)]
    if let Some(sink) = tests::TEST_SINK.with(|s| s.borrow_mut().take()) {
        return Ok(sink);
    }
    output_with(dest_url, title, source, Some(flush))
}

// What the write consumer hands back once the container is finalised.
struct SinkClose {
    bytes: u64,
    undelivered: Vec<usize>,
}

// Write-side `Sink`: applies each frame to the counting output stream and
// finalises the container on close. `close()` returns the payload-byte count
// plus any undelivered streams (see `MuxOutcome::undelivered_streams`).
struct WriteSink {
    output: CountingStream,
    bytes: Arc<AtomicU64>,
    late_configs: LateConfigs,
    // Set by the driver when the title's read failed: the output is incomplete.
    read_failed: Arc<AtomicBool>,
}

impl WriteSink {
    fn end(mut self, complete: bool) -> Result<SinkClose, Error> {
        self.apply_late_configs()?;
        match complete {
            true => self.output.finish(),
            false => self.output.finish_incomplete(),
        }
        .map_err(Error::from)?;
        // Sample AFTER finish(): the mp4 sink decides its drops there.
        Ok(SinkClose {
            bytes: self.output.bytes_written(),
            undelivered: self.output.undelivered_streams(),
        })
    }

    // End an output that `cause` cut short. The cause is the title's verdict: ending the
    // output can fail (an mkv with no frames is `MkvInvalid`), which is logged, never returned.
    fn end_cut_short(self, cause: &str) -> Result<SinkClose, Error> {
        let bytes = self.bytes.clone();
        self.end(false).or_else(|e| {
            tracing::warn!(target: "mux", error = %e, "ending the output {cause} cut short failed");
            Ok(SinkClose {
                bytes: bytes.load(Ordering::Relaxed),
                undelivered: Vec::new(),
            })
        })
    }

    fn apply_late_configs(&mut self) -> Result<(), Error> {
        let late = std::mem::take(&mut *lock_late(&self.late_configs));
        for (track, cp) in late {
            if !self
                .output
                .set_codec_private(track, &cp)
                .map_err(Error::from)?
            {
                // Most such sinks (m2ts/stdio/network) carry config in-band anyway.
                tracing::debug!(target: "mux", track, "sink does not record a late codec_private");
            }
        }
        Ok(())
    }
}

impl Sink<PesFrame> for WriteSink {
    type Output = SinkClose;

    fn apply(&mut self, frame: PesFrame) -> Result<Flow, Error> {
        self.apply_late_configs()?;
        self.output.write(&frame).map_err(Error::from)?;
        self.bytes
            .store(self.output.bytes_written(), Ordering::Relaxed);
        Ok(Flow::Continue)
    }

    fn close(self) -> Result<SinkClose, Error> {
        match self.read_failed.load(Ordering::Relaxed) {
            true => self.end_cut_short("a read failure"),
            false => self.end(true),
        }
    }

    // A stopped title is incomplete too: a wire sink must not end it cleanly.
    fn close_stopped(self) -> Result<SinkClose, Error> {
        self.end_cut_short("a stop")
    }
}

#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;
