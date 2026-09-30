//! High-level mux driver: runs the `construct → headers gate → open sink →
//! pump → finish` pipeline the consumers (CLI `pipe`/`pipe_disc`, autorip
//! `run_mux`) each used to hand-roll.
//!
//! [`mux_with_keys`] DRIVES the existing pipeline via the same ISO pipeline builders
//! and a [`WRITE_PIPELINE_DEPTH`]-deep write [`Pipeline`]; it does not replace either.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::decrypt::DecryptKeys;
use crate::disc::DiscTitle;
use crate::error::Error;
use crate::event::{BatchSizeReason, Event, EventKind};
use crate::halt::Halt;
use crate::io::FlushProgress;
use crate::io::pipeline::{Flow, Pipeline, Sink, WRITE_PIPELINE_DEPTH};
use crate::pes::{CountingStream, PesFrame, Stream};
use crate::sector::{FileSectorSource, SectorSource};
use crate::session::DiscSession;

use super::resolve::{
    InputOptions, StreamUrl, build_iso_pipeline, input_with_halt, output, output_with, parse_url,
};
use super::videomap::{Medium, SourceInfo};

// The source medium a parsed input URL denotes, used only for provenance. Non-legal-input URLs
// never reach a sink, so their arm is immaterial — falls to the `File` default.
fn url_medium(parsed: &StreamUrl) -> Medium {
    match parsed {
        StreamUrl::Disc { .. } => Medium::Disc,
        StreamUrl::Iso { .. } => Medium::Iso,
        StreamUrl::Mkv { .. }
        | StreamUrl::M2ts { .. }
        | StreamUrl::Mp4 { .. }
        | StreamUrl::Mpg { .. } => Medium::File,
        StreamUrl::Network { .. } | StreamUrl::Stdio => Medium::Stream,
        _ => Medium::File,
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
#[derive(Default)]
pub struct MuxOptions {
    /// Skip past read errors (zero-fill + continue) on the live-drive path
    /// instead of aborting. Wired onto `DiscStream::skip_errors`.
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
    /// Applies to the `Iso`, `Session` and `Live` inputs. It does NOT apply to
    /// [`MuxSource::Url`], which builds its demux inside `input()` — a Url-source
    /// caller sets `InputOptions::selection` instead. Setting this field for a Url
    /// input has no effect.
    pub selection: crate::StreamSelection,
}

/// Progress / event callbacks the consumer implements (CLI `CliProgress`,
/// autorip's stream event handler). Every method has a no-op default so a
/// consumer overrides only what it renders.
///
/// `Send + Sync + 'static` so [`mux_with_keys`] can clone the handle into the reader constructors'
/// `'static` `EventFn`. Progress is split into read-side and write-side callbacks (CLI renders
/// WRITE, autorip READ).
pub trait MuxEvents: Send + Sync + 'static {
    /// Fired once, immediately after the output sink is created. The title lists
    /// only streams the sink will write (tracks it refused are removed, so indices
    /// compact); `MuxOutcome::undelivered_streams` keeps source-title indices.
    fn on_output_opened(&self, _title: &DiscTitle) {}
    /// Fired periodically from the reader side with the running read-byte count
    /// and the source extents' total byte estimate.
    fn on_read_progress(&self, _bytes_read: u64, _bytes_total: u64) {}
    /// Fired periodically during the frame pump with the running written-byte
    /// count and the title's total byte estimate.
    fn on_write_progress(&self, _bytes_written: u64, _bytes_total: u64) {}
    /// A bad sector was skipped (zero-filled) at `lba`.
    fn on_sector_skipped(&self, _lba: u32) {}
    /// The adaptive read batch size changed.
    fn on_batch_size_changed(&self, _batch: u16, _reason: BatchSizeReason) {}
    /// A read error occurred at `lba`.
    fn on_read_error(&self, _lba: u32) {}
    /// While the output is being flushed at the end (the driver waiting on a closing
    /// consumer): more bytes became durable. One call per increase, at most 4 per second,
    /// none while nothing moves, so silence means a stalled flush (stop design §4.5).
    fn on_flush_progress(&self, _bytes_durable: u64, _bytes_total: u64) {}
}

/// A [`MuxEvents`] that ignores everything — test-only (production callers
/// supply their own events sink).
#[cfg(test)]
pub(crate) struct NoopEvents;
#[cfg(test)]
impl MuxEvents for NoopEvents {}

/// The result of a [`mux_with_keys`] run.
#[derive(Debug, Clone)]
pub struct MuxOutcome {
    /// The mux drained to a natural EOF, finalised cleanly, and produced real
    /// output. `false` on interrupt (halt), or a wedged/failed finalise.
    pub completed: bool,
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

// The mux of a source with no AACS key set (clear, CSS, `raw`, or a URL that scans):
// construct the source stream, open the `dest_url` sink and pump it (`drive_mux`).
fn mux_unkeyed(
    input_src: MuxSource,
    dest_url: &str,
    opts: &MuxOptions,
    halt: &Halt,
    events: std::sync::Arc<dyn MuxEvents>,
) -> std::io::Result<MuxOutcome> {
    // Construct the source stream (ISO/live/URL) — reader constructors need a
    // `'static` EventFn, so we clone `Arc<dyn MuxEvents>` into `reader_event_fn`.
    // Each arm also derives SourceInfo since `output()` has no access to source.
    let (stream, playlist_name, mut source): (Box<dyn Stream>, Option<String>, SourceInfo) =
        match input_src {
            // The Url path builds its demux INSIDE `input()`, pruned via
            // `InputOptions.selection`. `MuxOptions.selection` does NOT apply
            // here — that's the File/Session arms' field.
            MuxSource::Url { url, opts: in_opts } => {
                // Provenance: source URL verbatim, scheme's medium, and the title
                // `input()` will open. `playlist` fills in below from the opened
                // stream's scanned title — not known until the scan runs.
                let source = SourceInfo {
                    medium: url_medium(&parse_url(url)),
                    path: url.to_string(),
                    title: in_opts.title_index.unwrap_or(0),
                    ..SourceInfo::default()
                };
                // A Stop while `network://` waits for its sender is a clean interrupt.
                let stream = match input_with_halt(url, &in_opts, Some(halt)) {
                    Ok(s) => s,
                    Err(e) if crate::error::is_halt(&e) => {
                        return Ok(MuxOutcome {
                            completed: false,
                            output_opened: false,
                            bytes_written: 0,
                            errors: 0,
                            lost_bytes: 0,
                            streams: 0,
                            undelivered_streams: Vec::new(),
                        });
                    }
                    Err(e) => return Err(e),
                };
                let source = SourceInfo {
                    playlist: stream.info().playlist.clone(),
                    ..source
                };
                (stream, None, source)
            }
            MuxSource::Iso {
                path,
                title,
                format,
            } => {
                // Prune to selected audio/subtitle streams BEFORE the highway
                // builds demux state (and before the DVD AC-3 channel probe).
                // Video is always kept; no-op for default All/All.
                let mut title = title;
                opts.selection
                    .apply(&mut title)
                    .map_err(std::io::Error::from)?;
                // Provenance: staged image as an `iso://` URL, plus playlist. Title
                // INDEX is NOT reachable here — `MuxSource::Iso` carries an already-
                // scanned `DiscTitle` with no index, so it stays 0 rather than guessed.
                let source = SourceInfo {
                    medium: Medium::Iso,
                    path: format!("iso://{}", path.display()),
                    playlist: title.playlist.clone(),
                    ..SourceInfo::default()
                };
                let reader = FileSectorSource::open(path)?;
                let stream = build_iso_pipeline(
                    reader,
                    title,
                    DecryptKeys::None,
                    opts.batch_sectors,
                    format,
                    opts.raw,
                    Some(halt.clone()),
                    Some(reader_event_fn(events.clone())),
                )?;
                (Box::new(stream), None, source)
            }
            MuxSource::Session {
                session,
                title_index,
            } => {
                // Pull everything we need out of the disc as owned values so the
                // immutable disc borrow is released before the mutable
                // `take_reader` below.
                let (mut title, format, keys, playlist, source) = {
                    let disc = session.disc().ok_or_else(|| Error::DeviceNotReady {
                        path: session.device_path().to_string(),
                    })?;
                    let title =
                        disc.titles
                            .get(title_index)
                            .cloned()
                            .ok_or(Error::MuxTrackRange {
                                track: title_index,
                                tracks: disc.titles.len(),
                            })?;
                    let playlist = disc
                        .meta_title
                        .clone()
                        .unwrap_or_else(|| disc.volume_id.clone());
                    // Provenance: every member is reachable on this arm — the device
                    // the session is bound to, the title index the caller named, the
                    // title's own playlist, and the scanned volume id.
                    let source = SourceInfo {
                        medium: Medium::Disc,
                        path: format!("disc://{}", session.device_path()),
                        title: title_index,
                        playlist: title.playlist.clone(),
                        volume_id: disc.volume_id.clone(),
                    };
                    // DVD CSS is per-VTS: resolve the per-title key via the pipeline
                    // (see `session_mux_keys`), never the whole-disc `decrypt_keys()`.
                    (
                        title,
                        disc.content_format,
                        session_mux_keys(disc),
                        playlist,
                        source,
                    )
                };
                // Prune to selected streams before `DiscStream::new` builds demux tables.
                opts.selection
                    .apply(&mut title)
                    .map_err(std::io::Error::from)?;
                // A missing staged reader ("already consumed" / never staged) is a
                // clean error, not a panic (contract Q2).
                let reader = session.take_reader().ok_or_else(|| Error::DeviceNotReady {
                    path: session.device_path().to_string(),
                })?;
                let mut stream = crate::mux::DiscStream::new(
                    reader,
                    title,
                    keys,
                    opts.batch_sectors,
                    format,
                    opts.raw,
                    Some(halt.clone()),
                )?;
                if opts.raw {
                    stream.set_raw();
                }
                stream.skip_errors = opts.skip_errors;
                // Live path: the `DiscStream` emits the full reader-side vocabulary
                // (`SectorSkipped` on skip-mode zero-fill, `BatchSizeChanged` on the
                // adaptive sizer, `BytesRead` progress) — forward them all.
                stream.on_event(reader_event_fn(events.clone()));
                (Box::new(stream), Some(playlist), source)
            }
            MuxSource::Live {
                reader,
                title,
                format,
            } => {
                // Prune to selected streams, exactly as the Iso/Session arms do —
                // without this, selection was silently ignored on the live-drive
                // path despite the field's doc saying it was applied.
                let mut title = title;
                opts.selection
                    .apply(&mut title)
                    .map_err(std::io::Error::from)?;
                // Provenance: medium is certain, playlist is on hand. Device PATH
                // and title INDEX are NOT reachable — `MuxSource::Live` hands an
                // opaque reader/title with neither, so both stay empty/0.
                let source = SourceInfo {
                    medium: Medium::Disc,
                    playlist: title.playlist.clone(),
                    ..SourceInfo::default()
                };
                // INLINE `DiscStream`, same constructor as the `Session` arm, NOT
                // `build_iso_pipeline` — the highway would bypass the adaptive
                // batch-retry that lives in `DiscStream::fill_extents`.
                let mut stream = crate::mux::DiscStream::new(
                    reader,
                    title,
                    DecryptKeys::None,
                    opts.batch_sectors,
                    format,
                    opts.raw,
                    Some(halt.clone()),
                )?;
                if opts.raw {
                    stream.set_raw();
                }
                stream.skip_errors = opts.skip_errors;
                // Same reader-side event vocabulary as the `Session` arm
                // (`SectorSkipped` / `BatchSizeChanged` / `BytesRead`).
                stream.on_event(reader_event_fn(events.clone()));
                (Box::new(stream), None, source)
            }
        };

    // The consumer-supplied disc name overrides the title's own playlist for the
    // muxed title (see `drive_mux`); mirror that into the provenance so a
    // `fvi://` header and its sibling MKV agree on what the playlist was called.
    if let Some(name) = playlist_name.as_deref() {
        source.playlist = name.to_string();
    }

    drive_mux(
        stream,
        dest_url,
        halt,
        events.as_ref(),
        playlist_name.as_deref(),
        Some(&source),
    )
}

/// Where [`mux_with_keys`] reads its PES frames from (KU §3.1). No variant carries key
/// material: keys come only from the rip's [`ResolvedKeySet`](crate::keys::ResolvedKeySet).
pub enum MuxSource<'a> {
    /// Live single-pass mux off an opened [`DiscSession`] (its reader staged).
    Session {
        session: &'a mut DiscSession,
        /// Index into `session.disc().titles`.
        title_index: usize,
    },
    /// An image file and a title the caller ALREADY scanned (the drive scan, J14): the
    /// image is never rescanned, so a sweep's unreadable UDF/MPLS/AACS sectors do not
    /// matter. The set must cover the title and be for a sector-exact image of its disc.
    Iso {
        path: &'a Path,
        title: DiscTitle,
        format: crate::disc::ContentFormat,
    },
    /// Live single-pass mux off a raw disc reader (the inline `DiscStream`).
    Live {
        reader: Box<dyn SectorSource>,
        title: DiscTitle,
        format: crate::disc::ContentFormat,
    },
    /// Any URL-addressed source; the only variant that scans. `keys` goes into
    /// [`InputOptions::keys`].
    Url { url: &'a str, opts: InputOptions },
}

/// Run the decrypt + mux pipeline end-to-end with the rip's up-front key set (KU §3.1):
/// every AACS read goes through the set's readers (its map, and the on-arrival proof for
/// pieces it left unproven), with no key lookup. `keys == None`, a non-AACS set, or
/// `opts.raw`: no AACS decryption (CSS and clear discs decrypt as before). E7013 when the
/// set is not for the disc or does not cover the title; E7026 when the title needs
/// forensic keys that are Pending. Unresolved codec headers are [`Error::MkvInvalid`], a
/// zero-output drain [`Error::NoStreams`]; a `halt` mid-run yields `completed = false`.
pub fn mux_with_keys(
    source: MuxSource,
    keys: Option<&crate::keys::ResolvedKeySet>,
    dest_url: &str,
    opts: &MuxOptions,
    halt: &Halt,
    events: std::sync::Arc<dyn MuxEvents>,
) -> std::io::Result<MuxOutcome> {
    let set = keys.filter(|s| s.is_aacs() && !opts.raw);
    // KU §3.1: "`keys` must be `Some` for AACS". A BD-TS Iso/Live mux with no AACS set reads
    // through a keyless set: the first AACS-flagged unit is E7022, never muxed as content.
    // MPEG-PS keeps the old path (a DVD cracks its own CSS key; `MuxSource` has no disc).
    let keyless = match (&source, set) {
        (MuxSource::Iso { title, format, .. } | MuxSource::Live { title, format, .. }, None)
            if !opts.raw && *format == crate::disc::ContentFormat::BdTs =>
        {
            Some(crate::keys::ResolvedKeySet::keyless_for(title, *format))
        }
        // A Session over an AACS disc with no set: BD-TS as above; HD DVD is never
        // probed (KU §2.6), so it refuses up front.
        (
            MuxSource::Session {
                session,
                title_index,
            },
            None,
        ) if !opts.raw => match session.disc() {
            Some(d) if d.aacs.is_some() => {
                if d.content_format != crate::disc::ContentFormat::BdTs {
                    return Err(Error::NoDiscKey {
                        disc_hash: d.aacs_disc_hash(),
                    }
                    .into());
                }
                crate::keys::ResolvedKeySet::keyless_for_disc(d, *title_index)
            }
            _ => None,
        },
        _ => None,
    };
    let set = set.or(keyless.as_ref());
    match (source, set) {
        (MuxSource::Url { url, opts: mut o }, _) => {
            if let Some(k) = keys {
                o.keys = Some(k.clone());
            }
            mux_unkeyed(
                MuxSource::Url { url, opts: o },
                dest_url,
                opts,
                halt,
                events,
            )
        }
        (src, None) => mux_unkeyed(src, dest_url, opts, halt, events),
        (src, Some(set)) => mux_keyed(src, set, dest_url, opts, halt, events),
    }
}

// The keyed arms of `mux_with_keys`: the set's readers, never a resolve.
fn mux_keyed(
    source: MuxSource,
    set: &crate::keys::ResolvedKeySet,
    dest_url: &str,
    opts: &MuxOptions,
    halt: &Halt,
    events: std::sync::Arc<dyn MuxEvents>,
) -> std::io::Result<MuxOutcome> {
    let (stream, playlist, source): (Box<dyn Stream>, Option<String>, SourceInfo) = match source {
        MuxSource::Iso { path, title, .. } => {
            let mut title = title;
            opts.selection
                .apply(&mut title)
                .map_err(std::io::Error::from)?;
            let source = SourceInfo {
                medium: Medium::Iso,
                path: format!("iso://{}", path.display()),
                playlist: title.playlist.clone(),
                ..SourceInfo::default()
            };
            let reader = FileSectorSource::open(path)?;
            let stream = super::resolve::build_iso_pipeline_keyed(
                reader,
                title,
                set,
                opts.batch_sectors,
                Some(halt.clone()),
                Some(reader_event_fn(events.clone())),
            )?;
            (Box::new(stream), None, source)
        }
        MuxSource::Session {
            session,
            title_index,
        } => {
            let (title, format, playlist, source) = {
                let disc = session.disc().ok_or_else(|| Error::DeviceNotReady {
                    path: session.device_path().to_string(),
                })?;
                let scope = crate::keys::KeyScope::Titles(vec![title_index]);
                if !set.is_for(disc) || !set.covers(&scope) {
                    tracing::error!(target: "freemkv::keys", "key set is not for this session's title");
                    return Err(Error::DecryptFailed.into());
                }
                let title = disc
                    .titles
                    .get(title_index)
                    .cloned()
                    .ok_or(Error::MuxTrackRange {
                        track: title_index,
                        tracks: disc.titles.len(),
                    })?;
                let source = SourceInfo {
                    medium: Medium::Disc,
                    path: format!("disc://{}", session.device_path()),
                    title: title_index,
                    playlist: title.playlist.clone(),
                    volume_id: disc.volume_id.clone(),
                };
                let playlist = disc
                    .meta_title
                    .clone()
                    .unwrap_or_else(|| disc.volume_id.clone());
                (title, disc.content_format, playlist, source)
            };
            let reader = session.take_reader().ok_or_else(|| Error::DeviceNotReady {
                path: session.device_path().to_string(),
            })?;
            let stream = live_keyed(reader, title, format, set, opts, halt, &events)?;
            (stream, Some(playlist), source)
        }
        MuxSource::Live {
            reader,
            title,
            format,
        } => {
            let source = SourceInfo {
                medium: Medium::Disc,
                playlist: title.playlist.clone(),
                ..SourceInfo::default()
            };
            let stream = live_keyed(reader, title, format, set, opts, halt, &events)?;
            (stream, None, source)
        }
        MuxSource::Url { .. } => unreachable!("mux_with_keys routes Url to mux_unkeyed"),
    };
    let mut source = source;
    if let Some(name) = playlist.as_deref() {
        source.playlist = name.to_string();
    }
    drive_mux(
        stream,
        dest_url,
        halt,
        events.as_ref(),
        playlist.as_deref(),
        Some(&source),
    )
}

// The inline live-drive `DiscStream` over the set: its keys, map and on-arrival proof.
fn live_keyed(
    reader: Box<dyn SectorSource>,
    mut title: DiscTitle,
    format: crate::disc::ContentFormat,
    set: &crate::keys::ResolvedKeySet,
    opts: &MuxOptions,
    halt: &Halt,
    events: &std::sync::Arc<dyn MuxEvents>,
) -> std::io::Result<Box<dyn Stream>> {
    opts.selection
        .apply(&mut title)
        .map_err(std::io::Error::from)?;
    let ranges: Vec<(u32, u32)> = title
        .extents
        .iter()
        .map(|e| (e.start_lba, e.start_lba.saturating_add(e.sector_count)))
        .collect();
    set.gate(reader.random_access(), Some(&ranges), false)?;
    let stream = crate::mux::DiscStream::new(
        reader,
        title,
        set.decrypt_keys(),
        opts.batch_sectors,
        format,
        false,
        Some(halt.clone()),
    )?;
    let mut stream = crate::keys::install_key_map(stream, set.key_map());
    if let Some(a) = set.arrival(set.title_stop()) {
        stream = stream.with_arrival(a);
    }
    stream.skip_errors = opts.skip_errors;
    stream.on_event(reader_event_fn(events.clone()));
    Ok(Box::new(stream))
}

// Decrypt keys for the live `Session` mux of `disc`. A DVD is handed `DecryptKeys::None` so
// `DiscStream::new` cracks the CORRECT per-title CSS key rather than the whole-disc
// (largest-title) VTS key.
fn session_mux_keys(disc: &crate::disc::Disc) -> DecryptKeys {
    if matches!(disc.format, crate::disc::DiscFormat::Dvd) {
        DecryptKeys::None
    } else {
        disc.decrypt_keys()
    }
}

// Adapt reader events to mux progress callbacks with an owned, 'static closure.
fn reader_event_fn(events: Arc<dyn MuxEvents>) -> crate::sector::prefetched::EventFn {
    Box::new(move |e: Event| match e.kind {
        EventKind::BytesRead { bytes, total } => events.on_read_progress(bytes, total),
        EventKind::SectorSkipped { sector } => events.on_sector_skipped(sector as u32),
        EventKind::BatchSizeChanged { new_size, reason } => {
            events.on_batch_size_changed(new_size, reason)
        }
        EventKind::ReadError { sector, .. } => events.on_read_error(sector as u32),
        _ => {}
    })
}

// Join the write consumer after the pump. A send that hit its deadline means the
// consumer is wedged, so the join gets only the short grace, not JOIN_TIMEOUT.
// While it waits, each increase of the output's durable bytes is forwarded (§4.5, LP20).
fn finish_pumped<I: Send + 'static, R: Send + 'static>(
    pipe: Pipeline<I, R>,
    halt: &Halt,
    send_timed_out: bool,
    flush: &FlushProgress,
    events: &dyn MuxEvents,
) -> Result<R, Error> {
    let mut fwd = FlushForwarder {
        flush,
        events,
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

// At most one `on_flush_progress` per this long (4 Hz, §4.5).
const FLUSH_PROGRESS_EVERY: Duration = Duration::from_millis(250);

// Forwards increases of the output's durable bytes to `MuxEvents::on_flush_progress`:
// one call per increase, rate-limited, none while nothing moves.
struct FlushForwarder<'a> {
    flush: &'a FlushProgress,
    events: &'a dyn MuxEvents,
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
            self.events
                .on_flush_progress(done, self.flush.bytes_total());
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
    mut stream: Box<dyn Stream>,
    dest_url: &str,
    halt: &Halt,
    events: &dyn MuxEvents,
    playlist_name: Option<&str>,
    source: Option<&SourceInfo>,
) -> std::io::Result<MuxOutcome> {
    // Title assembled from the scanned metadata; the playlist name (disc name)
    // overrides `info().playlist` where the consumer supplied one.
    let mut out_title = stream.info().clone();
    if let Some(name) = playlist_name {
        out_title.playlist = name.to_string();
    }

    // ── chapters:// / json:// short-circuit — BEFORE the header pump/gate ──
    // These sinks write their whole file at `output()` time and consume no PES
    // frames — running the header gate first could false-fail a metadata export.
    if matches!(
        parse_url(dest_url),
        StreamUrl::Chapters { .. } | StreamUrl::Json { .. }
    ) {
        let mut sink = CountingStream::new(output(dest_url, &out_title, source)?);
        events.on_output_opened(&out_title);
        sink.finish()?;
        return Ok(MuxOutcome {
            completed: true,
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
    let mut buffered: Vec<PesFrame> = Vec::new();
    let mut buffered_bytes: usize = 0;
    while !stream.headers_ready() {
        if halt.is_cancelled() {
            return Ok(MuxOutcome {
                completed: false,
                output_opened: false,
                bytes_written: 0,
                errors: stream.errors(),
                lost_bytes: stream.lost_bytes(),
                streams: 0,
                undelivered_streams: Vec::new(),
            });
        }
        let read = match stream.read() {
            Ok(r) => r,
            // A halt landing DURING a blocking read surfaces as `Error::Halted`
            // (reads dominate wall-clock, so a stop usually lands here). Not a
            // failure — yield `completed = false` so stop-preserves-staging runs.
            Err(e) if crate::error::is_halt(&e) => {
                return Ok(MuxOutcome {
                    completed: false,
                    output_opened: false,
                    bytes_written: 0,
                    errors: stream.errors(),
                    lost_bytes: stream.lost_bytes(),
                    streams: 0,
                    undelivered_streams: Vec::new(),
                });
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
        return Ok(MuxOutcome {
            completed: false,
            output_opened: false,
            bytes_written: 0,
            errors: stream.errors(),
            lost_bytes: stream.lost_bytes(),
            streams: 0,
            undelivered_streams: Vec::new(),
        });
    }
    // The pump can break on EOF without headers resolving. Finalising then would
    // write a track header with no CODEC_PRIVATE — a structurally-invalid MKV the
    // zero-output guard does not catch. Refuse.
    if !stream.headers_ready() {
        return Err(Error::MkvInvalid.into());
    }

    // Assemble the output title now that codec_privates have resolved.
    let info = stream.info().clone();
    out_title.streams = info.streams.clone();
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
    let flush = FlushProgress::new(crate::halt::Progress::new());
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
    if drops_mp2_extensions(dest_url) {
        refused.extend((out_title.streams.iter().enumerate()).filter_map(|(i, s)| {
            matches!(s, crate::disc::Stream::Audio(a) if a.is_mp2_extension()).then_some(i)
        }));
    }
    if refused.is_empty() {
        events.on_output_opened(&out_title);
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
        events.on_output_opened(&opened);
    }
    let output_stream = CountingStream::new(output_stream);

    // The write consumer runs on its own thread so the latency-bound sink write
    // overlaps the next `stream.read()`. `bytes` mirrors the consumer's running
    // written-byte count out to the driving thread for `on_write_progress`.
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
        // loop below does — the watchdog is fed only from `on_write_progress`,
        // and no `stream.read()` runs here to do it for us.
        events.on_write_progress(bytes.load(Ordering::Relaxed), total_bytes);
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
                    events.on_write_progress(bytes.load(Ordering::Relaxed), total_bytes);
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
        match finish_pumped(pipe, halt, send_timed_out, &flush, events) {
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
fn collect_late_configs(stream: &dyn Stream, pending: &mut Vec<usize>, out: &LateConfigs) {
    pending.retain(|&track| match stream.codec_private(track) {
        Some(cp) => {
            lock_late(out).push((track, cp));
            false
        }
        None => true,
    });
}

// Sinks that never write a DVD MPEG-2 multichannel extension track (no mapping for 13818-3
// `ext_frame`s); the FMKV wire (network://, stdio://) carries it.
fn drops_mp2_extensions(dest_url: &str) -> bool {
    matches!(
        parse_url(dest_url),
        StreamUrl::Mkv { .. }
            | StreamUrl::Mp4 { .. }
            | StreamUrl::M2ts { .. }
            | StreamUrl::Demux { .. }
            | StreamUrl::Audio { .. }
    )
}

// The sink for `dest_url`; a test may substitute its own (thread-local seam).
fn open_output(
    dest_url: &str,
    title: &DiscTitle,
    source: Option<&SourceInfo>,
    flush: super::resolve::OutputFlush<'_>,
) -> std::io::Result<Box<dyn Stream>> {
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
        let complete = !self.read_failed.load(Ordering::Relaxed);
        self.end(complete)
    }

    // A stopped title is incomplete too: a wire sink must not end it cleanly.
    fn close_stopped(self) -> Result<SinkClose, Error> {
        self.end(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::DiscTitle;

    thread_local! {
        // A sink `drive_mux` opens instead of its URL's, once (see `open_output`).
        pub(super) static TEST_SINK: std::cell::RefCell<Option<Box<dyn Stream>>> =
            const { std::cell::RefCell::new(None) };
    }

    // A sink that logs how it ended ("finish" / "finish_incomplete") and can fail
    // its `fail_at`-th write with E9000.
    struct EndSpy {
        info: DiscTitle,
        writes: usize,
        fail_at: Option<usize>,
        log: Arc<std::sync::Mutex<Vec<&'static str>>>,
    }

    impl Stream for EndSpy {
        fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
            Ok(None)
        }
        fn write(&mut self, _frame: &PesFrame) -> std::io::Result<()> {
            self.writes += 1;
            if self.fail_at.is_some_and(|n| self.writes >= n) {
                return Err(Error::StreamReadOnly.into());
            }
            Ok(())
        }
        fn finish(&mut self) -> std::io::Result<()> {
            self.log.lock().unwrap().push("finish");
            Ok(())
        }
        fn finish_incomplete(&mut self) -> std::io::Result<()> {
            self.log.lock().unwrap().push("finish_incomplete");
            Ok(())
        }
        fn info(&self) -> &DiscTitle {
            &self.info
        }
    }

    // Run `stream` into an `EndSpy`; returns the result and the sink's end log.
    fn run_into_spy(
        stream: FakeStream,
        halt: &Halt,
        fail_at: Option<usize>,
    ) -> (std::io::Result<MuxOutcome>, Vec<&'static str>) {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let spy = EndSpy {
            info: stream.info.clone(),
            writes: 0,
            fail_at,
            log: log.clone(),
        };
        TEST_SINK.with(|s| *s.borrow_mut() = Some(Box::new(spy)));
        let res = drive_mux(Box::new(stream), "null://", halt, &NoopEvents, None, None);
        TEST_SINK.with(|s| s.borrow_mut().take());
        let log = log.lock().unwrap().clone();
        (res, log)
    }

    // A read failure mid-title is the reported error; the consumer is joined and
    // ends the output as incomplete (a network receiver sees a failure).
    #[test]
    fn a_read_failure_mid_title_is_the_error_and_ends_the_output_incomplete() {
        let mut fs = FakeStream::new(1).with_frames(10);
        fs.fail_read_at = Some(4);
        let (res, log) = run_into_spy(fs, &Halt::new(), None);
        let err = res.expect_err("a read failure is an error");
        assert_eq!(
            crate::error::error_code(&err),
            Some(crate::error::E_DISC_READ)
        );
        assert_eq!(log, vec!["finish_incomplete"], "joined, ended incomplete");
    }

    // The sink failed first: its write error is the root cause, not the later read error.
    #[test]
    fn a_write_failure_is_reported_over_the_read_failure_it_precedes() {
        let mut fs = FakeStream::new(1).with_frames(10);
        fs.fail_read_at = Some(6);
        let (res, log) = run_into_spy(fs, &Halt::new(), Some(2));
        let err = res.expect_err("a write failure is an error");
        assert_eq!(
            crate::error::error_code(&err),
            Some(crate::error::E_STREAM_READ_ONLY),
            "got {err}"
        );
        assert!(log.is_empty(), "a failed sink is never finalised: {log:?}");
    }

    // A stop ends the output incomplete; a clean drain finishes it.
    #[test]
    fn only_a_clean_drain_finishes_the_output() {
        let halt = Halt::new();
        let fs = FakeStream::new(1).with_frames(10).cancels(halt.clone(), 3);
        let (res, log) = run_into_spy(fs, &halt, None);
        assert!(!res.expect("a stop is not an error").completed);
        assert_eq!(log, vec!["finish_incomplete"]);

        let (res, log) = run_into_spy(FakeStream::new(1).with_frames(10), &Halt::new(), None);
        assert!(res.expect("clean drain").completed);
        assert_eq!(log, vec!["finish"]);
    }

    /// A synthetic [`Stream`] the tests fully control: a queue of frames, a
    /// configurable `headers_ready` behaviour, and an optional halt it cancels
    /// after `cancel_after` reads (to drive the mid-pump interrupt path).
    struct FakeStream {
        info: DiscTitle,
        frames: std::collections::VecDeque<PesFrame>,
        /// Number of successful `read()`s after which `headers_ready` flips to
        /// true. `usize::MAX` means "never ready".
        headers_ready_after: usize,
        reads: usize,
        codec_private_ready: bool,
        /// If set, `read()` cancels this halt once `reads` reaches the value.
        cancel_halt: Option<(Halt, usize)>,
        /// If set, every successful `read()` bumps this shared counter so a test
        /// can observe how many frames the pump consumed after the stream was
        /// moved into `drive_mux`.
        read_observer: Option<Arc<std::sync::atomic::AtomicUsize>>,
        /// If set, `read()` returns `Err(Error::Halted)` once `reads` reaches the
        /// value — simulating a halt landing DURING a blocking `fill_extents` read
        /// (the common operator-stop case).
        halt_err_at_read: Option<usize>,
        /// If set, `read()` fails with a disc read error (not a halt) at this read.
        fail_read_at: Option<usize>,
        /// If set, `headers_ready` also flips once `read()` has returned `None`.
        ready_on_eof: bool,
        eof_seen: bool,
    }

    fn audio_stream() -> crate::disc::Stream {
        use crate::disc::{AudioChannels, AudioStream, Codec, LabelPurpose, SampleRate, Stream};
        Stream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::Aac,
            channels: AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        })
    }

    impl FakeStream {
        fn new(streams: usize) -> Self {
            let mut info = DiscTitle::empty();
            info.streams = (0..streams).map(|_| audio_stream()).collect();
            info.size_bytes = 1_000_000;
            FakeStream {
                info,
                frames: std::collections::VecDeque::new(),
                headers_ready_after: 0,
                reads: 0,
                codec_private_ready: true,
                cancel_halt: None,
                read_observer: None,
                halt_err_at_read: None,
                fail_read_at: None,
                ready_on_eof: false,
                eof_seen: false,
            }
        }
        /// After `after` successful reads, the next `read()` returns
        /// `Err(Error::Halted)` (a stop landing mid-read).
        fn halt_errs_at(mut self, after: usize) -> Self {
            self.halt_err_at_read = Some(after);
            self
        }
        fn with_frames(mut self, n: usize) -> Self {
            for i in 0..n {
                self.frames.push_back(PesFrame {
                    discard_padding_ns: 0,
                    track: 0,
                    pts: i as i64,
                    keyframe: true,
                    data: vec![0xAB; 100],
                    duration_ns: None,
                    source: None,
                    coding: None,
                });
            }
            self
        }
        fn never_ready(mut self) -> Self {
            self.headers_ready_after = usize::MAX;
            self.codec_private_ready = false;
            self
        }
        fn cancels(mut self, halt: Halt, after: usize) -> Self {
            self.cancel_halt = Some((halt, after));
            self
        }
    }

    impl Stream for FakeStream {
        fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
            if let Some((halt, after)) = &self.cancel_halt
                && self.reads >= *after
            {
                halt.cancel();
            }
            if let Some(after) = self.halt_err_at_read
                && self.reads >= after
            {
                return Err(crate::error::Error::Halted.into());
            }
            if self.fail_read_at.is_some_and(|at| self.reads >= at) {
                return Err(crate::error::Error::DiscRead {
                    sector: 0,
                    status: Some(0x02),
                    sense: None,
                }
                .into());
            }
            let f = self.frames.pop_front();
            self.eof_seen |= f.is_none();
            if f.is_some() {
                self.reads += 1;
                if let Some(obs) = &self.read_observer {
                    obs.fetch_add(1, Ordering::SeqCst);
                }
            }
            Ok(f)
        }
        fn write(&mut self, _frame: &PesFrame) -> std::io::Result<()> {
            Ok(())
        }
        fn finish(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn info(&self) -> &DiscTitle {
            &self.info
        }
        fn codec_private(&self, _track: usize) -> Option<Vec<u8>> {
            self.codec_private_ready.then(|| vec![1, 2, 3])
        }
        fn headers_ready(&self) -> bool {
            self.reads >= self.headers_ready_after || (self.ready_on_eof && self.eof_seen)
        }
    }

    /// Records whether `on_output_opened` fired.
    struct SpyEvents {
        opened: AtomicBool,
    }
    impl SpyEvents {
        fn new() -> Self {
            SpyEvents {
                opened: AtomicBool::new(false),
            }
        }
    }
    impl MuxEvents for SpyEvents {
        fn on_output_opened(&self, _title: &DiscTitle) {
            self.opened.store(true, Ordering::SeqCst);
        }
    }

    fn mp2_ext_title() -> DiscTitle {
        use crate::disc::{AudioChannels, AudioStream, Codec, LabelPurpose, SampleRate, Stream};
        let audio = |pid, label: &str| {
            Stream::Audio(AudioStream {
                pid,
                codec: Codec::Mp2,
                channels: AudioChannels::Stereo,
                language: "eng".into(),
                sample_rate: SampleRate::S48,
                secondary: false,
                purpose: LabelPurpose::Normal,
                label: label.into(),
            })
        };
        let mut t = DiscTitle::empty();
        t.content_format = crate::disc::ContentFormat::MpegPs;
        t.streams = vec![
            audio(0x00C0, ""),
            audio(0x00D0, crate::disc::MP2_EXTENSION_LABEL),
        ];
        t
    }

    fn ext_frame() -> PesFrame {
        PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 1,
            pts: 0,
            keyframe: true,
            // 13818-3 2nd ed. §2.5.2.10: "ext_syncword - A 12 bit string '0111 1111 1111'".
            data: vec![0x7F, 0xF0, 0x00],
            duration_ns: None,
        }
    }

    fn mp2_extension_warnings(ev: &[crate::testlog::CapturedEvent]) -> usize {
        (ev.iter())
            .filter(|e| e.level == tracing::Level::WARN)
            .filter(|e| e.message().contains("MPEG-2 multichannel extension"))
            .count()
    }

    /// The declared-only warning check must cover finish() too; captures are thread-local, so the
    /// sink is finished on the capturing thread instead of the consumer thread.
    #[test]
    fn declared_only_mp2_extension_gives_no_warning_at_finish() {
        let title = mp2_ext_title();
        let dir = tempfile::tempdir().expect("tempdir");
        let d = dir.path().display();
        for url in [
            format!("mkv://{d}/o.mkv"),
            format!("m2ts://{d}/o.m2ts"),
            format!("demux://{d}/demux"),
            format!("audio://{d}/audio"),
        ] {
            let mut sink = crate::mux::resolve::output(&url, &title, None).expect("sink opens");
            let (res, ev) = crate::testlog::capture(|| sink.finish());
            let _ = res;
            assert_eq!(mp2_extension_warnings(&ev), 0, "{url}: declared only");
            assert!(sink.undelivered_streams().is_empty(), "{url}");
        }
    }

    /// Guard (mpg design §7): every sink but mpg/network/stdio lists an MPEG-2 multichannel
    /// extension track as excluded once its packets arrive, so lost surround is never silent;
    /// IFO coding mode 3 alone (no `0xD0|n` packet) gives no warning and no note.
    #[test]
    fn every_sink_that_cannot_store_an_mp2_extension_reports_it_once_seen() {
        let title = mp2_ext_title();
        let dir = tempfile::tempdir().expect("tempdir");
        let d = dir.path().display();
        for url in [
            format!("mkv://{d}/o.mkv"),
            format!("m2ts://{d}/o.m2ts"),
            format!("demux://{d}/demux"),
            format!("audio://{d}/audio"),
        ] {
            let (mut sink, ev) = crate::testlog::capture(|| {
                crate::mux::resolve::output(&url, &title, None).expect("sink opens")
            });
            assert_eq!(mp2_extension_warnings(&ev), 0, "{url}: declared only");
            assert!(
                sink.undelivered_streams().is_empty(),
                "{url}: declared only"
            );
            let ((), ev) = crate::testlog::capture(|| {
                sink.write(&ext_frame()).unwrap();
                sink.write(&ext_frame()).unwrap();
            });
            assert_eq!(mp2_extension_warnings(&ev), 1, "{url}: warned once");
            assert_eq!(sink.undelivered_streams(), vec![1], "{url}");
        }
        // mp4's pre-mux plan never lists it; the sink does once packets arrive (mp4 tests).
        let fit = crate::mux::mp4::fit_report(&title);
        assert!(
            !fit.skipped.iter().any(|&(i, _)| i == 1),
            "{:?}",
            fit.skipped
        );
        // The FMKV wire (network://, stdio://) keeps it: the label round-trips.
        let wire = crate::mux::meta::M2tsMeta::from_title(&title).to_title();
        assert!(matches!(&wire.streams[1], crate::disc::Stream::Audio(a) if a.is_mp2_extension()));
    }

    fn tmp(name: &str) -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(name);
        let url = format!("chapters://{}", path.display());
        (dir, url)
    }

    // The driver is the ONLY place that can supply a `fvi://` sink its
    // provenance. Pin that `SourceInfo` reaches the header verbatim and
    // the destination path is nowhere in it.
    #[test]
    fn drive_mux_threads_provenance_into_the_fvi_header() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dst = dir.path().join("index.fvi");
        let url = format!("fvi://{}", dst.display());
        let source = SourceInfo {
            medium: Medium::Iso,
            path: "iso://m.iso".into(),
            title: 1,
            playlist: "00800.mpls".into(),
            volume_id: "VOL".into(),
        };
        let stream = Box::new(FakeStream::new(1).with_frames(2));
        let halt = Halt::new();
        let spy = SpyEvents::new();
        drive_mux(stream, &url, &halt, &spy, None, Some(&source)).expect("fvi mux runs");

        let text = std::fs::read_to_string(&dst).expect("index written");
        let hdr: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
        assert_eq!(hdr["source"]["path"], "iso://m.iso");
        assert_eq!(hdr["source"]["medium"], "iso");
        assert_eq!(hdr["source"]["title"], 1);
        assert_eq!(hdr["source"]["playlist"], "00800.mpls");
        assert_eq!(hdr["source"]["volume_id"], "VOL");
        assert!(
            !text.contains("index.fvi"),
            "the destination path must never appear in the index it names"
        );
    }

    /// The provenance medium follows the SOURCE scheme, not the sink's. A disc
    /// image is `iso`, a container file is `file`, and a socket / stdio is
    /// `stream` — the header used to report `file` unconditionally.
    #[test]
    fn url_medium_follows_the_source_scheme() {
        assert_eq!(url_medium(&parse_url("iso://d.iso")), Medium::Iso);
        assert_eq!(url_medium(&parse_url("disc://")), Medium::Disc);
        assert_eq!(url_medium(&parse_url("mkv://m.mkv")), Medium::File);
        assert_eq!(url_medium(&parse_url("m2ts://m.m2ts")), Medium::File);
        assert_eq!(url_medium(&parse_url("mp4://m.mp4")), Medium::File);
        assert_eq!(url_medium(&parse_url("network://h:9000")), Medium::Stream);
        assert_eq!(url_medium(&parse_url("stdio://")), Medium::Stream);
    }

    // ── chapters:// / json:// short-circuit runs even when headers never
    //    resolve (the bug fix). Mutation: moving the header gate before the
    //    short-circuit makes this return Err(MkvInvalid) and the test fails.
    #[test]
    fn chapters_short_circuits_before_header_gate() {
        let stream = Box::new(FakeStream::new(2).never_ready());
        let (_dir, url) = tmp("out.xml");
        let halt = Halt::new();
        let spy = SpyEvents::new();
        let out =
            drive_mux(stream, &url, &halt, &spy, None, None).expect("chapters must short-circuit");
        assert!(out.completed, "metadata sink completes without headers");
        assert!(out.output_opened);
        assert!(spy.opened.load(Ordering::SeqCst), "sink was opened");
        assert_eq!(out.streams, 2);
    }

    #[test]
    fn json_short_circuits_before_header_gate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let url = format!("json://{}", dir.path().join("out.json").display());
        let stream = Box::new(FakeStream::new(1).never_ready());
        let halt = Halt::new();
        let out = drive_mux(stream, &url, &halt, &NoopEvents, None, None)
            .expect("json must short-circuit");
        assert!(out.completed);
        assert!(out.output_opened);
    }

    // ── header gate rejects a stream whose codec_private never resolves. ──
    // Mutation: dropping the gate lets it proceed to a NoStreams / success path.
    #[test]
    fn header_gate_rejects_unresolved_codec_private() {
        let stream = Box::new(FakeStream::new(1).with_frames(3).never_ready());
        let halt = Halt::new();
        let err = drive_mux(stream, "null://", &halt, &NoopEvents, None, None)
            .expect_err("unresolved headers must be refused");
        // This gate is the GENUINE stub case: no video track's codec_private
        // resolved. `MkvInvalid` now means only this (bad `mkv://` input is
        // `MkvSourceInvalid`), and must stay skippable for all-titles rips.
        assert!(
            crate::error::is_skippable_title_stub(&err),
            "MkvInvalid is a skippable stub, got {err}"
        );
        assert_eq!(err.to_string(), format!("E{}", crate::error::E_MKV_INVALID));
    }

    // ── zero-output gate: a headers-ready stream that yields no frames. ──
    // Mutation: dropping the gate returns completed=true with 0 bytes.
    #[test]
    fn zero_output_gate_refuses_empty_drain() {
        let stream = Box::new(FakeStream::new(1)); // headers ready, no frames
        let halt = Halt::new();
        let err = drive_mux(stream, "null://", &halt, &NoopEvents, None, None)
            .expect_err("empty drain must be refused");
        assert_eq!(err.to_string(), format!("E{}", crate::error::E_NO_STREAMS));
    }

    // ── halt mid-pump stops cleanly with completed=false, no panic. ──
    #[test]
    fn halt_mid_pump_stops_cleanly() {
        let halt = Halt::new();
        // Ready immediately, plenty of frames, cancels the halt after 2 reads.
        let stream = Box::new(
            FakeStream::new(1)
                .with_frames(1000)
                .cancels(halt.clone(), 2),
        );
        let out = drive_mux(stream, "null://", &halt, &NoopEvents, None, None)
            .expect("halt is a clean stop, not an error");
        assert!(!out.completed, "an interrupted mux is not complete");
        assert!(out.output_opened, "the sink was opened before the halt");
    }

    // A halt that ends the stream as Ok(None) can also release the header gate
    // (EOF expires the AAC wait); it must still stop before the sink opens.
    #[test]
    fn halt_ending_the_header_pump_never_opens_the_output() {
        let halt = Halt::new();
        let mut fs = FakeStream::new(1).with_frames(1).cancels(halt.clone(), 1);
        fs.headers_ready_after = usize::MAX;
        fs.ready_on_eof = true;
        let events = SpyEvents::new();
        let out = drive_mux(Box::new(fs), "null://", &halt, &events, None, None)
            .expect("halt is a clean stop");
        assert!(!out.completed);
        assert!(!out.output_opened, "no sink may be opened after a halt");
        assert!(!events.opened.load(Ordering::SeqCst));
    }

    // ── A halt landing mid-read (Err(Halted), the common operator-stop case)
    //    is a clean stop, NOT a failure — both pumps must yield completed=false.
    //    Mutation: propagating the read-arm Err instead of mapping it panics. ──
    #[test]
    fn halt_err_during_frame_read_yields_completed_false() {
        let halt = Halt::new();
        // Headers ready immediately; the 3rd read (in the frame pump) errors Halted.
        let stream = Box::new(FakeStream::new(1).with_frames(1000).halt_errs_at(2));
        let out = drive_mux(stream, "null://", &halt, &NoopEvents, None, None)
            .expect("a halt mid frame-read is a clean stop, not an Err");
        assert!(!out.completed, "interrupted mux is not complete");
        assert!(out.output_opened, "sink opened before the mid-read halt");
    }

    #[test]
    fn halt_err_during_header_read_yields_completed_false() {
        let halt = Halt::new();
        // Headers never resolve; the 2nd read (in the header pump) errors Halted.
        let stream = Box::new(
            FakeStream::new(1)
                .with_frames(1000)
                .never_ready()
                .halt_errs_at(1),
        );
        let out = drive_mux(stream, "null://", &halt, &NoopEvents, None, None)
            .expect("a halt mid header-read is a clean stop, not an Err");
        assert!(!out.completed, "interrupted mux is not complete");
        assert!(
            !out.output_opened,
            "halt before headers resolve → sink never opened"
        );
    }

    // ── a normal stream pumps N frames → bytes_written>0, completed=true. ──
    #[test]
    fn normal_stream_completes_with_bytes() {
        let stream = Box::new(FakeStream::new(2).with_frames(10));
        let halt = Halt::new();
        let spy = SpyEvents::new();
        let out =
            drive_mux(stream, "null://", &halt, &spy, None, None).expect("normal mux completes");
        assert!(out.completed);
        assert!(out.output_opened);
        assert!(spy.opened.load(Ordering::SeqCst));
        assert_eq!(out.bytes_written, 10 * 100, "10 frames × 100 bytes payload");
        assert_eq!(out.streams, 2);
    }

    // ── reader-side event forwarding through the Arc ────────────────────────

    /// A [`MuxEvents`] backed by atomics that records every callback so a test
    /// can assert reader-side events actually flowed through the `Arc` clone.
    struct CountingEvents {
        opened: AtomicBool,
        progress_calls: AtomicU64,
        /// Set once `on_read_progress` is called with a `total` equal to the ISO
        /// extents' byte total — the fingerprint of the *read-side* `BytesRead`
        /// event (the write-side `on_write_progress` carries the title's
        /// `size_bytes`, a different number), so it isolates the `EventFn`
        /// translation.
        saw_read_total: AtomicBool,
        read_total: u64,
        skipped: AtomicU64,
        batch_changed: AtomicU64,
        read_errors: AtomicU64,
    }
    impl CountingEvents {
        fn new(read_total: u64) -> std::sync::Arc<Self> {
            std::sync::Arc::new(CountingEvents {
                opened: AtomicBool::new(false),
                progress_calls: AtomicU64::new(0),
                saw_read_total: AtomicBool::new(false),
                read_total,
                skipped: AtomicU64::new(0),
                batch_changed: AtomicU64::new(0),
                read_errors: AtomicU64::new(0),
            })
        }
    }
    impl MuxEvents for CountingEvents {
        fn on_output_opened(&self, _title: &DiscTitle) {
            self.opened.store(true, Ordering::SeqCst);
        }
        fn on_read_progress(&self, _bytes_read: u64, bytes_total: u64) {
            self.progress_calls.fetch_add(1, Ordering::SeqCst);
            if bytes_total == self.read_total {
                self.saw_read_total.store(true, Ordering::SeqCst);
            }
        }
        fn on_sector_skipped(&self, _lba: u32) {
            self.skipped.fetch_add(1, Ordering::SeqCst);
        }
        fn on_batch_size_changed(&self, _batch: u16, _reason: BatchSizeReason) {
            self.batch_changed.fetch_add(1, Ordering::SeqCst);
        }
        fn on_read_error(&self, _lba: u32) {
            self.read_errors.fetch_add(1, Ordering::SeqCst);
        }
    }

    // `reader_event_fn` maps every real `EventKind` onto the matching
    // `MuxEvents` method. Mutation: dropping a match arm leaves that
    // counter at 0 and fails here.
    #[test]
    fn reader_event_fn_translates_every_variant() {
        let events = CountingEvents::new(6144);
        let f = reader_event_fn(events.clone());
        f(Event {
            kind: EventKind::BytesRead {
                bytes: 4096,
                total: 6144,
            },
        });
        f(Event {
            kind: EventKind::SectorSkipped { sector: 42 },
        });
        f(Event {
            kind: EventKind::BatchSizeChanged {
                new_size: 8,
                reason: BatchSizeReason::Shrunk,
            },
        });
        f(Event {
            kind: EventKind::ReadError {
                sector: 7,
                error: Error::NoStreams,
            },
        });
        assert_eq!(
            events.progress_calls.load(Ordering::SeqCst),
            1,
            "BytesRead → on_read_progress"
        );
        assert!(
            events.saw_read_total.load(Ordering::SeqCst),
            "read-side total forwarded"
        );
        assert_eq!(
            events.skipped.load(Ordering::SeqCst),
            1,
            "SectorSkipped → on_sector_skipped"
        );
        assert_eq!(
            events.batch_changed.load(Ordering::SeqCst),
            1,
            "BatchSizeChanged → on_batch_size_changed"
        );
        assert_eq!(
            events.read_errors.load(Ordering::SeqCst),
            1,
            "ReadError → on_read_error"
        );
    }

    /// A 192-byte BD-TS packet carrying `payload` as a payload-only TS packet on
    /// `pid` (4-byte TP_extra_header + 188-byte TS packet, AFC=payload-only).
    fn bdts_data_packet(pid: u16, pusi: bool, payload: &[u8]) -> [u8; 192] {
        let mut pkt = [0u8; 192];
        pkt[4] = 0x47;
        pkt[5] = ((pid >> 8) as u8) & 0x1F;
        if pusi {
            pkt[5] |= 0x40;
        }
        pkt[6] = (pid & 0xFF) as u8;
        pkt[7] = 0x10;
        let n = payload.len().min(184);
        pkt[8..8 + n].copy_from_slice(&payload[..n]);
        pkt
    }

    /// A complete audio PES (stream_id 0xC0, no PTS) carrying `es`.
    fn audio_pes(es: &[u8]) -> Vec<u8> {
        let mut v = vec![0x00, 0x00, 0x01, 0xC0];
        let len = (3 + es.len()) as u16;
        v.extend_from_slice(&len.to_be_bytes());
        v.extend_from_slice(&[0x80, 0x00, 0x00]);
        v.extend_from_slice(es);
        v
    }

    fn aac_audio_title(pid: u16) -> DiscTitle {
        use crate::disc::{AudioChannels, AudioStream, Codec, LabelPurpose, SampleRate, Stream};
        let mut t = DiscTitle::empty();
        t.streams.push(Stream::Audio(AudioStream {
            pid,
            codec: Codec::Aac,
            channels: AudioChannels::Stereo,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: String::new(),
        }));
        t
    }

    // End-to-end through `mux_unkeyed` on the ISO path: asserts reader-side
    // progress AND `on_output_opened` reach the `Arc<dyn MuxEvents>`.
    // Mutation: dropping `reader_event_fn` leaves `saw_read_total` false.
    #[test]
    fn mux_iso_forwards_reader_progress_through_arc() {
        // Spawns the prefetch producer, a Drive holder.
        let _serial = crate::sector::prefetched::holder_test_lock();
        let es = [0xDE, 0xAD, 0xBE, 0xEF, 0x11, 0x22];
        let pkt = bdts_data_packet(0x1100, true, &audio_pes(&es));
        let mut data = vec![0u8; 3 * 2048]; // 3 sectors = one AACS unit = 6144 bytes
        data[..192].copy_from_slice(&pkt);

        let dir = tempfile::tempdir().expect("tempdir");
        let iso_path = dir.path().join("clip.iso");
        std::fs::write(&iso_path, &data).expect("write iso");

        let mut title = aac_audio_title(0x1100);
        title.extents = vec![crate::disc::Extent {
            start_lba: 0,
            sector_count: 3,
        }];

        let events = CountingEvents::new(3 * 2048);
        let opts = MuxOptions {
            skip_errors: false,
            batch_sectors: 8192,
            raw: false,
            selection: Default::default(),
        };
        let halt = Halt::new();
        let out = mux_unkeyed(
            MuxSource::Iso {
                path: &iso_path,
                title,
                format: crate::disc::ContentFormat::BdTs,
            },
            "null://",
            &opts,
            &halt,
            events.clone(),
        )
        .expect("audio-only ISO muxes to null sink");

        assert!(out.completed, "the clip drained and finalised");
        assert!(
            events.saw_read_total.load(Ordering::SeqCst),
            "the reader-side BytesRead (total=6144) reached the Arc via the EventFn"
        );
        assert!(
            events.opened.load(Ordering::SeqCst),
            "on_output_opened fired through the Arc"
        );
        assert!(
            events.progress_calls.load(Ordering::SeqCst) > 0,
            "at least one on_read_progress call observed"
        );
    }

    // A `SectorSource` serving ONE genuinely-AACS-encrypted aligned unit
    // (6144 bytes) at LBA 0..3 and zeros elsewhere, so a UDF probe fails cleanly.
    struct AacsUnitReader {
        unit: Vec<u8>, // 6144 bytes, encrypted
        capacity: u32,
    }
    impl crate::sector::SectorSource for AacsUnitReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            let bytes = count as usize * 2048;
            buf[..bytes].fill(0);
            // The content unit lives at LBA 0..3; serve it whenever a read starts
            // there (the inline `DiscStream` reads the [0,3) extent as one batch).
            if lba == 0 && bytes >= self.unit.len() {
                buf[..self.unit.len()].copy_from_slice(&self.unit);
            }
            Ok(bytes)
        }
        fn capacity_sectors(&self) -> u32 {
            self.capacity
        }
    }

    /// Build one AACS-encrypted BD-TS aligned unit whose plaintext is a single
    /// audio PES (the same clip the ISO test muxes), encrypted under `unit_key`.
    fn encrypted_audio_unit(unit_key: &[u8; 16]) -> Vec<u8> {
        let es = [0xDE, 0xAD, 0xBE, 0xEF, 0x11, 0x22];
        let pkt = bdts_data_packet(0x1100, true, &audio_pes(&es));
        let mut unit = vec![0u8; 3 * 2048]; // one 6144-byte aligned unit
        unit[..192].copy_from_slice(&pkt);
        // Flag encrypted BEFORE encrypting: bytes 0..16 are the key seed.
        unit[0] |= 0xC0;
        assert!(
            crate::aacs::content::encrypt_unit(&mut unit, unit_key),
            "a full-length unit must encrypt"
        );
        unit
    }

    /// A synthetic AACS `Disc` with one title whose sole extent is the encrypted unit at
    /// LBA 0..3 — the disc a `MuxSource::Session` mux scans off a live drive, minus the
    /// hardware. It holds no key: AACS keys come only from a `ResolvedKeySet`.
    fn aacs_session_disc(title: DiscTitle) -> crate::disc::Disc {
        crate::disc::Disc {
            volume_id: "TEST".into(),
            meta_title: None,
            format: crate::DiscFormat::Uhd,
            capacity_sectors: 0,
            capacity_bytes: 0,
            layers: 1,
            titles: vec![title],
            region: crate::disc::DiscRegion::Free,
            aacs: Some(crate::disc::AacsState {
                version: crate::aacs::mkb::AACS_MAJOR_UHD,
                bus_encryption: false,
                mkb_version: None,
                disc_hash: "0xabc".into(),
                volume_id: [0u8; 16],
                uk_ro: Vec::new(),
                mkb: Vec::new(),
            }),
            css: None,
            encrypted: true,
            aacs_error: None,
            css_error: None,
            content_format: crate::ContentFormat::BdTs,
        }
    }

    // A `MuxSource::Session` with an unstaged reader (`take_reader()` →
    // `None`) must surface a clean typed error, NOT panic — guards
    // `ok_or_else(|| Error::DeviceNotReady …)` against `.unwrap()` regression.
    #[test]
    fn mux_session_missing_reader_is_clean_error_not_panic() {
        use crate::disc::Extent;
        use crate::session::DiscSession;

        let mut title = aac_audio_title(0x1100);
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: 3,
        }];
        let disc = aacs_session_disc(title);
        // reader: None — never staged.
        let mut session = DiscSession::from_parts_for_test(Some(disc), None);

        let opts = MuxOptions {
            skip_errors: false,
            batch_sectors: 3,
            raw: false,
            selection: Default::default(),
        };
        let halt = Halt::new();
        let err = mux_unkeyed(
            MuxSource::Session {
                session: &mut session,
                title_index: 0,
            },
            "null://",
            &opts,
            &halt,
            Arc::new(NoopEvents),
        )
        .expect_err("a missing staged reader must be a clean error, not a panic");
        // The device-name-carrying DeviceNotReady (code E4xxx) round-trips through
        // io::Error; assert it is NOT a decrypt/other-shaped failure.
        assert!(
            err.to_string().starts_with('E'),
            "expected a typed libfreemkv error (E<code>…), got: {err}"
        );
    }

    // FIX 3: a mux with a clean read side but a WEDGED write-finish must fall
    // through to `completed = false`, tested via the extracted pure fn
    // since the wedge is reachable only via real write-thread timing.
    #[test]
    fn finalize_failed_forces_incomplete_outcome() {
        // The load-bearing case: clean drain, wedged finalize → NOT completed.
        assert!(
            !mux_run_completed(false, true, false),
            "a wedged/halted finalize must force completed = false"
        );
        // A fully clean finish is the only path to completed = true.
        assert!(
            mux_run_completed(false, false, false),
            "a clean drain + clean finalize completes"
        );
        // The other two forcers likewise yield incomplete.
        assert!(
            !mux_run_completed(true, false, false),
            "operator stop → incomplete"
        );
        assert!(
            !mux_run_completed(false, false, true),
            "halt cancel → incomplete"
        );
    }

    // FIX 4: `MuxSource::Session` with a `title_index` past the disc's title
    // count must surface a clean `Error::MuxTrackRange` (E9011), NOT panic
    // on the out-of-range `titles.get(idx)`.
    #[test]
    fn mux_session_out_of_range_title_is_clean_error_not_panic() {
        use crate::disc::Extent;
        use crate::session::DiscSession;

        let unit_key = [0x5Au8; 16];
        let reader = Box::new(AacsUnitReader {
            unit: encrypted_audio_unit(&unit_key),
            capacity: 2048,
        });
        let mut title = aac_audio_title(0x1100);
        title.extents = vec![Extent {
            start_lba: 0,
            sector_count: 3,
        }];
        let disc = aacs_session_disc(title);
        let num_titles = disc.titles.len();
        let mut session = DiscSession::from_parts_for_test(Some(disc), Some(reader));

        let opts = MuxOptions {
            skip_errors: false,
            batch_sectors: 3,
            raw: false,
            selection: Default::default(),
        };
        let halt = Halt::new();
        let err = mux_unkeyed(
            MuxSource::Session {
                session: &mut session,
                title_index: num_titles + 5, // out of range
            },
            "null://",
            &opts,
            &halt,
            Arc::new(NoopEvents),
        )
        .expect_err("an out-of-range title index must be a clean error, not a panic");
        // MuxTrackRange renders as "E9011: track/tracks".
        assert!(
            err.to_string().contains("E9011"),
            "expected MuxTrackRange (E9011), got: {err}"
        );
    }

    // Video + a secondary AAC track whose first frame (and so its ASC) arrives at
    // 10 s, long after headers were finalised.
    struct LateAacStream {
        info: DiscTitle,
        frames: std::collections::VecDeque<PesFrame>,
        aac_seen: bool,
    }

    impl LateAacStream {
        fn new() -> Self {
            use crate::disc::{
                AudioChannels, AudioStream, Codec, ColorSpace, FrameRate, HdrFormat, LabelPurpose,
                Resolution, SampleRate, Stream as S, VideoStream,
            };
            let mut info = DiscTitle::empty();
            info.streams = vec![
                S::Video(VideoStream {
                    pid: 0x1011,
                    codec: Codec::H264,
                    resolution: Resolution::R1080p,
                    frame_rate: FrameRate::F25,
                    hdr: HdrFormat::Sdr,
                    color_space: ColorSpace::Bt709,
                    display_aspect: None,
                    secondary: false,
                    label: String::new(),
                    measured_cicp: None,
                }),
                S::Audio(AudioStream {
                    pid: 0x1100,
                    codec: Codec::Aac,
                    channels: AudioChannels::Stereo,
                    language: "eng".into(),
                    sample_rate: SampleRate::S44_1,
                    secondary: true,
                    purpose: LabelPurpose::Commentary,
                    label: String::new(),
                }),
            ];
            let frame = |track: usize, pts: i64| PesFrame {
                discard_padding_ns: 0,
                track,
                pts,
                keyframe: true,
                data: vec![0x11; 32],
                duration_ns: None,
                source: None,
                coding: None,
            };
            let mut frames = std::collections::VecDeque::new();
            for i in 0..300i64 {
                let pts = i * 40_000_000;
                frames.push_back(frame(0, pts));
                if pts >= 10_000_000_000 {
                    frames.push_back(frame(1, pts));
                }
            }
            LateAacStream {
                info,
                frames,
                aac_seen: false,
            }
        }
    }

    impl Stream for LateAacStream {
        fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
            let f = self.frames.pop_front();
            self.aac_seen |= f.as_ref().is_some_and(|f| f.track == 1);
            Ok(f)
        }
        fn write(&mut self, _frame: &PesFrame) -> std::io::Result<()> {
            Ok(())
        }
        fn finish(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn info(&self) -> &DiscTitle {
            &self.info
        }
        fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
            match track {
                0 => Some(vec![1, 0x64, 0, 0x28, 0xFF, 0xE0, 0, 0]),
                _ => self.aac_seen.then(|| vec![0x12, 0x10]),
            }
        }
    }

    // A seekable MKV must end up with the late AAC track's CodecPrivate.
    #[test]
    fn late_aac_config_is_backpatched_into_a_seekable_mkv() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("late.mkv");
        let halt = Halt::new();
        let out = drive_mux(
            Box::new(LateAacStream::new()),
            &format!("mkv://{}", path.display()),
            &halt,
            &NoopEvents,
            None,
            None,
        )
        .expect("mux succeeds");
        assert!(out.completed);
        let back = crate::mux::mkvstream::MkvStream::open(
            std::fs::File::open(&path).expect("output exists"),
        )
        .expect("output parses");
        assert_eq!(
            back.codec_private(1),
            Some(vec![0x12, 0x10]),
            "late AudioSpecificConfig must reach the track header"
        );
    }

    // Reserve fill covers every remainder shape (Void, none, the 1-byte case
    // absorbed by a wider size VINT); an unfilled reserve stays a valid Void.
    #[test]
    fn mkv_late_codec_private_fills_every_reserve_shape() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("shapes.mkv");
        let mut title = LateAacStream::new().info;
        let aac = title.streams[1].clone();
        title
            .streams
            .extend([aac.clone(), aac.clone(), aac.clone(), aac]);
        title.codec_privates = vec![Some(vec![1, 0x64, 0, 0x28, 0xFF, 0xE0, 0, 0])];
        let file = std::fs::File::create(&path).expect("create");
        let mut mkv = crate::mux::mkvstream::MkvStream::create(Box::new(file), &title, None)
            .expect("create mkv");
        let frame = |track: usize| PesFrame {
            discard_padding_ns: 0,
            track,
            pts: 0,
            keyframe: true,
            data: vec![0x11; 32],
            duration_ns: None,
            source: None,
            coding: None,
        };
        // Before activation the config lands in the pending track header.
        assert!(mkv.set_codec_private(5, &[0x11, 0x90]).expect("pending"));
        mkv.write(&frame(0)).expect("video activates the muxer");
        let cps: [Vec<u8>; 3] = [vec![0x12, 0x10], (0..12).collect(), (0..13).collect()];
        for (i, cp) in cps.iter().enumerate() {
            assert!(mkv.set_codec_private(i + 1, cp).expect("patch"));
            assert!(!mkv.set_codec_private(i + 1, cp).expect("second patch"));
        }
        // Track 4 still holds its reserve, so this reaches the size guard.
        assert!(!mkv.set_codec_private(4, &[0; 14]).expect("too big"));
        mkv.write(&frame(1)).expect("audio frame");
        mkv.finish().expect("finish");
        let back = crate::mux::mkvstream::MkvStream::open(std::fs::File::open(&path).unwrap())
            .expect("parses");
        for (i, cp) in cps.iter().enumerate() {
            assert_eq!(
                back.codec_private(i + 1).as_ref(),
                Some(cp),
                "track {}",
                i + 1
            );
        }
        assert_eq!(back.codec_private(4), None, "unfilled reserve is a Void");
        assert_eq!(back.codec_private(5), Some(vec![0x11, 0x90]));
        assert_eq!(back.info().streams.len(), 6);
    }

    // MVC folds the dependent view into the base track, shifting later stream
    // indices down: a late config must land on the remapped AAC track.
    #[test]
    fn mkv_late_codec_private_follows_the_mvc_track_remap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("mvc.mkv");
        let mut title = LateAacStream::new().info;
        let crate::disc::Stream::Video(base) = title.streams[0].clone() else {
            unreachable!()
        };
        let dep = crate::disc::VideoStream {
            pid: 0x1012,
            label: crate::disc::MVC_DEPENDENT_LABEL.to_string(),
            ..base
        };
        title.streams.insert(1, crate::disc::Stream::Video(dep));
        title.codec_privates = vec![Some(vec![1, 0x64, 0, 0x28, 0xFF, 0xE0, 0, 0])];
        let file = std::fs::File::create(&path).expect("create");
        let mut mkv = crate::mux::mkvstream::MkvStream::create(Box::new(file), &title, None)
            .expect("create mkv");
        assert!(
            !mkv.set_codec_private(1, &[1])
                .expect("dependent has no track")
        );
        assert!(mkv.set_codec_private(2, &[0x12, 0x10]).expect("aac"));
        mkv.write(&PesFrame {
            discard_padding_ns: 0,
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![0x11; 32],
            duration_ns: None,
            source: None,
            coding: None,
        })
        .expect("video");
        mkv.finish().expect("finish");
        let back = crate::mux::mkvstream::MkvStream::open(std::fs::File::open(&path).unwrap())
            .expect("parses");
        assert_eq!(back.info().streams.len(), 2, "dependent folded into base");
        assert_eq!(back.codec_private(1), Some(vec![0x12, 0x10]));
    }

    // Non-seekable/other sinks keep the old behaviour: no late patch, no error.
    #[test]
    fn late_aac_config_is_harmless_on_a_sink_without_backpatch() {
        let halt = Halt::new();
        let out = drive_mux(
            Box::new(LateAacStream::new()),
            "null://",
            &halt,
            &NoopEvents,
            None,
            None,
        )
        .expect("mux succeeds");
        assert!(out.completed);
    }

    /// A sink that accepts every frame but reports one stream it could not put in
    /// the finished container — the `mp4://` shape (an audio track dropped at
    /// `finish()` because no frame yielded a parseable sample entry).
    struct UndeliveringSink {
        info: DiscTitle,
    }

    impl Stream for UndeliveringSink {
        fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
            Ok(None)
        }
        fn write(&mut self, _frame: &PesFrame) -> std::io::Result<()> {
            Ok(())
        }
        fn finish(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn info(&self) -> &DiscTitle {
            &self.info
        }
        fn undelivered_streams(&self) -> Vec<usize> {
            vec![1]
        }
    }

    /// A source stream carrying one BD LPCM track, declared per `channels`/`rate`,
    /// whose parser reported `layout` (the tagged BD header byte) as codec_private.
    struct LpcmSource {
        info: DiscTitle,
        layout: Option<Vec<u8>>,
        frames: usize,
    }
    impl LpcmSource {
        fn new(
            channels: crate::disc::AudioChannels,
            rate: crate::disc::SampleRate,
            layout: Option<Vec<u8>>,
        ) -> Self {
            let lpcm = crate::disc::Codec::Lpcm;
            let mut info = DiscTitle::empty();
            info.streams = vec![crate::disc::Stream::Audio(crate::disc::AudioStream {
                pid: 0x1100,
                codec: lpcm,
                channels,
                language: "eng".into(),
                sample_rate: rate,
                secondary: false,
                purpose: crate::disc::LabelPurpose::Normal,
                label: crate::labels::generate_audio_label(&lpcm, &channels, false),
            })];
            LpcmSource {
                info,
                layout,
                frames: 1,
            }
        }
    }
    impl Stream for LpcmSource {
        fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
            if self.frames == 0 {
                return Ok(None);
            }
            self.frames -= 1;
            Ok(Some(PesFrame {
                discard_padding_ns: 0,
                track: 0,
                pts: 0,
                keyframe: true,
                data: vec![0; 240 * 8 * 3],
                duration_ns: None,
                source: None,
                coding: None,
            }))
        }
        fn write(&mut self, _frame: &PesFrame) -> std::io::Result<()> {
            Ok(())
        }
        fn finish(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn info(&self) -> &DiscTitle {
            &self.info
        }
        fn codec_private(&self, _track: usize) -> Option<Vec<u8>> {
            self.layout.clone()
        }
    }

    /// Keeps the title handed to `on_output_opened`.
    #[derive(Default)]
    struct TitleSpy(std::sync::Mutex<Option<DiscTitle>>);
    impl MuxEvents for TitleSpy {
        fn on_output_opened(&self, title: &DiscTitle) {
            *self.0.lock().unwrap() = Some(title.clone());
        }
    }

    fn run(src: LpcmSource, url: &str, spy: &TitleSpy) -> MuxOutcome {
        drive_mux(Box::new(src), url, &Halt::new(), spy, None, None).unwrap()
    }

    // Every entry point (Url/Session/Iso/Live) funnels into drive_mux: the BD LPCM
    // layout byte must override the playlist's "5.1" for 7.1 audio there.
    #[test]
    fn drive_mux_corrects_lpcm_channels_from_the_parser_layout() {
        use crate::disc::{AudioChannels, SampleRate};
        let dir = tempfile::tempdir().unwrap();
        let url = format!("mkv://{}", dir.path().join("o.mkv").display());
        let layout = Some(b"BDLP\xB4".to_vec());
        let src = LpcmSource::new(AudioChannels::Surround51, SampleRate::S48, layout);
        let spy = TitleSpy::default();
        run(src, &url, &spy);
        let t = spy.0.lock().unwrap().clone().unwrap();
        let crate::disc::Stream::Audio(a) = &t.streams[0] else {
            panic!("audio")
        };
        assert_eq!(a.channels, AudioChannels::Surround71);
        assert_eq!(a.sample_rate, SampleRate::S96);
        let want = crate::labels::generate_audio_label(&a.codec, &AudioChannels::Surround71, false);
        assert_eq!(a.label, want);
    }

    /// BD source whose LPCM layout byte exists only once the first PES has been
    /// parsed, gated by the real `HeaderGate` (as DiscStream/PipelinedPesStream are).
    struct GatedLpcm {
        info: DiscTitle,
        parser: crate::mux::codec::lpcm::LpcmParser,
        gate: crate::mux::header_gate::HeaderGate,
        pes: std::collections::VecDeque<Vec<u8>>,
    }
    impl GatedLpcm {
        fn new(with_video: bool) -> Self {
            use crate::mux::codec::CodecParser as _;
            let src = LpcmSource::new(
                crate::disc::AudioChannels::Surround51,
                crate::disc::SampleRate::S48,
                None,
            );
            let mut info = src.info;
            if with_video {
                let v = LateAacStream::new().info.streams[0].clone();
                info.streams.insert(0, v);
            }
            // 7.1 (assignment 11) @ 96 kHz, 24-bit: 480 samples = 5 ms.
            let mut pes = vec![0x00, 0x00, 0xB4, 0xC0];
            pes.extend(vec![0u8; 480 * 8 * 3]);
            let parser = crate::mux::codec::lpcm::LpcmParser::new();
            assert!(parser.codec_private().is_none());
            GatedLpcm {
                info,
                parser,
                gate: Default::default(),
                pes: [pes.clone(), pes].into(),
            }
        }
        fn lpcm_track(&self) -> usize {
            self.info.streams.len() - 1
        }
    }
    impl Stream for GatedLpcm {
        fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
            use crate::mux::codec::CodecParser as _;
            let Some(data) = self.pes.pop_front() else {
                self.gate.expire();
                return Ok(None);
            };
            let pkt = crate::mux::ts::PesPacket {
                source: None,
                pid: 0x1100,
                pts: Some(0),
                dts: None,
                data,
                discontinuity: false,
            };
            let f = self.parser.parse(&pkt).remove(0);
            let frame = PesFrame {
                discard_padding_ns: 0,
                track: self.lpcm_track(),
                pts: f.pts_ns,
                keyframe: true,
                data: f.data,
                duration_ns: None,
                source: None,
                coding: None,
            };
            self.gate.observe(&frame);
            Ok(Some(frame))
        }
        fn write(&mut self, _frame: &PesFrame) -> std::io::Result<()> {
            Ok(())
        }
        fn finish(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn info(&self) -> &DiscTitle {
            &self.info
        }
        fn codec_private(&self, track: usize) -> Option<Vec<u8>> {
            use crate::mux::codec::CodecParser as _;
            if track == self.lpcm_track() {
                self.parser.codec_private()
            } else {
                Some(vec![1, 0x64, 0, 0x28, 0xFF, 0xE0, 0, 0]) // minimal avcC
            }
        }
        fn headers_ready(&self) -> bool {
            self.gate.ready(&self.info, |i| self.codec_private(i))
        }
    }

    // The layout byte arrives only after the first read: the header gate must wait
    // for it, or a 7.1/96k track is declared as the playlist's 5.1/48k.
    #[test]
    fn late_lpcm_layout_byte_still_corrects_the_header() {
        use crate::disc::{AudioChannels, SampleRate};
        for with_video in [false, true] {
            let src = GatedLpcm::new(with_video);
            assert!(!src.headers_ready(), "BD LPCM waits for its layout byte");
            let dir = tempfile::tempdir().unwrap();
            // m2ts: an MKV with a declared but frameless video track is refused.
            let url = format!("m2ts://{}", dir.path().join("o.m2ts").display());
            let spy = TitleSpy::default();
            drive_mux(Box::new(src), &url, &Halt::new(), &spy, None, None).unwrap();
            let t = spy.0.lock().unwrap().clone().unwrap();
            let crate::disc::Stream::Audio(a) = t.streams.last().unwrap() else {
                panic!("audio")
            };
            assert_eq!(a.channels, AudioChannels::Surround71, "video={with_video}");
            assert_eq!(a.sample_rate, SampleRate::S96, "video={with_video}");
        }
    }

    #[test]
    fn m2ts_dropped_lpcm_is_undelivered_and_not_in_the_opened_title() {
        use crate::disc::{AudioChannels, SampleRate};
        let dir = tempfile::tempdir().unwrap();
        let url = format!("m2ts://{}", dir.path().join("o.m2ts").display());
        // Track 0 (48 kHz) is written; track 1 (44.1 kHz) cannot be BD LPCM.
        let mut src = LpcmSource::new(AudioChannels::Stereo, SampleRate::S48, None);
        let other = LpcmSource::new(AudioChannels::Stereo, SampleRate::S44_1, None);
        src.info.streams.extend(other.info.streams);
        let spy = TitleSpy::default();
        let out = run(src, &url, &spy);
        assert_eq!(out.undelivered_streams, vec![1]);
        let opened = spy.0.lock().unwrap().clone().unwrap();
        assert_eq!(opened.streams.len(), 1, "only the carried track is listed");
    }

    /// A declared MPEG-2 extension track that a sink can never write is not in the opened title
    /// (CLI and GUI list the same streams), and with no `0xD0|n` packet nothing is reported lost.
    #[test]
    fn mkv_opened_title_omits_a_declared_mp2_extension_without_warning() {
        use crate::disc::{AudioChannels, AudioStream, Codec, LabelPurpose, SampleRate, Stream};
        let dir = tempfile::tempdir().unwrap();
        let url = format!("mkv://{}", dir.path().join("o.mkv").display());
        let mut src = LpcmSource::new(AudioChannels::Stereo, SampleRate::S48, None);
        src.info.streams.push(Stream::Audio(AudioStream {
            pid: 0x00D0,
            codec: Codec::Mp2,
            channels: AudioChannels::Unknown,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: crate::disc::MP2_EXTENSION_LABEL.into(),
        }));
        let spy = TitleSpy::default();
        let (out, ev) = crate::testlog::capture(|| run(src, &url, &spy));
        let opened = spy.0.lock().unwrap().clone().unwrap();
        assert!(
            !opened
                .streams
                .iter()
                .any(|s| matches!(s, Stream::Audio(a) if a.is_mp2_extension())),
            "{:?}",
            opened.streams
        );
        assert_eq!(opened.streams.len(), 1);
        assert!(out.undelivered_streams.is_empty());
        assert_eq!(mp2_extension_warnings(&ev), 0);
    }

    // The sink lives in the consumer thread and is destroyed with it, so an
    // undelivered stream is knowable ONLY at `close()`, which must carry it
    // out alongside the byte count or `MuxOutcome` can never report it.
    #[test]
    fn write_sink_carries_undelivered_streams_out_of_the_consumer_thread() {
        let sink = WriteSink {
            output: CountingStream::new(Box::new(UndeliveringSink {
                info: DiscTitle::empty(),
            })),
            bytes: Arc::new(AtomicU64::new(0)),
            late_configs: LateConfigs::default(),
            read_failed: Arc::default(),
        };
        let SinkClose { bytes, undelivered } = sink.close().expect("close succeeds");
        assert_eq!(bytes, 0);
        assert_eq!(
            undelivered,
            vec![1],
            "the sink's undelivered stream must reach the driver"
        );
    }

    // ── Regression A: header-buffer cap fails fast instead of OOM ───────────
    // Must refuse once the buffer passes `HEADER_BUFFER_CAP_BYTES`, BEFORE
    // draining the whole stream — asserted via a bounded read count.
    #[test]
    fn header_buffer_cap_fails_fast_instead_of_oom() {
        use std::sync::atomic::AtomicUsize;
        const FRAME: usize = 64 * 1024 * 1024; // 64 MiB per frame
        let cap_frames = HEADER_BUFFER_CAP_BYTES / FRAME; // 8 frames == cap
        const MANY: usize = 200; // vastly more than the cap needs

        let reads_seen = Arc::new(AtomicUsize::new(0));
        let mut fs = FakeStream::new(1).never_ready();
        fs.read_observer = Some(reads_seen.clone());
        for i in 0..MANY {
            fs.frames.push_back(PesFrame {
                discard_padding_ns: 0,
                track: 0,
                pts: i as i64,
                keyframe: true,
                data: vec![0u8; FRAME],
                duration_ns: None,
                source: None,
                coding: None,
            });
        }
        let halt = Halt::new();
        let err = drive_mux(Box::new(fs), "null://", &halt, &NoopEvents, None, None)
            .expect_err("over-cap header buffer must fail fast, not OOM");
        // The cap overflow must carry its OWN code, not `MkvInvalid`: that code
        // means "skippable empty stub" and would silently drop a title that
        // had just produced 512 MiB of real frames.
        assert_eq!(
            err.to_string(),
            format!("E{}: {}", crate::error::E_MUX_HEADER_BUFFER_EXCEEDED, {
                let frames = cap_frames + 1;
                frames * FRAME
            }),
            "cap-exceeded must report its own code plus the buffered byte count"
        );
        assert!(
            !crate::error::is_skippable_title_stub(&err),
            "a cap overflow is a real title, never a skippable stub"
        );
        let reads = reads_seen.load(Ordering::SeqCst);
        assert!(
            reads <= cap_frames + 1,
            "must fail after ~{cap_frames} frames (cap), not drain all {MANY} (read {reads})"
        );
    }

    // ── Regression B: watchdog fed during the buffered header drain ─────────
    // Headers resolve only after K frames buffer, then flush to the sink.
    // Every flushed frame must fire `on_write_progress`, the sole watchdog feed.
    #[test]
    fn write_progress_fed_during_header_drain() {
        const K: usize = 6;
        let mut fs = FakeStream::new(1).with_frames(K);
        // Headers stay unresolved until all K frames have been read+buffered.
        fs.headers_ready_after = K;

        struct WriteProgressCounter {
            writes: AtomicU64,
        }
        impl MuxEvents for WriteProgressCounter {
            fn on_write_progress(&self, _bytes_written: u64, _bytes_total: u64) {
                self.writes.fetch_add(1, Ordering::SeqCst);
            }
        }
        let events = WriteProgressCounter {
            writes: AtomicU64::new(0),
        };
        let halt = Halt::new();
        let out = drive_mux(Box::new(fs), "null://", &halt, &events, None, None)
            .expect("K-frame stream muxes cleanly");
        assert!(out.completed);
        assert_eq!(
            events.writes.load(Ordering::SeqCst),
            K as u64,
            "each of the K buffered header frames must feed on_write_progress on drain"
        );
    }

    // ── Regression C: Session key selection special-cases DVD ───────────────
    // A DVD must get `None` (so DiscStream cracks the correct per-title/VTS
    // CSS key); a non-DVD passes `decrypt_keys()` through unconditionally.
    fn disc_with_css(format: crate::disc::DiscFormat) -> crate::disc::Disc {
        crate::disc::Disc {
            volume_id: "TEST".into(),
            meta_title: None,
            format,
            capacity_sectors: 0,
            capacity_bytes: 0,
            layers: 1,
            titles: Vec::new(),
            region: crate::disc::DiscRegion::Free,
            aacs: None,
            css: Some(crate::css::CssState {
                title_key: [1, 2, 3, 4, 5],
                crack_span: None,
            }),
            encrypted: true,
            aacs_error: None,
            css_error: None,
            content_format: crate::disc::ContentFormat::MpegPs,
        }
    }

    #[test]
    fn session_mux_keys_uses_none_for_dvd() {
        // DVD: must be None so the pipeline cracks the correct per-title key,
        // even though decrypt_keys() would hand back a whole-disc Css key.
        let dvd = disc_with_css(crate::disc::DiscFormat::Dvd);
        assert!(
            matches!(dvd.decrypt_keys(), DecryptKeys::Css { .. }),
            "precondition: a CSS disc's decrypt_keys() is Css{{..}}"
        );
        assert!(
            matches!(session_mux_keys(&dvd), DecryptKeys::None),
            "a DVD must be handed None so DiscStream cracks the per-title key"
        );
    }

    // T27 / ST-X1b: `MuxOptions` has no per-frame deadline; no source names it again.
    #[test]
    fn removed_deadline_field_is_named_nowhere() {
        fn walk(dir: &std::path::Path, needle: &str, hits: &mut Vec<String>) {
            for e in std::fs::read_dir(dir).unwrap() {
                let path = e.unwrap().path();
                if path.is_dir() {
                    walk(&path, needle, hits);
                } else if path.extension().is_some_and(|x| x == "rs")
                    && std::fs::read_to_string(&path).is_ok_and(|src| src.contains(needle))
                {
                    hits.push(path.display().to_string());
                }
            }
        }
        let needle = ["send", "deadline"].join("_");
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut hits = Vec::new();
        walk(&root, &needle, &mut hits);
        assert!(hits.is_empty(), "{needle} still named in: {hits:?}");
    }

    #[test]
    fn session_mux_keys_passes_decrypt_keys_for_non_dvd() {
        // Non-DVD (e.g. BluRay): the whole-disc key IS the per-title key, so it
        // passes through — proving the special-case keys on FORMAT, not on the
        // mere presence of a key.
        let bd = disc_with_css(crate::disc::DiscFormat::BluRay);
        assert!(
            matches!(session_mux_keys(&bd), DecryptKeys::Css { .. }),
            "a non-DVD passes decrypt_keys() through unchanged"
        );
    }

    // A consumer wedged past the send deadline must not hold the finish for the
    // full JOIN_TIMEOUT (600 s).
    #[test]
    fn a_send_timeout_bounds_the_final_join() {
        // Only the first write wedges, returning after finish has given up; the
        // leaked consumer must then neither apply the queued item nor close.
        struct Stuck {
            closed: Arc<std::sync::atomic::AtomicBool>,
            woke: std::sync::mpsc::Sender<()>,
            first: bool,
            applied: Arc<std::sync::atomic::AtomicUsize>,
        }
        impl Sink<u32> for Stuck {
            type Output = ();
            fn apply(&mut self, _: u32) -> Result<Flow, Error> {
                self.applied.fetch_add(1, Ordering::SeqCst);
                if std::mem::take(&mut self.first) {
                    std::thread::sleep(Duration::from_secs(7));
                    let _ = self.woke.send(());
                }
                Ok(Flow::Continue)
            }
            fn close(self) -> Result<(), Error> {
                self.closed.store(true, Ordering::SeqCst);
                Ok(())
            }
        }
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let applied = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (woke_tx, woke) = std::sync::mpsc::channel();
        let stuck = Stuck {
            closed: closed.clone(),
            woke: woke_tx,
            first: true,
            applied: applied.clone(),
        };
        let pipe = Pipeline::spawn(1, stuck).unwrap();
        let halt = Halt::new();
        let mut timed_out = false;
        for i in 0..4 {
            if pipe
                .send_with_halt(i, &halt, Duration::from_millis(50))
                .is_err()
            {
                timed_out = true;
                break;
            }
        }
        assert!(timed_out, "the stuck consumer trips the send deadline");
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let flush = FlushProgress::default();
            let _ = tx.send(finish_pumped(pipe, &halt, true, &flush, &NoopEvents).is_err());
        });
        let failed = rx
            .recv_timeout(Duration::from_secs(30))
            .expect("the join must be bounded by the grace, not JOIN_TIMEOUT");
        assert!(failed);
        woke.recv_timeout(Duration::from_secs(30))
            .expect("the wedged write returns");
        std::thread::sleep(Duration::from_millis(500));
        assert!(
            !closed.load(Ordering::SeqCst),
            "an abandoned consumer must not finalise the output late"
        );
        assert_eq!(
            applied.load(Ordering::SeqCst),
            1,
            "nor apply the queued item"
        );
    }

    // An Opus-style CodecDelay/SeekPreRoll on an mkv:// source must reach the
    // mkv:// output through the driver (the sink's set_track_timing wiring).
    #[test]
    fn track_timing_propagates_from_source_to_mkv_output() {
        use crate::disc::{Codec, ColorSpace, FrameRate, HdrFormat, Resolution, VideoStream};
        use crate::mux::mkvstream::MkvStream;
        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("src.mkv");
        let dst = dir.path().join("dst.mkv");
        let mut title = DiscTitle {
            streams: vec![crate::disc::Stream::Video(VideoStream {
                pid: 0x1011,
                codec: Codec::H264,
                resolution: Resolution::R1080p,
                frame_rate: FrameRate::F24,
                hdr: HdrFormat::Sdr,
                color_space: ColorSpace::Bt709,
                display_aspect: None,
                secondary: false,
                label: String::new(),
                measured_cicp: None,
            })],
            ..DiscTitle::empty()
        };
        title.codec_privates = vec![Some(vec![0x01, 0x64, 0x00, 0x1F, 0xFF, 0xE1])];
        let timing = crate::pes::TrackTiming {
            codec_delay_ns: 6_500_000,
            seek_preroll_ns: 80_000_000,
        };
        let mut w = MkvStream::create(Box::new(std::fs::File::create(&src).unwrap()), &title, None)
            .unwrap();
        w.set_track_timing(0, timing).unwrap();
        w.write(&PesFrame {
            discard_padding_ns: 0,
            coding: None,
            source: None,
            track: 0,
            pts: 0,
            keyframe: true,
            data: vec![0xA1; 16],
            duration_ns: None,
        })
        .unwrap();
        w.finish().unwrap();

        let stream = Box::new(MkvStream::open(std::fs::File::open(&src).unwrap()).unwrap());
        let out = drive_mux(
            stream,
            &format!("mkv://{}", dst.display()),
            &Halt::new(),
            &SpyEvents::new(),
            None,
            None,
        )
        .expect("mkv remux runs");
        assert!(out.completed);
        let back = MkvStream::open(std::fs::File::open(&dst).unwrap()).unwrap();
        assert_eq!(back.track_timing(0), timing);
    }

    // Records every `on_flush_progress` call.
    #[derive(Default)]
    struct FlushSpy(std::sync::Mutex<Vec<(u64, u64)>>);

    impl MuxEvents for FlushSpy {
        fn on_flush_progress(&self, durable: u64, total: u64) {
            self.0.lock().unwrap().push((durable, total));
        }
    }

    // `close()` makes `steps` bytes durable, `gap` apart, then idles `tail`.
    struct FlushingClose {
        flush: FlushProgress,
        steps: u64,
        gap: Duration,
        tail: Duration,
    }

    impl Sink<u64> for FlushingClose {
        type Output = ();
        fn apply(&mut self, _: u64) -> Result<Flow, Error> {
            Ok(Flow::Continue)
        }
        fn close(self) -> Result<(), Error> {
            self.flush.note_total(self.steps * 1000);
            for _ in 0..self.steps {
                std::thread::sleep(self.gap);
                self.flush.add_durable(1000);
            }
            std::thread::sleep(self.tail);
            Ok(())
        }
    }

    fn finish_flushing(steps: u64, gap: Duration, tail: Duration) -> Vec<(u64, u64)> {
        let flush = FlushProgress::new(crate::halt::Progress::new());
        let sink = FlushingClose {
            flush: flush.clone(),
            steps,
            gap,
            tail,
        };
        let progress = flush.progress().clone();
        let pipe = Pipeline::spawn_named_with_progress("t-flush", 4, sink, progress).unwrap();
        let spy = FlushSpy::default();
        finish_pumped(pipe, &Halt::new(), false, &flush, &spy).unwrap();
        spy.0.into_inner().unwrap()
    }

    /// LP20 (§4.5): while the driver waits on a closing consumer, each increase of the
    /// flusher's bytes produces one `on_flush_progress`, and none while it is static.
    #[test]
    fn finish_with_halt_forwards_flush_progress() {
        let calls = finish_flushing(4, Duration::from_millis(300), Duration::from_millis(600));
        let want: Vec<(u64, u64)> = (1..=4).map(|i| (i * 1000, 4000)).collect();
        assert_eq!(
            calls, want,
            "one call per increase, none during the idle tail"
        );
    }

    /// LP20, the rate limit: a burst of increases is forwarded at most 4 times a second,
    /// and the last value still arrives.
    #[test]
    fn flush_progress_is_rate_limited_to_4hz() {
        let calls = finish_flushing(20, Duration::from_millis(5), Duration::from_millis(600));
        assert!(!calls.is_empty() && calls.len() <= 3, "{calls:?}");
        assert_eq!(calls.last(), Some(&(20_000, 20_000)));
    }

    // ── mux_with_keys (KU §3.1): keys come only from the rip's set ─────────────

    fn keyed_opts() -> MuxOptions {
        MuxOptions {
            skip_errors: false,
            batch_sectors: 3,
            raw: false,
            selection: Default::default(),
        }
    }

    // One encrypted audio unit at LBA 0..3 under `key`, its title, and a set keying it.
    fn keyed_live(key: [u8; 16]) -> (Box<AacsUnitReader>, DiscTitle, crate::keys::ResolvedKeySet) {
        let reader = Box::new(AacsUnitReader {
            unit: encrypted_audio_unit(&key),
            capacity: 16,
        });
        let mut title = aac_audio_title(0x1100);
        title.extents = vec![crate::disc::Extent {
            start_lba: 0,
            sector_count: 3,
        }];
        let mut disc = aacs_session_disc(title.clone());
        disc.capacity_sectors = 16;
        let set = crate::keys::ResolvedKeySet::keyed_for_test(&disc, key, &[(0, 3)]);
        (reader, title, set)
    }

    // The plaintext audio ES inside `encrypted_audio_unit`, and a muxed MKV holding it.
    const PLAIN_ES: [u8; 6] = [0xDE, 0xAD, 0xBE, 0xEF, 0x11, 0x22];
    fn holds_plain_es(path: &std::path::Path) -> bool {
        std::fs::read(path)
            .unwrap()
            .windows(PLAIN_ES.len())
            .any(|w| w == PLAIN_ES)
    }

    /// `MuxSource::Live` decrypts through the set's reader: no key banked on a disc, no
    /// resolution in the driver.
    #[test]
    fn mux_with_keys_live_decrypts_through_the_set() {
        let (reader, title, set) = keyed_live([0x5A; 16]);
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("live.mkv");
        let out = mux_with_keys(
            MuxSource::Live {
                reader,
                title,
                format: crate::disc::ContentFormat::BdTs,
            },
            Some(&set),
            &format!("mkv://{}", out_path.display()),
            &keyed_opts(),
            &Halt::new(),
            Arc::new(NoopEvents),
        )
        .expect("the set's key opens the unit");
        assert!(out.completed);
        assert!(
            holds_plain_es(&out_path),
            "the muxed audio is the plaintext"
        );
    }

    /// `MuxSource::Session` decrypts with the set: the disc holds no key.
    #[test]
    fn mux_with_keys_session_uses_the_set() {
        let key = [0x5A; 16];
        let (reader, title, _) = keyed_live(key);
        let mut disc = aacs_session_disc(title);
        disc.capacity_sectors = 16;
        let set = crate::keys::ResolvedKeySet::keyed_for_test(&disc, key, &[(0, 3)]);
        let mut session = DiscSession::from_parts_for_test(Some(disc), Some(reader));
        let out = mux_with_keys(
            MuxSource::Session {
                session: &mut session,
                title_index: 0,
            },
            Some(&set),
            "null://",
            &keyed_opts(),
            &Halt::new(),
            Arc::new(NoopEvents),
        )
        .expect("the set's key opens the unit");
        assert!(out.completed && out.bytes_written > 0);
    }

    /// A key set for another disc is a typed E7013 on a session, never a debug-build panic.
    #[test]
    fn mux_with_keys_session_wrong_disc_set_is_e7013() {
        let key = [0x5A; 16];
        let (reader, title, _) = keyed_live(key);
        let mut other = aacs_session_disc(title.clone());
        let mut disc = aacs_session_disc(title);
        disc.capacity_sectors = 16;
        if let Some(a) = other.aacs.as_mut() {
            a.disc_hash = "0xdef".into();
        }
        let set = crate::keys::ResolvedKeySet::keyed_for_test(&other, key, &[(0, 3)]);
        let mut session = DiscSession::from_parts_for_test(Some(disc), Some(reader));
        let err = mux_with_keys(
            MuxSource::Session {
                session: &mut session,
                title_index: 0,
            },
            Some(&set),
            "null://",
            &keyed_opts(),
            &Halt::new(),
            Arc::new(NoopEvents),
        )
        .expect_err("a set for another disc is refused");
        assert!(err.to_string().contains("E7013"), "got: {err}");
    }

    /// J14: `MuxSource::Iso` muxes the caller's already-scanned title out of an image with
    /// no filesystem at all; opening the same image by URL (which scans) fails.
    #[test]
    fn mux_with_keys_iso_muxes_a_scanned_title_without_rescanning() {
        let _serial = crate::sector::prefetched::holder_test_lock();
        let key = [0x5A; 16];
        let (_, title, set) = keyed_live(key);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("staged.iso");
        let mut image = encrypted_audio_unit(&key);
        image.resize(16 * 2048, 0);
        std::fs::write(&path, &image).unwrap();
        let out_path = dir.path().join("iso.mkv");
        let out = mux_with_keys(
            MuxSource::Iso {
                path: &path,
                title,
                format: crate::disc::ContentFormat::BdTs,
            },
            Some(&set),
            &format!("mkv://{}", out_path.display()),
            &keyed_opts(),
            &Halt::new(),
            Arc::new(NoopEvents),
        )
        .expect("no rescan: the scanned title is muxed as given");
        assert!(out.completed);
        assert!(
            holds_plain_es(&out_path),
            "the muxed audio is the plaintext"
        );
        let url = format!("iso://{}", path.display());
        let rescanned = mux_with_keys(
            MuxSource::Url {
                url: &url,
                opts: InputOptions::default(),
            },
            Some(&set),
            "null://",
            &keyed_opts(),
            &Halt::new(),
            Arc::new(NoopEvents),
        );
        assert!(
            rescanned.is_err(),
            "a URL source scans the image, which has no filesystem"
        );
    }

    /// KU §3.1 (review item 3): "`keys` must be `Some` for AACS". With no AACS set (none, or a
    /// non-AACS set) and not raw, `Iso` and `Live` refuse E7022 at the first AACS-flagged
    /// unit rather than muxing ciphertext; raw passes it through, and clear content muxes.
    #[test]
    fn mux_with_keys_without_an_aacs_set_refuses_aacs_content() {
        let _serial = crate::sector::prefetched::holder_test_lock();
        let key = [0x5A; 16];
        let none = crate::keys::ResolvedKeySet::none();
        let live = |keys: Option<&crate::keys::ResolvedKeySet>, raw: bool, unit: Vec<u8>| {
            let (_, title, _) = keyed_live(key);
            let reader = Box::new(AacsUnitReader { unit, capacity: 16 });
            let opts = MuxOptions {
                raw,
                ..keyed_opts()
            };
            mux_with_keys(
                MuxSource::Live {
                    reader,
                    title,
                    format: crate::disc::ContentFormat::BdTs,
                },
                keys,
                "null://",
                &opts,
                &Halt::new(),
                Arc::new(NoopEvents),
            )
        };
        let code = |r: std::io::Result<MuxOutcome>| r.err().and_then(|e| crate::error_code(&e));
        let e7022 = Some(crate::error::E_NO_DISC_KEY);
        assert_eq!(code(live(None, false, encrypted_audio_unit(&key))), e7022);
        assert_eq!(
            code(live(Some(&none), false, encrypted_audio_unit(&key))),
            e7022
        );
        assert!(
            live(None, true, encrypted_audio_unit(&key)).is_ok(),
            "raw passes"
        );
        let mut clear = encrypted_audio_unit(&key);
        crate::test_util::decrypt_unit(&mut clear, &key);
        clear.chunks_mut(192).for_each(|p| p[0] &= 0x3F);
        let out = live(None, false, clear).expect("clear content needs no key");
        assert!(out.completed && out.bytes_written > 0);

        let (_, title, _) = keyed_live(key);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("staged.iso");
        let mut image = encrypted_audio_unit(&key);
        image.resize(16 * 2048, 0);
        std::fs::write(&path, &image).unwrap();
        let iso = mux_with_keys(
            MuxSource::Iso {
                path: &path,
                title,
                format: crate::disc::ContentFormat::BdTs,
            },
            None,
            "null://",
            &keyed_opts(),
            &Halt::new(),
            Arc::new(NoopEvents),
        );
        assert_eq!(code(iso), e7022);

        // Session over an AACS disc with no set (review B1): BD-TS stops at the first
        // AACS unit; HD DVD refuses up front (it is never probed, KU §2.6).
        for format in [crate::DiscFormat::Uhd, crate::DiscFormat::HdDvd] {
            let (_, title, _) = keyed_live(key);
            let mut disc = aacs_session_disc(title);
            disc.capacity_sectors = 16;
            disc.format = format;
            if format == crate::DiscFormat::HdDvd {
                disc.content_format = crate::ContentFormat::MpegPs;
            }
            let reader = Box::new(AacsUnitReader {
                unit: encrypted_audio_unit(&key),
                capacity: 16,
            });
            let mut session = DiscSession::from_parts_for_test(Some(disc), Some(reader));
            let r = mux_with_keys(
                MuxSource::Session {
                    session: &mut session,
                    title_index: 0,
                },
                None,
                "null://",
                &keyed_opts(),
                &Halt::new(),
                Arc::new(NoopEvents),
            );
            let err = r.expect_err("no key, no mux");
            assert_eq!(crate::error_code(&err), e7022, "{format:?}");
            let text = err.to_string().to_ascii_lowercase();
            assert!(
                text.contains("abc"),
                "{format:?}: E7022 names the disc: {text}"
            );
        }
    }

    // A staged UHD image (AACS 2.0, bus encryption on) of `units` encrypted audio units under
    // `key`, damaged by `damage`, its title over all of them, and a set keying the title.
    fn damaged_uhd_image(
        key: [u8; 16],
        units: u32,
        damage: impl Fn(&mut [u8]),
    ) -> (
        tempfile::TempDir,
        std::path::PathBuf,
        DiscTitle,
        crate::keys::ResolvedKeySet,
    ) {
        let (_, mut title, _) = keyed_live(key);
        let sectors = units * 3;
        title.extents = vec![crate::disc::Extent {
            start_lba: 0,
            sector_count: sectors,
        }];
        let mut disc = aacs_session_disc(title.clone());
        disc.aacs.as_mut().unwrap().bus_encryption = true;
        disc.capacity_sectors = sectors + 16;
        let set = crate::keys::ResolvedKeySet::keyed_for_test(&disc, key, &[(0, sectors)]);
        let mut image = encrypted_audio_unit(&key).repeat(units as usize);
        damage(&mut image);
        image.resize((sectors + 16) as usize * 2048, 0);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("staged.iso");
        std::fs::write(&path, &image).unwrap();
        (dir, path, title, set)
    }

    fn mux_damaged_iso(
        path: &std::path::Path,
        title: DiscTitle,
        set: &crate::keys::ResolvedKeySet,
        dest: &str,
    ) -> std::io::Result<MuxOutcome> {
        mux_with_keys(
            MuxSource::Iso {
                path,
                title,
                format: crate::disc::ContentFormat::BdTs,
            },
            Some(set),
            dest,
            &MuxOptions {
                batch_sectors: 64,
                ..keyed_opts()
            },
            &Halt::new(),
            Arc::new(NoopEvents),
        )
    }

    /// "We rip bad discs": a sweep hole (whole zero units) mid-title muxes through, as before
    /// KU. KS-5 [BD] §3.10.2: CPI "shall be set to 00₂ if the data is not encrypted" — a zero
    /// unit is not flagged, so no key is applied to it and none is judged by it.
    #[test]
    fn iso_mux_through_a_zero_filled_run_mid_title() {
        assert!(
            crate::spec::keys::KS_5_CPI
                .text
                .contains("00₂ if the data is not encrypted")
        );
        let _serial = crate::sector::prefetched::holder_test_lock();
        let key = [0x5A; 16];
        let (dir, path, title, set) =
            damaged_uhd_image(key, 9, |im| im[3 * 6144..6 * 6144].fill(0));
        let out_path = dir.path().join("hole.mkv");
        let out = mux_damaged_iso(&path, title, &set, &format!("mkv://{}", out_path.display()))
            .expect("a zero-filled run is read damage, never E7013");
        assert!(out.completed);
        assert!(holds_plain_es(&out_path), "units around the hole decrypt");
    }

    /// Regression (b4d322e): a unit on the edge of a sweep hole whose first sector read back as
    /// garbage (CPI 11₂, no TS sync) is read damage — a hole — not E7013. KS-4 [BD] §3.10.1:
    /// "The first 16 bytes of each Aligned Unit is used as the seed for calculating the Block Key."
    #[test]
    fn iso_mux_through_a_damaged_unit_seed_is_a_hole_not_e7013() {
        assert!(
            crate::spec::keys::KS_4_SEED
                .text
                .contains("used as the seed")
        );
        let _serial = crate::sector::prefetched::holder_test_lock();
        let key = [0x5A; 16];
        let (dir, path, title, set) = damaged_uhd_image(key, 9, |im| {
            im[3 * 6144..5 * 6144].fill(0);
            crate::test_util::damage_unit_seed(&mut im[5 * 6144..6 * 6144]);
        });
        let out_path = dir.path().join("hole.mkv");
        let out = mux_damaged_iso(&path, title, &set, &format!("mkv://{}", out_path.display()))
            .expect("a damaged unit seed is read damage, never E7013");
        assert!(out.completed);
        assert_eq!(out.lost_bytes, 6144, "the blanked unit is counted as loss");
        assert!(holds_plain_es(&out_path), "units around the hole decrypt");
    }

    /// The real sweep holes are not unit-aligned (Dunkirk: runs of 260, 611, 352 sectors): a
    /// run that starts in one unit's tail and ends in another's head muxes through as holes.
    /// KS-3 [BD] §3.10.1: "A new CBC cipher chain is started for each Aligned Unit".
    #[test]
    fn iso_mux_through_a_zero_run_cutting_units_at_both_edges() {
        assert!(
            crate::spec::keys::KS_3_CBC_PER_UNIT
                .text
                .contains("new CBC cipher chain")
        );
        let _serial = crate::sector::prefetched::holder_test_lock();
        let key = [0x5A; 16];
        let (dir, path, title, set) =
            damaged_uhd_image(key, 9, |im| im[7 * 2048..16 * 2048].fill(0));
        let out_path = dir.path().join("hole.mkv");
        let out = mux_damaged_iso(&path, title, &set, &format!("mkv://{}", out_path.display()))
            .expect("an unaligned zero run is read damage, never E7013");
        assert!(out.completed);
        assert!(holds_plain_es(&out_path), "units around the hole decrypt");
    }

    // A random-access reader over an in-memory image (zeros past its end).
    struct ImageReader(Vec<u8>);
    impl crate::sector::SectorSource for ImageReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            let (at, n) = (lba as usize * 2048, count as usize * 2048);
            buf[..n].fill(0);
            let have = self.0.len().saturating_sub(at).min(n);
            buf[..have].copy_from_slice(&self.0[at..at + have]);
            Ok(n)
        }
        fn capacity_sectors(&self) -> u32 {
            (self.0.len() / 2048) as u32
        }
        fn random_access(&self) -> bool {
            true
        }
    }

    // `damaged_uhd_image`'s title, re-cut into `extents`, muxed from the image (ISO) and from
    // a live reader over the same bytes; `batch` sectors per read.
    fn mux_both(
        units: u32,
        extents: &[(u32, u32)],
        batch: u16,
        damage: impl Fn(&mut [u8]),
    ) -> [std::io::Result<MuxOutcome>; 2] {
        let _serial = crate::sector::prefetched::holder_test_lock();
        let key = [0x5A; 16];
        let (_dir, path, mut title, set) = damaged_uhd_image(key, units, damage);
        title.extents = extents
            .iter()
            .map(|&(start_lba, sector_count)| crate::disc::Extent {
                start_lba,
                sector_count,
            })
            .collect();
        let opts = MuxOptions {
            batch_sectors: batch,
            ..keyed_opts()
        };
        let iso = mux_with_keys(
            MuxSource::Iso {
                path: &path,
                title: title.clone(),
                format: crate::disc::ContentFormat::BdTs,
            },
            Some(&set),
            "null://",
            &opts,
            &Halt::new(),
            Arc::new(NoopEvents),
        );
        let live = mux_with_keys(
            MuxSource::Live {
                reader: Box::new(ImageReader(std::fs::read(&path).unwrap())),
                title,
                format: crate::disc::ContentFormat::BdTs,
            },
            Some(&set),
            "null://",
            &opts,
            &Halt::new(),
            Arc::new(NoopEvents),
        );
        [iso, live]
    }

    /// An extent that starts inside a cluster of damaged units (its first reads show no intact
    /// unit) is holes, not E7013: the grid verdict waits for the extent's intact units. KS-4
    /// [BD] §3.10.1: "The first 16 bytes of each Aligned Unit is used as the seed".
    #[test]
    fn an_extent_starting_inside_a_damage_cluster_muxes_through() {
        let [iso, live] = mux_both(9, &[(0, 12), (12, 15)], 6, |im| {
            (3..6).for_each(|u| {
                crate::test_util::damage_unit_seed(&mut im[u * 6144..(u + 1) * 6144])
            });
        });
        let (iso, live) = (
            iso.expect("iso: damage is blanked"),
            live.expect("live: blanked"),
        );
        assert!(iso.completed && live.completed);
        assert_eq!(
            (iso.lost_bytes, live.lost_bytes),
            (3 * 6144, 3 * 6144),
            "3 units counted"
        );
        assert!(
            iso.errors >= 3 && live.errors >= 3,
            "a damaged rip never looks clean"
        );
    }

    /// Units read off their file's grid look like damage (a flagged seed without TS sync): as
    /// in 1.7.7 the mux carries on, never E7013 — here blanked and counted as loss, on ISO and
    /// live alike. The second extent starts 2 sectors off the grid.
    #[test]
    fn an_off_grid_extent_is_blanked_never_e7013_on_iso_and_live() {
        let [iso, live] = mux_both(9, &[(0, 9), (11, 15)], 64, |_| {});
        let (iso, live) = (
            iso.expect("iso: never E7013"),
            live.expect("live: never E7013"),
        );
        assert!(
            iso.lost_bytes > 0,
            "the flagged off-grid chunks are counted"
        );
        assert_eq!(
            iso.lost_bytes, live.lost_bytes,
            "ISO and live never deviate"
        );
    }

    /// Dunkirk (AACS 2.0, bus encryption, one 55.5 GB clip): five unaligned zero runs of
    /// 260, 611, 352, 192 and 546 sectors, laid out as on the real image, plus a unit on a run's
    /// edge whose head read back as garbage. Every grid phase muxes through on ISO and live.
    #[test]
    fn a_dunkirk_damage_pattern_muxes_through_on_iso_and_live() {
        const RUNS: [(u32, u32); 5] =
            [(96, 260), (928, 611), (8899, 352), (9280, 192), (9633, 546)];
        for phase in 0..3u32 {
            let [iso, live] = mux_both(3400, &[(0, 10200)], 64, |im| {
                for (at, n) in RUNS {
                    let a = (at - phase) as usize * 2048;
                    im[a..a + n as usize * 2048].fill(0);
                }
                let u = (928 - phase as usize + 611) / 3 + 1;
                crate::test_util::damage_unit_seed(&mut im[u * 6144..(u + 1) * 6144]);
            });
            let (iso, live) = (iso.expect("iso"), live.expect("live"));
            assert!(iso.completed && live.completed, "phase {phase}");
            // Blanked: the garbage head, and each run end that zero-filled a unit's head sector
            // but not its tail (a lost seed over ciphertext). A run start keeps its head.
            let lost_seeds = RUNS
                .iter()
                .filter(|&&(at, n)| (at - phase + n) % 3 != 0)
                .count();
            let want = (1 + lost_seeds as u64) * 6144;
            assert_eq!(
                iso.lost_bytes, want,
                "phase {phase}: blanked units are counted"
            );
            assert_eq!(live.lost_bytes, iso.lost_bytes, "phase {phase}");
        }
    }
}
