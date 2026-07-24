//! High-level mux driver — the one place that runs the
//! `construct → headers gate → open sink → pump → finish` pipeline the
//! consumers (CLI `pipe`/`pipe_disc`, autorip `run_mux`) each hand-rolled.
//!
//! `mux_stream` DRIVES the existing highway; it does not replace it. For a
//! file/ISO source it constructs the SAME
//! [`build_iso_pipeline`](crate::mux::resolve::build_iso_pipeline) 3-stage
//! prefetch → demux → parse chain the consumers build today (the 660 MB/s
//! highway), reads frames off it exactly where the consumers call
//! `stream.read()`, and writes them through a
//! [`WRITE_PIPELINE_DEPTH`]-deep write [`Pipeline`] so the latency-bound sink
//! write overlaps the next read. No wrapper is inserted around the reader or
//! the frames — the only threads are the three inside `build_iso_pipeline` plus
//! the single write consumer, exactly as in the consumers today.
//!
//! ## Gate ordering (bug fix)
//!
//! The `chapters://` / `json://` metadata sinks write their whole file from the
//! scanned title at `output()` time and consume no PES frames, so they need no
//! codec headers. The CLI put that short-circuit AFTER the `headers_resolved`
//! gate (`pipe.rs`), so a metadata export on a title whose video headers never
//! resolved failed with `MkvInvalid`. Here the short-circuit runs BEFORE the
//! header pump/gate — by construction a metadata sink can never trip the header
//! gate.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::decrypt::{AacsKeyMap, DecryptKeys};
use crate::disc::DiscTitle;
use crate::error::Error;
use crate::event::{BatchSizeReason, Event, EventKind};
use crate::halt::Halt;
use crate::io::pipeline::{Flow, Pipeline, Sink, WRITE_PIPELINE_DEPTH};
use crate::pes::{CountingStream, PesFrame, Stream};
use crate::sector::{FileSectorSource, KeyFetch, SectorSource};
use crate::session::DiscSession;

use super::resolve::{InputOptions, StreamUrl, build_iso_pipeline, input, output, parse_url};

/// Per-frame send deadline for the write pipeline. Longer than a soft stall
/// warning, shorter than the pipeline's own join backstop, so a wedged sink
/// surfaces here as a per-frame timeout rather than blocking the whole mux.
/// Mirrors autorip's `MUX_SEND_DEADLINE_SECS`.
const MUX_SEND_DEADLINE_SECS: u64 = 60;

/// Ceiling on bytes buffered while waiting for `headers_ready()` to resolve.
/// Normally that resolves after a handful of frames; a damaged/undecryptable
/// title whose video `codec_private` never resolves would otherwise grow the
/// pre-headers buffer without bound (the whole 30-90 GB title, one PES frame at
/// a time) until the process is OOM-killed. 512 MiB is far more than any real
/// codec-private resolution needs but small enough to fail fast rather than
/// swap the box to death. Once exceeded the mux is refused exactly as the
/// headers-never-resolved gate refuses it (`Error::MkvInvalid`). Mirrors
/// autorip's pre-refactor `HEADER_BUFFER_CAP_BYTES`.
const HEADER_BUFFER_CAP_BYTES: usize = 512 * 1024 * 1024;

/// Where [`mux_stream`] reads its PES frames from. The driver owns the
/// construction of the underlying [`Stream`] so consumers stop hand-rolling
/// `DiscStream::new` / `build_iso_pipeline` / `input()`.
pub enum MuxInput<'a> {
    /// Live single-pass mux off an opened [`DiscSession`]. The driver takes the
    /// session's staged reader ([`DiscSession::take_reader`]); a missing reader
    /// (never staged, or already consumed) is a clean error, never a panic.
    Session {
        /// The opened, scanned session (its keys resolved via
        /// [`DiscSession::resolve_keys`]).
        session: &'a mut DiscSession,
        /// Index into `session.disc().titles` of the title to mux.
        title_index: usize,
    },
    /// Multipass / remux from a staged ISO on disk. Keys/map/fetch are the
    /// pre-resolved material the consumer already banked — the driver does no
    /// internal re-resolution beyond what [`build_iso_pipeline`] performs.
    Iso {
        /// Path to the ISO image.
        path: &'a Path,
        /// The scanned title to mux out of the image.
        title: DiscTitle,
        /// Container format of the title (TS vs PS demuxer selection).
        format: crate::disc::ContentFormat,
        /// Decryption keys for the title (`DecryptKeys::None` for raw/clear).
        keys: DecryptKeys,
        /// Optional pre-resolved AACS key map. Carried for forward-compat and
        /// the live path; the file highway re-derives its own map from
        /// `keys`/`key_fetch` inside [`build_iso_pipeline`].
        key_map: Option<Arc<AacsKeyMap>>,
        /// Optional read-time key fetch closure (banked by `resolve_keys`).
        key_fetch: Option<KeyFetch>,
    },
    /// Live single-pass mux off a raw disc [`SectorSource`] (autorip's
    /// live-drive path). The driver wraps the reader in a
    /// [`DiscStream`](crate::mux::DiscStream) — the **INLINE** stream, NOT the
    /// prefetch highway — so the consumer's adaptive batch-retry in
    /// `DiscStream::fill_extents` still fires on a bad sector (the highway would
    /// bypass it). The live analogue of [`MuxInput::Iso`]: the consumer resolves
    /// keys as its own app-layer policy and hands the already-banked material in;
    /// the driver does no internal re-resolution.
    Live {
        /// The raw sector source (physical drive). Boxed `dyn` — the driver owns
        /// it and moves it into `DiscStream::new` (whose reader param is exactly
        /// `Box<dyn SectorSource>`), so no concrete drive type leaks in here.
        reader: Box<dyn SectorSource>,
        /// The scanned title to mux.
        title: DiscTitle,
        /// Container format (TS vs PS demux selection).
        format: crate::disc::ContentFormat,
        /// Decryption keys the consumer already banked (`DecryptKeys::None` for
        /// raw/clear). The driver consumes them as-is — never re-resolves.
        keys: DecryptKeys,
        /// Optional pre-resolved forensic AACS key map, applied VERBATIM via
        /// [`DiscStream::with_key_map`](crate::mux::DiscStream::with_key_map)
        /// BEFORE any read. `None` for every non-FMTS disc. Single-pass FMTS
        /// correctness depends on this reaching the inline reader: `with_key_map`
        /// rewrites the extent walk to our-phase units only and installs the map
        /// so each unit decrypts with its mapped key.
        key_map: Option<Arc<AacsKeyMap>>,
    },
    /// Any URL-addressed source (`iso://`, `mkv://`, `m2ts://`, `network://`,
    /// stdio) opened via [`input`].
    Url {
        /// The source URL.
        url: &'a str,
        /// Input options forwarded to [`input`].
        opts: InputOptions,
    },
}

/// Tuning / behaviour knobs for a mux run.
pub struct MuxOptions {
    /// Skip past read errors (zero-fill + continue) on the live-drive path
    /// instead of aborting. Wired onto `DiscStream::skip_errors`.
    pub skip_errors: bool,
    /// Read batch size in logical (2048-byte) sectors.
    pub batch_sectors: u16,
    /// Ciphertext passthrough — skip decryption / CSS self-crack.
    pub raw: bool,
}

/// Progress / event callbacks the consumer implements (CLI `CliProgress`,
/// autorip's stream event handler). Every method has a no-op default so a
/// consumer overrides only what it renders.
///
/// `Send + Sync + 'static` because [`mux_stream`] takes the handle as an
/// [`Arc<dyn MuxEvents>`] and clones it into the constructors' `'static`
/// [`EventFn`] (via [`reader_event_fn`]) so the reader-side events fire from the
/// highway's producer thread / the live `DiscStream`'s read loop. The driver
/// fires [`Self::on_output_opened`] and the write-side [`Self::on_write_progress`]
/// from the driving thread; the reader-side events
/// ([`Self::on_sector_skipped`] / [`Self::on_batch_size_changed`] /
/// [`Self::on_read_error`], plus [`Self::on_read_progress`]) are fired
/// by that cloned `EventFn`.
///
/// Progress is split into two callbacks because consumers drive their progress
/// UI from different sides of the pipeline: the CLI renders from the WRITE side
/// (bytes finalised to the sink), autorip from the READ side (bytes pulled off
/// the disc). Keeping them distinct avoids the old ambiguous single
/// `on_progress(bytes, total)` where the caller couldn't tell which number it
/// was handed.
pub trait MuxEvents: Send + Sync + 'static {
    /// Fired once, immediately after the output sink is created.
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
}

/// A [`MuxEvents`] that ignores everything — for callers that render no
/// progress.
pub struct NoopEvents;
impl MuxEvents for NoopEvents {}

/// The result of a [`mux_stream`] run.
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
}

/// Run the decrypt + mux pipeline end-to-end: construct the source stream from
/// `input`, open the `dest_url` sink, and pump PES frames through a write
/// pipeline until EOF (or `halt`).
///
/// This is the shared driver extracted from the CLI's `pipe`/`pipe_disc` and
/// autorip's `run_mux`. It preserves their exact semantics:
/// - `chapters://` / `json://` sinks short-circuit BEFORE the header gate (they
///   need no codec headers — bug fix over the CLI's post-gate placement);
/// - a stream whose codec headers never resolve is refused with
///   [`Error::MkvInvalid`] (a structurally-invalid MKV must not be finalised);
/// - a natural drain that produced no streams or zero payload bytes is refused
///   with [`Error::NoStreams`] (the zero-output / undecryptable-input guard);
/// - a `halt` mid-run yields `completed = false` rather than a success marker.
///
/// `halt` is mandatory: there is no global interrupt flag and no `None` — the
/// caller threads a real [`Halt`] so `/api/stop` / SIGINT stop the pump at the
/// next boundary.
pub fn mux_stream(
    input_src: MuxInput,
    dest_url: &str,
    opts: &MuxOptions,
    halt: &Halt,
    events: std::sync::Arc<dyn MuxEvents>,
) -> std::io::Result<MuxOutcome> {
    // Construct the source stream. The file/ISO path calls the untouched
    // `build_iso_pipeline` highway (zero added copies); the live path builds a
    // `DiscStream`; a URL source goes through `input()`. The reader-side
    // constructors take a `'static` `EventFn`; we clone the `Arc<dyn MuxEvents>`
    // into one (`reader_event_fn`) so the highway's producer thread — and the
    // live `DiscStream`'s read loop — forward `BytesRead`/`SectorSkipped`/
    // `BatchSizeChanged`/`ReadError` back to the consumer's handle.
    let (stream, playlist_name): (Box<dyn Stream>, Option<String>) = match input_src {
        MuxInput::Url { url, opts: in_opts } => (input(url, &in_opts)?, None),
        MuxInput::Iso {
            path,
            title,
            format,
            keys,
            key_map: _,
            key_fetch,
        } => {
            let reader = FileSectorSource::open(path)?;
            let stream = build_iso_pipeline(
                reader,
                title,
                keys,
                opts.batch_sectors,
                format,
                opts.raw,
                Some(halt.clone()),
                Some(reader_event_fn(events.clone())),
                key_fetch,
            )?;
            (Box::new(stream), None)
        }
        MuxInput::Session {
            session,
            title_index,
        } => {
            // Pull everything we need out of the disc as owned values so the
            // immutable disc borrow is released before the mutable
            // `take_reader` below.
            let (title, format, keys, playlist) = {
                let disc = session.disc().ok_or_else(|| Error::DeviceNotReady {
                    path: session.device_path().to_string(),
                })?;
                let title = disc
                    .titles
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
                // DVD CSS is per-VTS: resolve the per-title key via the pipeline
                // (see `session_mux_keys`), never the whole-disc `decrypt_keys()`.
                (title, disc.content_format, session_mux_keys(disc), playlist)
            };
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
            (Box::new(stream), Some(playlist))
        }
        MuxInput::Live {
            reader,
            title,
            format,
            keys,
            key_map,
        } => {
            // INLINE `DiscStream` — the same constructor the `Session` arm uses,
            // NOT `build_iso_pipeline` (the prefetch highway). The consumer's
            // adaptive batch-retry lives in `DiscStream::fill_extents`, which the
            // highway would bypass; the live single-pass path must keep it.
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
            // Apply the forensic FMTS key map BEFORE reads begin — rewrites the
            // extent walk to our-phase units only and installs the map so each
            // unit decrypts with its mapped key. `None` leaves the walk unchanged
            // (identical to a plain single/multi-CPS disc). Single-pass FMTS
            // correctness depends on this: dropping it reads the alternate
            // device-group units and mis-decrypts the forensic segment.
            if let Some(map) = key_map {
                stream = stream.with_key_map(map);
            }
            stream.skip_errors = opts.skip_errors;
            // Same reader-side event vocabulary as the `Session` arm
            // (`SectorSkipped` / `BatchSizeChanged` / `BytesRead`).
            stream.on_event(reader_event_fn(events.clone()));
            (Box::new(stream), None)
        }
    };

    drive_mux(
        stream,
        dest_url,
        halt,
        events.as_ref(),
        playlist_name.as_deref(),
    )
}

/// Translate libfreemkv's reader-side [`Event`]s into [`MuxEvents`] calls,
/// producing the `'static` [`EventFn`](crate::sector::prefetched::EventFn) the
/// file highway ([`build_iso_pipeline`]) and the live
/// [`DiscStream`](crate::mux::DiscStream) constructors require. Cloning the
/// `Arc` into the returned closure is precisely what lets a borrowed-lifetime
/// consumer's events reach the highway's producer thread — a `&dyn MuxEvents`
/// borrow cannot satisfy the `'static` bound.
///
/// Mapping (real [`EventKind`] variants):
/// - `BytesRead { bytes, total }`   → [`MuxEvents::on_read_progress`] (read-side;
///   the file highway's only reader event — `total` is the extents' byte total)
/// - `SectorSkipped { sector }`     → [`MuxEvents::on_sector_skipped`] (live only)
/// - `BatchSizeChanged { new_size, reason }` → [`MuxEvents::on_batch_size_changed`]
///   (live only)
/// - `ReadError { sector, .. }`     → [`MuxEvents::on_read_error`]
///
/// Sector numbers are the library's `u64`; the `MuxEvents` LBA hooks take `u32`
/// (the disc's LBA space), so they are narrowed with `as u32`.
/// Decrypt keys for the live `Session` mux of `disc`.
///
/// A DVD is handed [`DecryptKeys::None`] so [`DiscStream::new`](crate::mux::DiscStream)
/// cracks the CORRECT per-title CSS key: CSS title keys are per-VTS, and
/// [`Disc::decrypt_keys`](crate::disc::Disc::decrypt_keys) returns the LARGEST
/// title's VTS key — muxing a bonus title in a DIFFERENT VTS with it descrambles
/// to corrupt MPEG-PS. `resolve_dvd_title_key`'s crack-guard only fires when
/// `keys` is `None`, so passing the whole-disc key would skip the per-title
/// crack entirely. This mirrors the sibling `resolve.rs::input()` DVD
/// special-case (and the pre-refactor CLI `pipe.rs`). AACS / genuinely-clear
/// discs resolve from `decrypt_keys()` with no read.
fn session_mux_keys(disc: &crate::disc::Disc) -> DecryptKeys {
    if matches!(disc.format, crate::disc::DiscFormat::Dvd) {
        DecryptKeys::None
    } else {
        disc.decrypt_keys()
    }
}

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

/// The reader-agnostic driver body: headers → gate → sink → pump → finish.
/// Split out so it can be unit-tested against a synthetic [`Stream`] (the
/// injection seam), independent of which constructor built `stream`.
fn drive_mux(
    mut stream: Box<dyn Stream>,
    dest_url: &str,
    halt: &Halt,
    events: &dyn MuxEvents,
    playlist_name: Option<&str>,
) -> std::io::Result<MuxOutcome> {
    // Title assembled from the scanned metadata; the playlist name (disc name)
    // overrides `info().playlist` where the consumer supplied one.
    let mut out_title = stream.info().clone();
    if let Some(name) = playlist_name {
        out_title.playlist = name.to_string();
    }

    // ── chapters:// / json:// short-circuit — BEFORE the header pump/gate ──
    //
    // These sinks write their whole file from the scanned title at `output()`
    // time and consume no PES frames, so they need no codec headers. Running
    // the header pump/gate first (the CLI's ordering) would false-fail a
    // metadata export on a title whose video headers never resolve. Do it here
    // by construction instead.
    if matches!(
        parse_url(dest_url),
        StreamUrl::Chapters { .. } | StreamUrl::Json { .. }
    ) {
        let mut sink = CountingStream::new(output(dest_url, &out_title)?);
        events.on_output_opened(&out_title);
        sink.finish()?;
        return Ok(MuxOutcome {
            completed: true,
            output_opened: true,
            bytes_written: sink.bytes_written(),
            errors: stream.errors(),
            lost_bytes: stream.lost_bytes(),
            streams: out_title.streams.len(),
        });
    }

    // ── Header pump ──
    //
    // Buffer frames until every video track's codec_private has resolved; MKV
    // can't write a track header without codec init data. The loop breaks on
    // EOF/None too, so the gate below re-checks.
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
            });
        }
        match stream.read()? {
            Some(frame) => {
                buffered_bytes = buffered_bytes.saturating_add(frame.data.len());
                buffered.push(frame);
                // Bounded header buffer: a title whose codec_private never
                // resolves would otherwise buffer the entire (tens-of-GB)
                // stream into RAM until OOM. Treat cap-exceeded identically to
                // "headers never resolved" — fail fast with the SAME
                // `Error::MkvInvalid` the header gate below returns, instead of
                // swapping the box to death.
                if buffered_bytes > HEADER_BUFFER_CAP_BYTES {
                    return Err(Error::MkvInvalid.into());
                }
            }
            None => break,
        }
    }

    // ── Header gate ──
    //
    // The pump can break on EOF (or a read error re-surfaced as `?`) without
    // headers ever resolving. Finalising then writes a track header with no
    // CODEC_PRIVATE — a structurally-invalid MKV the zero-output guard does not
    // catch. Refuse.
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
    let total_bytes = info.size_bytes;
    let num_streams = info.streams.len();

    // ── Open the sink, wrap in a byte counter, hand it to the write pipeline ──
    let output_stream = CountingStream::new(output(dest_url, &out_title)?);
    events.on_output_opened(&out_title);

    // The write consumer runs on its own thread so the latency-bound sink write
    // overlaps the next `stream.read()`. `bytes` mirrors the consumer's running
    // written-byte count out to the driving thread for `on_write_progress`.
    let bytes = Arc::new(AtomicU64::new(0));
    let sink = WriteSink {
        output: output_stream,
        bytes: bytes.clone(),
    };
    let pipe = Pipeline::spawn_named("freemkv-mux-consumer", WRITE_PIPELINE_DEPTH, sink)
        .map_err(std::io::Error::from)?;

    // ── Frame pump ──
    let deadline = Duration::from_secs(MUX_SEND_DEADLINE_SECS);
    let mut interrupted = false;

    // Buffered header frames first, in order.
    for frame in buffered {
        if pipe.send_with_halt(frame, halt, deadline).is_err() {
            interrupted = true;
            break;
        }
        // Feed write-side progress during the drain exactly as the steady-state
        // loop below does. The watchdog is fed only from `on_write_progress`;
        // no `stream.read()` runs here (reads finished in the header pump), so
        // omitting this leaves the watchdog unfed for the whole header-drain
        // phase and it can false-escalate on a long/slow drain.
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
                    if pipe.send_with_halt(frame, halt, deadline).is_err() {
                        interrupted = true;
                        break;
                    }
                    events.on_write_progress(bytes.load(Ordering::Relaxed), total_bytes);
                }
                Ok(None) => break,
                Err(e) => {
                    // Drain + join the consumer so its output file handle is
                    // released, then propagate the read error.
                    let _ = pipe.finish_with_halt(Some(halt));
                    return Err(e);
                }
            }
        }
    }

    // ── Finish ──
    //
    // Drop the producer side and join the consumer; its `close()` finalises the
    // container and returns the payload-byte count. On halt/wedge this returns
    // an error variant; we translate that to `completed = false` rather than
    // surfacing it as a hard failure (a clean operator stop is not an error).
    let (bytes_written, finalize_failed) = match pipe.finish_with_halt(Some(halt)) {
        Ok(b) => (b, false),
        Err(Error::Halted | Error::PipelineJoinTimeout) => (bytes.load(Ordering::Relaxed), true),
        Err(e) => return Err(e.into()),
    };

    if interrupted || finalize_failed || halt.is_cancelled() {
        return Ok(MuxOutcome {
            completed: false,
            output_opened: true,
            bytes_written,
            errors: stream.errors(),
            lost_bytes: stream.lost_bytes(),
            streams: num_streams,
        });
    }

    // ── Zero-output / NoStreams gate ──
    //
    // A natural drain that wrote no streams or not a single payload byte is the
    // empty/undecryptable-input silent failure — refuse to report it complete.
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
    })
}

/// Write-side [`Sink`]: applies each frame to the counting output stream and
/// finalises the container on close. `close()` returns the payload-byte count.
struct WriteSink {
    output: CountingStream,
    bytes: Arc<AtomicU64>,
}

impl Sink<PesFrame> for WriteSink {
    type Output = u64;

    fn apply(&mut self, frame: PesFrame) -> Result<Flow, Error> {
        self.output.write(&frame).map_err(Error::from)?;
        self.bytes
            .store(self.output.bytes_written(), Ordering::Relaxed);
        Ok(Flow::Continue)
    }

    fn close(mut self) -> Result<u64, Error> {
        self.output.finish().map_err(Error::from)?;
        Ok(self.output.bytes_written())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disc::DiscTitle;
    use std::sync::atomic::AtomicBool;

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
            }
        }
        fn with_frames(mut self, n: usize) -> Self {
            for i in 0..n {
                self.frames.push_back(PesFrame {
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
            if let Some((halt, after)) = &self.cancel_halt {
                if self.reads >= *after {
                    halt.cancel();
                }
            }
            let f = self.frames.pop_front();
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
            self.reads >= self.headers_ready_after
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

    fn tmp(name: &str) -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(name);
        let url = format!("chapters://{}", path.display());
        (dir, url)
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
        let out = drive_mux(stream, &url, &halt, &spy, None).expect("chapters must short-circuit");
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
        let out =
            drive_mux(stream, &url, &halt, &NoopEvents, None).expect("json must short-circuit");
        assert!(out.completed);
        assert!(out.output_opened);
    }

    // ── header gate rejects a stream whose codec_private never resolves. ──
    // Mutation: dropping the gate lets it proceed to a NoStreams / success path.
    #[test]
    fn header_gate_rejects_unresolved_codec_private() {
        let stream = Box::new(FakeStream::new(1).with_frames(3).never_ready());
        let halt = Halt::new();
        let err = drive_mux(stream, "null://", &halt, &NoopEvents, None)
            .expect_err("unresolved headers must be refused");
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
        let err = drive_mux(stream, "null://", &halt, &NoopEvents, None)
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
        let out = drive_mux(stream, "null://", &halt, &NoopEvents, None)
            .expect("halt is a clean stop, not an error");
        assert!(!out.completed, "an interrupted mux is not complete");
        assert!(out.output_opened, "the sink was opened before the halt");
    }

    // ── a normal stream pumps N frames → bytes_written>0, completed=true. ──
    #[test]
    fn normal_stream_completes_with_bytes() {
        let stream = Box::new(FakeStream::new(2).with_frames(10));
        let halt = Halt::new();
        let spy = SpyEvents::new();
        let out = drive_mux(stream, "null://", &halt, &spy, None).expect("normal mux completes");
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

    /// The `reader_event_fn` translation maps every real [`EventKind`] the
    /// reader emits onto the matching [`MuxEvents`] method, dispatched through
    /// the cloned `Arc`. Mutation: dropping any match arm (or narrowing the
    /// `Arc` clone so nothing fires) leaves that counter at 0 and fails here.
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

    /// End-to-end through `mux_stream` on the ISO path: a real 3-sector ISO on
    /// disk (one BD-TS audio PES, one AACS unit) muxed to `null://`. Asserts the
    /// reader-side `BytesRead` progress AND `on_output_opened` reach the
    /// `Arc<dyn MuxEvents>` — i.e. the constructor `EventFn` really carries the
    /// consumer's events across the highway's producer thread.
    ///
    /// Mutation: passing `None` (instead of `Some(reader_event_fn(...))`) to
    /// `build_iso_pipeline` in the ISO arm leaves `saw_read_total` false — the
    /// write-side `on_write_progress` carries `size_bytes` (0 here), never the
    /// 6144 extents total — and this test fails.
    #[test]
    fn mux_stream_iso_forwards_reader_progress_through_arc() {
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
        };
        let halt = Halt::new();
        let out = mux_stream(
            MuxInput::Iso {
                path: &iso_path,
                title,
                format: crate::disc::ContentFormat::BdTs,
                keys: DecryptKeys::None,
                key_map: None,
                key_fetch: None,
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

    /// Recording `SectorSource` for the live-path tests: logs every read's
    /// `(lba, count)` and returns zeroed sectors (never flagged scrambled, so a
    /// synthetic key map's decrypt is a pass-through — no real crypto). The
    /// recorded LBAs are the observable proof of WHICH extent walk the inline
    /// `DiscStream` executed.
    struct RecordingReader {
        capacity: u32,
        log: std::sync::Arc<std::sync::Mutex<Vec<(u32, u16)>>>,
    }
    impl crate::sector::SectorSource for RecordingReader {
        fn read_sectors(
            &mut self,
            lba: u32,
            count: u16,
            buf: &mut [u8],
            _recovery: bool,
        ) -> crate::error::Result<usize> {
            self.log.lock().unwrap().push((lba, count));
            let bytes = count as usize * 2048;
            buf[..bytes].fill(0);
            Ok(bytes)
        }
        fn capacity_sectors(&self) -> u32 {
            self.capacity
        }
    }

    /// `MuxInput::Live` builds the INLINE `DiscStream` (not the highway) AND
    /// applies the forensic `key_map` before reading. Proof is the recorded LBA
    /// set: `with_key_map` rewrites the extent walk via `AacsKeyMap::read_plan`
    /// so the alternate-phase (odd) forensic units are NEVER fetched off the
    /// source. The zeroed reader can't resolve codec headers, so the mux drains
    /// to a `NoStreams`/`MkvInvalid` refusal — irrelevant here; the test asserts
    /// only the read plan the reader actually executed.
    ///
    /// Mutation: deleting the `stream = stream.with_key_map(map)` line in the
    /// `Live` arm leaves the extents un-rewritten, so the reader DOES fetch the
    /// dropped odd-phase LBAs and the "never read" assertion below fails. (A
    /// second mutation — routing `Live` through `build_iso_pipeline` instead of
    /// `DiscStream::new` — would not compile against a `Box<dyn SectorSource>`
    /// and drop the inline `fill_extents` retry entirely.)
    #[test]
    fn mux_input_live_uses_inline_discstream_and_applies_key_map() {
        use crate::decrypt::{AacsKeyMap, Phase};
        use crate::disc::Extent;

        // 3 sectors == one AACS aligned unit. A 10-unit Even forensic segment at
        // LBA [1030, 1060): kept even units start at 1030,1036,1042,1048,1054;
        // the odd units 1033,1039,1045,1051,1057 must never be read.
        let us = 3u32;
        let map = std::sync::Arc::new(AacsKeyMap::from_ranges_phased(vec![(
            1030,
            1060,
            5,
            Phase::Even,
        )]));

        let mut title = aac_audio_title(0x1100);
        title.extents = vec![Extent {
            start_lba: 1000,
            sector_count: 300,
        }];

        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(u32, u16)>::new()));
        let reader = Box::new(RecordingReader {
            capacity: 4000,
            log: log.clone(),
        });

        let opts = MuxOptions {
            skip_errors: false,
            // One unit per read so the recorded LBAs land on unit boundaries and
            // an odd-phase unit can't hide inside a larger coalesced batch.
            batch_sectors: us as u16,
            raw: false,
        };
        let halt = Halt::new();
        // Drains to a NoStreams refusal (zeroed data resolves no headers); we
        // only care about the reads it performed on the way there.
        let _ = mux_stream(
            MuxInput::Live {
                reader,
                title,
                format: crate::disc::ContentFormat::BdTs,
                keys: DecryptKeys::None,
                key_map: Some(map),
            },
            "null://",
            &opts,
            &halt,
            Arc::new(NoopEvents),
        );

        let reads = log.lock().unwrap();
        let read_lbas: std::collections::HashSet<u32> = reads.iter().map(|&(lba, _)| lba).collect();
        // The dropped odd-phase forensic units must never be fetched.
        for odd in [1033u32, 1039, 1045, 1051, 1057] {
            assert!(
                !read_lbas.contains(&odd),
                "alternate-phase forensic unit LBA {odd} was read — key_map not applied \
                 (with_key_map dropped from the Live arm?)"
            );
        }
        // Sanity: our-phase units on either side ARE read (the walk still ran).
        assert!(
            read_lbas.contains(&1030) && read_lbas.contains(&1054),
            "our-phase forensic units must still be read (kept LBAs 1030/1054)"
        );
    }

    // ── Regression A: header-buffer cap fails fast instead of OOM ───────────
    //
    // A stream whose headers never resolve but that keeps yielding frames must
    // be refused once the pre-headers buffer passes `HEADER_BUFFER_CAP_BYTES`,
    // BEFORE draining the whole (unbounded, OOM-inducing) stream. Frames are
    // zero-filled `Vec`s (`alloc_zeroed`, lazily paged) so the nominal size is
    // cheap in RSS. We hand the pump FAR more frames than the cap needs and
    // assert both that it fails with the same `MkvInvalid` outcome AND that it
    // stopped EARLY (bounded read count) — the latter is what distinguishes the
    // capped path from the un-capped one.
    //
    // Mutation: removing the cap makes the pump drain all `MANY` frames (reads
    // == MANY) before failing at the header gate → the `reads_seen` bound fails.
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
        let err = drive_mux(Box::new(fs), "null://", &halt, &NoopEvents, None)
            .expect_err("over-cap header buffer must fail fast, not OOM");
        assert_eq!(
            err.to_string(),
            format!("E{}", crate::error::E_MKV_INVALID),
            "cap-exceeded returns the same MkvInvalid as headers-never-resolved"
        );
        let reads = reads_seen.load(Ordering::SeqCst);
        assert!(
            reads <= cap_frames + 1,
            "must fail after ~{cap_frames} frames (cap), not drain all {MANY} (read {reads})"
        );
    }

    // ── Regression B: watchdog fed during the buffered header drain ─────────
    //
    // Headers resolve only after K frames are buffered; those K frames are then
    // flushed to the sink in the drain loop. Every flushed frame must fire
    // `on_write_progress` (the sole watchdog feed) — with exactly K frames and
    // no remainder, ALL `on_write_progress` calls come from the drain.
    //
    // Mutation: dropping the `events.on_write_progress(...)` call in the drain
    // loop leaves the count at 0 → this fails.
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
        let out = drive_mux(Box::new(fs), "null://", &halt, &events, None)
            .expect("K-frame stream muxes cleanly");
        assert!(out.completed);
        assert_eq!(
            events.writes.load(Ordering::SeqCst),
            K as u64,
            "each of the K buffered header frames must feed on_write_progress on drain"
        );
    }

    // ── Regression C: Session key selection special-cases DVD ───────────────
    //
    // `session_mux_keys` is the seam the `MuxInput::Session` arm uses to pick
    // decrypt keys. A DVD must get `None` (so DiscStream cracks the correct
    // per-title/per-VTS CSS key); a non-DVD passes `decrypt_keys()` through.
    // Building a full multi-VTS DiscSession fixture is infeasible in a unit
    // test, so this asserts the is_dvd→None DECISION at the tightest seam.
    //
    // Mutation: reverting to unconditional `disc.decrypt_keys()` returns
    // `Css{..}` for the DVD case → the `None` assertion fails.
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
}
