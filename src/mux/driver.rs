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
use crate::event::BatchSizeReason;
use crate::halt::Halt;
use crate::io::pipeline::{Flow, Pipeline, Sink, WRITE_PIPELINE_DEPTH};
use crate::pes::{CountingStream, PesFrame, Stream};
use crate::sector::{FileSectorSource, KeyFetch};
use crate::session::DiscSession;

use super::resolve::{InputOptions, StreamUrl, build_iso_pipeline, input, output, parse_url};

/// Per-frame send deadline for the write pipeline. Longer than a soft stall
/// warning, shorter than the pipeline's own join backstop, so a wedged sink
/// surfaces here as a per-frame timeout rather than blocking the whole mux.
/// Mirrors autorip's `MUX_SEND_DEADLINE_SECS`.
const MUX_SEND_DEADLINE_SECS: u64 = 60;

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
/// consumer overrides only what it renders. `Send + Sync` so a future
/// reader-side wiring can share it across the highway's threads.
///
/// The driver fires [`Self::on_output_opened`] and [`Self::on_progress`] from
/// the driving thread. The reader-side events
/// ([`Self::on_sector_skipped`] / [`Self::on_batch_size_changed`] /
/// [`Self::on_read_error`]) are declared here for the consumer to consume once
/// the constructor `event_fn` is wired (steps 4b/4c) — the file highway's
/// `EventFn` is `'static`, so it can only carry an owned (`Arc`) events handle,
/// not the `&dyn` borrow this driver takes.
pub trait MuxEvents: Send + Sync {
    /// Fired once, immediately after the output sink is created.
    fn on_output_opened(&self, _title: &DiscTitle) {}
    /// Fired periodically during the frame pump with the running written-byte
    /// count and the title's total byte estimate.
    fn on_progress(&self, _bytes_written: u64, _bytes_total: u64) {}
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
    events: &dyn MuxEvents,
) -> std::io::Result<MuxOutcome> {
    // Construct the source stream. The file/ISO path calls the untouched
    // `build_iso_pipeline` highway (zero added copies); the live path builds a
    // `DiscStream`; a URL source goes through `input()`. `event_fn` is `None`
    // here — the highway's `EventFn` is `'static` and cannot carry the `&dyn`
    // events borrow; reader-side event forwarding is wired in 4b/4c.
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
                None,
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
                    path: session.drive().device_path().to_string(),
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
                (title, disc.content_format, disc.decrypt_keys(), playlist)
            };
            // A missing staged reader ("already consumed" / never staged) is a
            // clean error, not a panic (contract Q2).
            let reader = session.take_reader().ok_or_else(|| Error::DeviceNotReady {
                path: session.drive().device_path().to_string(),
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
            (Box::new(stream), Some(playlist))
        }
    };

    drive_mux(stream, dest_url, halt, events, playlist_name.as_deref())
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
            Some(frame) => buffered.push(frame),
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
    // written-byte count out to the driving thread for `on_progress`.
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
                    events.on_progress(bytes.load(Ordering::Relaxed), total_bytes);
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
}
