use super::*;
use crate::disc::DiscTitle;

thread_local! {
    // A sink `drive_mux` opens instead of its URL's, once (see `open_output`).
    pub(super) static TEST_SINK: std::cell::RefCell<Option<Box<dyn PesSink>>> =
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

impl crate::pes::PesSource for EndSpy {
    fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
        Ok(None)
    }

    fn info(&self) -> &DiscTitle {
        &self.info
    }
}

impl crate::pes::PesSink for EndSpy {
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
    let res = drive_mux(
        Box::new(stream),
        "null://",
        &crate::ctx::Ctx::new(halt.clone()),
        None,
        None,
    );
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

// A strict read failure before any frame lands: the mkv's zero-frame `MkvInvalid` on
// ending the output is a skippable stub code and must not replace the read error.
#[test]
fn a_read_failure_before_any_frame_is_not_a_skippable_stub() {
    let dir = tempfile::tempdir().expect("tempdir");
    let url = format!("mkv://{}", dir.path().join("o.mkv").display());
    let mut fs = FakeStream::new(1).with_frames(10);
    fs.fail_read_at = Some(0);
    let ctx = crate::ctx::Ctx::new(Halt::new());
    let err = drive_mux(Box::new(fs), &url, &ctx, None, None).expect_err("a read failure");
    assert_eq!(
        crate::error::error_code(&err),
        Some(crate::error::E_DISC_READ),
        "got {err}"
    );
    assert!(!crate::error::is_skippable_title_stub(&err));
}

// A Stop after the mkv output opens but before any frame reaches it: a halted outcome,
// never the zero-frame `MkvInvalid` from ending the empty output.
#[test]
fn a_stop_before_any_frame_reaches_an_open_mkv_is_halted_not_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let url = format!("mkv://{}", dir.path().join("o.mkv").display());
    let halt = Halt::new();
    let fs = FakeStream::new(1).with_frames(10).cancels(halt.clone(), 0);
    let ctx = crate::ctx::Ctx::new(halt.clone());
    let o = drive_mux(Box::new(fs), &url, &ctx, None, None).expect("a stop is not an error");
    assert!(o.output_opened && !o.completed && o.halted, "{o:?}");
    assert_eq!(o.bytes_written, 0);
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

impl crate::pes::PesSource for FakeStream {
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

impl crate::pes::PesSink for FakeStream {
    fn write(&mut self, _frame: &PesFrame) -> std::io::Result<()> {
        Ok(())
    }

    fn finish(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn info(&self) -> &DiscTitle {
        &self.info
    }
}

/// Records whether `OutputOpened` fired.
struct SpyEvents {
    opened: AtomicBool,
}
impl SpyEvents {
    fn new() -> Arc<Self> {
        Arc::new(SpyEvents {
            opened: AtomicBool::new(false),
        })
    }
}
impl crate::event::Events for SpyEvents {
    fn event(&self, e: &Event<'_>) {
        if let Event::OutputOpened { .. } = e {
            self.opened.store(true, Ordering::SeqCst);
        }
    }
}

// BUG-6: a playlist labels a 7.1/Atmos TrueHD track 5.1 at 48 kHz. Every source's mux
// completes the label from the track's first major sync before the output opens, and a
// metadata sink (no frames) still gets the completed title.
#[test]
fn truehd_labels_complete_from_the_stream_for_every_sink() {
    use crate::disc::{AudioChannels, AudioStream, Codec, LabelPurpose, SampleRate, Stream};
    // format_info: top nibble 0x1 -> 96 kHz; low 13 bits 0x1F -> 7.1 (8ch); 4 substreams.
    let mut es = vec![0u8; 24];
    es[0] = 0xAA;
    es[1] = 0xBB;
    es[2..6].copy_from_slice(&0xF872_6FBAu32.to_be_bytes());
    es[6..10].copy_from_slice(&((0x1u32 << 28) | 0x1F).to_be_bytes());
    es[2 + 16] = 4 << 4;
    let opened_with = |dest: &str| {
        let mut s = FakeStream::new(0);
        s.info.streams = vec![Stream::Audio(AudioStream {
            pid: 0x1100,
            codec: Codec::TrueHd,
            channels: AudioChannels::Surround51,
            language: "eng".into(),
            sample_rate: SampleRate::S48,
            secondary: false,
            purpose: LabelPurpose::Normal,
            label: crate::labels::generate_audio_label(
                &Codec::TrueHd,
                &AudioChannels::Surround51,
                false,
            ),
        })];
        for pts in 0..3 {
            s.frames.push_back(PesFrame {
                discard_padding_ns: 0,
                track: 0,
                pts,
                keyframe: true,
                data: es.clone(),
                duration_ns: None,
                source: None,
                coding: None,
            });
        }
        let seen: Arc<std::sync::Mutex<Option<DiscTitle>>> = Arc::default();
        let got = seen.clone();
        let ctx = Ctx::default().with_events(Arc::new(move |e: &Event<'_>| {
            if let Event::OutputOpened { title } = e {
                *got.lock().unwrap() = Some((*title).clone());
            }
        }));
        let _ = drive_mux(Box::new(s), dest, &ctx, None, None);
        let t = seen.lock().unwrap().take().expect("output opened");
        let Stream::Audio(a) = &t.streams[0] else {
            panic!("audio")
        };
        (a.channels, a.sample_rate, a.label.clone())
    };
    let dir = tempfile::tempdir().unwrap();
    let json = format!("json://{}", dir.path().join("t.json").display());
    let atmos = crate::labels::generate_audio_label_atmos(
        &Codec::TrueHd,
        &AudioChannels::Surround71,
        false,
    );
    for dest in ["null://", json.as_str()] {
        assert_eq!(
            opened_with(dest),
            (AudioChannels::Surround71, SampleRate::S96, atmos.clone()),
            "{dest}"
        );
    }
}

// A never-syncing TrueHD title with `n` non-sync frames of `size` bytes each.
fn unsynced_truehd_stream(n: usize, size: usize) -> FakeStream {
    use crate::disc::{AudioChannels, AudioStream, Codec, LabelPurpose, SampleRate, Stream};
    let mut s = FakeStream::new(0);
    s.info.streams = vec![Stream::Audio(AudioStream {
        pid: 0x1100,
        codec: Codec::TrueHd,
        channels: AudioChannels::Surround51,
        language: "eng".into(),
        sample_rate: SampleRate::S48,
        secondary: false,
        purpose: LabelPurpose::Normal,
        label: String::new(),
    })];
    for pts in 0..n as i64 {
        s.frames.push_back(PesFrame {
            discard_padding_ns: 0,
            track: 0,
            pts,
            keyframe: true,
            data: vec![0u8; size],
            duration_ns: None,
            source: None,
            coding: None,
        });
    }
    s
}

// A Stop during the TrueHD probe ends the mux before any output opens, for both a
// frame sink and a metadata sink, whether it shows as a cancelled halt or Err(Halted).
#[test]
fn a_stop_during_the_truehd_probe_stops_before_the_output_opens() {
    let dir = tempfile::tempdir().unwrap();
    let json = format!("json://{}", dir.path().join("t.json").display());
    for dest in ["null://", json.as_str()] {
        let halt = Halt::new();
        let s = unsynced_truehd_stream(5, 64).cancels(halt.clone(), 1);
        let out = drive_mux(Box::new(s), dest, &Ctx::new(halt), None, None).unwrap();
        assert!(out.halted && !out.completed && !out.output_opened, "{dest}");

        let s = unsynced_truehd_stream(5, 64).halt_errs_at(1);
        let out = drive_mux(Box::new(s), dest, &Ctx::default(), None, None).unwrap();
        assert!(!out.completed && !out.output_opened, "{dest} Err(Halted)");
    }
}

// The probe reads at most TRUEHD_PROBE_BYTES before the output opens; a title that never
// syncs is not buffered whole.
#[test]
fn the_truehd_probe_is_byte_capped() {
    let mut s = unsynced_truehd_stream(20, 1 << 20);
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    s.read_observer = Some(reads.clone());
    let at_open = Arc::new(std::sync::atomic::AtomicUsize::new(usize::MAX));
    let (seen, counter) = (at_open.clone(), reads.clone());
    let ctx = Ctx::default().with_events(Arc::new(move |e: &Event<'_>| {
        if let Event::OutputOpened { .. } = e {
            seen.store(counter.load(Ordering::SeqCst), Ordering::SeqCst);
        }
    }));
    drive_mux(Box::new(s), "null://", &ctx, None, None).unwrap();
    let n = at_open.load(Ordering::SeqCst);
    assert!(n <= 10, "{n} MiB frames read before the output opened");
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
    drive_mux(
        stream,
        &url,
        &crate::ctx::Ctx::new(halt.clone()).with_events(spy.clone()),
        None,
        Some(&source),
    )
    .expect("fvi mux runs");

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
    // A disc folder is an image-level source (a synthesized UDF volume).
    assert_eq!(url_medium(&parse_url("dir:///m/BD")), Medium::Iso);
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
    let out = drive_mux(
        stream,
        &url,
        &crate::ctx::Ctx::new(halt.clone()).with_events(spy.clone()),
        None,
        None,
    )
    .expect("chapters must short-circuit");
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
    let out = drive_mux(
        stream,
        &url,
        &crate::ctx::Ctx::new(halt.clone()),
        None,
        None,
    )
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
    let err = drive_mux(
        stream,
        "null://",
        &crate::ctx::Ctx::new(halt.clone()),
        None,
        None,
    )
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
    let err = drive_mux(
        stream,
        "null://",
        &crate::ctx::Ctx::new(halt.clone()),
        None,
        None,
    )
    .expect_err("empty drain must be refused");
    assert_eq!(err.to_string(), format!("E{}", crate::error::E_NO_STREAMS));
}

// HP2: a Stop landing during the open (here the CSS crack of a `Live` DVD title) is the
// same outcome as one in the pump: `completed = false, halted = true`, never an error.
#[test]
fn a_stop_during_the_open_is_a_halted_outcome_not_an_error() {
    // A live mux spawns the prefetch producer, a Drive holder.
    let _serial = crate::sector::prefetched::holder_test_lock();
    let ctx = Ctx::default();
    ctx.halt.cancel();
    let title = DiscTitle {
        extents: vec![crate::disc::Extent {
            start_lba: 0,
            sector_count: 16,
        }],
        ..DiscTitle::empty()
    };
    let reader = crate::test_util::MemSource::new(vec![0u8; 16 * 2048]);
    let src = Source::from_reader(
        Box::new(reader),
        ScannedTitle::new(title, crate::disc::ContentFormat::MpegPs),
    );
    let opts = MuxOptions {
        batch_sectors: 16,
        ..Default::default()
    };
    let out = mux_with_keys(src, None, "null://", &opts, &ctx).expect("a Stop is not an error");
    assert!(out.halted && !out.completed && !out.output_opened);
}

// A Stop landing in `open_source` (the scan / bring-up) is the documented stopped
// outcome from `mux_url`, not an `E_HALTED` error; any other open failure stays an error.
#[test]
fn a_stop_during_mux_urls_open_is_a_halted_outcome() {
    let ctx = Ctx::default();
    ctx.halt.cancel();
    let out = open_failed(Error::Halted, &ctx).expect("a Stop is not an error");
    assert!(out.halted && !out.completed && !out.output_opened);
    assert!(open_failed(Error::NoStreams, &ctx).is_err());
    let live = Ctx::default();
    assert!(
        open_failed(Error::Halted, &live).is_err(),
        "Err(Halted) with no Stop requested is not a stopped outcome"
    );
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
    let out = drive_mux(
        stream,
        "null://",
        &crate::ctx::Ctx::new(halt.clone()),
        None,
        None,
    )
    .expect("halt is a clean stop, not an error");
    assert!(!out.completed, "an interrupted mux is not complete");
    assert!(out.output_opened, "the sink was opened before the halt");
    assert!(
        out.halted,
        "a Stop is reported as halted so callers map it to Cancelled"
    );
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
    let out = drive_mux(
        Box::new(fs),
        "null://",
        &crate::ctx::Ctx::new(halt.clone()).with_events(events.clone()),
        None,
        None,
    )
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
    let out = drive_mux(
        stream,
        "null://",
        &crate::ctx::Ctx::new(halt.clone()),
        None,
        None,
    )
    .expect("a halt mid frame-read is a clean stop, not an Err");
    assert!(!out.halted, "halt was never cancelled: not a Stop");
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
    let out = drive_mux(
        stream,
        "null://",
        &crate::ctx::Ctx::new(halt.clone()),
        None,
        None,
    )
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
    let out = drive_mux(
        stream,
        "null://",
        &crate::ctx::Ctx::new(halt.clone()).with_events(spy.clone()),
        None,
        None,
    )
    .expect("normal mux completes");
    assert!(out.completed);
    assert!(out.output_opened);
    assert!(spy.opened.load(Ordering::SeqCst));
    assert_eq!(out.bytes_written, 10 * 100, "10 frames × 100 bytes payload");
    assert_eq!(out.streams, 2);
}

// ── reader-side event forwarding through the Arc ────────────────────────

/// An [`Events`](crate::event::Events) backed by atomics that records the events a test
/// asserts reached the run's context.
struct CountingEvents {
    opened: AtomicBool,
    progress_calls: AtomicU64,
    /// Set once `BytesRead` carries a `total` equal to the ISO extents' byte total — the
    /// fingerprint of the *read-side* event (the write-side `BytesWritten` carries the
    /// title's `size_bytes`, a different number).
    saw_read_total: AtomicBool,
    read_total: u64,
}
impl CountingEvents {
    fn new(read_total: u64) -> std::sync::Arc<Self> {
        std::sync::Arc::new(CountingEvents {
            opened: AtomicBool::new(false),
            progress_calls: AtomicU64::new(0),
            saw_read_total: AtomicBool::new(false),
            read_total,
        })
    }
}
impl crate::event::Events for CountingEvents {
    fn event(&self, e: &Event<'_>) {
        match *e {
            Event::OutputOpened { .. } => self.opened.store(true, Ordering::SeqCst),
            Event::BytesRead { total, .. } => {
                self.progress_calls.fetch_add(1, Ordering::SeqCst);
                if total == self.read_total {
                    self.saw_read_total.store(true, Ordering::SeqCst);
                }
            }
            _ => {}
        }
    }
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
// progress AND `OutputOpened` reach the run's events.
#[test]
fn mux_iso_reports_reader_progress_to_the_ctx() {
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
        title_index: 0,
    };
    let halt = Halt::new();
    let out = mux_with_keys(
        Source::from_image(
            &iso_path,
            ScannedTitle::new(title, crate::disc::ContentFormat::BdTs),
        ),
        None,
        "null://",
        &opts,
        &crate::ctx::Ctx::new(halt.clone()).with_events(events.clone()),
    )
    .expect("audio-only ISO muxes to null sink");

    assert!(out.completed, "the clip drained and finalised");
    assert!(
        events.saw_read_total.load(Ordering::SeqCst),
        "the reader-side BytesRead (total=6144) reached the ctx's events"
    );
    assert!(
        events.opened.load(Ordering::SeqCst),
        "OutputOpened reached the ctx's events"
    );
    assert!(
        events.progress_calls.load(Ordering::SeqCst) > 0,
        "at least one BytesRead observed"
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
        // there (the live Read stage reads the [0,3) extent as one batch).
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
/// hardware. It holds no key: AACS keys come only from a `KeyRing`.
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
    // A live mux spawns the prefetch producer, a Drive holder.
    let _serial = crate::sector::prefetched::holder_test_lock();
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
        title_index: 0,
    };
    let halt = Halt::new();
    let err = mux_with_keys(
        Source::from_session(&mut session),
        None,
        "null://",
        &opts,
        &crate::ctx::Ctx::new(halt.clone()),
    )
    .expect_err("a missing staged reader must be a clean error, not a panic");
    // The device-name-carrying DeviceNotReady round-trips through io::Error.
    assert_eq!(
        crate::error::error_code(&err),
        Some(crate::error::E_DEVICE_NOT_READY),
        "got {err}"
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
    // A live mux spawns the prefetch producer, a Drive holder.
    let _serial = crate::sector::prefetched::holder_test_lock();
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
        title_index: num_titles + 5,
    };
    let halt = Halt::new();
    let err = mux_with_keys(
        Source::from_session(&mut session),
        None,
        "null://",
        &opts,
        &crate::ctx::Ctx::new(halt.clone()),
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

impl crate::pes::PesSource for LateAacStream {
    fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
        let f = self.frames.pop_front();
        self.aac_seen |= f.as_ref().is_some_and(|f| f.track == 1);
        Ok(f)
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

impl crate::pes::PesSink for LateAacStream {
    fn write(&mut self, _frame: &PesFrame) -> std::io::Result<()> {
        Ok(())
    }

    fn finish(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn info(&self) -> &DiscTitle {
        &self.info
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
        &crate::ctx::Ctx::new(halt.clone()),
        None,
        None,
    )
    .expect("mux succeeds");
    assert!(out.completed);
    let back =
        crate::mux::mkvstream::MkvStream::open(std::fs::File::open(&path).expect("output exists"))
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
    let mut mkv =
        crate::mux::mkvstream::MkvStream::create(Box::new(file), &title, None).expect("create mkv");
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
    let mut mkv =
        crate::mux::mkvstream::MkvStream::create(Box::new(file), &title, None).expect("create mkv");
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
        &crate::ctx::Ctx::new(halt.clone()),
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

impl crate::pes::PesSource for UndeliveringSink {
    fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
        Ok(None)
    }

    fn info(&self) -> &DiscTitle {
        &self.info
    }
}

impl crate::pes::PesSink for UndeliveringSink {
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
impl crate::pes::PesSource for LpcmSource {
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

    fn info(&self) -> &DiscTitle {
        &self.info
    }

    fn codec_private(&self, _track: usize) -> Option<Vec<u8>> {
        self.layout.clone()
    }
}

impl crate::pes::PesSink for LpcmSource {
    fn write(&mut self, _frame: &PesFrame) -> std::io::Result<()> {
        Ok(())
    }

    fn finish(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn info(&self) -> &DiscTitle {
        &self.info
    }
}

/// Keeps the title of `OutputOpened`.
#[derive(Default)]
struct TitleSpy(std::sync::Mutex<Option<DiscTitle>>);
impl crate::event::Events for TitleSpy {
    fn event(&self, e: &Event<'_>) {
        if let Event::OutputOpened { title } = *e {
            *self.0.lock().unwrap() = Some(title.clone());
        }
    }
}

fn run(src: LpcmSource, url: &str, spy: &Arc<TitleSpy>) -> MuxOutcome {
    let ctx = crate::ctx::Ctx::default().with_events(spy.clone());
    drive_mux(Box::new(src), url, &ctx, None, None).unwrap()
}

// Every entry point (Url/Session/Iso/Live) funnels into drive_mux: the BD LPCM
// layout byte must override the playlist's "5.1" for 7.1 audio there.
#[test]
fn drive_mux_corrects_lpcm_channels_from_the_parser_layout() {
    use crate::disc::{AudioChannels, SampleRate};
    let dir = tempfile::tempdir().unwrap();
    let url = format!("mkv://{}", dir.path().join("o.mkv").display());
    let layout = Some(b"BDLP\xB4\x18".to_vec());
    let src = LpcmSource::new(AudioChannels::Surround51, SampleRate::S48, layout);
    let spy = Arc::new(TitleSpy::default());
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
/// parsed, gated by the real `HeaderGate` (as PipelinedPesStream is).
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
impl crate::pes::PesSource for GatedLpcm {
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

impl crate::pes::PesSink for GatedLpcm {
    fn write(&mut self, _frame: &PesFrame) -> std::io::Result<()> {
        Ok(())
    }

    fn finish(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn info(&self) -> &DiscTitle {
        &self.info
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
        let spy = Arc::new(TitleSpy::default());
        drive_mux(
            Box::new(src),
            &url,
            &crate::ctx::Ctx::default().with_events(spy.clone()),
            None,
            None,
        )
        .unwrap();
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
    let spy = Arc::new(TitleSpy::default());
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
    let spy = Arc::new(TitleSpy::default());
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
    // A few past the cap: enough to prove the cap stops the pump, without
    // allocating (commit-charged on Windows) hundreds of 64 MiB frames.
    let many = cap_frames + 4;

    let reads_seen = Arc::new(AtomicUsize::new(0));
    let mut fs = FakeStream::new(1).never_ready();
    fs.read_observer = Some(reads_seen.clone());
    for i in 0..many {
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
    let err = drive_mux(
        Box::new(fs),
        "null://",
        &crate::ctx::Ctx::new(halt.clone()),
        None,
        None,
    )
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
        "must fail after ~{cap_frames} frames (cap), not drain all {many} (read {reads})"
    );
}

// ── Regression B: watchdog fed during the buffered header drain ─────────
// Headers resolve only after K frames buffer, then flush to the sink.
// Every flushed frame must fire `BytesWritten`, the sole watchdog feed.
#[test]
fn write_progress_fed_during_header_drain() {
    const K: usize = 6;
    let mut fs = FakeStream::new(1).with_frames(K);
    // Headers stay unresolved until all K frames have been read+buffered.
    fs.headers_ready_after = K;

    struct WriteProgressCounter {
        writes: AtomicU64,
    }
    impl crate::event::Events for WriteProgressCounter {
        fn event(&self, e: &Event<'_>) {
            if let Event::BytesWritten { .. } = e {
                self.writes.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    let events = Arc::new(WriteProgressCounter {
        writes: AtomicU64::new(0),
    });
    let halt = Halt::new();
    let out = drive_mux(
        Box::new(fs),
        "null://",
        &crate::ctx::Ctx::new(halt.clone()).with_events(events.clone()),
        None,
        None,
    )
    .expect("K-frame stream muxes cleanly");
    assert!(out.completed);
    assert_eq!(
        events.writes.load(Ordering::SeqCst),
        K as u64,
        "each of the K buffered header frames must feed BytesWritten on drain"
    );
}

// ── Regression C: Session key selection special-cases DVD ───────────────
// A DVD must get `None` (so the pipeline cracks the correct per-title/VTS
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

// The keyless AACS ring is the Blu-ray transport stream's: neither program-stream format
// (HD DVD's `.evo`, a DVD's `.vob`) gets one, so neither is read through AACS checks here.
#[test]
fn keyless_ring_is_bdts_only() {
    let title = DiscTitle::empty();
    let opts = MuxOptions::default();
    for format in [
        crate::disc::ContentFormat::MpegPs,
        crate::disc::ContentFormat::DvdPs,
    ] {
        assert!(
            keyless_ring(&title, format, None, &opts).is_none(),
            "{format:?}"
        );
    }
    assert!(keyless_ring(&title, crate::disc::ContentFormat::BdTs, None, &opts).is_some());
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
        "a DVD must be handed None so the pipeline cracks the per-title key"
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
        let _ =
            tx.send(finish_pumped(pipe, &halt, true, &flush, &crate::ctx::Ctx::default()).is_err());
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
    let mut w =
        MkvStream::create(Box::new(std::fs::File::create(&src).unwrap()), &title, None).unwrap();
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
        &crate::ctx::Ctx::default().with_events(SpyEvents::new()),
        None,
        None,
    )
    .expect("mkv remux runs");
    assert!(out.completed);
    let back = MkvStream::open(std::fs::File::open(&dst).unwrap()).unwrap();
    assert_eq!(back.track_timing(0), timing);
}

// Records every `BytesDurable` event.
#[derive(Default)]
struct FlushSpy(std::sync::Mutex<Vec<(u64, u64)>>);

impl crate::event::Events for FlushSpy {
    fn event(&self, e: &Event<'_>) {
        if let Event::BytesDurable { bytes, total } = *e {
            self.0.lock().unwrap().push((bytes, total));
        }
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

// The forwarded `BytesDurable` calls, and how long the finish took.
fn finish_flushing(steps: u64, gap: Duration, tail: Duration) -> (Vec<(u64, u64)>, Duration) {
    let flush = FlushProgress::new(crate::halt::Liveness::new());
    let sink = FlushingClose {
        flush: flush.clone(),
        steps,
        gap,
        tail,
    };
    let progress = flush.progress().clone();
    let pipe = Pipeline::spawn_named_with_progress("t-flush", 4, sink, progress).unwrap();
    let spy = Arc::new(FlushSpy::default());
    let ctx = crate::ctx::Ctx::default().with_events(spy.clone());
    let t = std::time::Instant::now();
    finish_pumped(pipe, &Halt::new(), false, &flush, &ctx).unwrap();
    let took = t.elapsed();
    drop(ctx);
    let spy = Arc::try_unwrap(spy).ok().expect("the only handle");
    (spy.0.into_inner().unwrap(), took)
}

/// LP20 (§4.5): while the driver waits on a closing consumer, each increase of the
/// flusher's bytes produces one `BytesDurable`, and none while it is static.
#[test]
fn finish_with_halt_forwards_flush_progress() {
    // Each gap is 4× the rate limit, so no increase is coalesced into its neighbour.
    let (calls, _) = finish_flushing(3, FLUSH_PROGRESS_EVERY * 4, Duration::from_millis(600));
    let want: Vec<(u64, u64)> = (1..=3).map(|i| (i * 1000, 3000)).collect();
    assert_eq!(
        calls, want,
        "one call per increase, none during the idle tail"
    );
}

/// LP20, the rate limit: a burst of increases is forwarded at most 4 times a second,
/// and the last value still arrives.
#[test]
fn flush_progress_is_rate_limited_to_4hz() {
    let (calls, took) = finish_flushing(20, Duration::from_millis(5), Duration::from_millis(600));
    // One call at the start of each rate-limit period, plus the final value.
    let periods = (took.as_millis() / FLUSH_PROGRESS_EVERY.as_millis()) as usize;
    assert!(
        !calls.is_empty() && calls.len() <= periods + 2 && calls.len() < 20,
        "{calls:?} in {took:?}"
    );
    assert_eq!(calls.last(), Some(&(20_000, 20_000)));
}

// ── mux_with_keys (KU §3.1): keys come only from the rip's set ─────────────

fn keyed_opts() -> MuxOptions {
    MuxOptions {
        skip_errors: false,
        batch_sectors: 3,
        raw: false,
        selection: Default::default(),
        title_index: 0,
    }
}

// One encrypted audio unit at LBA 0..3 under `key`, its title, and a set keying it.
fn keyed_live(key: [u8; 16]) -> (Box<AacsUnitReader>, DiscTitle, crate::keys::KeyRing) {
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
    let set = crate::keys::KeyRing::keyed_for_test(&disc, key, &[(0, 3)]);
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
    // A live mux spawns the prefetch producer, a Drive holder.
    let _serial = crate::sector::prefetched::holder_test_lock();
    let (reader, title, set) = keyed_live([0x5A; 16]);
    let dir = tempfile::tempdir().unwrap();
    let out_path = dir.path().join("live.mkv");
    let out = mux_with_keys(
        Source::from_reader(
            reader,
            ScannedTitle::new(title, crate::disc::ContentFormat::BdTs),
        ),
        Some(&set),
        &format!("mkv://{}", out_path.display()),
        &keyed_opts(),
        &crate::ctx::Ctx::default(),
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
    // A live mux spawns the prefetch producer, a Drive holder.
    let _serial = crate::sector::prefetched::holder_test_lock();
    let key = [0x5A; 16];
    let (reader, title, _) = keyed_live(key);
    let mut disc = aacs_session_disc(title);
    disc.capacity_sectors = 16;
    let set = crate::keys::KeyRing::keyed_for_test(&disc, key, &[(0, 3)]);
    let mut session = DiscSession::from_parts_for_test(Some(disc), Some(reader));
    let out = mux_with_keys(
        Source::from_session(&mut session),
        Some(&set),
        "null://",
        &keyed_opts(),
        &crate::ctx::Ctx::default(),
    )
    .expect("the set's key opens the unit");
    assert!(out.completed && out.bytes_written > 0);
}

/// A refused selection leaves the session's reader staged: a retry on the same
/// session muxes instead of failing DeviceNotReady.
#[test]
fn mux_with_keys_session_keeps_its_reader_when_the_selection_is_refused() {
    // A live mux spawns the prefetch producer, a Drive holder.
    let _serial = crate::sector::prefetched::holder_test_lock();
    let key = [0x5A; 16];
    let (reader, title, _) = keyed_live(key);
    let mut disc = aacs_session_disc(title);
    disc.capacity_sectors = 16;
    let set = crate::keys::KeyRing::keyed_for_test(&disc, key, &[(0, 3)]);
    let mut session = DiscSession::from_parts_for_test(Some(disc), Some(reader));
    let mut bad = keyed_opts();
    bad.selection.audio = crate::mux::select::PidFilter::Only(vec![0x0FFF]);
    let run = |session: &mut DiscSession, opts: &MuxOptions| {
        mux_with_keys(
            Source::from_session(session),
            Some(&set),
            "null://",
            opts,
            &crate::ctx::Ctx::default(),
        )
    };
    let err = run(&mut session, &bad).expect_err("unknown PID is refused");
    assert_eq!(
        crate::error::error_code(&err),
        Some(crate::error::E_SELECTION_PID_UNKNOWN)
    );
    let out = run(&mut session, &keyed_opts()).expect("the retry still has its reader");
    assert!(out.completed);
}

/// A key set for another disc is a typed E7013 on a session, never a debug-build panic.
#[test]
fn mux_with_keys_session_wrong_disc_set_is_e7013() {
    // A live mux spawns the prefetch producer, a Drive holder.
    let _serial = crate::sector::prefetched::holder_test_lock();
    let key = [0x5A; 16];
    let (reader, title, _) = keyed_live(key);
    let mut other = aacs_session_disc(title.clone());
    let mut disc = aacs_session_disc(title);
    disc.capacity_sectors = 16;
    if let Some(a) = other.aacs.as_mut() {
        a.disc_hash = "0xdef".into();
    }
    let set = crate::keys::KeyRing::keyed_for_test(&other, key, &[(0, 3)]);
    let mut session = DiscSession::from_parts_for_test(Some(disc), Some(reader));
    let err = mux_with_keys(
        Source::from_session(&mut session),
        Some(&set),
        "null://",
        &keyed_opts(),
        &crate::ctx::Ctx::default(),
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
        Source::from_image(
            &path,
            ScannedTitle::new(title, crate::disc::ContentFormat::BdTs),
        ),
        Some(&set),
        &format!("mkv://{}", out_path.display()),
        &keyed_opts(),
        &crate::ctx::Ctx::default(),
    )
    .expect("no rescan: the scanned title is muxed as given");
    assert!(out.completed);
    assert!(
        holds_plain_es(&out_path),
        "the muxed audio is the plaintext"
    );
    let url = format!("iso://{}", path.display());
    let rescanned = mux_url(
        &url,
        Some(&set),
        "null://",
        &keyed_opts(),
        &crate::ctx::Ctx::default(),
    );
    // The scan reads the UDF anchor (LBA 256) past this 16-sector image's end.
    let err = rescanned.expect_err("a URL source scans the image, which has no filesystem");
    assert_eq!(
        crate::error::error_code(&err),
        Some(crate::error::E_IMAGE_ENDS_BEFORE_READ),
        "got {err}"
    );
}

/// KU §3.1 (review item 3): "`keys` must be `Some` for AACS". With no AACS set (none, or a
/// non-AACS set) and not raw, `Iso` and `Live` refuse E7022 at the first AACS-flagged
/// unit rather than muxing ciphertext; raw passes it through, and clear content muxes.
#[test]
fn mux_with_keys_without_an_aacs_set_refuses_aacs_content() {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let key = [0x5A; 16];
    let none = crate::keys::KeyRing::none();
    let live = |keys: Option<&crate::keys::KeyRing>, raw: bool, unit: Vec<u8>| {
        let (_, title, _) = keyed_live(key);
        let reader = Box::new(AacsUnitReader { unit, capacity: 16 });
        let opts = MuxOptions {
            raw,
            ..keyed_opts()
        };
        mux_with_keys(
            Source::from_reader(
                reader,
                ScannedTitle::new(title, crate::disc::ContentFormat::BdTs),
            ),
            keys,
            "null://",
            &opts,
            &crate::ctx::Ctx::default(),
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
        Source::from_image(
            &path,
            ScannedTitle::new(title, crate::disc::ContentFormat::BdTs),
        ),
        None,
        "null://",
        &keyed_opts(),
        &crate::ctx::Ctx::default(),
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
            Source::from_session(&mut session),
            None,
            "null://",
            &keyed_opts(),
            &crate::ctx::Ctx::default(),
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
    crate::keys::KeyRing,
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
    let set = crate::keys::KeyRing::keyed_for_test(&disc, key, &[(0, sectors)]);
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
    set: &crate::keys::KeyRing,
    dest: &str,
) -> std::io::Result<MuxOutcome> {
    mux_with_keys(
        Source::from_image(
            path,
            ScannedTitle::new(title, crate::disc::ContentFormat::BdTs),
        ),
        Some(set),
        dest,
        &MuxOptions {
            batch_sectors: 64,
            ..keyed_opts()
        },
        &crate::ctx::Ctx::default(),
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
    let (dir, path, title, set) = damaged_uhd_image(key, 9, |im| im[3 * 6144..6 * 6144].fill(0));
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
    let (dir, path, title, set) = damaged_uhd_image(key, 9, |im| im[7 * 2048..16 * 2048].fill(0));
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
        Source::from_image(
            &path,
            ScannedTitle::new(title.clone(), crate::disc::ContentFormat::BdTs),
        ),
        Some(&set),
        "null://",
        &opts,
        &crate::ctx::Ctx::default(),
    );
    let live = mux_with_keys(
        Source::from_reader(
            Box::new(ImageReader(std::fs::read(&path).unwrap())),
            ScannedTitle::new(title, crate::disc::ContentFormat::BdTs),
        ),
        Some(&set),
        "null://",
        &opts,
        &crate::ctx::Ctx::default(),
    );
    [iso, live]
}

/// An extent that starts inside a cluster of damaged units (its first reads show no intact
/// unit) is holes, not E7013: the grid verdict waits for the extent's intact units. KS-4
/// [BD] §3.10.1: "The first 16 bytes of each Aligned Unit is used as the seed".
#[test]
fn an_extent_starting_inside_a_damage_cluster_muxes_through() {
    let [iso, live] = mux_both(9, &[(0, 12), (12, 15)], 6, |im| {
        (3..6).for_each(|u| crate::test_util::damage_unit_seed(&mut im[u * 6144..(u + 1) * 6144]));
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
    const RUNS: [(u32, u32); 5] = [(96, 260), (928, 611), (8899, 352), (9280, 192), (9633, 546)];
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

// Clear BD-TS audio: `units` aligned units, each 32 one-packet audio PES on PID 0x1100.
fn clear_audio_image(units: usize) -> Vec<u8> {
    let pkt = bdts_data_packet(0x1100, true, &audio_pes(&PLAIN_ES));
    pkt.repeat(units * 32)
}

// Serves an in-memory image; any read covering `bad` fails as a disc read error.
struct BadSectorImage {
    data: Vec<u8>,
    bad: Option<u32>,
}
impl SectorSource for BadSectorImage {
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        _recovery: bool,
    ) -> crate::error::Result<usize> {
        if self
            .bad
            .is_some_and(|b| (lba..lba + count as u32).contains(&b))
        {
            return Err(Error::DiscRead {
                sector: lba as u64,
                status: Some(0x02),
                sense: None,
            });
        }
        let bytes = count as usize * 2048;
        let start = lba as usize * 2048;
        for (i, b) in buf[..bytes].iter_mut().enumerate() {
            *b = self.data.get(start + i).copied().unwrap_or(0);
        }
        Ok(bytes)
    }
    fn capacity_sectors(&self) -> u32 {
        (self.data.len() / 2048) as u32
    }
}

// A two-audio-stream (0x1100, 0x1101) title over `units` clear units.
fn clear_title(units: usize) -> DiscTitle {
    let mut title = aac_audio_title(0x1100);
    title.streams.extend(aac_audio_title(0x1101).streams);
    title.extents = vec![crate::disc::Extent {
        start_lba: 0,
        sector_count: units as u32 * 3,
    }];
    title
}

fn clear_live(units: usize, bad: Option<u32>) -> Source<'static> {
    Source::from_reader(
        Box::new(BadSectorImage {
            data: clear_audio_image(units),
            bad,
        }),
        ScannedTitle::new(clear_title(units), crate::disc::ContentFormat::BdTs),
    )
}

// `raw` routes a clear Live source through mux_unkeyed; otherwise mux_keyed runs it
// over the keyless set.
fn clear_opts(raw: bool) -> MuxOptions {
    MuxOptions {
        raw,
        ..keyed_opts()
    }
}

// Stop pressed while the sink is wedged in a write: the join gives up after the
// grace and the run is reported incomplete (finalize_failed), not a hard error.
#[test]
fn a_wedged_sink_at_stop_forces_an_incomplete_outcome() {
    struct WedgedFinish {
        info: DiscTitle,
        halt: Halt,
    }
    impl crate::pes::PesSource for WedgedFinish {
        fn read(&mut self) -> std::io::Result<Option<PesFrame>> {
            Ok(None)
        }

        fn info(&self) -> &DiscTitle {
            &self.info
        }
    }

    impl crate::pes::PesSink for WedgedFinish {
        fn write(&mut self, _frame: &PesFrame) -> std::io::Result<()> {
            self.halt.cancel();
            std::thread::sleep(Duration::from_secs(9));
            Ok(())
        }

        fn finish(&mut self) -> std::io::Result<()> {
            Ok(())
        }

        fn info(&self) -> &DiscTitle {
            &self.info
        }
    }
    let fs = FakeStream::new(1).with_frames(5);
    let halt = Halt::new();
    let sink = WedgedFinish {
        info: fs.info.clone(),
        halt: halt.clone(),
    };
    TEST_SINK.with(|s| *s.borrow_mut() = Some(Box::new(sink)));
    let t = std::time::Instant::now();
    let out = drive_mux(
        Box::new(fs),
        "null://",
        &crate::ctx::Ctx::new(halt.clone()),
        None,
        None,
    )
    .expect("a wedged finalise is an incomplete run, not an error");
    TEST_SINK.with(|s| s.borrow_mut().take());
    assert!(!out.completed && out.output_opened);
    assert!(t.elapsed() < Duration::from_secs(8), "bounded by the grace");
}

// skip_errors reaches the live Read stage on every arm that builds one.
#[test]
fn skip_errors_reaches_every_live_arm() {
    // A live mux spawns the prefetch producer, a Drive holder.
    let _serial = crate::sector::prefetched::holder_test_lock();
    let session = |bad| {
        let mut disc = aacs_session_disc(clear_title(2));
        disc.aacs = None;
        disc.encrypted = false;
        let reader = BadSectorImage {
            data: clear_audio_image(2),
            bad,
        };
        DiscSession::from_parts_for_test(Some(disc), Some(Box::new(reader)))
    };
    for arm in ["live-unkeyed", "live-keyed", "session"] {
        for skip in [true, false] {
            let mut s = session(Some(4));
            let (src, raw) = match arm {
                "live-unkeyed" => (clear_live(2, Some(4)), true),
                "live-keyed" => (clear_live(2, Some(4)), false),
                _ => (Source::from_session(&mut s), false),
            };
            let opts = MuxOptions {
                skip_errors: skip,
                ..clear_opts(raw)
            };
            let res = mux_with_keys(src, None, "null://", &opts, &crate::ctx::Ctx::default());
            match skip {
                true => {
                    let out = res.expect("the bad sector is skipped");
                    assert!(out.completed && out.errors > 0, "{arm}");
                }
                false => assert_eq!(
                    crate::error::error_code(&res.expect_err("bad sector")),
                    Some(crate::error::E_DISC_READ),
                    "{arm}"
                ),
            }
        }
    }
}

// The selection prunes the opened title on every MuxSource arm.
#[test]
fn the_selection_prunes_every_arm() {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let dir = tempfile::tempdir().unwrap();
    let iso = dir.path().join("clip.iso");
    std::fs::write(&iso, clear_audio_image(2)).unwrap();
    let mut disc = aacs_session_disc(clear_title(2));
    disc.aacs = None;
    disc.encrypted = false;
    let mut session = DiscSession::from_parts_for_test(
        Some(disc),
        Some(Box::new(BadSectorImage {
            data: clear_audio_image(2),
            bad: None,
        })),
    );
    let sel = crate::StreamSelection {
        audio: crate::mux::select::PidFilter::Only(vec![0x1100]),
        ..Default::default()
    };
    for arm in [
        "live-unkeyed",
        "live-keyed",
        "iso-unkeyed",
        "iso-keyed",
        "session",
    ] {
        let (src, raw) = match arm {
            "live-unkeyed" => (clear_live(2, None), true),
            "live-keyed" => (clear_live(2, None), false),
            "session" => (Source::from_session(&mut session), false),
            _ => (
                Source::from_image(
                    &iso,
                    ScannedTitle::new(clear_title(2), crate::disc::ContentFormat::BdTs),
                ),
                arm == "iso-unkeyed",
            ),
        };
        let opts = MuxOptions {
            selection: sel.clone(),
            ..clear_opts(raw)
        };
        let spy = Arc::new(TitleSpy(std::sync::Mutex::new(None)));
        mux_with_keys(
            src,
            None,
            "null://",
            &opts,
            &crate::ctx::Ctx::default().with_events(spy.clone()),
        )
        .unwrap_or_else(|e| panic!("{arm}: {e}"));
        let opened = spy.0.lock().unwrap().take().expect("opened");
        assert_eq!(opened.streams.len(), 1, "{arm}");
    }
}
