use super::{InputOptions, PipelinedPesStream, image_input_scanned};

// `open_source`'s image probe, then the image stream, over a test reader.
fn image_input<S>(
    mut reader: S,
    opts: &InputOptions,
    folder: bool,
    ctx: &crate::ctx::Ctx,
) -> std::io::Result<PipelinedPesStream>
where
    S: SectorSource + Send + 'static,
{
    let cap = reader.capacity_sectors();
    let scan = crate::disc::ScanOptions {
        halt: Some(ctx.halt.clone()),
        ..Default::default()
    };
    let mut disc = crate::disc::Disc::scan_image(&mut reader, cap, &scan)?;
    if folder {
        crate::session::apply_folder_encryption_verdict(&mut reader, &mut disc)?;
    }
    image_input_scanned(reader, disc, opts, ctx)
}
use crate::pes::PesSource as _;
use crate::sector::SectorSource;
use std::sync::mpsc;
use std::time::Duration;

// A folder image whose prefetch producer stalls in its first read until released: the
// shape of a drive grinding on a bad sector under an `iso://`/`dir://` mux.
struct StallsInPrefetch {
    inner: crate::DirImage,
    entered: mpsc::Sender<()>,
    release: std::sync::Mutex<mpsc::Receiver<()>>,
}

impl StallsInPrefetch {
    fn new(dir: &std::path::Path) -> (Self, mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (entered, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let s = Self {
            inner: crate::DirImage::open(dir).unwrap(),
            entered,
            release: std::sync::Mutex::new(release_rx),
        };
        (s, entered_rx, release_tx)
    }
}

impl SectorSource for StallsInPrefetch {
    fn capacity_sectors(&self) -> u32 {
        self.inner.capacity_sectors()
    }
    fn read_sectors(
        &mut self,
        lba: u32,
        count: u16,
        buf: &mut [u8],
        recovery: bool,
    ) -> crate::error::Result<usize> {
        if std::thread::current().name() == Some("freemkv-prefetch") {
            let _ = self.entered.send(());
            // Blocks until the test drops the sender, then fails the read.
            let _ = self.release.lock().unwrap().recv();
            return Err(crate::error::Error::SourceTerminated);
        }
        self.inner.read_sectors(lba, count, buf, recovery)
    }
}

// BUG-4: Stop reaches an image pipeline blocked inside a read; the read ends Halted
// within a wait slice instead of waiting on the stalled source.
#[test]
fn stop_reaches_a_dir_pipeline_blocked_in_a_read() {
    let _serial = crate::sector::prefetched::holder_test_lock();
    let s = crate::dirimage::tests::playable_bdmv("stopblocked", false);
    let (src, entered_rx, release_tx) = StallsInPrefetch::new(s.path());
    let ctx = crate::ctx::Ctx::default();
    let mut stream =
        image_input(src, &InputOptions::default(), true, &ctx).expect("the folder opens");
    entered_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the producer reached its read");
    let (done_tx, done_rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let r = stream.read().map(|f| f.is_some());
        let _ = done_tx.send(());
        (r, stream)
    });
    ctx.halt.cancel();
    let stopped = done_rx.recv_timeout(Duration::from_secs(5)).is_ok();
    drop(release_tx);
    let (r, stream) = reader.join().unwrap();
    drop(stream);
    assert!(
        stopped,
        "a Stop must end the blocked read without the source"
    );
    assert!(
        crate::error::is_halt(&r.expect_err("a stopped read is not a frame")),
        "the blocked read ends Halted"
    );
}
