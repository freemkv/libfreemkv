use super::*;
use std::time::Duration;

struct Instant0;
impl FlushOps for Instant0 {
    fn chunk(&self, _: &File) -> io::Result<()> {
        Ok(())
    }
    fn range(&self, _: &File, _: u64, _: u64) -> Option<io::Result<()>> {
        None
    }
    fn finish(&self, _: &File) -> io::Result<()> {
        Ok(())
    }
    fn sample(&self) -> Option<u64> {
        None
    }
}

// Bytes past `requested` (left over after the chunk shrank) must be handed to the worker
// by a blocked writer, not waited on until the stall latches `SyncTimeout`.
#[test]
fn wait_room_requests_unrequested_bytes() {
    let file = tempfile::tempfile().unwrap();
    let timing = FlushTiming {
        stall: Duration::from_secs(5),
        slow_chunk: Duration::from_secs(10),
        chunk_min: 100,
        chunk_max: 100,
        sample_every: Duration::from_millis(50),
    };
    let f = Flusher::spawn(
        &file,
        Arc::new(Instant0),
        timing,
        FlushProgress::default(),
        0,
    )
    .unwrap();
    f.wait_room(10_000, None).unwrap();
    assert!(f.error().is_none());
}

// Unrequested bytes appearing after the writer blocked (the chunk shrank mid-wait) must
// still be handed over; a one-shot check before the wait idles the worker to a stall.
#[test]
fn wait_room_requests_bytes_unrequested_while_blocked() {
    let file = tempfile::tempfile().unwrap();
    let timing = FlushTiming {
        stall: Duration::from_secs(5),
        slow_chunk: Duration::from_secs(10),
        chunk_min: 100,
        chunk_max: 100,
        sample_every: Duration::from_millis(50),
    };
    let f = Arc::new(
        Flusher::spawn(
            &file,
            Arc::new(Instant0),
            timing,
            FlushProgress::default(),
            0,
        )
        .unwrap(),
    );
    // Once the worker idles: requested but not notified, so the writer blocks and hands
    // nothing over; the bytes then become unrequested while it waits.
    std::thread::sleep(Duration::from_millis(50));
    f.lock().requested = 10_000;
    let writer = {
        let f = f.clone();
        std::thread::spawn(move || f.wait_room(10_000, None))
    };
    std::thread::sleep(Duration::from_millis(50));
    f.lock().requested = 0;
    writer.join().unwrap().unwrap();
    assert!(f.error().is_none());
}

struct FailingChunk;
impl FlushOps for FailingChunk {
    fn chunk(&self, _: &File) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(28))
    }
    fn range(&self, _: &File, _: u64, _: u64) -> Option<io::Result<()>> {
        None
    }
    fn finish(&self, _: &File) -> io::Result<()> {
        Ok(())
    }
    fn sample(&self) -> Option<u64> {
        None
    }
}

// A failed chunk flush latches its own errno: the waiter gets the real error at once,
// not `SyncTimeout` after the stall window, and it stays latched.
#[test]
fn a_chunk_error_is_latched_and_reported_as_itself() {
    let file = tempfile::tempfile().unwrap();
    let timing = FlushTiming {
        stall: Duration::from_secs(5),
        slow_chunk: Duration::from_secs(10),
        chunk_min: 100,
        chunk_max: 100,
        sample_every: Duration::from_millis(50),
    };
    let f = Flusher::spawn(
        &file,
        Arc::new(FailingChunk),
        timing,
        FlushProgress::default(),
        0,
    )
    .unwrap();
    let start = Instant::now();
    let e = f.drain(100, None, None).expect_err("the chunk failed");
    assert_eq!(e.raw_os_error(), Some(28), "{e}");
    assert!(
        start.elapsed() < Duration::from_millis(2500),
        "waited a stall"
    );
    assert_eq!(f.error().and_then(|e| e.raw_os_error()), Some(28));
}
