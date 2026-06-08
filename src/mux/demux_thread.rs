//! `DemuxThread` — runs the read+decrypt+demux pipeline on a
//! dedicated thread, feeding completed `PesPacket` batches to the
//! caller via a bounded channel.
//!
//! ## Why a second worker thread
//!
//! With [`crate::sector::PrefetchedSectorSource`] alone, read+decrypt
//! already runs on a producer thread; the *consumer* (main) thread
//! still serialises `ts_demuxer.feed` (M2TS parsing) with the codec
//! parsers. Profiling on the rip1 testbed showed feed at ~37 % and
//! codec parse at ~44 % of consumer wall time — i.e. feed is heavy
//! enough that pipelining it with parse pays for itself.
//!
//! Splitting them: feed runs in [`DemuxThread`]; the consumer thread
//! receives `Vec<PesPacket>` batches and runs codec parse + frame
//! emission only. Total throughput becomes `1/max(feed, parse)`
//! instead of `1/(feed + parse)`.
//!
//! ## Lifecycle
//!
//! [`DemuxThread::spawn_zero_copy`] consumes the prefetch channels and
//! the demuxer state, returning a handle plus a `Receiver<DemuxBatch>`.
//! Dropping the handle closes the channel which signals the worker to
//! exit; the join in `Drop::drop` blocks until the worker observes
//! channel closure and returns (no timeout — a wedged downstream would
//! block the drop until it releases the channel).

use crate::halt::Halt;
use crossbeam_channel::{Receiver, Sender, bounded};
use std::thread::JoinHandle;

/// Output channel depth. Two batches in flight keeps the consumer
/// (codec parser) busy without piling up demuxed bytes if it stalls.
const DEMUX_CHANNEL_DEPTH: usize = 2;

/// One demuxed batch flowing from the demux thread to the consumer.
pub enum DemuxBatch {
    /// Successfully demuxed PesPackets — non-empty.
    Ts(Vec<super::ts::PesPacket>),
    Ps(Vec<super::ps::PsPacket>),
    /// Underlying reader returned an error. Terminal.
    Err(std::io::Error),
    /// Explicit clean-completion sentinel. The worker sends this as its
    /// LAST message on every non-error exit (input exhausted, or halt
    /// cancelled) so the consumer can distinguish a normal end-of-stream
    /// from a bare channel disconnection. A worker that panics mid-stream
    /// drops `tx` without sending this, so the consumer sees `RecvError`
    /// and reports the panic rather than silently truncating output.
    Eof,
}

/// Spawned demux thread. Drop joins.
///
/// In zero-copy mode the thread also owns an opaque
/// `producer_shell: Option<Box<dyn Send>>` — the join handle of the
/// upstream producer (sector or byte prefetcher). Dropping the
/// `DemuxThread` runs the shell's `Drop`, which joins the producer.
/// `Box<dyn Send>` rather than a concrete type so the same demux
/// worker can be wired behind either prefetcher kind.
pub struct DemuxThread {
    handle: Option<JoinHandle<()>>,
    #[allow(dead_code)]
    producer_shell: Option<Box<dyn Send>>,
}

impl DemuxThread {
    /// Spawn the demux thread. Instead of taking a `SectorSource` and
    /// memcpy-ing through its `read_sectors` API, this constructor
    /// consumes the prefetch channels directly: filled buffers come
    /// in via `prefetch_rx`, the demux thread feeds them, then
    /// returns them to `recycle_tx` for the producer to re-fill.
    /// Eliminates the 16 MiB memcpy per batch that the SectorSource
    /// adapter incurred (and, with the producer-side recycling pool,
    /// also eliminates the per-batch heap alloc / cross-thread free
    /// that was costing 40 %+ of demux-thread time before).
    ///
    /// `producer_shell` is an opaque handle whose only purpose is to
    /// outlive the demux thread and join the upstream producer when
    /// dropped. Both
    /// [`crate::sector::PrefetchedSectorSource::into_channels`] and
    /// [`crate::io::byte_prefetcher::BytePrefetcher::into_channels`]
    /// hand back a shell that fits — pass either.
    pub fn spawn_zero_copy<S: Send + 'static>(
        prefetch_rx: Receiver<std::io::Result<Vec<u8>>>,
        recycle_tx: Sender<Vec<u8>>,
        producer_shell: S,
        halt: Option<Halt>,
        ts: Option<super::ts::TsDemuxer>,
        ps: Option<super::ps::PsDemuxer>,
    ) -> crate::error::Result<(Self, Receiver<DemuxBatch>)> {
        let (tx, rx) = bounded::<DemuxBatch>(DEMUX_CHANNEL_DEPTH);
        let mut ts = ts;
        let mut ps = ps;

        let handle = std::thread::Builder::new()
            .name("freemkv-demux".into())
            .spawn(move || {
                let prof = std::env::var_os("FREEMKV_PROFILE").is_some();
                let mut prof_started = std::time::Instant::now();
                let mut prof_last_dump = prof_started;
                let mut prof_read_ns: u128 = 0;
                let mut prof_feed_ns: u128 = 0;
                let mut prof_bytes: u64 = 0;
                loop {
                    if halt.as_ref().map(|h| h.is_cancelled()).unwrap_or(false) {
                        // Caller-initiated stop is a clean termination —
                        // send the Eof sentinel so the consumer doesn't
                        // mistake it for a worker panic.
                        let _ = tx.send(DemuxBatch::Eof);
                        return;
                    }
                    let t0 = if prof {
                        Some(std::time::Instant::now())
                    } else {
                        None
                    };
                    let buf = match prefetch_rx.recv() {
                        Ok(Ok(b)) => b,
                        Ok(Err(e)) => {
                            let _ = tx.send(DemuxBatch::Err(e));
                            return;
                        }
                        Err(_) => break, // producer done → EOF
                    };
                    let t1 = if prof {
                        Some(std::time::Instant::now())
                    } else {
                        None
                    };
                    let n = buf.len();
                    if let Some(ref mut d) = ts {
                        let pkts = d.feed(&buf);
                        let t2 = if prof {
                            Some(std::time::Instant::now())
                        } else {
                            None
                        };
                        // Recycle the buffer back to the producer
                        // before pushing the demuxed packets. If the
                        // recycle channel is closed the producer has
                        // exited; we drop the buffer and continue.
                        let _ = recycle_tx.send(buf);
                        if !pkts.is_empty() && tx.send(DemuxBatch::Ts(pkts)).is_err() {
                            return;
                        }
                        if prof {
                            prof_read_ns += t1.unwrap().duration_since(t0.unwrap()).as_nanos();
                            prof_feed_ns += t2.unwrap().duration_since(t1.unwrap()).as_nanos();
                            prof_bytes += n as u64;
                            let now = std::time::Instant::now();
                            if now.duration_since(prof_last_dump)
                                >= std::time::Duration::from_secs(5)
                            {
                                let el = now.duration_since(prof_started).as_millis().max(1);
                                let mbps = prof_bytes as u128 * 1000 / 1_000_000 / el;
                                eprintln!(
                                    "[demux] elapsed={}ms in={}MB/s read={}% feed={}%",
                                    el,
                                    mbps,
                                    prof_read_ns / 10_000 / el,
                                    prof_feed_ns / 10_000 / el,
                                );
                                prof_last_dump = now;
                                prof_started = now;
                                prof_read_ns = 0;
                                prof_feed_ns = 0;
                                prof_bytes = 0;
                            }
                        }
                    } else if let Some(ref mut d) = ps {
                        let pkts = d.feed(&buf);
                        let _ = recycle_tx.send(buf);
                        if !pkts.is_empty() && tx.send(DemuxBatch::Ps(pkts)).is_err() {
                            return;
                        }
                    } else {
                        let _ = recycle_tx.send(buf);
                    }
                }
                // Flush tail packets at EOF.
                if let Some(ref mut d) = ts {
                    let tail = d.flush();
                    if !tail.is_empty() {
                        let _ = tx.send(DemuxBatch::Ts(tail));
                    }
                } else if let Some(ref mut d) = ps {
                    let tail = d.flush();
                    if !tail.is_empty() {
                        let _ = tx.send(DemuxBatch::Ps(tail));
                    }
                }
                // Clean end-of-stream sentinel. Reaching here means no
                // panic occurred; a panic during `feed`/`flush` skips
                // this and drops `tx`, which the consumer reads as an
                // error rather than a clean EOF.
                let _ = tx.send(DemuxBatch::Eof);
            })
            .map_err(|e| crate::error::Error::IoError { source: e })?;

        Ok((
            Self {
                handle: Some(handle),
                producer_shell: Some(Box::new(producer_shell)),
            },
            rx,
        ))
    }
}

impl Drop for DemuxThread {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}
