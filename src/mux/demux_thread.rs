//! `DemuxThread` — runs the read+decrypt+demux pipeline on a
//! dedicated thread, feeding completed `PesPacket` batches to the
//! caller via a bounded channel. Splits `ts_demuxer.feed` off the
//! consumer thread so feed and codec parse pipeline instead of serialise.
//!
//! [`DemuxThread::spawn_zero_copy`] returns a handle plus a
//! `Receiver<DemuxBatch>`. `Drop::drop` joins the worker, which exits only once
//! that receiver is gone: drop the receiver FIRST (an owner declares it before
//! the handle), or a worker blocked on a full channel deadlocks the join.

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
    /// LAST message when the input is exhausted and no Stop is pending (a Stop
    /// is `Err(Halted)`, never EOF) so the consumer can distinguish a normal end-of-stream
    /// from a bare channel disconnection. A worker that panics mid-stream
    /// drops `tx` without sending this, so the consumer sees `RecvError`
    /// and reports the panic rather than silently truncating output.
    Eof,
}

fn halted(halt: Option<&Halt>) -> bool {
    halt.is_some_and(Halt::is_cancelled)
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
    /// Spawn the demux thread. Consumes prefetch channels directly:
    /// filled buffers arrive via `prefetch_rx`, get fed, then are
    /// returned to `recycle_tx` for the producer to re-fill.
    ///
    /// `producer_shell` is an opaque handle that outlives the demux thread and joins the
    /// upstream producer on drop: the shell from
    /// [`crate::sector::PrefetchedSectorSource::into_channels`].
    pub fn spawn_zero_copy<S: Send + 'static>(
        prefetch_rx: Receiver<std::io::Result<Vec<u8>>>,
        recycle_tx: Sender<Vec<u8>>,
        producer_shell: S,
        ctx: &crate::ctx::Ctx,
        ts: Option<super::ts::TsDemuxer>,
        ps: Option<super::ps::PsDemuxer>,
    ) -> crate::error::Result<(Self, Receiver<DemuxBatch>)> {
        let (tx, rx) = bounded::<DemuxBatch>(DEMUX_CHANNEL_DEPTH);
        let halt = Some(ctx.halt.clone());
        let prof = ctx.diag.profile;
        let mut ts = ts;
        let mut ps = ps;

        // SAFETY: on `spawn` failure the dropped `move` closure drops `prefetch_rx`/
        // `recycle_tx`, so the producer disconnects and exits before `producer_shell`
        // (uncaptured, joins on Drop) is dropped on the Err path — non-blocking.
        let spawn_result = std::thread::Builder::new()
            .name("freemkv-demux".into())
            .spawn(move || {
                let mut prof_started = std::time::Instant::now();
                let mut prof_last_dump = prof_started;
                let mut prof_read_ns: u128 = 0;
                let mut prof_feed_ns: u128 = 0;
                let mut prof_bytes: u64 = 0;
                // Liveness heartbeat: a stuck upstream/downstream shows up as the beat
                // going silent. Total is unknown, so `pos` is cumulative bytes fed.
                let mut hb = crate::progress::Heartbeat::new("demux_feed");
                let mut fed_bytes: u64 = 0;
                loop {
                    hb.tick(fed_bytes, 0);
                    // A Stop is `Halted`, never a clean EOF (LP11): a truncated
                    // title must not finalise as a complete container.
                    if halted(halt.as_ref()) {
                        let _ = tx.send(DemuxBatch::Err(crate::error::Error::Halted.into()));
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
                    // Source byte offset of this buffer's first byte, threaded into the
                    // demuxer so every PES it cuts carries its SourcePos.
                    let buf_base = fed_bytes;
                    fed_bytes += n as u64;
                    if let Some(ref mut d) = ts {
                        let pkts = d.feed_at(buf_base, &buf);
                        let t2 = if prof {
                            Some(std::time::Instant::now())
                        } else {
                            None
                        };
                        // Recycle before pushing the packets. A closed recycle channel
                        // means the producer exited; drop the buffer and continue.
                        let _ = recycle_tx.send(buf);
                        // Always send, even empty batches: send() is how an early
                        // consumer disconnect is detected, and skipping empty sends on
                        // mostly-null extents could hide it for a long time.
                        if tx.send(DemuxBatch::Ts(pkts)).is_err() {
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
                                tracing::debug!(
                                    target: "mux",
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
                        let pkts = d.feed_at(buf_base, &buf);
                        let _ = recycle_tx.send(buf);
                        // Always send (even empty) — same early-disconnect
                        // detection rationale as the TS branch above.
                        if tx.send(DemuxBatch::Ps(pkts)).is_err() {
                            return;
                        }
                    } else {
                        let _ = recycle_tx.send(buf);
                        // No demuxer (zero-stream title): still send an empty batch to
                        // detect an early consumer disconnect, or this worker reads
                        // the whole disc even after the consumer has dropped.
                        if tx.send(DemuxBatch::Ts(Vec::new())).is_err() {
                            return;
                        }
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
                // Clean EOF sentinel. A panic during `feed`/`flush` skips this and
                // drops `tx`, which the consumer reads as an error, not clean EOF. A
                // halted prefetcher closes its channel too: that close is a Stop.
                let last = if halted(halt.as_ref()) {
                    DemuxBatch::Err(crate::error::Error::Halted.into())
                } else {
                    DemuxBatch::Eof
                };
                let _ = tx.send(last);
            });

        let handle = match spawn_result {
            Ok(h) => h,
            Err(e) => {
                // `prefetch_rx`/`recycle_tx` were moved into the (now-dropped)
                // failed spawn closure, so the producer already sees disconnection.
                // Dropping producer_shell here joins that already-exiting producer.
                drop(producer_shell);
                return Err(crate::error::Error::IoError { source: e });
            }
        };

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

#[cfg(test)]
#[path = "demux_thread_tests.rs"]
mod tests;
