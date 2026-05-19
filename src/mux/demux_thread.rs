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
//! [`DemuxThread::spawn`] takes ownership of the inner reader and the
//! demuxer state, returns a handle plus a `Receiver<DemuxBatch>`.
//! Dropping the handle closes the channel which signals the thread
//! to exit; the join in `Drop::drop` is bounded.

use crate::halt::Halt;
use crate::sector::SectorSource;
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
    /// Spawn the demux thread. Returns the thread handle and a
    /// receiver for [`DemuxBatch`] items.
    ///
    /// `reader` is the fully-composed read+decrypt stack (e.g.
    /// [`PrefetchedSectorSource`](crate::sector::PrefetchedSectorSource)
    /// wrapping
    /// [`DecryptingSectorSource`](crate::sector::DecryptingSectorSource)).
    /// `extents` is what the thread walks; it issues one
    /// `read_sectors` per batch of `batch_sectors` sectors (aligned
    /// to 3-sector AACS units when possible).
    pub fn spawn<S: SectorSource + Send + 'static>(
        mut reader: S,
        extents: Vec<crate::disc::Extent>,
        batch_sectors: u16,
        halt: Option<Halt>,
        ts: Option<super::ts::TsDemuxer>,
        ps: Option<super::ps::PsDemuxer>,
    ) -> (Self, Receiver<DemuxBatch>) {
        let (tx, rx) = bounded::<DemuxBatch>(DEMUX_CHANNEL_DEPTH);
        let mut ts = ts;
        let mut ps = ps;

        let handle = std::thread::Builder::new()
            .name("freemkv-demux".into())
            .spawn(move || {
                let mut buf = vec![0u8; batch_sectors as usize * 2048];
                let mut ext_idx = 0usize;
                let mut offset: u32 = 0;
                let prof = std::env::var_os("FREEMKV_PROFILE").is_some();
                let mut prof_started = std::time::Instant::now();
                let mut prof_last_dump = prof_started;
                let mut prof_read_ns: u128 = 0;
                let mut prof_feed_ns: u128 = 0;
                let mut prof_send_ns: u128 = 0;
                let mut prof_bytes: u64 = 0;
                while ext_idx < extents.len() {
                    if halt.as_ref().map(|h| h.is_cancelled()).unwrap_or(false) {
                        return;
                    }
                    let ext = &extents[ext_idx];
                    let remaining = ext.sector_count.saturating_sub(offset);
                    if remaining == 0 {
                        ext_idx += 1;
                        offset = 0;
                        continue;
                    }
                    let mut sectors = remaining.min(batch_sectors as u32) as u16;
                    if sectors >= 3 {
                        sectors -= sectors % 3;
                    }
                    let bytes = sectors as usize * 2048;
                    if buf.len() < bytes {
                        buf.resize(bytes, 0);
                    }
                    let lba = ext.start_lba + offset;
                    let t0 = if prof {
                        Some(std::time::Instant::now())
                    } else {
                        None
                    };
                    let n = match reader.read_sectors(lba, sectors, &mut buf[..bytes], false) {
                        Ok(n) => n,
                        Err(e) => {
                            let _ = tx.send(DemuxBatch::Err(e.into()));
                            return;
                        }
                    };
                    let t1 = if prof {
                        Some(std::time::Instant::now())
                    } else {
                        None
                    };
                    offset += sectors as u32;

                    // Demux this batch immediately so the channel
                    // carries already-parsed PesPackets, not raw
                    // sector bytes.
                    if let Some(ref mut d) = ts {
                        let pkts = d.feed(&buf[..n]);
                        let t2 = if prof {
                            Some(std::time::Instant::now())
                        } else {
                            None
                        };
                        if !pkts.is_empty() && tx.send(DemuxBatch::Ts(pkts)).is_err() {
                            return; // consumer dropped
                        }
                        let t3 = if prof {
                            Some(std::time::Instant::now())
                        } else {
                            None
                        };
                        if prof {
                            prof_read_ns += t1.unwrap().duration_since(t0.unwrap()).as_nanos();
                            prof_feed_ns += t2.unwrap().duration_since(t1.unwrap()).as_nanos();
                            prof_send_ns += t3.unwrap().duration_since(t2.unwrap()).as_nanos();
                            prof_bytes += n as u64;
                            let now = t3.unwrap();
                            if now.duration_since(prof_last_dump)
                                >= std::time::Duration::from_secs(5)
                            {
                                let el = now.duration_since(prof_started).as_millis().max(1);
                                let mbps = prof_bytes as u128 * 1000 / 1_000_000 / el;
                                eprintln!(
                                    "[demux] elapsed={}ms in={}MB/s read={}% feed={}% send={}%",
                                    el,
                                    mbps,
                                    prof_read_ns / 10_000 / el,
                                    prof_feed_ns / 10_000 / el,
                                    prof_send_ns / 10_000 / el,
                                );
                                prof_last_dump = now;
                                prof_started = now;
                                prof_read_ns = 0;
                                prof_feed_ns = 0;
                                prof_send_ns = 0;
                                prof_bytes = 0;
                            }
                        }
                    } else if let Some(ref mut d) = ps {
                        let pkts = d.feed(&buf[..n]);
                        if !pkts.is_empty() && tx.send(DemuxBatch::Ps(pkts)).is_err() {
                            return;
                        }
                    }
                }
                // EOF — emit any flushed packets too.
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
                // Sender drops here -> consumer sees RecvError → EOF.
            })
            .expect("freemkv-demux thread spawn failed");

        (
            Self {
                handle: Some(handle),
                producer_shell: None,
            },
            rx,
        )
    }

    /// Zero-copy variant. Instead of taking a `SectorSource` and
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
    ) -> (Self, Receiver<DemuxBatch>) {
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
            })
            .expect("freemkv-demux thread spawn failed");

        (
            Self {
                handle: Some(handle),
                producer_shell: Some(Box::new(producer_shell)),
            },
            rx,
        )
    }
}

impl Drop for DemuxThread {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}
