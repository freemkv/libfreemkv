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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::halt::Halt;
    use crossbeam_channel::bounded;
    use std::time::Duration;

    /// Build one 192-byte BD-TS packet on `pid` carrying a complete PES with
    /// a `00 00 01 E0` start, hdr_len 0, then `payload` as ES. The TS payload
    /// region after the PES header is padded with a stuffing adaptation field
    /// so `payload` is the exact ES (no zero padding the unbounded PES would
    /// absorb). ISO 13818-1 packet layout: sync 0x47 at TS offset 0 (BD off 4).
    fn bdts_pes_packet(pid: u16, payload: &[u8]) -> Vec<u8> {
        const SYNC: u8 = 0x47;
        const TS_PAYLOAD: usize = 184;
        let mut pes = vec![0x00, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        pes.extend_from_slice(payload);
        assert!(pes.len() <= TS_PAYLOAD);
        let mut pkt = vec![0u8; 192];
        pkt[4] = SYNC;
        pkt[5] = (((pid >> 8) as u8) & 0x1F) | 0x40; // PUSI
        pkt[6] = (pid & 0xFF) as u8;
        let pad = TS_PAYLOAD - pes.len();
        if pad == 0 {
            pkt[7] = 0x10; // payload only
            pkt[8..8 + pes.len()].copy_from_slice(&pes);
        } else {
            pkt[7] = 0x30; // AF + payload
            let af_field_len = pad - 1;
            pkt[8] = af_field_len as u8;
            if af_field_len >= 1 {
                pkt[9] = 0x00; // flags
                for b in pkt.iter_mut().skip(10).take(af_field_len - 1) {
                    *b = 0xFF;
                }
            }
            let off = 8 + pad;
            pkt[off..off + pes.len()].copy_from_slice(&pes);
        }
        pkt
    }

    /// Drain a receiver into a Vec, blocking up to `budget` total.
    fn collect_batches(rx: &Receiver<DemuxBatch>, budget: Duration) -> Vec<DemuxBatch> {
        let mut out = Vec::new();
        let deadline = std::time::Instant::now() + budget;
        loop {
            let now = std::time::Instant::now();
            if now >= deadline {
                break;
            }
            match rx.recv_timeout(deadline - now) {
                Ok(b) => {
                    let is_terminal = matches!(b, DemuxBatch::Eof | DemuxBatch::Err(_));
                    out.push(b);
                    if is_terminal {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        out
    }

    #[test]
    fn clean_eof_sentinel_sent_after_input_exhausted() {
        // The worker must send exactly one Eof as its LAST message on a
        // normal end-of-stream so the consumer can distinguish clean
        // completion from a panic (which drops tx without Eof).
        let (pf_tx, pf_rx) = bounded::<std::io::Result<Vec<u8>>>(4);
        let (rc_tx, _rc_rx) = bounded::<Vec<u8>>(4);
        let pid = 0x1011;
        let ts = super::super::ts::TsDemuxer::new(&[pid]);
        let (_dt, rx) =
            DemuxThread::spawn_zero_copy(pf_rx, rc_tx, (), None, Some(ts), None).unwrap();

        pf_tx.send(Ok(bdts_pes_packet(pid, &[0xDE, 0xAD]))).unwrap();
        drop(pf_tx); // producer done → EOF

        let batches = collect_batches(&rx, Duration::from_secs(5));
        // Last batch must be the Eof sentinel.
        assert!(
            matches!(batches.last(), Some(DemuxBatch::Eof)),
            "stream must terminate with the Eof sentinel"
        );
        // The PES bytes must surface before EOF (the demuxer holds the PES
        // until flush at EOF since there's no following PUSI).
        let saw_pes = batches.iter().any(|b| match b {
            DemuxBatch::Ts(p) => p.iter().any(|pes| pes.data == vec![0xDE, 0xAD]),
            _ => false,
        });
        assert!(saw_pes, "the demuxed PES must be delivered");
    }

    #[test]
    fn flush_tail_emitted_before_eof() {
        // A PES with no trailing PUSI is only completed by flush() at EOF.
        // The worker must flush after the producer disconnects, emitting the
        // tail PES BEFORE the Eof sentinel — never dropping the last frame.
        let (pf_tx, pf_rx) = bounded::<std::io::Result<Vec<u8>>>(4);
        let (rc_tx, _rc_rx) = bounded::<Vec<u8>>(4);
        let pid = 0x1011;
        let ts = super::super::ts::TsDemuxer::new(&[pid]);
        let (_dt, rx) =
            DemuxThread::spawn_zero_copy(pf_rx, rc_tx, (), None, Some(ts), None).unwrap();

        pf_tx
            .send(Ok(bdts_pes_packet(pid, &[0x11, 0x22, 0x33])))
            .unwrap();
        drop(pf_tx);

        let batches = collect_batches(&rx, Duration::from_secs(5));
        // Find the tail PES and the Eof; tail must precede Eof.
        let pes_idx = batches.iter().position(|b| {
            matches!(b, DemuxBatch::Ts(p) if p.iter().any(|x| x.data == vec![0x11, 0x22, 0x33]))
        });
        let eof_idx = batches.iter().position(|b| matches!(b, DemuxBatch::Eof));
        assert!(pes_idx.is_some(), "flushed tail PES delivered");
        assert!(eof_idx.is_some(), "Eof delivered");
        assert!(pes_idx.unwrap() < eof_idx.unwrap(), "tail before Eof");
    }

    #[test]
    fn halt_cancellation_sends_eof_not_panic() {
        // A caller-initiated halt is a CLEAN termination — the worker must
        // send Eof (not just drop tx), so the consumer doesn't mistake the
        // stop for a worker panic.
        let (pf_tx, pf_rx) = bounded::<std::io::Result<Vec<u8>>>(4);
        let (rc_tx, _rc_rx) = bounded::<Vec<u8>>(4);
        let halt = Halt::new();
        halt.cancel(); // already cancelled before the loop runs
        let ts = super::super::ts::TsDemuxer::new(&[0x1011]);
        let (_dt, rx) =
            DemuxThread::spawn_zero_copy(pf_rx, rc_tx, (), Some(halt), Some(ts), None).unwrap();
        // Keep pf_tx alive so the ONLY exit is the halt path, not producer
        // disconnect.
        let batches = collect_batches(&rx, Duration::from_secs(5));
        drop(pf_tx);
        assert!(
            matches!(batches.last(), Some(DemuxBatch::Eof)),
            "halt cancellation must yield a clean Eof sentinel"
        );
    }

    #[test]
    fn upstream_error_is_propagated_as_err_terminal() {
        // An error from the prefetch channel must be forwarded as a terminal
        // DemuxBatch::Err — the worker then returns (no Eof after an error).
        let (pf_tx, pf_rx) = bounded::<std::io::Result<Vec<u8>>>(4);
        let (rc_tx, _rc_rx) = bounded::<Vec<u8>>(4);
        let ts = super::super::ts::TsDemuxer::new(&[0x1011]);
        let (_dt, rx) =
            DemuxThread::spawn_zero_copy(pf_rx, rc_tx, (), None, Some(ts), None).unwrap();

        pf_tx
            .send(Err(std::io::Error::new(std::io::ErrorKind::Other, "boom")))
            .unwrap();
        drop(pf_tx);

        let batches = collect_batches(&rx, Duration::from_secs(5));
        assert!(
            matches!(batches.last(), Some(DemuxBatch::Err(_))),
            "upstream error must terminate the stream with Err"
        );
        // No Eof must follow an Err (the worker returns immediately).
        assert!(
            !batches.iter().any(|b| matches!(b, DemuxBatch::Eof)),
            "Err is terminal; no Eof after it"
        );
    }

    #[test]
    fn buffers_are_recycled_to_producer() {
        // The worker must return each consumed buffer to recycle_tx so the
        // producer can re-fill it (the zero-copy pool contract). Verify a
        // fed buffer comes back on the recycle channel.
        let (pf_tx, pf_rx) = bounded::<std::io::Result<Vec<u8>>>(4);
        let (rc_tx, rc_rx) = bounded::<Vec<u8>>(4);
        let pid = 0x1011;
        let ts = super::super::ts::TsDemuxer::new(&[pid]);
        let (_dt, _rx) =
            DemuxThread::spawn_zero_copy(pf_rx, rc_tx, (), None, Some(ts), None).unwrap();

        pf_tx.send(Ok(bdts_pes_packet(pid, &[0xAA]))).unwrap();
        let recycled = rc_rx.recv_timeout(Duration::from_secs(5));
        assert!(recycled.is_ok(), "consumed buffer must be recycled");
        assert_eq!(recycled.unwrap().len(), 192, "the original buffer returned");
        drop(pf_tx);
    }

    #[test]
    fn ps_path_demuxes_and_eofs() {
        // The PS branch must demux MPEG-2 Program Stream input and also send
        // the Eof sentinel on clean exit. Feed a complete PES + program-end
        // delimiter so the PsDemuxer emits it without waiting for flush.
        let (pf_tx, pf_rx) = bounded::<std::io::Result<Vec<u8>>>(4);
        let (rc_tx, _rc_rx) = bounded::<Vec<u8>>(4);
        let ps = super::super::ps::PsDemuxer::new();
        let (_dt, rx) =
            DemuxThread::spawn_zero_copy(pf_rx, rc_tx, (), None, None, Some(ps)).unwrap();

        // PES (video 0xE0, bounded length 5) + program-end delimiter.
        let mut buf = vec![
            0x00, 0x00, 0x01, 0xE0, 0x00, 0x05, 0x80, 0x00, 0x00, 0x77, 0x88,
        ];
        buf.extend_from_slice(&[0x00, 0x00, 0x01, 0xB9]); // program end
        pf_tx.send(Ok(buf)).unwrap();
        drop(pf_tx);

        let batches = collect_batches(&rx, Duration::from_secs(5));
        assert!(
            matches!(batches.last(), Some(DemuxBatch::Eof)),
            "PS path sends Eof"
        );
        let saw = batches.iter().any(|b| match b {
            DemuxBatch::Ps(p) => p.iter().any(|x| x.data == vec![0x77, 0x88]),
            _ => false,
        });
        assert!(saw, "PS PES must be demuxed and delivered");
    }

    #[test]
    fn no_demuxer_configured_still_recycles_and_eofs() {
        // With neither ts nor ps set, the worker must still recycle buffers
        // and terminate with Eof — never emit a spurious Ts/Ps batch.
        let (pf_tx, pf_rx) = bounded::<std::io::Result<Vec<u8>>>(4);
        let (rc_tx, rc_rx) = bounded::<Vec<u8>>(4);
        let (_dt, rx) = DemuxThread::spawn_zero_copy(pf_rx, rc_tx, (), None, None, None).unwrap();

        pf_tx.send(Ok(vec![0u8; 192])).unwrap();
        assert!(
            rc_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "buffer recycled"
        );
        drop(pf_tx);

        let batches = collect_batches(&rx, Duration::from_secs(5));
        assert_eq!(batches.len(), 1, "only the Eof sentinel");
        assert!(matches!(batches[0], DemuxBatch::Eof));
    }

    #[test]
    fn empty_batches_are_not_forwarded() {
        // The worker only forwards NON-empty packet vecs (`!pkts.is_empty()`).
        // A buffer that yields no complete PES (e.g. a single continuation
        // packet with no PUSI ever) must not produce a Ts batch — only Eof.
        let (pf_tx, pf_rx) = bounded::<std::io::Result<Vec<u8>>>(4);
        let (rc_tx, _rc_rx) = bounded::<Vec<u8>>(4);
        let pid = 0x1011;
        let ts = super::super::ts::TsDemuxer::new(&[pid]);
        let (_dt, rx) =
            DemuxThread::spawn_zero_copy(pf_rx, rc_tx, (), None, Some(ts), None).unwrap();

        // A non-PUSI packet on a tracked PID with header_remaining 0 and no
        // active PES: process_packet pushes nothing (asm inactive), so feed
        // returns empty and flush also returns empty.
        const SYNC: u8 = 0x47;
        let mut pkt = vec![0u8; 192];
        pkt[4] = SYNC;
        pkt[5] = ((pid >> 8) as u8) & 0x1F; // no PUSI
        pkt[6] = (pid & 0xFF) as u8;
        pkt[7] = 0x10; // payload only
        pf_tx.send(Ok(pkt)).unwrap();
        drop(pf_tx);

        let batches = collect_batches(&rx, Duration::from_secs(5));
        assert_eq!(batches.len(), 1, "only Eof; no empty Ts batch forwarded");
        assert!(matches!(batches[0], DemuxBatch::Eof));
    }
}
