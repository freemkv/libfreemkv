//! Byte-sized bounded producer/consumer channel.
//!
//! Wraps `std::sync::mpsc::sync_channel` with a byte-accounting
//! `Mutex<usize> + Condvar` cap. Sender blocks (cooperatively) when
//! `used_bytes + item.byte_size() > capacity_bytes`. Receiver
//! decrements `used_bytes` when it takes the item.
//!
//! Why: the existing producer→consumer channel between `DiscStream`
//! (PES producer) and `MuxSink` (PES consumer) is bounded by frame
//! count. Frame sizes vary 100× between metadata and keyframes, so a
//! count-based cap either starves on small frames or buffers far too
//! much memory on big ones. Byte-sized accounting sizes the buffer for
//! the worst-case input stall (NFS read p99 ≈ 1–2 s × ~15 MB/s peak
//! compressed bitrate ≈ ~30 MB) directly.
//!
//! The underlying mpsc channel is created with a very large slot count
//! so the byte cap (not the slot count) is the real backpressure. Slot
//! count is only there to give the kernel a small chunk to wake on.
//!
//! See `freemkv-private/memory/project_buffering_architecture.md` §
//! Pipeline channel — sizing.

use std::sync::mpsc::{Receiver as MpscReceiver, RecvError, SendError, SyncSender, sync_channel};
use std::sync::{Arc, Condvar, Mutex};

/// Default byte cap for the muxer's input channel. Sized to hide a
/// worst-case ~2 s NFS read refill at UHD peak compressed bitrate
/// (~15 MB/s); 64 MiB gives headroom. Tweakable; not magic.
pub const BYTE_CHANNEL_DEFAULT_CAPACITY: usize = 64 * 1024 * 1024;

/// Slot capacity of the inner `sync_channel`. Large so the byte cap is
/// the real backpressure mechanism — the mpsc slot count only exists
/// to give the kernel a chunk to wake on. PES frames are typically
/// ~700 B each, so 64 MiB ≈ 90 k frames; 200 k is comfortable headroom.
const INNER_SLOT_CAPACITY: usize = 200_000;

/// Anything whose in-memory cost can be accounted by a single
/// `usize`. Implement on the item type sent through [`Sender`].
pub trait HasByteSize {
    /// Bytes this item contributes to the channel's used budget.
    /// Must be > 0 to make progress (a 0-byte item would never
    /// block the sender no matter the cap; see send_blocks_at_capacity
    /// test).
    fn byte_size(&self) -> usize;
}

impl HasByteSize for crate::pes::PesFrame {
    fn byte_size(&self) -> usize {
        // Frame data + the fixed header overhead the serializer
        // writes (track + pts + keyframe + len). The `Vec<u8>` heap
        // allocation also has alloc-header overhead but that's
        // <0.1 % at typical frame sizes — folding it in would just
        // add noise to the budget.
        self.data.len() + 14
    }
}

/// Shared book-keeping between [`Sender`] and [`Receiver`]. Wrapped in
/// an `Arc` because both halves hold it independently.
struct Accounting {
    used: Mutex<usize>,
    cv: Condvar,
    capacity: usize,
}

/// Send half of the byte-bounded channel.
///
/// `send` blocks (on a `Condvar`) when adding the item would push
/// `used_bytes` past `capacity_bytes`. Unblocks when the receiver
/// `recv`s items out and notifies. Returns `Err(item)` if the
/// receiver has been dropped — mirrors `mpsc::SyncSender::send`.
pub struct Sender<T: HasByteSize> {
    tx: SyncSender<T>,
    acct: Arc<Accounting>,
}

impl<T: HasByteSize> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            tx: self.tx.clone(),
            acct: self.acct.clone(),
        }
    }
}

impl<T: HasByteSize> Sender<T> {
    /// Push one item. Blocks until adding it would not exceed the
    /// capacity, then sends through the inner mpsc channel.
    pub fn send(&self, item: T) -> Result<(), SendError<T>> {
        let sz = item.byte_size();
        // Reserve capacity first. The reservation is observable to
        // other senders via `used`; only after we win the slot do we
        // hand the item to the inner mpsc channel. That ordering means
        // `used` is always a conservative upper bound on what's in the
        // mpsc queue + about-to-be-sent.
        {
            let mut used = self.acct.used.lock().expect("byte_channel poisoned");
            // An item bigger than the whole capacity will never fit; let
            // it through anyway as a one-shot reservation, otherwise the
            // sender deadlocks forever waiting for `used == 0` AND
            // nothing in flight. The receiver will drain it on the
            // other side. Same behaviour as `std::sync::mpsc` for
            // arbitrarily large messages.
            while *used + sz > self.acct.capacity && *used > 0 {
                used = self.acct.cv.wait(used).expect("byte_channel cv poisoned");
            }
            *used += sz;
        }
        match self.tx.send(item) {
            Ok(()) => Ok(()),
            Err(SendError(returned)) => {
                // Receiver dropped — refund the reservation so a later
                // sender on a clone doesn't observe phantom used bytes
                // (the receiver is gone so nobody will decrement).
                let mut used = self.acct.used.lock().expect("byte_channel poisoned");
                *used = used.saturating_sub(sz);
                self.acct.cv.notify_all();
                Err(SendError(returned))
            }
        }
    }
}

/// Receive half of the byte-bounded channel.
///
/// `recv` blocks on the inner mpsc until an item is available, then
/// decrements the byte-accounting and wakes any sender waiting on
/// capacity.
pub struct Receiver<T: HasByteSize> {
    rx: MpscReceiver<T>,
    acct: Arc<Accounting>,
}

impl<T: HasByteSize> Receiver<T> {
    /// Take the next item. Returns `Err(RecvError)` when all senders
    /// have been dropped and the channel is empty.
    pub fn recv(&self) -> Result<T, RecvError> {
        let item = self.rx.recv()?;
        let sz = item.byte_size();
        let mut used = self.acct.used.lock().expect("byte_channel poisoned");
        *used = used.saturating_sub(sz);
        // Notify all so multi-sender setups wake every blocked sender,
        // not just one. Wasted wakeups are cheap; missed wakeups would
        // be a deadlock.
        self.acct.cv.notify_all();
        Ok(item)
    }
}

/// Create a byte-bounded channel with the given capacity in bytes.
/// Returns a `(Sender, Receiver)` pair; clone the `Sender` for
/// multi-producer setups.
pub fn channel<T: HasByteSize>(capacity_bytes: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = sync_channel::<T>(INNER_SLOT_CAPACITY);
    let acct = Arc::new(Accounting {
        used: Mutex::new(0),
        cv: Condvar::new(),
        capacity: capacity_bytes,
    });
    (
        Sender {
            tx,
            acct: acct.clone(),
        },
        Receiver { rx, acct },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    /// Test payload — its `byte_size` returns whatever we passed at
    /// construction so capacity math is exact and predictable.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Item {
        sz: usize,
        tag: u32,
    }

    impl HasByteSize for Item {
        fn byte_size(&self) -> usize {
            self.sz
        }
    }

    #[test]
    fn send_recv_round_trip() {
        let (tx, rx) = channel::<Item>(1024);
        for i in 0..5 {
            tx.send(Item { sz: 100, tag: i }).unwrap();
        }
        for i in 0..5 {
            let got = rx.recv().unwrap();
            assert_eq!(got, Item { sz: 100, tag: i });
        }
    }

    #[test]
    fn byte_accounting_decrements_on_recv() {
        // Internal book-keeping check via observable side-effect: after
        // sending K items totalling N bytes and receiving them all, a
        // subsequent send of an N-byte item must NOT block (no items
        // in flight, all capacity refunded).
        let (tx, rx) = channel::<Item>(1024);
        for _ in 0..4 {
            tx.send(Item { sz: 256, tag: 0 }).unwrap();
        }
        for _ in 0..4 {
            rx.recv().unwrap();
        }
        // Cap is now fully available again. Send a 1024-byte item; the
        // `used > 0` guard means it goes through alone (no wait).
        let start = Instant::now();
        tx.send(Item { sz: 1024, tag: 99 }).unwrap();
        assert!(start.elapsed() < Duration::from_millis(100));
        let got = rx.recv().unwrap();
        assert_eq!(got.tag, 99);
    }

    #[test]
    fn send_blocks_at_capacity_unblocks_on_recv() {
        // Cap = 200 bytes, item = 100 bytes. First two sends fit
        // exactly; the third must block until a recv frees capacity.
        let (tx, rx) = channel::<Item>(200);
        tx.send(Item { sz: 100, tag: 0 }).unwrap();
        tx.send(Item { sz: 100, tag: 1 }).unwrap();

        let tx2 = tx.clone();
        let sent_at = Arc::new(Mutex::new(None::<Instant>));
        let sent_at2 = sent_at.clone();
        let h = thread::spawn(move || {
            tx2.send(Item { sz: 100, tag: 2 }).unwrap();
            *sent_at2.lock().unwrap() = Some(Instant::now());
        });

        // Give the sender thread a head start; it should be parked in
        // `cv.wait` because used (200) + 100 > capacity (200).
        thread::sleep(Duration::from_millis(100));
        assert!(
            sent_at.lock().unwrap().is_none(),
            "third send should be blocked at capacity"
        );

        // Drain one. Sender wakes and completes.
        let recv_at = Instant::now();
        let got = rx.recv().unwrap();
        assert_eq!(got.tag, 0);
        h.join().unwrap();

        let sent_when = sent_at.lock().unwrap().unwrap();
        assert!(
            sent_when >= recv_at,
            "sender must complete AFTER receiver freed capacity"
        );

        // Drain the remaining two.
        assert_eq!(rx.recv().unwrap().tag, 1);
        assert_eq!(rx.recv().unwrap().tag, 2);
    }

    #[test]
    fn item_larger_than_capacity_still_goes_through() {
        // Pathological case: a single item bigger than the capacity.
        // The guard `*used > 0` lets it through when the channel is
        // empty (otherwise the sender deadlocks forever). Matches
        // `mpsc::SyncSender` semantics for oversize messages.
        let (tx, rx) = channel::<Item>(100);
        tx.send(Item { sz: 1000, tag: 7 }).unwrap();
        let got = rx.recv().unwrap();
        assert_eq!(got, Item { sz: 1000, tag: 7 });
    }

    #[test]
    fn concurrent_send_recv_stress() {
        // 4 sender threads × 1k items each, 1 receiver. Verify byte
        // accounting stays sane (channel never deadlocks, every item
        // arrives exactly once) under contention.
        const SENDERS: u32 = 4;
        const PER_SENDER: u32 = 1000;
        const TOTAL: u32 = SENDERS * PER_SENDER;

        let (tx, rx) = channel::<Item>(8 * 1024);
        let sent = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for s in 0..SENDERS {
            let tx = tx.clone();
            let sent = sent.clone();
            handles.push(thread::spawn(move || {
                for i in 0..PER_SENDER {
                    // Vary item size so accounting actually has to
                    // multiplex differently-sized blockers. 1B → 256B.
                    let sz = 1 + ((i as usize) % 256);
                    tx.send(Item {
                        sz,
                        tag: s * PER_SENDER + i,
                    })
                    .unwrap();
                    sent.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }
        // Drop our local sender so the receiver can eventually see
        // RecvError once all sender clones are done. Cloning the
        // sender into each producer means each clone Drop'd separately.
        drop(tx);

        let mut received = 0u32;
        while let Ok(_item) = rx.recv() {
            received += 1;
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(received, TOTAL);
        assert_eq!(sent.load(Ordering::SeqCst) as u32, TOTAL);
    }

    #[test]
    fn send_after_recv_dropped_returns_err() {
        let (tx, rx) = channel::<Item>(1024);
        drop(rx);
        let r = tx.send(Item { sz: 10, tag: 0 });
        assert!(r.is_err());
    }
}
