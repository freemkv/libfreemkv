use super::{observed, raise};
use loom::cell::UnsafeCell;
use loom::sync::Arc;
use loom::sync::atomic::AtomicBool;

/// per spec; do not change without a spec citation proving otherwise — SS-23:
/// Release "all previous writes become visible to all threads that perform an
/// Acquire (or stronger) load of this value". A write before `cancel` is visible
/// to every thread that observes the cancel.
#[test]
fn loom_halt_cancel_is_release_acquire() {
    loom::model(|| {
        let flag = Arc::new(AtomicBool::new(false));
        let data = Arc::new(UnsafeCell::new(0u32));
        let (f2, d2) = (flag.clone(), data.clone());
        let canceller = loom::thread::spawn(move || {
            // SAFETY: the only write; the reader touches it only after observing
            // the cancel, which loom checks is ordered after this write.
            d2.with_mut(|p| unsafe { *p = 7 });
            raise(&*f2);
        });
        if observed(&*flag) {
            // SAFETY: ordered after the write by the Release/Acquire pair.
            assert_eq!(data.with(|p| unsafe { *p }), 7);
        }
        canceller.join().unwrap();
    });
}
