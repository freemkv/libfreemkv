// Counting global allocator shared by the mux benches: allocations and bytes
// requested (alloc + realloc growth), read before and after a measured run.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `System` with this layout.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(
            new_size.saturating_sub(layout.size()) as u64,
            Ordering::Relaxed,
        );
        // SAFETY: `ptr`/`layout` came from `System`; forwarded unchanged.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// `(allocations, bytes)` so far.
pub fn snapshot() -> (u64, u64) {
    (
        ALLOCS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    )
}

/// Run `f` once and return the `(allocations, bytes)` it made.
pub fn measure<R>(f: impl FnOnce() -> R) -> (u64, u64, R) {
    let (a0, b0) = snapshot();
    let r = f();
    let (a1, b1) = snapshot();
    (a1 - a0, b1 - b0, r)
}
