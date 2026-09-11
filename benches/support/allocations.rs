#[cfg(feature = "_bench_alloc")]
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[cfg(feature = "_bench_alloc")]
pub(crate) struct CountingAllocator;

static ENABLED: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "_bench_alloc")]
fn record(ptr: *mut u8, size: usize) {
    if !ptr.is_null() && ENABLED.load(Ordering::Relaxed) {
        CALLS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(size as u64, Ordering::Relaxed);
    }
}

// Allocation and deallocation are delegated unchanged to the system allocator.
#[cfg(feature = "_bench_alloc")]
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        record(ptr, layout.size());
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        record(ptr, layout.size());
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let ptr = unsafe { System.realloc(ptr, layout, new_size) };
        record(ptr, new_size);
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }
}

pub(crate) struct Measurement;

impl Measurement {
    pub(crate) fn start() -> Self {
        assert!(
            !ENABLED.load(Ordering::Relaxed),
            "nested allocation measurement"
        );
        CALLS.store(0, Ordering::Relaxed);
        BYTES.store(0, Ordering::Relaxed);
        ENABLED.store(true, Ordering::Relaxed);
        Self
    }

    pub(crate) fn finish(self) -> (u64, u64) {
        ENABLED.store(false, Ordering::Relaxed);
        (CALLS.load(Ordering::Relaxed), BYTES.load(Ordering::Relaxed))
    }
}

impl Drop for Measurement {
    fn drop(&mut self) {
        ENABLED.store(false, Ordering::Relaxed);
    }
}
