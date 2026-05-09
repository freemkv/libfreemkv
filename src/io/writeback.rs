//! Per-platform writeback pipeline. On Linux, drains dirty pages
//! continuously at chunk granularity to keep the kernel's writeback
//! queue bounded. On macOS and Windows, a no-op stub.
//!
//! The platform decision lives entirely in this file (the cfg-gated
//! `pub use` below). Callers — and `DiskWriter` itself — are
//! platform-independent.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(not(target_os = "linux"))]
mod noop;

#[cfg(target_os = "linux")]
pub(super) use linux::WritebackPipeline;
#[cfg(not(target_os = "linux"))]
pub(super) use noop::WritebackPipeline;
