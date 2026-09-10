//! Multi-inflight file throughput: per-file actors versus the shared pool.
//!
//! Deliberately distinct from `file_overhead`, which is the matched QD1
//! overhead baseline. This suite keeps several operations in flight at once,
//! which is the shape that exercises `UringFile`'s commuting-batch actor and
//! the pool's shared-ring concurrency, and it measures throughput, not
//! per-operation overhead. The two suites must not be compared.

#[cfg(target_os = "linux")]
use criterion::{criterion_group, criterion_main};

#[cfg(target_os = "linux")]
#[path = "file_multi_inflight/linux.rs"]
mod linux;

#[cfg(target_os = "linux")]
criterion_group!(benches, linux::multi_inflight_benchmarks);
#[cfg(target_os = "linux")]
criterion_main!(benches);

#[cfg(not(target_os = "linux"))]
fn main() {}
