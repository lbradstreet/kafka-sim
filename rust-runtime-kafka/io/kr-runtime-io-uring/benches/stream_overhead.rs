//! Matched QD1 stream echo overhead: blocking syscalls, per-stream actors,
//! and the shared-ring pool.
//!
//! Deliberately distinct from `stream_many_connections`, which measures
//! throughput under connection-level concurrency. This suite awaits every
//! round trip before starting the next, so its numbers are per-operation
//! overhead on one connection. The two suites must not be compared.

#[cfg(target_os = "linux")]
use criterion::{criterion_group, criterion_main};

#[cfg(target_os = "linux")]
#[path = "stream_overhead/linux.rs"]
mod linux;

#[cfg(target_os = "linux")]
criterion_group!(benches, linux::stream_overhead_benchmarks);
#[cfg(target_os = "linux")]
criterion_main!(benches);

#[cfg(not(target_os = "linux"))]
fn main() {}
