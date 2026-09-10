//! Concurrent-connection stream throughput: per-stream actors versus the
//! shared-ring pool.
//!
//! Deliberately distinct from `stream_overhead`, which is the matched QD1
//! overhead baseline on one connection. This suite drives one round trip on
//! every connection concurrently, which is the shape that exercises the
//! pool's single coordinator against the per-stream provider's
//! three-threads-per-connection fleet, and it measures throughput, not
//! per-operation overhead. The two suites must not be compared.

#[cfg(target_os = "linux")]
use criterion::{criterion_group, criterion_main};

#[cfg(target_os = "linux")]
#[path = "stream_many_connections/linux.rs"]
mod linux;

#[cfg(target_os = "linux")]
criterion_group!(benches, linux::many_connections_benchmarks);
#[cfg(target_os = "linux")]
criterion_main!(benches);

#[cfg(not(target_os = "linux"))]
fn main() {}
