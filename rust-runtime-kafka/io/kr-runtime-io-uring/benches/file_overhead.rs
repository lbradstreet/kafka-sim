//! Linux file-I/O overhead ladder for the raw ring and `UringFile`.

#[cfg(target_os = "linux")]
use criterion::{criterion_group, criterion_main};

#[cfg(target_os = "linux")]
#[path = "file_overhead/linux.rs"]
mod linux;

#[cfg(target_os = "linux")]
criterion_group!(benches, linux::file_benchmarks);
#[cfg(target_os = "linux")]
criterion_main!(benches);

#[cfg(not(target_os = "linux"))]
fn main() {}
