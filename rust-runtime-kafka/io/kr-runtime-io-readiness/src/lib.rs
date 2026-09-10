//! Bounded native Linux TCP readiness provider.
//!
//! One epoll thread owns sockets and admitted operations. Handles submit owned
//! commands; ordinary completion wakers notify the caller's executor. No Tokio
//! runtime, borrowed asynchronous buffers, or per-stream threads are involved.
#![forbid(unsafe_code)]
#![cfg(target_os = "linux")]

mod driver;
mod provider;

pub use provider::{
    ReadinessConfig, ReadinessListener, ReadinessNet, ReadinessObserver, ReadinessStatus,
    ReadinessStream,
};
