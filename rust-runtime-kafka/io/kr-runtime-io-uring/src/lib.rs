//! Linux io_uring provider for `kr-runtime-io` owned file and network I/O.
//!
//! Each opened resource has a bounded command actor, while related actors share
//! an eventfd-driven reactor and one kernel submission/completion ring. Calls
//! reserve actor capacity immediately, and actors retain every resource and
//! owned buffer until the matching `user_data` completion is dispatched.

#[cfg(target_os = "linux")]
mod operation;
#[cfg(target_os = "linux")]
pub use operation::UringOperation;
#[cfg(target_os = "linux")]
#[doc(hidden)]
pub mod host;
#[cfg(target_os = "linux")]
mod ring;
#[cfg(target_os = "linux")]
mod support;
#[cfg(all(test, target_os = "linux"))]
mod test_support;

#[cfg(target_os = "linux")]
mod env;
#[cfg(target_os = "linux")]
pub use env::{UringEnv, UringEnvConfig, UringEnvOpenError};

#[cfg(target_os = "linux")]
mod file;
#[cfg(target_os = "linux")]
pub use file::{UringFile, UringFileConfig, UringFileOpenError, UringFileOpenOutcome};

#[cfg(target_os = "linux")]
mod pooled;
#[cfg(target_os = "linux")]
pub use pooled::{PooledUringFile, UringIoPool, UringPoolConfig, UringPoolOpenError};

#[cfg(target_os = "linux")]
mod pooled_network;
#[cfg(target_os = "linux")]
pub use pooled_network::{
    PooledUringListener, PooledUringStream, UringNetPool, UringNetPoolConfig, UringNetPoolObserver,
    UringNetPoolOpenError, UringNetPoolStatus,
};

#[cfg(target_os = "linux")]
mod network;
#[cfg(target_os = "linux")]
pub use network::{
    DESCRIPTORS_PER_STREAM, UringByteStream, UringListener, UringNetwork, UringNetworkConfig,
    UringNetworkOpenError, UringNetworkProviderConfig,
};

#[cfg(target_os = "linux")]
mod datagram;
#[cfg(target_os = "linux")]
pub use datagram::{
    UringDatagram, UringDatagramConfig, UringDatagramOpenError, UringDatagramSocket,
};
