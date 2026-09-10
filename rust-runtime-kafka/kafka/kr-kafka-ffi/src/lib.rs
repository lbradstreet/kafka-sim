//! Audited C boundary. Native ownership, pointer validity and version checks are
//! confined here; the runtime, protocol and producer retain forbidden unsafe.
#![deny(unsafe_op_in_unsafe_fn)]
mod abi;
mod config;
mod foreign;
mod memory;
mod submission;
mod types;
pub use abi::*;
pub use types::*;
