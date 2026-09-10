use kr_runtime::{
    AbortHandle, HostConfig, HostConfigError, HostControl, HostRunErrorKind, HostRuntime,
    HostSendHandle, HostSendJoinHandle, HostStatus, JoinError, JoinHandle, RunErrorDisposition,
    RunErrorKind, RuntimeDuration, RuntimeInstant, SimDuration, SimRuntime, Sleep, SpawnError,
    TaskFailure, TimeError, yield_now,
};
use std::cell::{Cell, RefCell};
use std::future::{Future, pending};
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

#[path = "host/blocking.rs"]
mod blocking;
mod common;
#[path = "host/cross_thread.rs"]
mod cross_thread;
#[path = "host/lifecycle.rs"]
mod lifecycle;
#[path = "host/parity.rs"]
mod parity;
#[path = "host/timers.rs"]
mod timers;

fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    Pin::new(future).poll(&mut context)
}
