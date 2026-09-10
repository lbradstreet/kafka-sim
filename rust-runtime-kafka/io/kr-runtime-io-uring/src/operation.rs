//! Thread-safe single-response futures used by bounded resource actors.
//!
//! The future and responder are the shared `kr-runtime-io` completion primitives;
//! this module adds the actor-lifecycle guards that terminalize queued and
//! active commands when a resource actor stops or unwinds.

use kr_runtime::contain_panic;
use kr_runtime_io::completion::{SyncOperation, SyncResponder};

/// Future for one operation admitted to an io_uring resource actor.
///
/// Dropping it abandons only response delivery. The provider retains the
/// admitted operation and every owned resource until its terminal completion.
pub type UringOperation<T> = SyncOperation<T>;

pub(crate) type Responder<T> = SyncResponder<T>;

pub(crate) fn operation<T>() -> (UringOperation<T>, Responder<T>) {
    SyncOperation::channel()
}

pub(crate) fn ready<T>(output: T) -> UringOperation<T> {
    SyncOperation::ready(output)
}

/// A queued actor command that completes its response if its receiver dies.
pub(crate) struct TerminalCommand<C: DriverStoppedCommand> {
    command: Option<C>,
}

pub(crate) trait DriverStoppedCommand {
    fn complete_driver_stopped(self);
}

impl<C: DriverStoppedCommand> TerminalCommand<C> {
    pub(crate) const fn new(command: C) -> Self {
        Self {
            command: Some(command),
        }
    }

    pub(crate) fn take(&mut self) -> C {
        self.command.take().expect("actor command is available")
    }

    pub(crate) fn into_inner(mut self) -> C {
        self.take()
    }
}

impl<C: DriverStoppedCommand> Drop for TerminalCommand<C> {
    fn drop(&mut self) {
        if let Some(command) = self.command.take() {
            // Queue teardown often runs while the actor thread is already
            // unwinding. A caller-supplied waker must not cause a double panic
            // or prevent later queued commands from receiving terminal state.
            contain_panic(|| {
                command.complete_driver_stopped();
            });
        }
    }
}

/// Terminalizes a no-buffer active command if its actor unwinds.
pub(crate) struct ActiveResponder<T> {
    response: Option<Responder<T>>,
    driver_stopped: fn() -> T,
}

impl<T> ActiveResponder<T> {
    pub(crate) const fn new(response: Responder<T>, driver_stopped: fn() -> T) -> Self {
        Self {
            response: Some(response),
            driver_stopped,
        }
    }

    pub(crate) fn complete(mut self, output: T) {
        self.response
            .take()
            .expect("active actor response is available")
            .complete(output);
    }
}

impl<T> Drop for ActiveResponder<T> {
    fn drop(&mut self) {
        if let Some(response) = self.response.take() {
            let driver_stopped = self.driver_stopped;
            contain_panic(|| {
                response.complete(driver_stopped());
            });
        }
    }
}

/// Makes panics inside pointer-bearing io_uring operations process-fatal.
///
/// Unwinding such an operation cannot return its caller buffer honestly, and
/// could release memory while the kernel still owns its address. Aborting is
/// the only sound fail-stop behavior for an internal invariant panic.
pub(crate) struct FailStopOnPanic;

impl Drop for FailStopOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            std::process::abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    use super::*;

    struct CountingWake(Arc<AtomicUsize>);

    struct PanickingWake;

    struct PanickingPayload;

    struct TestCommand(Responder<Result<u8, &'static str>>);

    impl DriverStoppedCommand for TestCommand {
        fn complete_driver_stopped(self) {
            self.0.complete(Err("driver stopped"));
        }
    }

    fn driver_stopped() -> Result<u8, &'static str> {
        Err("driver stopped")
    }

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            std::panic::panic_any(PanickingPayload);
        }
    }

    impl Drop for PanickingPayload {
        fn drop(&mut self) {
            panic!("io_uring response wake payload destructor panic");
        }
    }

    #[test]
    fn abandoned_response_does_not_wake() {
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountingWake(Arc::clone(&wakes))));
        let mut context = Context::from_waker(&waker);
        let (mut future, response) = operation::<u8>();
        assert!(Pin::new(&mut future).poll(&mut context).is_pending());

        drop(future);
        response.complete(7);

        assert_eq!(wakes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn response_contains_waker_and_payload_destructor_panics() {
        let waker = Waker::from(Arc::new(PanickingWake));
        let mut context = Context::from_waker(&waker);
        let (mut future, response) = operation();
        assert!(Pin::new(&mut future).poll(&mut context).is_pending());

        response.complete(7);

        let mut context = Context::from_waker(Waker::noop());
        assert_eq!(Pin::new(&mut future).poll(&mut context), Poll::Ready(7));
    }

    #[test]
    fn queued_and_active_response_guards_terminalize_on_drop() {
        let (mut queued, response) = operation();
        drop(TerminalCommand::new(TestCommand(response)));
        let mut context = Context::from_waker(Waker::noop());
        assert_eq!(
            Pin::new(&mut queued).poll(&mut context),
            Poll::Ready(Err("driver stopped"))
        );

        let (mut active, response) = operation();
        drop(ActiveResponder::new(response, driver_stopped));
        assert_eq!(
            Pin::new(&mut active).poll(&mut context),
            Poll::Ready(Err("driver stopped"))
        );
    }
}
