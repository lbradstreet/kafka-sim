//! Panic fixtures shared by the integration test binaries.

use std::panic::panic_any;
use std::sync::Arc;
use std::task::Wake;

/// A join-observer waker that panics with its message when woken.
pub(crate) struct PanicWake(pub(crate) &'static str);

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic_any(self.0);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        panic_any(self.0);
    }
}
