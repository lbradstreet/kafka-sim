//! Shared plumbing for bounded io_uring resource actors.
//!
//! Providers in this crate follow the same actor disciplines: counted
//! live-resource permits, readiness handshakes for newly spawned actor
//! threads, terminal command admission, and joins that never target the
//! current thread. This module holds the one implementation of each so the
//! file, network, and datagram providers cannot drift apart.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};

use crate::operation::{DriverStoppedCommand, TerminalCommand};
use crate::ring::RoutedCompletion;

/// A pooled coordinator's wake sentinel on its routed completion channel,
/// mirroring the reactor's `WAKE_USER_DATA`: ingress producers push a
/// message and send this token so the coordinator never polls. Operation
/// tokens are allocated from one, so the sentinel cannot collide.
pub(crate) const WAKE_TOKEN: u64 = 0;

/// A pooled coordinator's ingress: a message deque plus the wake sentinel
/// that makes it observable without polling.
pub(crate) struct Ingress<M> {
    messages: Mutex<VecDeque<M>>,
    wake: Mutex<Sender<RoutedCompletion>>,
}

impl<M> Ingress<M> {
    pub(crate) fn new(wake: Sender<RoutedCompletion>) -> Self {
        Self {
            messages: Mutex::new(VecDeque::new()),
            wake: Mutex::new(wake),
        }
    }

    pub(crate) fn push(&self, message: M) {
        lock_unpoisoned(&self.messages).push_back(message);
        // A dead coordinator has already drained or rejected everything the
        // message could refer to, so a failed wake is not an error path.
        let _ = lock_unpoisoned(&self.wake).send(RoutedCompletion {
            token: WAKE_TOKEN,
            result: Ok(0),
            segments: None,
        });
    }

    pub(crate) fn len(&self) -> usize {
        lock_unpoisoned(&self.messages).len()
    }

    pub(crate) fn pop(&self) -> Option<M> {
        lock_unpoisoned(&self.messages).pop_front()
    }
}

/// A counted bound on live resources shared by all clones of one provider.
pub(crate) struct ResourcePool {
    limit: usize,
    in_use: AtomicUsize,
}

impl ResourcePool {
    pub(crate) const fn new(limit: usize) -> Self {
        Self {
            limit,
            in_use: AtomicUsize::new(0),
        }
    }

    pub(crate) fn in_use(&self) -> usize {
        self.in_use.load(Ordering::Acquire)
    }

    pub(crate) fn acquire(self: &Arc<Self>) -> Option<ResourcePermit> {
        self.in_use
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |in_use| {
                (in_use < self.limit).then_some(in_use + 1)
            })
            .ok()
            .map(|_| ResourcePermit {
                pool: Arc::clone(self),
            })
    }
}

/// One live-resource reservation, released on drop.
pub(crate) struct ResourcePermit {
    pool: Arc<ResourcePool>,
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        let previous = self.pool.in_use.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "resource permit count underflowed");
    }
}

/// Locks a mutex, adopting the guarded state if the holder panicked.
pub(crate) fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Why a command could not be admitted to a bounded actor queue.
///
/// Each domain maps this to its own queue-exhausted or driver-stopped error;
/// both outcomes reject before any effect.
#[derive(Clone, Copy)]
pub(crate) enum Rejection {
    Full,
    Stopped,
}

/// Admits one command to a bounded actor queue.
///
/// A rejection returns the caller's command untouched together with why it
/// was refused, so the caller keeps every owned buffer and responder. A
/// `None` sender means the handle already stopped its actor.
pub(crate) fn try_send_command<C: DriverStoppedCommand>(
    sender: Option<&SyncSender<TerminalCommand<C>>>,
    command: C,
) -> Result<(), (C, Rejection)> {
    let Some(sender) = sender else {
        return Err((command, Rejection::Stopped));
    };
    match sender.try_send(TerminalCommand::new(command)) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(command)) => Err((command.into_inner(), Rejection::Full)),
        Err(TrySendError::Disconnected(command)) => Err((command.into_inner(), Rejection::Stopped)),
    }
}

/// Completes a spawned actor's readiness handshake.
///
/// The sender and join handle are released together only after the actor
/// reported readiness. A handshake error or a channel closed before the
/// report joins the failed thread first, so no caller observes a live sender
/// to an actor that never became ready; a silent close maps to the domain's
/// `driver_stopped` error.
pub(crate) fn finish_actor_start<C, E>(
    sender: SyncSender<C>,
    join: JoinHandle<()>,
    ready: Receiver<Result<(), E>>,
    driver_stopped: E,
) -> Result<(SyncSender<C>, JoinHandle<()>), E> {
    match ready.recv() {
        Ok(Ok(())) => Ok((sender, join)),
        Ok(Err(error)) => {
            let _ = join.join();
            Err(error)
        }
        Err(_) => {
            let _ = join.join();
            Err(driver_stopped)
        }
    }
}

/// Joins an actor thread unless the caller is that thread.
///
/// Cleanup can run on the actor thread itself when the last owning handle is
/// dropped there; joining would deadlock, so ownership is released instead.
pub(crate) fn join_if_other_thread(join: Option<JoinHandle<()>>) {
    if let Some(join) = join
        && join.thread().id() != thread::current().id()
    {
        let _ = join.join();
    }
}
