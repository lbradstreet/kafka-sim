//! Bounded lifecycle support for threads that host owned io_uring actors.

use std::sync::{Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::support::lock_unpoisoned;

/// Terminal failure from a checked actor-host join.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ActorHostJoinError {
    /// The host thread panicked.
    DriverStopped,
    /// The configured shutdown deadline elapsed.
    TimedOut,
}

/// Owns a host thread and provides a bounded explicit join.
///
/// Dropping this value always detaches. The actor thread continues owning its
/// descriptors, buffers, and kernel pointer targets until it exits safely.
pub struct ActorHost {
    join: Mutex<Option<JoinHandle<()>>>,
    exited: Mutex<mpsc::Receiver<()>>,
    shutdown_timeout: Duration,
}

impl ActorHost {
    /// Creates a host from its join handle and exit-notification receiver.
    pub fn new(
        join: JoinHandle<()>,
        exited: mpsc::Receiver<()>,
        shutdown_timeout: Duration,
    ) -> Self {
        Self {
            join: Mutex::new(Some(join)),
            exited: Mutex::new(exited),
            shutdown_timeout,
        }
    }

    /// Waits up to the configured deadline for actor exit.
    ///
    /// # Errors
    ///
    /// Returns [`ActorHostJoinError::TimedOut`] when the shutdown deadline
    /// elapses before the actor thread exits, or
    /// [`ActorHostJoinError::DriverStopped`] when the host thread panicked.
    pub fn join(&self) -> Result<(), ActorHostJoinError> {
        let Some(join) = lock_unpoisoned(&self.join).take() else {
            return Ok(());
        };
        if join.thread().id() == thread::current().id() {
            return Ok(());
        }
        let started = std::time::Instant::now();
        match lock_unpoisoned(&self.exited).recv_timeout(self.shutdown_timeout) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {
                drop(join);
                return Err(ActorHostJoinError::TimedOut);
            }
        }
        // ActorExit is an ordinary stack guard. Its notification can precede
        // thread-local destructors, so it is not by itself proof that join()
        // cannot block. Spend only the original deadline's remaining budget
        // waiting for JoinHandle::is_finished before performing the join.
        while !join.is_finished() {
            let remaining = self.shutdown_timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                drop(join);
                return Err(ActorHostJoinError::TimedOut);
            }
            thread::park_timeout(remaining.min(Duration::from_millis(1)));
        }
        join.join().map_err(|_| ActorHostJoinError::DriverStopped)
    }
}

impl Drop for ActorHost {
    fn drop(&mut self) {
        // Explicit join is bounded and checked. Implicit drop must not block an
        // unrelated unwind, so dropping the JoinHandle detaches.
        lock_unpoisoned(&self.join).take();
    }
}

/// Sends one best-effort exit notification when the actor thread unwinds.
pub struct ActorExit(mpsc::SyncSender<()>);

impl ActorExit {
    /// Creates an exit guard for a one-entry notification channel.
    pub fn new(sender: mpsc::SyncSender<()>) -> Self {
        Self(sender)
    }
}

impl Drop for ActorExit {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    #[test]
    fn checked_join_is_bounded_and_detaches_on_timeout() {
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let (exit_sender, exit_receiver) = mpsc::sync_channel(1);
        let join = thread::spawn(move || {
            let _exit = ActorExit::new(exit_sender);
            let _ = release_receiver.recv();
        });
        let host = ActorHost::new(join, exit_receiver, Duration::from_millis(1));

        let started = Instant::now();
        assert_eq!(host.join(), Err(ActorHostJoinError::TimedOut));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "bounded host join took an unreasonable amount of time"
        );
        release_sender.send(()).expect("release detached host");
    }

    #[test]
    fn exit_notification_does_not_make_a_still_running_thread_safe_to_join() {
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let (exit_sender, exit_receiver) = mpsc::sync_channel(1);
        let join = thread::spawn(move || {
            drop(ActorExit::new(exit_sender));
            let _ = release_receiver.recv();
        });
        let host = ActorHost::new(join, exit_receiver, Duration::from_millis(1));

        let started = Instant::now();
        assert_eq!(host.join(), Err(ActorHostJoinError::TimedOut));
        assert!(started.elapsed() < Duration::from_secs(1));
        release_sender.send(()).expect("release detached host");
    }

    #[test]
    fn drop_detaches_without_waiting_for_exit() {
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let (exit_sender, exit_receiver) = mpsc::sync_channel(1);
        let join = thread::spawn(move || {
            let _exit = ActorExit::new(exit_sender);
            let _ = release_receiver.recv();
        });
        let host = ActorHost::new(join, exit_receiver, Duration::from_secs(30));

        let started = Instant::now();
        drop(host);
        assert!(started.elapsed() < Duration::from_secs(1));
        release_sender.send(()).expect("release detached host");
    }
}
