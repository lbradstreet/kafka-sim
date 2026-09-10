//! Explicitly enabled native benchmark diagnostics; no executor or provider ownership.
use kr_runtime::{HostRuntimeDiagnostics, HostRuntimeObserver};
use kr_runtime_io::completion::{CompletionMetrics, CompletionSnapshot};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProviderPressure {
    /// Commands awaiting the native coordinator, not kernel SQEs.
    pub queued_commands: usize,
    pub streams: usize,
    pub listeners: usize,
    /// Readiness operations include terminal responses awaiting consumption.
    /// io_uring uses the independent completion counters for that measurement.
    pub retained_operations: Option<usize>,
    pub retained_bytes: Option<usize>,
}
#[derive(Clone, Debug)]
pub struct HostDiagnosticsSnapshot {
    pub runtime: Option<HostRuntimeDiagnostics>,
    pub provider: Option<ProviderPressure>,
    pub completions: CompletionSnapshot,
}
#[cfg(target_os = "linux")]
pub(crate) enum ProviderObserver {
    Uring(kr_runtime_io_uring::UringNetPoolObserver),
    Readiness(kr_runtime_io_readiness::ReadinessObserver),
}
#[cfg(target_os = "linux")]
impl ProviderObserver {
    fn snapshot(&self) -> Option<ProviderPressure> {
        match self {
            Self::Uring(o) => o.status().map(|s| ProviderPressure {
                queued_commands: s.queued_commands,
                streams: s.streams,
                listeners: s.listeners,
                retained_operations: None,
                retained_bytes: None,
            }),
            Self::Readiness(o) => o.status().map(|s| ProviderPressure {
                queued_commands: s.queued_commands,
                streams: s.streams,
                listeners: s.listeners,
                retained_operations: [
                    s.read_operations,
                    s.write_operations,
                    s.control_operations,
                    s.close_operations,
                ]
                .into_iter()
                .try_fold(0usize, usize::checked_add),
                retained_bytes: s
                    .outstanding_read_bytes
                    .checked_add(s.outstanding_write_bytes),
            }),
        }
    }
}
#[derive(Default)]
struct Sources {
    runtime: Option<HostRuntimeObserver>,
    #[cfg(target_os = "linux")]
    provider: Option<ProviderObserver>,
}
/// Cloning this handle retains only counters and weak source observers. It does
/// not keep the runtime, native provider, buffers, or completion cells alive.
#[derive(Clone, Default)]
pub struct HostDiagnostics {
    pub(crate) completions: Arc<CompletionMetrics>,
    sources: Arc<Mutex<Sources>>,
}
impl HostDiagnostics {
    #[cfg(target_os = "linux")]
    pub(crate) fn install(&self, runtime: HostRuntimeObserver, provider: ProviderObserver) {
        *self.sources.lock().unwrap_or_else(|p| p.into_inner()) = Sources {
            runtime: Some(runtime),
            provider: Some(provider),
        };
    }
    pub fn snapshot(&self) -> HostDiagnosticsSnapshot {
        let sources = self.sources.lock().unwrap_or_else(|p| p.into_inner());
        HostDiagnosticsSnapshot {
            runtime: sources
                .runtime
                .as_ref()
                .and_then(HostRuntimeObserver::snapshot),
            #[cfg(target_os = "linux")]
            provider: sources
                .provider
                .as_ref()
                .and_then(ProviderObserver::snapshot),
            #[cfg(not(target_os = "linux"))]
            provider: None,
            completions: self.completions.snapshot(),
        }
    }
}
