//! Coherent runtime scenario shared by the standalone and embedded SBE samples.

use std::error::Error;
use std::rc::Rc;

use kr_runtime::trace::sbe::{SbeRecordingTrace, SbeTraceRetention};
use kr_runtime::{RuntimeConfig, SimDuration, SimRuntime, yield_now};
use kr_runtime_trace_tool::{
    TraceArtifactMetadata, validate_sbe_trace_artifact, write_buffered_sbe_trace_artifact,
};

pub(crate) fn build_browser_sbe_sample() -> Result<Vec<u8>, Box<dyn Error>> {
    let trace = Rc::new(SbeRecordingTrace::with_retention(
        SbeTraceRetention::PrefixAndTail {
            prefix_capacity_bytes: 64 * 1_024,
            tail_capacity_bytes: 64 * 1_024,
        },
    ));
    let mut runtime = SimRuntime::with_trace(
        RuntimeConfig {
            seed: 0x05ee_dfdb,
            ..RuntimeConfig::default()
        },
        trace.clone(),
    );
    let handle = runtime.handle();

    runtime.block_on(async move {
        let worker_handle = handle.clone();
        let worker = handle
            .spawn(async move {
                worker_handle
                    .sleep(SimDuration::from_nanos(5_000))
                    .await
                    .expect("sample timer should fit");
                worker_handle
                    .random_below(8)
                    .expect("sample random bound is non-zero")
            })
            .expect("sample worker should spawn");

        let cleanup_handle = handle.clone();
        let cleanup = handle
            .spawn(async move {
                cleanup_handle
                    .sleep(SimDuration::from_nanos(20_000))
                    .await
                    .expect("sample timer should fit");
            })
            .expect("sample cleanup task should spawn");

        yield_now().await;
        cleanup.abort();
        let _ = cleanup.await;
        let _chosen_partition = worker.await.expect("sample worker should complete");
    })?;
    runtime.shutdown()?;

    let mut output = Vec::new();
    write_buffered_sbe_trace_artifact(
        &mut output,
        trace.as_ref(),
        &runtime.snapshot(),
        TraceArtifactMetadata::new("trace-viewer-sbe-example/1", "completed"),
    )?;
    validate_sbe_trace_artifact(output.as_slice())?;
    Ok(output)
}
