//! Comparable post-run binary artifact encoding benchmarks.
//!
//! The deterministic fixture is produced once by the real simulation runtime
//! and retained in both typed and byte-backed recorders before timing begins.
//! Every timed path allocates its output `Vec`, so the results include output
//! allocation as well as encoding.

use std::hint::black_box;
use std::rc::Rc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use kr_runtime::trace::sbe::{SbeRecordingTrace, encoded_event_length};
use kr_runtime::trace::{RecordingTrace, TraceSink};
use kr_runtime::{RuntimeConfig, RuntimeSnapshot, SimDuration, SimRuntime, yield_now};
use kr_runtime_trace_tool::{
    TraceArtifactMetadata, validate_sbe_trace_artifact, write_buffered_sbe_trace_artifact,
    write_sbe_trace_artifact,
};

const WORKLOAD_STEPS: usize = 1_024;
const TYPED_EVENT_CAPACITY: usize = 64 * 1_024;
const METADATA: TraceArtifactMetadata<'static> =
    TraceArtifactMetadata::new("artifact-benchmark/1", "completed");

struct Fixture {
    typed: Rc<RecordingTrace>,
    buffered: SbeRecordingTrace,
    snapshot: RuntimeSnapshot,
    event_count: usize,
}

impl Fixture {
    fn build() -> Self {
        let typed = Rc::new(RecordingTrace::new(TYPED_EVENT_CAPACITY));
        let mut runtime = SimRuntime::with_trace(RuntimeConfig::default(), typed.clone());
        let handle = runtime.handle();
        runtime
            .block_on(async move {
                for _ in 0..WORKLOAD_STEPS {
                    yield_now().await;
                    handle
                        .sleep(SimDuration::from_nanos(1))
                        .await
                        .expect("fixture timer completes");
                    black_box(handle.random_u64().expect("runtime is active"));
                }
            })
            .expect("fixture workload completes");
        runtime.shutdown().expect("fixture runtime shuts down");
        let snapshot = runtime.snapshot();

        assert_eq!(typed.dropped(), 0, "typed fixture capacity is sufficient");
        let events = typed.events();
        let event_count = events.len();
        assert!(
            event_count > WORKLOAD_STEPS,
            "fixture emitted runtime events"
        );

        let buffered_capacity = events
            .iter()
            .map(|event| encoded_event_length(event).expect("fixture event is SBE encodable"))
            .sum();
        let buffered = SbeRecordingTrace::new(buffered_capacity);
        for event in events {
            buffered.record(event);
        }
        assert_eq!(buffered.encoding_failures(), 0);
        assert_eq!(buffered.dropped(), 0);
        assert_eq!(buffered.len(), event_count);

        let mut typed_sbe = Vec::new();
        write_sbe_trace_artifact(&mut typed_sbe, &typed, &snapshot, METADATA)
            .expect("fixture SBE artifact encodes");
        validate_sbe_trace_artifact(typed_sbe.as_slice()).expect("fixture SBE artifact validates");

        Self {
            typed,
            buffered,
            snapshot,
            event_count,
        }
    }
}

fn artifact_benchmarks(criterion: &mut Criterion) {
    let fixture = Fixture::build();
    let mut group = criterion.benchmark_group("trace_artifact");
    group.throughput(Throughput::Elements(fixture.event_count as u64));

    group.bench_function("typed_to_sbe", |bencher| {
        bencher.iter(|| {
            let mut output = Vec::new();
            write_sbe_trace_artifact(
                &mut output,
                black_box(fixture.typed.as_ref()),
                black_box(&fixture.snapshot),
                METADATA,
            )
            .expect("SBE artifact encodes");
            black_box(output)
        });
    });

    group.bench_function("buffered_sbe_zero_copy_export", |bencher| {
        bencher.iter(|| {
            let mut output = Vec::new();
            write_buffered_sbe_trace_artifact(
                &mut output,
                black_box(&fixture.buffered),
                black_box(&fixture.snapshot),
                METADATA,
            )
            .expect("buffered SBE artifact encodes");
            black_box(output)
        });
    });

    group.finish();
}

criterion_group!(benches, artifact_benchmarks);
criterion_main!(benches);
