# kr-runtime Trace Explorer

This tool writes portable binary SBE artifacts from bounded `kr-runtime`
traces and opens them in a dependency-free browser visualizer. For low-overhead
capture, use `SbeRecordingTrace` and export its
retained event frames directly to a binary artifact after the run. The browser
decodes that artifact locally without an intermediate text format. The timeline is
ordered by the runtime's diagnostic event sequence by default, with a
virtual-time view available for timing analysis. Virtual time defaults to an
exact offset from the run's recorded start and can be switched to exact
absolute virtual time. The time axis remains bounded by the recorded start and
terminal instants even when filters hide events at either edge.

Generate a binary trace and open the viewer:

```text
cargo run -p kr-runtime-trace-tool --example generate_sbe_trace -- /tmp/dst-trace.sbe
open tools/trace-tool/index.html
```

Two artifacts captured from the same seed can be compared with
`kr_runtime_trace_tool::diff_sbe_trace_artifacts`, which reports the first
divergent retained event — or, when the event streams agree, the first
differing terminal field — instead of a bare determinism-checkpoint
mismatch. Artifacts whose reproduction inputs differ (seed, versions,
runtime configuration, retention or sampling policy) are rejected as
incomparable rather than diffed.

Choose `/tmp/dst-trace.sbe` in the page. Runtime trace input is SBE-only. The
page also starts with a generated, embedded trace from the same coherent runtime
scenario as `generate_sbe_trace`, decoded through the same strict binary path
as a selected file. It can therefore be inspected without generating a file or
running a server. Everything is local: the viewer uses the browser File API and
makes no network requests.

The runtime page admits at most 100,000 presentation events, 4,096 task catalog
entries, and 16 event families. It folds task lanes separately and caps canvas
CSS dimensions, backing pixels, and pointer hit regions so a valid bounded file
cannot request an unbounded rendering surface.

## Reusable static viewer foundation

Scenario-specific viewers can reuse the dependency-free foundation exercised by
the ring and network examples:

- `trace-viewer-core.js` provides exact unsigned integer handling, strict
  generated-wrapper parsing, compact and nanosecond-exact time formatting,
  adjacent-step navigation, and token-preserving JSON syntax highlighting.
- `trace-viewer-ui.js` provides safe JSON DOM rendering, responsive SVG operation
  timelines, file loading/reset/error handling, resize observation, and an
  accessible keyboard-aware step navigator.
- `trace-viewer.css` provides the shared layout, theme tokens, timeline, controls,
  selected-step detail, outcome colors, and JSON dump styling.

Keep artifact validation in a DOM-free `<component>-trace-model.js` and keep
domain diagrams in `<component>-trace-viewer.js` plus a small component
stylesheet. Load the scripts as classic scripts in dependency order so the page
continues to work directly from `file://`; no bundler, server, or network access
is required.

## Reusable schema-driven SBE decoding

`sbe-ir.js` is a dependency-free, schema-neutral decoder for browser viewers.
It reads the official binary SBE intermediate representation, decodes the
schema's message header, and walks fixed fields, composites, enums, sets,
repeating groups, and variable data without generated JavaScript codecs.
Signed and unsigned 64-bit integers remain `BigInt` until a domain adapter
chooses a display representation; unencoded variable data is returned as a
copied `Uint8Array`. All reads are bounded, UTF-8 decoding is fatal, and
caller-overridable limits cover IR size, message size, nesting, group counts,
and variable data.

SBE does not define transport or file framing. A viewer still needs a small
adapter for its envelope, frame boundaries, record order, and domain
invariants. For example, `sbe-decoder.js` owns the `DSTRSBE` container and trace
semantics; the generic layer owns only schema-directed SBE decoding.

Compile any SBE XML schema with the repository's pinned official tool and emit
a classic-script module that can be loaded directly from `file://`:

```text
./scripts/generate-browser-sbe-ir.sh path/to/schema.xml MySchemaIr path/to/my-schema-ir.js
```

Load `sbe-ir.js` before that generated module. A framing adapter can then decode
one bounded message as follows:

```js
const irBytes = SbeIr.decodeBase64(MySchemaIr.base64);
const schema = SbeIr.parse(irBytes);
const decoded = SbeIr.decodeMessage(schema, frameBytes, {
  offset: messageHeaderOffset,
  end: frameEnd,
  limits: { maxVarDataBytes: 1024 * 1024 },
});
```

The XML compiler remains the authority for XSD validation and SBE layout
semantics. The generated module embeds its `.sbeir` bytes and tool version; it
does not embed executable schema-derived decoding logic.

The committed viewer samples are stable, versioned demonstrations rather than
snapshots of every change to the runtime scenarios that produced them. Rust
tests check that each current generator is deterministic, valid, and still
covers its contract milestones; browser tests independently validate and render
the committed samples. Refresh a bundled sample only when intentionally
improving the demonstration or dropping support for its schema version. Exact
wire-format goldens remain byte-pinned.

## File-ring wrap and recovery trace

The ring-specific page is generated from the same assertion-bearing scenario as
`trim_checkpoint_releases_space_and_recovery_crosses_implicit_wrap`. It follows
the simulated file provider through physical capacity pressure, a pending trim,
durable reclamation, wraparound, crash recovery, and final cleanup:

```text
cargo run -p kr-runtime-trace-tool --example generate_ring_trace
open tools/trace-tool/ring-trace.html
```

With no output argument, the generator deterministically refreshes the bundled
`ring-trace-data.js` sample. It can also write a trace elsewhere:

```text
cargo run -p kr-runtime-trace-tool --example generate_ring_trace -- /tmp/ring-trace.js
```

Use the page's file chooser to load either the generated JavaScript wrapper or
raw JSON containing the same ring-trace artifact. The reset control restores
the bundled sample without requiring a page reload. The page and its bundled
sample work directly from `file://`; no local server or network access is
required. Outside form controls, Left Arrow and Right Arrow select the previous
and next diagnostic steps without wrapping at the trace boundaries.

Its step order is a diagnostic scenario order, not the runtime's authoritative
trace sequence. Explicit `status()` probes provide the logical and physical
snapshots shown after each operation. The model admits at most 4,096
presentation steps. An `unknown` domain outcome remains visually and textually
distinct from a known rejection. Run the source scenario, deterministic
artifact check, and browser model checks together with:

```text
./scripts/check-ring-trace-viewer.sh
```

## SimNetwork directional flow trace

The network example applies the same viewer foundation to a different trace
shape. Its assertion-bearing scenario follows both directions of one simulated
connection through partial I/O, a partitioned write that is known not to have
applied, capacity backpressure, recovery, half-close, buffer draining, and EOF:

```text
cargo run -p kr-runtime-trace-tool --example generate_network_trace
open tools/trace-tool/network-trace.html
```

With no output argument, the generator deterministically refreshes the bundled
`network-trace-data.js` sample. The page works directly from `file://` and can
also load raw JSON or another generated wrapper. Its before/after duplex view,
directional occupancy chart, operation timeline, keyboard stepper, artifact
loader, and colorized field dump deliberately demonstrate where shared viewer
components end and domain-specific rendering begins.

Run the shared browser checks, source scenario, and deterministic generator test
together with:

```text
./scripts/check-network-trace-viewer.sh
```

## SimStorage SBE durability trace

The storage example exercises both reusable viewer layers: the shared static UI
and the schema-driven SBE decoder. Its assertion-bearing scenario compares
`SimStorage` with an independent accepted/durable byte-vector model while it
passes through an ambiguous failed `sync`, clean crash rollback, reopen, a
successful durability fence, an unsynced overwrite, and final recovery.

Generate the bundled single-message SBE artifact and open the viewer:

```text
cargo run -p kr-runtime-trace-tool --example generate_storage_trace
open tools/trace-tool/storage-trace.html
```

The page works directly from `file://`. To generate a raw artifact for the file
chooser instead of refreshing `storage-trace-data.js`, pass a `.sbe` path:

```text
cargo run -p kr-runtime-trace-tool --example generate_storage_trace -- /tmp/storage-trace.sbe
```

The presentation-only schema is
`trace/kr-runtime-ring-trace-wire/schema/storage-trace.xml`; it does not
change the runtime diagnostic trace contract. The pinned official SBE tool
generates both its Rust codec and the browser IR consumed by `sbe-ir.js`:

```text
./scripts/regenerate-storage-trace-sbe-codecs.sh
```

Use `./scripts/regenerate-storage-trace-sbe-codecs.sh --check` to regenerate
into a temporary directory and compare the result with the committed codec and
browser IR without changing the worktree. The storage viewer check below runs
this drift check automatically, so it requires the pinned SBE jar too.

Run the source scenario, codec round trip, deterministic artifact generation,
shared UI checks, and browser-model corruption suite together with:

```text
./scripts/check-storage-trace-viewer.sh
```

## Capturing into bytes

Keep the trace sink passive during execution, then export once the run reaches
the point that should be diagnosed. Capacities are bytes and include every
event's four-byte total-length prefix and eight-byte SBE message header:

```rust
use std::rc::Rc;
use kr_runtime::{RuntimeConfig, SimRuntime};
use kr_runtime::trace::sbe::{SbeRecordingTrace, SbeTraceRetention};
use kr_runtime_trace_tool::{TraceArtifactMetadata, write_buffered_sbe_trace_artifact};

let trace = Rc::new(SbeRecordingTrace::with_retention(
    SbeTraceRetention::PrefixAndTail {
        prefix_capacity_bytes: 64 * 1_024,
        tail_capacity_bytes: 64 * 1_024,
    },
));
let mut runtime = SimRuntime::with_trace(RuntimeConfig::default(), trace.clone());

// Run the simulation and capture its terminal snapshot.
runtime.shutdown()?;
let snapshot = runtime.snapshot();

let mut artifact = Vec::new();
write_buffered_sbe_trace_artifact(
    &mut artifact,
    trace.as_ref(),
    &snapshot,
    TraceArtifactMetadata::new("my-harness/1", "completed"),
)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Construction allocates the configured prefix and tail buffers at their final
sizes. Recording a schema-valid runtime event then allocates no memory and does
no filesystem I/O. Frames are encoded directly into the buffer; a tail may move
surviving bytes in place when it evicts old frames. Prefix retention closes on
the first event that does not fit, while tail retention evicts complete oldest
frames. An oversized or unencodable event clears the old tail before later
events are retained so the tail remains a true suffix.

The buffered exporter is zero-copy for event frames within the exporter: it
visits the retained slices and writes them without decoding, allocating, or
re-encoding those frames. The destination writer or operating system may still
copy the bytes, and header and terminal snapshot encoding still allocate after
the run. If the recorder reports any SBE encoding failure, export returns
`ExportError::BufferedTraceEncodingFailures`; it never silently publishes the
remaining frames. Capacity drops remain valid artifacts and are described in
the artifact header.

`write_sbe_trace_artifact` can translate an event-count `RecordingTrace` to
binary after the run, but it must first snapshot and SBE-encode its retained
events.

## Binary artifact and validation

The current format has four independent version coordinates:

```text
container magic/version       DSTRSBE\0 / 1
SBE schema ID/version         1 / 1 (semantic version 2.0.0)
artifact envelope schema      8
diagnostic trace schema       5
```

Artifact schema 8 describes the binary container. It is not SBE schema version
8. The binary file begins with a 16-byte preamble, then little-endian `u32`
total-length-framed SBE messages in this order:

1. One artifact header.
2. The declared deterministic random-stream checkpoints.
3. The declared live-task snapshots.
4. The declared retained runtime events.

The Rust validator and browser decoder validate the preamble, exact schema
versions, message order and templates, frame and metadata bounds, declared
counts, sampling membership, canonical presence fields, and absence of trailing
data. A harness can validate a completed artifact without converting it:

```rust
use std::fs::File;
use std::io::BufReader;
use kr_runtime_trace_tool::validate_sbe_trace_artifact;

let input = BufReader::new(File::open("trace.sbe")?);
validate_sbe_trace_artifact(input)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The browser's dependency-free JavaScript decoder applies the same fail-closed
container, schema, framing, metadata, UTF-8, event-count, and trailing-data
checks before installing a trace. It uses `BigInt` while reading every `uint64`
and converts those values to decimal strings only at the presentation boundary,
preserving full precision. Runtime trace interchange remains binary SBE.

The committed browser golden pins container 1, SBE schema 1 version 1,
artifact schema 8, and diagnostic trace schema 5. Its exhaustive fixture covers
every event template and nested wire shape, all task-snapshot states, all random
streams, byte prefix-plus-tail retention, sampling and ordering metadata,
leading UTF-8 BOM data, and full-width integers. Separately, the same generator
writes `dst-trace-sample.js` from a coherent runtime execution for the page's
initial sample. Rust checks keep that sample generator deterministic and valid,
while the Deno suite checks the committed module's complete RNG snapshot and
draw history. Run those checks and the
golden parity/corruption suite together with:

```text
./scripts/check-trace-viewer-sbe.sh
```

The Deno command is granted read access only to the committed viewer fixtures
and source files exercised by its wiring tests; the viewer itself still has no
runtime dependency. After an intentional format change, regenerate the
reference artifact, review it with the schema/code change, and rerun the
hardening command:

```text
cargo run -p kr-runtime-trace-tool --example generate_browser_sbe_goldens
./scripts/check-trace-viewer-sbe.sh
```

The artifact header carries the kernel reproduction and checkpoint schema
versions, complete `RuntimeConfig`, trace integrity, byte- or event-based
capacity, and truncation metadata. The `retention` header describes
the capture policy. For event-count recorders it identifies whether a non-zero
`dropped_events` count omitted later, earlier, or middle events. For byte-count
recorders, it reports generically that observed events are absent because an
oversized frame can act as a suffix barrier. An `ordering_violation` marks a
rejected non-increasing event sequence; it is shown as corruption independently
of retention truncation.

When the byte recorder is wrapped in `SamplingTrace`, export with
`write_sampled_buffered_sbe_trace_artifact` and pass both the recorder and
sampling wrapper. The corresponding typed entry point is
`write_sampled_sbe_trace_artifact`.
The schema then records the periodic sampling mode, algorithm version, period,
phase, and sampled fingerprint scope separately from retention drops. The
browser shows sampling as an incompleteness warning even when the recorder
itself dropped no events. This artifact version requires the sampler to wrap
that recorder directly; nested sampling policies are rejected rather than
serialized incompletely.

The artifact envelope has its own `artifact_schema` version, separate from the
SBE codec, runtime reproduction, determinism checkpoint, and diagnostic
`trace_schema` versions. The binary reader currently requires the
exact supported container, SBE, artifact, and trace schema coordinates; unknown
versions and templates are rejected instead of guessed.

For unsampled traces, `trace_fingerprint` covers the full ordering-valid
observed diagnostic stream, including events omitted after the recorder fills
or before SBE retention succeeds. `RecordingTrace` and `SbeRecordingTrace`
produce the same fingerprint for the same input events because it hashes the
canonical typed event fields, not retained data or SBE bytes. For sampled traces
it covers only admitted events and their original gapped sequences. It is trace
integrity metadata, not replay identity. Readers preserve this stored
fingerprint rather than recomputing it from the retained subset.

## Regenerating the SBE codecs

The source schema is `trace/kr-runtime-trace-wire/schema/dst-trace.xml`. Generated Rust
files under `trace/kr-runtime-trace-wire/src` and the browser's `dst-trace-sbe-ir.js` module
are replaced from the same compiler run and must not be edited by hand. The
regeneration script pins SBE tool `1.38.1` and verifies the jar's SHA-256 before
running it. Put `sbe-all-1.38.1.jar` at the script's default `/tmp` path, or set
`SBE_JAR` to the verified jar, then run:

```text
./scripts/regenerate-trace-sbe-codecs.sh
cargo test -p kr-runtime-trace-wire -p kr-runtime -p kr-runtime-trace-tool
```

For a non-mutating drift check, run:

```text
./scripts/regenerate-trace-sbe-codecs.sh --check
```

This regenerates into a temporary directory and compares both the Rust codec
and browser IR with their committed versions. `check-trace-viewer-sbe.sh` runs
the drift check automatically, so it also requires the pinned SBE jar.

An XML or codec change is a persisted-format change. Review whether the SBE
schema, artifact envelope, diagnostic trace schema, and container version need
to advance independently, and keep round-trip, corruption, and compatibility
tests synchronized. A coordinate change must also rename and intentionally
refresh the version-labelled browser goldens under `testdata`.

## Adding component key points

The viewer visualizes the runtime's existing task, timer, random-choice,
time, cancellation, and failure events. Storage, ring-buffer, broker,
and checker key points should use causal operation IDs and bounded scalar fields.
Task lanes use stable task IDs; `TaskSpawned` records only the task and optional
parent relationship.
Diagnostic-only annotations should use a separate ordering envelope when they
are not part of the runtime trace. Adding a trace event changes only the trace
sequence and trace fingerprint, not the untraced runtime or terminal
determinism checkpoint. Behavior-defining fault and I/O outcomes should remain
typed, versioned events when traced.

## Producer experiment explorer

Open `producer-experiment.html` directly from the filesystem. It loads the pinned,
replayed 48-record broker-crash diagnostic without a server or network dependency.
Use **Load report or bundle** to inspect experiment JSON or the exact generated
JavaScript wrapper (48 MiB file limit; 5 MiB per run). The separate experiment CLI
and 37 scenario families are documented in `kafka/kr-kafka-experiments/README.md`.

The same explorer also accepts `kr-producer-comparison/v1` paired artifacts from
the classic Java/Panama scenario runner. Generate a self-contained comparison
gallery from completed evidence, without rerunning the simulations:

```sh
python3 -B kafka/kr-kafka-experiments/analysis/classic_visualization.py \
  target/classic-scenarios/final-test target/classic-scenarios/final-full \
  --out target/classic-scenarios/viewer
open target/classic-scenarios/viewer/index.html
```

Paired mode overlays classic Java (blue, solid) and native/Panama (orange, dashed)
with shared time controls, fault bands, exact population counts, latency curves
and partition selection. The selector changes a matched variant/profile pair
together. Missing native-only Java metrics stay explicitly unavailable; Java
failure does not become a native certainty category. Read the
[comparison contract](../../kafka/kr-kafka-experiments/CLASSIC_COMPARISON.md)
before interpreting differences. A compressed complete recovery fixture generates
the byte-pinned `producer-comparison-data.js` sample with the exporter's `--sample`
flag. The ordinary native sample remains the explorer's default.

For the Shared/PartitionPressure outage trial,
[`classic_admission.py`](../../kafka/kr-kafka-experiments/analysis/classic_admission.py)
adds exact healthy-destination refusal counts and 100 ms progress windows. It
uses these styles and links back into the paired explorer for zooming and
partition selection. Run it after exporting the Full original-profile
independent-source policy pairs; common-profile pairs do not retain
PartitionPressure. See the script's `--help` for its input and output options.

Each catalogue page explains its experimental question before the charts. The
setup describes the actual primary run's traffic, destinations, producer limits,
transport, shared capacities and timed faults, with a readable comparison of the
parameters on that page. Changing the primary run updates the setup. Test-size
adjustments come from the recorded load rather than the variant's requested
count; repeated bursts and large programs are summarized with explicit limits.
The interpretation notes distinguish source feedback, admission refusals and
consumed delivery latency. The topology section is labeled **Partition topology
at cursor time**.

Select a primary run and up to five overlays. Drag the fault strip or enter time
bounds; `[` / `]` pan, `-` / `=` zoom, `0` resets, and arrow keys step through events.
Native controls retain their own keyboard behavior. All time charts share a
cursor, and topology follows the selected bucket's immutable partition identity.
The canvas heatmap retains all bounded partitions; topology lanes and reason rows
are paginated. Pool selection supports six simultaneous observed-utilization
series. Canvas backing stores cap device ratio at two and allocation at 16 Mi pixels.

Whole-run summary tables, ECDF ranks, attempts and reasons use the complete
population. The scatter and its brushed statistics use retained record rows and
explicitly state sample/population counts. Bucket selection includes intersecting
buckets; exact fault-window assertions are derived in Rust before sampling.
Client dispatch, local write completion, broker receipt, consumed delivery and
owner HDR intervals retain distinct measurement origins. HDR quantiles are
ranges, and pool occupancy is observed rather than continuously measured.

Run `RUSTC_WRAPPER= ./scripts/check-producer-experiment-viewer.sh` for deterministic
sample generation, DOM-free corruption tests, shared timeline tests, and viewer
execution with a small DOM/canvas test double. That execution test checks control
wiring and inert text at desktop/narrow dimensions; it does not replace visual
browser, theme, accessibility or console inspection.
