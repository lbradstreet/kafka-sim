# Test-only simulation bridge

This cdylib exposes the producer experiment catalogue, shared broker/transport
environment, and optional native `ProducerClient` actor to an owner-thread Java
driver through Panama. See the [comparison guide](../kr-kafka-experiments/CLASSIC_COMPARISON.md).
It is separate from `kr-kafka-ffi` and does not change the production ABI.

The 64-bit interface has two exported symbols:

```c
const char *kr_sim_call(const char *request);
size_t kr_sim_reply_len(void);
```

`request` is a readable NUL-terminated UTF-8 JSON object of at most 16 MiB.
The response is `{"ok": value}` or `{"error": message}`. Its pointer is borrowed,
read-only and valid until the next call on that same thread. The reported length
includes its NUL terminator. Copy/read the reply before another call. Unsigned
numeric arguments accept JSON integers or decimal strings; use strings for
full-width Java seeds. JSON evidence integers require an exact integer parser.

Each thread owns at most one session. `init` requires no existing session;
`destroy` is idempotent. **Destroy the session on its creating thread before the
thread exits or the library unloads.** Runtime cleanup uses owner-thread state;
it must not rely on platform TLS destructor order. The Java bridge uses
`try/finally` and a confined library arena. A caught simulation panic destroys
the session and returns an error. Simulated time advances only with `advance`.

| `op` | Arguments / effect |
| --- | --- |
| `catalogue` | Enumerate scenario IDs, variants and titles. |
| `init` | `scenario`, `variant`, `size`, `seed`, `adapter`, optional `profile` and `batch_target_mode` (`Raw` or `EstimatedWire`); returns original/effective manifests and adjustments. |
| `record` | `load`, `index`, current metadata `partitions`; materialize the existing Rust record recipe. |
| `accept` | `id`; submit to the native client or register a successful external admission. Refusals do not register an admitted ID. |
| `metadata` | Native adapter `topic` index; return known partition count. |
| `connect` | External adapter connection `id`, `broker`, `timeout_ns`; completion arrives through `advance`. Java uses a new ID for every physical connection. |
| `write` | External adapter `id`, unsigned `bytes` array containing a full framed request; FIFO stream writes and completion are asynchronous. |
| `disconnect` | External adapter `id`; fence stale connection completion and close its stream. |
| `advance` | Relative `until_ns`; run until the next external completion or this deadline. Return current relative time and pending completions/controls. |
| `poll` | Drain native client events, including exact delivery outcomes/reasons. |
| `close` | Native adapter `timeout_ns`; initiate producer close. |
| `evidence` | Validate stored payloads and return the complete broker log, domain history and fault counters. |
| `export` | `path`; stream the same complete evidence to a caller-selected file. |
| `history`, `export_history` | Complete diagnostic history, optionally streamed to `path`. |
| `shutdown` | Cancel runtime tasks/release streams; require zero retained network operations, connections and byte obligations. This is teardown, not a producer close observation. |
| `destroy` | Shut down and remove the session. |

The session uses the manifest's task, timer, history, network and record bounds.
Record recipes are retained compactly for independent broker-payload validation.
Large Full histories remain complete and are streamed at export rather than
duplicated into a JSON response. This experimental JSON command contract is not
a stable public client interface.

The outer `init` boundary also accepts `KR_SIM_BATCH_TARGET_MODE=Raw|EstimatedWire`
so the frozen Java harness can select either native policy without source changes.
It becomes an explicit init input, effective-manifest field and adjustment;
the original catalogue manifest is retained. Conflicting command/environment
values and unknown modes fail initialization. The session never reads the
environment. `scripts/run-classic-scenarios.py --batch-target-mode raw|estimated-wire`
sets this value explicitly and clears inherited overrides when the option is absent.
The policy controls the native actor; it does not configure classic Java batching.

`scripts/run-batching-policy-matrix.py --kafka <frozen-checkout> --out <new-directory>`
runs all Full native variants in both profiles and policies, with complete replay,
an immutable library copy, checked policy provenance, resumable jobs and lossless
artifact archival. `--cases <json>` selects explicit
`[scenario, variant, profile, seed-string]` rows for follow-up seeds.

```sh
RUSTC_WRAPPER= cargo test -p kr-kafka-sim-ffi
RUSTC_WRAPPER= cargo build --release -p kr-kafka-sim-ffi
```

The `init` command also accepts `request_batching_policy` with `Sealed`,
`SinglePartition`, or `BrokerReady`. The outer bridge accepts the equivalent
`KR_SIM_REQUEST_BATCHING_POLICY` harness override, records it in the effective
manifest and rejects conflicting command inputs. The original manifest stays
intact. `scripts/run-classic-scenarios.py --request-batching-policy` selects it;
`run-batching-policy-matrix.py --policy request-batching` compares Sealed and
BrokerReady with an immutable library and complete replay evidence.
