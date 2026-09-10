# Kafka producer host

`producer::HostProducer::start(config)` validates and provisions a native Linux
producer on one dedicated owner thread. It returns a thread-safe `ProducerClient`,
selected backend and startup calibration; normal events report broker and topic
readiness. `close(timeout)` fences admission, while `join()` waits for actual
actor termination. Dropping the wrapper requests immediate close and detaches
observation. Application-owned input leases must still be released. Startup
failure/panic returns an error; owner failure fences the client and is visible
through `status()` and `join()`. Unsupported platforms fail explicitly.

Before allocating production pools, the owner encodes a fixed mixed-entropy
1 MiB sample with the actual zstd encoder. The measured `encode_bytes_per_poll`
is clamped to 16 KiB..1 MiB and remains fixed for the run; simulation retains its
explicit scenario quota.

The wrapper projects validated `ProducerConfig` through `connection_config()`
and supplies `ProducerSetupBudget(engine.credits())` to the reusable host. This
adapter uses the existing producer ControlReserve authority without a second
charge. Native setup remains cold until first poll. The producer actor applies
its strict API capability profile before accepting the negotiated connection.
Generic DNS/TLS/SASL implementation and provider diagnostics remain in
[`kr-kafka-host`](../kr-kafka-host/README.md).

`start_with_diagnostics` enables bounded completion timing and passive weak
provider/runtime observers. Cloning `diagnostics()` never keeps an operation,
provider, runtime or owner thread alive.

Rust callers moving from `kr-kafka-host::producer` must depend on this crate and
import `kr_kafka_producer_host::producer::{HostProducer, HostStatus}`. Calibration
similarly moves to `kr_kafka_producer_host::calibration`. No compatibility alias
creates a producer dependency inside the reusable host. The public C ABI and
benchmark CLI stay unchanged.

Run `RUSTC_WRAPPER= cargo test -p kr-kafka-producer-host --offline`. Portable tests
cover actual zstd calibration, retained setup-credit ownership and owner thread
startup/abandonment/close/panic behavior. The Linux smoke example now belongs to
this package: `cargo run -p kr-kafka-producer-host --example produce -- …`.
