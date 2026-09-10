# Kafka producer

This workspace implements the nontransactional producer in
[producer_design.md](../producer_design.md). Brokers must support **Produce v13**,
Metadata v12 and InitProducerId v4. Topic UUIDs are immutable identities; an old
handle never follows a deleted topic name to its replacement. Produce fallback
is deferred.

The producer supports idempotent acknowledgement-all delivery, none/zstd
compression, keyed and byte-quota partitioning, native bulk/run partitioners,
external partition hints, bounded accepted-prefix admission, input leases,
delivery events, flush, cancellation and close. Linux hosting provides explicit
io_uring/readiness backends, TLS, and SASL PLAIN/SCRAM-SHA-256/SCRAM-SHA-512.
PLAIN requires verified TLS. Transactions are outside this version's scope.

The portable actor, independent broker simulation and native Linux provider
have conformance tests, including blocked warm/cold vectored ownership checks.
Auto fallback remains disabled pending an explicit policy change. See the
[simulation guide](kr-kafka-sim/README.md) and
[real broker harness](integration/README.md) for validation commands.

## Shared client boundary

`kr-kafka-client` owns the request-independent connection driver, connector
contract, endpoint/security settings, topic identity types, and checked
ApiVersions/Metadata/SASL codecs. It has no producer or record-encoder dependency.
`kr-kafka-host` implements native connections, TLS, SASL and bounded control work
against that shared contract. Producer policy remains in `kr-kafka-producer`;
`kr-kafka-producer-host` composes it with the host adapters and owns `HostProducer`
and compression calibration.

See [the shared client guide](kr-kafka-client/README.md) for the dependency graph
and the remaining consumer work. Rust callers of `kr_kafka_host::producer` move
to `kr_kafka_producer_host::producer` and add the matching Cargo dependency.
The C ABI and benchmark command line retain their existing interfaces.

## Try the native client

On Linux, provision a topic on a compatible broker, then run:

```sh
cargo run -p kr-kafka-producer-host --example produce -- readiness localhost 9092 events hello
# Use `uring` to require io_uring explicitly.
```

The example sends one unkeyed zstd record, prints public events, verifies its
acknowledgement and flush, and joins the producer owner on every exit path. It
uses plaintext and capacity for at most four brokers; adjust `ProducerConfig`
for the deployment. Authentication and trust configuration are described in the
[host guide](kr-kafka-host/README.md).

## APIs and validation

- [ProducerClient input ownership](kr-kafka-producer/INPUT.md): copy, acquire/commit
  and immutable registered input, with separate input-release and delivery events.
- [C ABI and Python pinning](kr-kafka-ffi/README.md): fixed-width versioned structs,
  opaque handles, accepted prefixes and actual-provider retirement at destroy.
- [Simulation and replay](kr-kafka-sim/README.md): actual Kafka frames, independent
  delivery/log oracle, seeded faults and bounded failure artifacts.
- [Host measurement and integration tools](benchmarks/README.md): fixed offered
  load, Java/librdkafka comparisons, optional native diagnostics and broker fixtures.

`Acked` means a matching broker success or duplicate acknowledgement was parsed.
`NotWritten` carries proof that the record did not commit. `Unknown` preserves an
ambiguous outcome; retrying it at the application level can duplicate a commit.
The producer can recover after an ambiguous expiry by changing its epoch once
old transmitted work settles. At epoch exhaustion it obtains a fresh producer
ID, as the nontransactional Java client does. See the
[recovery contract and ordering limits](kr-kafka-producer/RECOVERY.md).
Flush reports settlement of its captured accepted prefix, including failures.
Always inspect the delivery outcomes separately.

Input can be released before broker acknowledgement. Keep a native/foreign lease
immutable until its `InputReleased` event, and poll the event queue to return
reserved completion capacity. A delivery or close deadline cannot release
memory still owned by a provider; native join/destroy may wait longer for safe
retirement.
