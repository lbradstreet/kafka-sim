# Shared Kafka client foundation

This crate provides the request-independent layers used by the producer and
available to a future consumer. It is not a consumer implementation.

```text
kr-kafka-client        → protocol, runtime, owned I/O, shared bytes
kr-kafka-host          → client, native providers, TLS/SASL libraries
kr-kafka-producer      → client, record encoder, protocol, runtime
kr-kafka-producer-host → producer, host, record calibration
kr-kafka-ffi           → producer-host, producer
```

The shared client owns the framed connection driver, immutable send plans and
admission fences, the cold `Connector` contract, endpoint/security configuration,
passive transport counters, topic identity/metadata value types, and checked
ApiVersions, Metadata, SASL and stateless Fetch v13 request/response codecs. Host adapters implement
DNS, TLS, SCRAM and native I/O without importing producer state.

Capability discovery returns the broker's validated advertised API ranges.
Each workload applies its own requirements. Advertisements share immutable
backing across clones; native setup attaches its reservation before returning
them. Retaining capabilities after discarding a connection still retains their
setup budget. Later authentication parses use the remaining owned-response
allowance.

The producer still requires Produce v13, Metadata v12 and InitProducerId v4;
an unrelated workload does not
inherit those requirements. Producer-only response interpretation, identity,
sequencing, retries, batching and delivery remain above this crate.

Constructing a connect future admits no work. First poll starts bounded setup;
after admission its owner must retain it through actual completion. Setup and
provider buffers carry opaque lifetime guards so dropping an observer does not
return a live resource's budget. Socket progress remains distinct from a Kafka
acknowledgement. Passive transport counters do not read clocks or invoke user
callbacks.

The optional `request-observation` feature exposes a passive driver sink installed
before enqueue or I/O. It observes first transport submission for each attempt,
successful completion of every byte across partial writes, and response/retirement
completion. Enqueued requests retired without dispatch are explicitly marked.
Callbacks use the caller-supplied poll timestamp and borrow the immutable send
plan only during dispatch. Sinks must remain bounded, avoid clocks and wakes, and
report diagnostic overflow separately. With no sink, the driver allocates no
observation state. Tests compare complete driver events and runtime checkpoints
with observation enabled and disabled for staging and vectored partial writes.

`kr-kafka-producer` re-exports common endpoint, identity, connector and transport
types through its existing module paths. Rust applications using
`kr_kafka_host::producer::HostProducer` must instead depend on
`kr-kafka-producer-host` and import
`kr_kafka_producer_host::producer::HostProducer`. The C ABI and benchmark command
line are unchanged. Generic transport plan limits now specify a segment count;
producer request construction derives that count from its own partition bound.

`fetch::FetchRequest` describes one explicit topic UUID, partition, offset,
current leader epoch and byte allowance. `ControlCodec::fetch13_request` requires
the broker to advertise Fetch v13 and emits a normal replica `-1` request with
zero wait/minimum bytes, read-uncommitted visibility, session ID `0`/epoch `-1`,
and no rack, forgotten-partition or follower-history hints. There is no version
fallback. `parse_fetch13` checks the whole frame, correlation, session and exact
single UUID/partition before exposing `FetchResponse::records` as a borrowed,
opaque byte slice. Null and empty remain distinct; records are neither copied,
CRC-checked nor decompressed at this layer. The separate record crate can inspect
them under its own decoding budgets.

Kafka's requested byte allowance is soft: a first batch can exceed it to allow
progress. `ControlLimits::frame_bytes` independently bounds the actual complete
response; callers must size it for the largest supported batch plus framing.
The caller owns connection selection, deadlines, retries and its explicit next
offset. The codec never changes a metadata cache, routes to a preferred replica,
advances an offset or starts another fetch. Long polling, retention behavior and
incremental fetch sessions are outside this readback surface.

This is not a consumer implementation. ListOffsets, offset commits, coordinator
discovery, assignment, group management and consumer state machines remain
outside the crate. Producer transactions and older-Produce fallback stay deferred.
