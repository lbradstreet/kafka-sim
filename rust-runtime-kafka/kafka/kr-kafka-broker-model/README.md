# Passive Kafka broker model

`BrokerModel` is a bounded `no_std + alloc` cluster model with no producer
crate dependency, executor, clock, sockets, or hidden tasks. A per-connection
runtime actor supplies complete frames to `handle_frame(broker_id, bytes, fault)`
and writes the returned `BrokerAction::Reply` bytes through its real stream.
That actor owns framing accumulation, partial I/O, delay, and connection closure.

The model decodes and emits actual generated Kafka protocol frames. It serves
ApiVersions 0/3, Metadata 12, InitProducerId 4, Produce 9–13, and Fetch 13. Its configured
Produce maximum is 9 or 13; the strict producer uses 13. SASL is explicitly
outside this unauthenticated broker model, and is not advertised. Metadata can
select by name or ID, lists leaders/epochs and replicas, and supports partition
expansion. Deleted IDs are never reassigned, their committed logs remain, and
recreating a name allocates a fresh ID. Configuration bounds lifetime topic IDs,
partitions, brokers, producer states, strings, frame size, records and log bytes.
Capacity exhaustion rejects before appending or advancing producer sequence state.

Produce validates exactly one magic-2 batch per partition, CRC32C, every record,
and none/zstd compression through the bounded record inspector. Zstd expansion
and decoder window size have independent caps. Committed logs retain neutral
record fields (including values and headers), topic IDs, producer identity,
sequence and offsets. The last five sequence ranges per producer/partition are
retained for deduplication. Epoch changes and 31-bit sequence wrap follow Kafka's
producer-state rules. Lost-response retries return the original successful offset
by default; `duplicate_sequence_error` exercises the explicit duplicate code.

Fetch is a stateless, immediate, single-UUID/partition raw probe. It supports
explicit offsets, current leader epochs, empty/end responses, and whole batches
for offsets inside a batch. Global and partition byte limits are soft for the
first batch; the encoded frame bound is always hard. A first batch that cannot
fit that absolute bound returns an explicit model limit error. Unknown/deleted
IDs, invalid partitions, wrong leaders/epochs and out-of-range offsets return
Kafka partition errors. Sessions, groups, transactions, long polling and retention
are outside this probe; requests for those semantics are rejected.

The log also retains the original wire batch, patching only base offset and
leader epoch outside the CRC-covered bytes. `wire_batch(index)` aligns with the
independent decoded `log()`. Per-partition log indexes support binary offset
lookup. `max_log_bytes` and `stats().log_bytes` include both logical decoded batch
bytes and original-wire allocation capacity before sequence/offset mutation;
`decoded_log_bytes` and `wire_log_bytes` expose the breakdown. Container metadata
has cardinality bounds and is not a claimed total allocator/RSS measurement.

`FaultPlan` can drop a request after parsing, reject before committing, drop a
response after committing, disconnect before responding, move a leader before
processing, and set throttle time. The returned drop/disconnect actions report
how many batches committed in that invocation. No fault fabricates a commit;
retry and outcome assertions examine the actual log.

Two protocol facts are deliberately represented:

- Nontransactional InitProducerId always returns a **fresh PID, epoch zero**,
  even when expected PID/epoch fields are supplied. Recovery must install the
  returned identity. This follows pinned `TransactionCoordinator.scala` lines
  124–132, rather than synthesizing an epoch increment.
- Produce 9–12 resolves names when the broker processes a request. A same-name
  recreation after a client's metadata check can therefore receive that request;
  no old topic ID is present to validate. A regression demonstrates this race
  alongside Produce 13 rejecting the old ID. The producer's strict identity mode
  requires Produce 13.

`DeliveryOracle` consumes neutral accepted-prefix observations, independently
assigned routing identities, parsed-response evidence, delivery/release/flush/
close events, input-consumption observations, credit snapshots, and provider
retention. It checks deliveries against actual committed record bytes using a
harness-supplied token extractor. Harness tokens uniquely identify accepted
records and can differ from repeated application user tokens. Observation
ordinals express happens-before; simulated timestamps may be equal. Oracle
memory has separate explicit bounds. Provider reference observations must be
released alongside terminal provider completion before checking teardown.

Tests cover real frames for every declared API/layout, all loss points, sequence
history eviction and wrap, fresh identity allocation, malformed batches and
capacity rejection, metadata/leadership/recreation, and 32 replay seeds. Oracle
meta-tests drop, duplicate, reorder and rebind committed records, omit events,
flip outcomes, and violate release/flush/credit contracts. These are pure model
tests; they do not substitute for the producer actor's `SimRuntime` campaign or
production transport integration tests.

```sh
cargo test -p kr-kafka-broker-model
cargo clippy -p kr-kafka-broker-model --all-targets -- -D warnings
```

Behavior was checked against Apache Kafka revision
[`7be741d08b3b06f6414ac868e57bf9b958f53a72`](https://github.com/apache/kafka/tree/7be741d08b3b06f6414ac868e57bf9b958f53a72),
especially `TransactionCoordinator.scala`, `ProducerAppendInfo.java`,
`ProducerStateEntry.java` and `UnifiedLog.java`. The record crate's fixtures were
independently captured with the pinned Apache Kafka Java client.
