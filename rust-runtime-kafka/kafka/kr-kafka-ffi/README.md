# C ABI

`include/kr_kafka.h` exposes ABI version 2 using fixed-width integers, explicit
byte spans and opaque producer handles. Build the shared and static libraries
with `cargo build -p kr-kafka-ffi`. Native producer startup currently requires
Linux. Required io_uring fails if unavailable; explicit readiness is supported;
Auto fallback remains disabled until its native conformance gate passes.

Initialize configuration with `kr_producer_config_init`, then replace selected
fields. `struct_size` must exactly match the header for configuration, brokers,
records, headers and every event output slot. All configuration and credential
spans are copied before `kr_producer_create` returns. The constructor starts one
Rust owner thread; C callers never drive runtime or network progress.

Open a topic, submit a bounded record array, and drain events using
`kr_poll_events`. Submission returns the accepted prefix length. Each accepted
record owns exactly one terminal delivery event; rejected suffix records have
no token or delivery obligation. `user_token` is application correlation data.
The operation's nonzero error or `kr_last_error` explains rejection. Last error
is shared by all callers, so applications requiring an exact diagnostic must
serialize a call and its error query.

Admission commits before submission returns. A concurrent `kr_poll_events`
caller can observe a delivery during that interval. Serialize submission and
event polling when the application needs return-before-observation ordering.

Three input ownership paths are enabled:

* `kr_submitv_copy` reads immutable caller spans only during the call and copies
  accepted payload into a bounded native allocation. Free or mutate the source
  immediately after return.
* `kr_buffer_acquire` returns exclusively writable native memory and a generation
  checked lease ID. Stop every write before `kr_buffer_commit`. A failed commit
  preserves the writable acquisition. After success, use pointers within the
  committed prefix in `kr_submitv_leased`; that path validates offsets and retains
  the same allocation. `kr_buffer_release` forbids future submissions. Exactly
  one `InputReleased` event follows after every accepted record has consumed or
  discarded its input. It does not imply broker delivery.
* `kr_lease_register` retains immutable foreign memory without copying it. The
  binding must pin the full allocation against mutation, movement and reclamation
  across producer threads until `InputReleased` or until destroy returns. Register
  the full byte capacity, including any unused initialized suffix, up to
  `UINT32_MAX` bytes. On rejection no pointer remains registered.

The C lifetime harness uses read-only mapped memory and removes read access at
the exact release event; the Python fixture preinstalls a bounded strong pin to
immutable `bytes` before calling Rust and drops it only after `InputReleased` or
`kr_destroy` returns. It requests `PyBUF_SIMPLE`, retains the exporter, and calls
`PyBuffer_Release` only after that boundary, following the
[CPython buffer protocol](https://docs.python.org/3/c-api/buffer.html).
Mutable Python sources are rejected. Each additional language binding must pass
equivalent lifetime tests before exposing foreign registration. No application
callbacks run from Rust destructors or completion paths. Go and JVM foreign
bindings are currently unavailable: Go must pin retained Go allocations with
`runtime.Pinner`; JVM must retain direct `ByteBuffer` storage. Neither has binding
pinning evidence in this crate, and movable managed heap pointers are invalid.

Pointer arguments must denote live, correctly aligned storage of the declared
length, immutable for input calls and writable for output calls. Outputs must
not alias inputs. Numeric checks reject null nonempty spans, misalignment,
overflow, invalid versions and leased ranges outside the committed prefix;
they cannot establish that an arbitrary address is live. A present empty span
may use a null pointer. The explicit `*_is_null` fields distinguish Kafka null
from present empty values. Header keys are UTF-8 and cannot be null.

The FFI lease registry charges its actual vector capacity to `InputBytes` on
lane zero. Bounded, transient record/header conversion vectors charge their
actual capacities to the same pool while a submission runs. Native input is
charged once for its complete allocation. These charges can reject admission
even when payload alone would fit. Calls convert at most 4096 records and drain
at most 1024 events, further limited by configuration.

`kr_close` takes a relative timeout in milliseconds. Drain until normal `Closed`
or terminal ABORTED owner status, then call `kr_destroy` exactly once. Destroy is exclusive: finish all concurrent
ABI calls and all writes to acquired buffers first. It releases outstanding
native handles and joins the owner through actual provider retirement; elapsed
delivery deadlines never free buffers still owned by I/O. It can therefore wait
longer than the delivery timeout. A caught Rust panic fences admission and
requests cooperative runtime failure through an independent failure flag, even
when the ordinary mailbox is full. Terminal events remain drainable.

The `Closed` event's `count` is the lifetime number of `Unknown` deliveries,
saturated at `UINT32_MAX`; per-record outcomes remain exact. The native engine
status retains the full 64-bit lifetime count. Repeated recovery can exceed the
32-bit close-event summary without failing the producer.

Verification:

```sh
RUSTC_WRAPPER= cargo test -p kr-kafka-ffi --offline
RUSTC_WRAPPER= cargo clippy -p kr-kafka-ffi --offline --all-targets -- -D warnings
RUSTC_WRAPPER= cargo build -p kr-kafka-ffi --offline
cc -std=c11 -Werror -Ikafka/kr-kafka-ffi/include \
  kafka/kr-kafka-ffi/tests/abi_smoke.c -Ltarget/debug -lkr_kafka_ffi \
  -Wl,-rpath,"$PWD/target/debug" -o /tmp/kr-kafka-c-abi-smoke
/tmp/kr-kafka-c-abi-smoke
python3 kafka/kr-kafka-ffi/tests/ctypes_smoke.py target/debug/libkr_kafka_ffi.dylib
RUSTC_WRAPPER= cargo build -p kr-kafka-ffi --offline --features binding-test-hooks
cc -std=c11 -Werror -Ikafka/kr-kafka-ffi/include \
  kafka/kr-kafka-ffi/tests/binding_lifetime.c -Ltarget/debug -lkr_kafka_ffi \
  -Wl,-rpath,"$PWD/target/debug" -o /tmp/kr-kafka-c-binding-lifetime
/tmp/kr-kafka-c-binding-lifetime
python3 kafka/kr-kafka-ffi/tests/binding_lifetime.py target/debug/libkr_kafka_ffi.dylib
```

Use `libkr_kafka_ffi.so` on Linux. Rust tests compile static assertions for every
C field offset, size and alignment. Ownership tests use an actual owner thread
with an injected unavailable connector, independently of native Linux test
availability. The explicit `binding-test-hooks` artifact provides a paused
injected connector for actual C/Python ownership tests. Those hook symbols are
absent in the default production library. Build without that feature for use in
applications. The binding tests exercise real admission, owner execution, event
drain and destruction; they are not Kafka broker interoperability tests.

`tests/binding_test_hooks.h` also declares bounded submit/flush return barriers
for language-binding concurrency tests. They wait on actual owner publication
without consuming its events, and can hold a rejected submit after its diagnostic
is recorded. Observation and barrier release preserve `last_error`. A bounded
replay hook deliberately corrupts one previously drained delivery to verify
duplicate and stale-token handling. These functions are absent from the public
header and production library.

Before packaging, inspect the exact production artifact (use `.so` on Linux):

```sh
python3 scripts/kafka-java-native-symbols.py target/debug/libkr_kafka_ffi.dylib
```

This verifies every public C function is exported and rejects any `kr_test_*`
symbol. For the separate conformance artifact, `--test-artifact` instead requires
the full hook inventory. Keep its build output separate from packaged binaries.

## Metadata and owner lifecycle (ABI 2)

`kr_topic_get_status` distinguishes resolving, ready, failed, deleted, closing,
retired and stale. Closing fences admission synchronously. Retirement means the
old name registration has been removed and may be opened again; the old handle
is never reused, and accepted records retain the old UUID and their delivery
obligations until terminal publication. Failed topics retain their actual native
failure reason. `kr_topic_refresh` invalidates the current shared cache and queues
a coalesced refresh; metadata is also refreshed by the native age timer.

`kr_metadata_acquire` pins one immutable snapshot, including its UUID, refresh
generation, broker endpoints/racks, partition leaders/epochs/errors and actual
replica/ISR/offline node IDs. Every successful refresh advances the snapshot
generation, including replica-only or endpoint-only changes. Broker and partition
rows are copied in caller-owned pages of at most 1024; strings and node lists have
separate bounded readers. Snapshots survive metadata changes, topic retirement
and same-name recreation. At most `max_open_topics` handles may be pinned per
producer; held data is charged to input bytes. `kr_metadata_release` releases a
handle once; stale handles fail without affecting another snapshot. Destruction
releases all remaining snapshots. These calls require the matching ABI 4 library;
ABI 1 clients must fail the exact-version handshake rather than use mismatched
symbols or layouts.

`kr_owner_status` publishes RUNNING, CLOSED or ABORTED. Terminal status is visible
only after the owner's terminal record/fence events have been published. ABORTED
has no normal CLOSED event and may still have provider-owned input awaiting
physical release. Continue draining available terminal events, quiesce writers,
and call destroy exclusively to join remaining retirement; an idle poll does not
prove termination. Snapshot readers and status remain callable after owner
failure; snapshots acquired before termination remain readable until release.

## Batch target policy (ABI 3)

`kr_producer_config.batch_target_mode` defaults to 0 (estimated wire bytes,
including the 61-byte header). Set 1 for legacy raw-byte targets. Other values
are rejected. ABI 3 appends this field; configuration size and ABI checks reject
older layouts before reading the new field. The hard raw/output limits remain
independent of the compression estimate.

## Request grouping (ABI 4)

`request_batching_policy` is 0 for Sealed (default), 1 for SinglePartition, and
2 for BrokerReady. `reserved_request_policy` must be zero. ABI 4 has a distinct
configuration size; use matching headers, bindings and library. The
[policy contract](../kr-kafka-producer/REQUEST_BATCHING.md) describes bounded
preparation, exact request limits, fairness and independent batch targets.
