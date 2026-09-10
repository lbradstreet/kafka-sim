# Kafka protocol codecs

`kr-kafka-protocol` provides offline-generated Rust codecs for the initial
idempotent, nontransactional producer and explicit stateless Fetch v13 readback.
It is `no_std + alloc`, forbids unsafe
code, has no runtime or I/O dependency, and borrows decoded strings, bytes,
records, and arrays directly from a validated frame.

The tested negotiation surface is deliberately independent of the latest
versions recognized by the JSON schemas:

| API | Versions | Purpose |
| --- | --- | --- |
| Produce | 9–13 | Topic names at 9–12, topic IDs at 13 |
| ApiVersions | 0, 3 | Negotiation and explicit v0 error fallback |
| Metadata | 12 | Resolve names and refresh by topic ID |
| Fetch | 13 | Read by explicit topic ID, partition and offset |
| InitProducerId | 4 | Producer identity and epoch |
| SaslHandshake | 1 | Authentication mechanism negotiation |
| SaslAuthenticate | 2 | Authentication messages |

Both requests and responses are generated, together with request headers 1/2
and response headers 0/1. `SUPPORTED` lists every tested `(api_key, version)` and
its header versions. Recognizing a schema version does not enable it for broker
negotiation. Record-batch encoding, compression, authentication mechanisms,
transport, and the producer state machine belong to their respective layers;
`records` is an opaque byte span here. Fetch v13 uses request header 2 and
response header 1. The reusable client's checked Fetch wrapper fixes a single
UUID/partition, immediate stateless request; generating this wire layout adds no
consumer state machine. Fetch byte allowances are soft on the broker, while
`DecodeLimits::max_bytes` remains the hard bound on received frames.

## Use

```rust
use kr_kafka_protocol::{Request, api_versions_request};
use kr_kafka_protocol::plan::EncodeLimits;

let request = Request::ApiVersionsRequest(api_versions_request::View::V3(
    api_versions_request::v3::ApiVersionsRequest {
        client_software_name: "kr-kafka",
        client_software_version: "0.1.0",
        ..Default::default()
    },
));
let plan = request.plan_frame(3, 42, Some("my-producer"), EncodeLimits::default())?;
let bytes = plan.to_vec()?; // Explicit contiguous staging for a small request.
# Ok::<(), kr_kafka_protocol::wire::Error>(())
```

For Produce, construct the appropriate `produce_request::v9` or `v13` types,
wrap arrays with `Sequence::from_slice`, and use `Records::Chunks` to reference
`SharedBytes` allocations. Planning copies protocol metadata into an arena and
retains shared record spans. Adjacent metadata writes coalesce. `EncodeLimits`
caps bytes, metadata bytes, segments, aggregate array elements, tags, and depth.
A limit failure returns no plan from the top-level planning method.

`SendPlan::try_into_owned` moves a plan to `'static` without allocating or
copying when its external spans all use shared ownership. A plan with borrowed
record spans is returned unchanged on failure. `SendPlan::cursor` supports
bounded staging; advancing it requires the transport's confirmed byte count,
so short writes preserve the unsent suffix. The planner does not perform I/O.

`frame::decode_response` checks the complete frame length, expected correlation
ID, header, body, nested arrays, and known tag payloads before returning a view.
`DecodeLimits` bounds total input bytes, aggregate array elements and tags, and
nesting depth. Empty and null remain distinct. Unknown tags are retained as
borrowed spans and merged in numeric order on re-encoding. Duplicate tags,
noncanonical varints, invalid UTF-8, invalid boolean bytes, trailing bytes, and
truncated nested values fail closed.

Array views validate eagerly without allocating. Their iterators re-decode the
validated span and yield `Result<T>`; retaining long-lived metadata requires an
explicit copy by the caller. Repeated traversal of nested views costs additional
parsing, bounded by the configured depth. Raw `Reader` and `Writer` helpers are
available for composing codecs; discard either after an error. Top-level decode
and plan functions enforce complete success before exposing a result.

## Compiler and regeneration

The compiler is `tools/kafka-codegen`. The original user-supplied JSON files stay
in the repository's root `schemas` directory. `schemas/PROVENANCE.lock` records
an immutable Apache Kafka commit, source archive checksum, and every schema's
SHA-256. All 201 schemas were verified byte-for-byte against that commit.

```sh
cargo run -p kr-kafka-codegen
cargo run -p kr-kafka-codegen -- --check
cargo test -p kr-kafka-codegen -p kr-kafka-protocol
```

Generation is offline; there is no `build.rs`, network fetch, or Java dependency
in a normal build. The compiler verifies the complete schema file set and
checksums, parses comment-aware JSON, validates every schema, resolves the
selected versions, groups recursively equivalent layouts, and emits codecs,
the supported API registry, and Java fixture checks. The generated files are
checked in and stamped with the provenance manifest's SHA-256. `--check` is a
read-only comparison against freshly generated, rustfmt-formatted output.

The parser rejects unknown properties, types, malformed version ranges,
invalid defaults, reference cycles, inconsistent common structs, invalid
nullability, invalid tag ranges, duplicate tags, and unsupported constructs.
The backend validates its public IR before emission, including Rust identifier
collisions. Schema parsing and layout expansion have explicit resource bounds.

Each layout class has a distinct Rust type. Absent fields cannot be represented
in that type, so changing versions cannot silently drop a non-default field.
A `View` accepts only versions in its class. Nested generated codecs contain
resolved encodings rather than repeated version checks. Defaults and known-tag
elision follow the schemas; nullable struct markers and field-level classic
string overrides are resolved explicitly.

The parser handles the full pinned corpus, while the emitted production surface
is the selection above. Constructs outside that corpus which the backend cannot
faithfully represent return an explicit compiler error; successful parsing alone
is not a promise that every hypothetical schema can be emitted. The backend is
also tested by compiling and executing synthetic generated Rust.

## Independent wire oracle

`tests/fixtures/java-wire.json` contains 68 cases captured by Kafka's Java
message classes: defaults, populated requests/responses, null/empty values,
topic IDs, unknown tags, and Produce/ApiVersions/Fetch known tags. Rust tests check
all selected API versions, decoded field values, default encoding, exact wire
round trips, every truncated fixture prefix, and trailing data. Deterministic
malformed-input campaigns and planner tests cover resource bounds and ownership.

See [fixture provenance and capture instructions](tests/fixtures/README.md).
Fixture refresh is a separate explicit command that uses pinned Java artifacts;
it is unnecessary for normal regeneration or Rust tests.
