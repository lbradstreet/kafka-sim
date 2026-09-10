# Java wire fixtures

`java-wire.json` contains 68 independent message/header body encodings captured
with the generated `org.apache.kafka.common.message.*Data` classes in
`org.apache.kafka:kafka-clients:4.3.0`. Each case records its schema message name,
wire version, input field overrides, and lowercase hexadecimal wire bytes.
There is no frame length or request/response header attached to message bodies;
headers have their own fixtures.

Coverage includes both directions of Produce 9–13, ApiVersions 0 and 3,
Metadata 12, Fetch 13, InitProducerId 4, SaslHandshake 1, and SaslAuthenticate 2, plus
RequestHeader 1/2 and ResponseHeader 0/1. Cases exercise Java constructor
defaults, nullable and empty values, UTF-8 byte lengths, int64 values above the
exact integer range of binary64, UUID byte order, opaque record bytes, nested
arrays, known tagged fields, omitted default struct tags, and unknown tags at
multiple nesting levels. `ApiVersionsResponse` always uses ResponseHeader 0;
its body can nevertheless use flexible version 3.

The field description uses the schema's PascalCase names. Omitted fields use
the Java constructor's schema default. Objects and arrays are recursive. UUIDs
use canonical hyphenated hexadecimal; bytes and records use `{"hex": "..."}`.
`_unknownTaggedFields` contains `{"tag": 4, "dataHex": "00ff"}` entries. These
records payloads are deliberately opaque; record-batch parsing is a separate
layer. Fetch cases pin the immediate sessionless request fields, offsets above
binary64's exact range, null/empty/opaque record sets, unknown topic IDs and known
and unknown tags at nested levels. The ApiVersions v0 request case with software-name/version overrides
checks Java's `ignorable` behavior: these fields do not exist in v0 and the
encoded body is empty.

To check the fixtures using JDK 17+ and Python 3:

```sh
python3 scripts/kafka-java-fixtures.py --check
```

To replace their captured bytes after intentionally editing case inputs:

```sh
python3 scripts/kafka-java-fixtures.py --write
```

To additionally verify the supplied schemas against the immutable upstream
source archive:

```sh
python3 scripts/kafka-java-fixtures.py --check --verify-upstream
```

The script downloads the pinned jars to a temporary cache directory (override
with `--cache PATH`), verifies hard-coded SHA-512 digests, compiles the capture
helper, and uses Java `Message.size`, `write`, and `read`. It checks size/write
agreement, complete decoding, and decode/re-encode identity. No Rust encoder,
Kafka broker, Maven, or Gradle participates in capture. Normal Rust tests read
the checked-in fixtures and require neither network access nor Java.

All 201 supplied JSON schemas byte-for-byte match Apache Kafka commit
[`7be741d08b3b06f6414ac868e57bf9b958f53a72`](https://github.com/apache/kafka/tree/7be741d08b3b06f6414ac868e57bf9b958f53a72/clients/src/main/resources/common/message).
`schemas/PROVENANCE.lock` pins that archive and the SHA-256 of every schema.
The independent Java oracle is the published 4.3.0 release, source commit
[`a9ce3221537b8653448750697915607dc7936cf3`](https://github.com/apache/kafka/tree/a9ce3221537b8653448750697915607dc7936cf3).
Every selected producer schema except ApiVersionsRequest/Response is identical
to that release. The supplied ApiVersions schemas add version 5; versions 0
and 3 exercised here are unchanged. The artifact's SHA-512 was also compared
with [Maven Central's published checksum](https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/4.3.0/kafka-clients-4.3.0.jar.sha512)
when these fixtures were captured.

`Errors.java` is copied verbatim from the same pinned source revision as the
schemas, at `clients/src/main/java/org/apache/kafka/common/protocol/Errors.java`.
Its SHA-256 is `5a1a3184d1403cb61893fecae0ccf2185c4b61fd089262fd9c0978df51054428`.
Kafka's JSON message schemas contain error-code fields but do not define the
error enumeration. `tests/errors.rs` checks this source's digest and revision,
extracts all 137 names/numbers independently, and requires an explicit producer
classification for every entry in `src/errors.rs`. Source bumps must update
and review both the inventory and the producer action policy.
