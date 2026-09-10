# Independent Java record fixtures

`java-none-{0,1,2}.hex` are complete magic-2 Kafka record batches emitted by
Apache Kafka 4.3.0 `MemoryRecords.withIdempotentRecords` and checked by Java's
`MutableRecordBatch.ensureValid`. Producer identity is ID 42, epoch 3, sequence
11; base offset is 0 and partition leader epoch is -1.

Inputs are explicit in `Capture.java` and independently in Rust tests:

- Case 0: three records, null versus empty key/value, negative timestamp delta,
  UTF-8 key/header bytes, repeated header keys, empty and null header values.
- Case 1: null and empty fields, timestamp 0 followed by `Long.MAX_VALUE`, covering
  the full 10-byte signed timestamp-varint boundary.
- Case 2: 64- and 128-byte payloads, multi-byte length prefixes, decreasing
  timestamps and repeated keys.

`verify.py` checks jar SHA-512 pins through the workspace's independent Java
schema fixture helper, compiles the Java source in a temporary directory, and
checks fixture equality. `--write` regenerates deliberately. Rust zstd tests
compare decompressed payloads with these bytes: compressed byte identity with
Java is not required because independent valid zstd encoders can choose different
frames. The Rust tests verify the zstd attribute, unknown content size, CRC and
identical output across incremental work quotas.
