package io.krkafka.producer;

import io.krkafka.ffi.kr_event;
import io.krkafka.ffi.kr_header;
import io.krkafka.ffi.kr_kafka_h;
import io.krkafka.ffi.kr_metadata_broker;
import io.krkafka.ffi.kr_metadata_partition;
import io.krkafka.ffi.kr_metadata_snapshot;
import io.krkafka.ffi.kr_record;
import io.krkafka.ffi.kr_span;
import io.krkafka.ffi.kr_topic_status;
import io.krkafka.loader.NativeLibrary;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.Uuid;

/** Internal ABI adapter. Every instance method requires the caller's callGate. */
final class NativeAccess {
    static final int OK = 0, INVALID = -1, EXHAUSTED = -3, NOT_READY = -4, CLOSED = -5, FAILED = -7;
    final MemorySegment handle;

    record Event(int kind, long token, long userToken, int topic, int partition, int outcome,
                 int reason, long offset, boolean hasOffset, long timestamp, boolean hasTimestamp,
                 int attempts, int count, Uuid topicId) {
        Event(int kind, long token, long userToken, int topic, int partition, int outcome,
              int reason, long offset, boolean hasOffset, long timestamp, boolean hasTimestamp,
              int attempts, int count) {
            this(kind, token, userToken, topic, partition, outcome, reason, offset, hasOffset,
                    timestamp, hasTimestamp, attempts, count, Uuid.ZERO_UUID);
        }
    }
    record Metadata(Cluster cluster, long generation) {}
    record TopicStatus(int status, int reason, long generation, Uuid topicId) {}

    static final class ProtocolFailure extends KafkaException {
        ProtocolFailure(String message) { super(message); }
        ProtocolFailure(String message, Throwable cause) { super(message, cause); }
    }

    static final class CallFailure extends KafkaException {
        final int code;
        CallFailure(String operation, int code) {
            super(operation + " failed with native error " + code);
            this.code = code;
        }
    }

    NativeAccess(MemorySegment handle) {
        if (handle.equals(MemorySegment.NULL)) throw new IllegalArgumentException("null native producer");
        this.handle = handle;
    }

    static NativeAccess create(ProducerSettings settings) {
        NativeLibrary.load();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(ValueLayout.ADDRESS);
            int code = kr_kafka_h.kr_producer_create(settings.nativeConfig(arena), out);
            if (code != OK) throw new CallFailure("producer create", code);
            return new NativeAccess(out.get(ValueLayout.ADDRESS, 0));
        }
    }

    int topicOpen(String name) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeName = arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes);
            MemorySegment out = arena.allocate(ValueLayout.JAVA_INT);
            check("topic open", kr_kafka_h.kr_topic_open(handle, nativeName, bytes.length, out));
            return out.get(ValueLayout.JAVA_INT, 0);
        }
    }

    int submit(MemorySegment record) {
        int accepted = kr_kafka_h.kr_submitv_copy(handle, record, 1);
        if (accepted == 0) throw new CallFailure("record admission", kr_kafka_h.kr_last_error(handle));
        if (accepted != 1) throw new ProtocolFailure("Native submit accepted an impossible prefix: " + accepted);
        return accepted;
    }

    long flush() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(ValueLayout.JAVA_LONG);
            check("flush", kr_kafka_h.kr_flush(handle, out));
            return out.get(ValueLayout.JAVA_LONG, 0);
        }
    }

    void close(long timeoutMs) { check("close", kr_kafka_h.kr_close(handle, timeoutMs)); }
    void destroy() { kr_kafka_h.kr_destroy(handle); }

    Event[] poll(MemorySegment events, int capacity) {
        for (int i = 0; i < capacity; i++)
            kr_event.struct_size(kr_event.asSlice(events, i), (int) kr_event.sizeof());
        int count = kr_kafka_h.kr_poll_events(handle, events, capacity);
        if (count == 0) {
            int code = kr_kafka_h.kr_last_error(handle);
            if (code != OK) throw new CallFailure("event drain", code);
        }
        if (count < 0 || count > capacity) throw new ProtocolFailure("Invalid native event count");
        Event[] result = new Event[count];
        for (int i = 0; i < count; i++) {
            MemorySegment event = kr_event.asSlice(events, i);
            int offsetPresent = kr_event.base_offset_present(event);
            int timestampPresent = kr_event.timestamp_present(event);
            if ((offsetPresent & ~1) != 0 || (timestampPresent & ~1) != 0)
                throw new ProtocolFailure("Invalid native event presence flag");
            result[i] = new Event(kr_event.kind(event), kr_event.token(event), kr_event.user_token(event),
                    kr_event.topic(event), kr_event.partition(event), kr_event.outcome(event),
                    kr_event.reason(event), kr_event.base_offset(event), offsetPresent == 1,
                    kr_event.timestamp_ms(event), timestampPresent == 1,
                    kr_event.attempts(event), kr_event.count(event), uuid(kr_event.topic_id(event)));
        }
        return result;
    }

    private void check(String operation, int code) {
        if (code != OK) throw new CallFailure(operation, code);
    }

    void refresh(int topic) { check("metadata refresh", kr_kafka_h.kr_topic_refresh(handle, topic)); }
    void topicClose(int topic) { check("topic close", kr_kafka_h.kr_topic_close(handle, topic)); }

    TopicStatus topicStatus(int topic) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment status = kr_topic_status.allocate(arena);
            kr_topic_status.struct_size(status, (int) kr_topic_status.sizeof());
            check("topic status", kr_kafka_h.kr_topic_get_status(handle, topic, status));
            int state = kr_topic_status.status(status);
            int reason = kr_topic_status.reason(status);
            validateTopicStatus(state, reason);
            return new TopicStatus(state, reason, kr_topic_status.generation(status), uuid(kr_topic_status.topic_id(status)));
        }
    }

    int ownerStatus() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment status = arena.allocate(ValueLayout.JAVA_INT);
            check("owner status", kr_kafka_h.kr_owner_status(handle, status));
            int state = status.get(ValueLayout.JAVA_INT, 0);
            if (state < 0 || state > 2) throw new ProtocolFailure("Unknown native owner status " + state);
            return state;
        }
    }

    /** Output is bounded to 65,536 partitions, 4,096 brokers and 16 MiB of row data. */
    Metadata metadata(int topic, String name) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment info = kr_metadata_snapshot.allocate(arena);
            kr_metadata_snapshot.struct_size(info, (int) kr_metadata_snapshot.sizeof());
            check("metadata acquire", kr_kafka_h.kr_metadata_acquire(handle, topic, info));
            long snapshot = kr_metadata_snapshot.snapshot(info);
            try {
                if (snapshot == 0 || kr_metadata_snapshot.status(info) != 1 || kr_metadata_snapshot.reason(info) != 0)
                    throw new ProtocolFailure("Malformed ready metadata snapshot");
                int brokerCount = bounded(kr_metadata_snapshot.broker_count(info), 4096, "broker count");
                int partitionCount = bounded(kr_metadata_snapshot.partition_count(info), 65536, "partition count");
                long[] byteBudget = {16 * 1024 * 1024};
                HashMap<Integer, Node> nodes = new HashMap<>();
                MemorySegment written = arena.allocate(ValueLayout.JAVA_INT);
                MemorySegment brokers = kr_metadata_broker.allocateArray(Math.min(1024, Math.max(1, brokerCount)), arena);
                for (int start = 0; start < brokerCount;) {
                    int capacity = Math.min(1024, brokerCount - start);
                    for (int i = 0; i < capacity; i++)
                        kr_metadata_broker.struct_size(kr_metadata_broker.asSlice(brokers, i), (int) kr_metadata_broker.sizeof());
                    check("metadata brokers", kr_kafka_h.kr_metadata_brokers(handle, snapshot, start, brokers, capacity, written));
                    int count = pageCount(written, capacity);
                    for (int i = 0; i < count; i++) {
                        MemorySegment broker = kr_metadata_broker.asSlice(brokers, i);
                        int id = kr_metadata_broker.id(broker);
                        int port = bounded(kr_metadata_broker.port(broker), 65535, "broker port");
                        String host = metadataString(arena, snapshot, start + i, 0, kr_metadata_broker.host_len(broker), byteBudget);
                        int rackPresent = kr_metadata_broker.rack_present(broker);
                        if (rackPresent != 0 && rackPresent != 1) throw new ProtocolFailure("Invalid broker rack presence");
                        String rack = rackPresent == 0 ? null : metadataString(arena, snapshot, start + i, 1,
                                kr_metadata_broker.rack_len(broker), byteBudget);
                        if (nodes.putIfAbsent(id, new Node(id, host, port, rack)) != null)
                            throw new ProtocolFailure("Duplicate broker in metadata snapshot");
                    }
                    start += count;
                }
                ArrayList<PartitionInfo> partitions = new ArrayList<>(partitionCount);
                Set<Integer> seen = new java.util.HashSet<>();
                MemorySegment rows = kr_metadata_partition.allocateArray(Math.min(1024, Math.max(1, partitionCount)), arena);
                for (int start = 0; start < partitionCount;) {
                    int capacity = Math.min(1024, partitionCount - start);
                    for (int i = 0; i < capacity; i++)
                        kr_metadata_partition.struct_size(kr_metadata_partition.asSlice(rows, i), (int) kr_metadata_partition.sizeof());
                    check("metadata partitions", kr_kafka_h.kr_metadata_partitions(handle, snapshot, start, rows, capacity, written));
                    int count = pageCount(written, capacity);
                    for (int i = 0; i < count; i++) {
                        MemorySegment row = kr_metadata_partition.asSlice(rows, i);
                        int partition = kr_metadata_partition.partition(row);
                        if (partition < 0 || !seen.add(partition)) throw new ProtocolFailure("Invalid/duplicate metadata partition");
                        int leader = kr_metadata_partition.leader(row);
                        Node[] replicas = metadataNodes(arena, snapshot, start + i, 0, kr_metadata_partition.replica_count(row), nodes, byteBudget);
                        Node[] isr = metadataNodes(arena, snapshot, start + i, 1, kr_metadata_partition.isr_count(row), nodes, byteBudget);
                        Node[] offline = metadataNodes(arena, snapshot, start + i, 2, kr_metadata_partition.offline_count(row), nodes, byteBudget);
                        // A failed partition is unavailable even if its last known leader is retained.
                        Node leaderNode = kr_metadata_partition.error_code(row) == 0 && leader >= 0 ? nodes.get(leader) : null;
                        partitions.add(new PartitionInfo(name, partition, leaderNode, replicas, isr, offline));
                        charge(byteBudget, 64);
                    }
                    start += count;
                }
                Cluster cluster = new Cluster(null, nodes.values(), partitions, Set.of(), Set.of(), Set.of(), null,
                        Map.of(name, uuid(kr_metadata_snapshot.topic_id(info))));
                return new Metadata(cluster, kr_metadata_snapshot.generation(info));
            } finally { check("metadata release", kr_kafka_h.kr_metadata_release(handle, snapshot)); }
        }
    }

    private String metadataString(Arena arena, long snapshot, int broker, int kind, int length, long[] budget) {
        bounded(length, 65536, "metadata string length");
        charge(budget, length);
        if (length == 0) return "";
        MemorySegment bytes = arena.allocate(length);
        MemorySegment written = arena.allocate(ValueLayout.JAVA_INT);
        check("metadata string", kr_kafka_h.kr_metadata_string(handle, snapshot, broker, kind, 0, bytes, length, written));
        if (written.get(ValueLayout.JAVA_INT, 0) != length) throw new ProtocolFailure("Truncated metadata string");
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .decode(bytes.asByteBuffer()).toString();
        } catch (java.nio.charset.CharacterCodingException error) { throw new ProtocolFailure("Invalid metadata UTF-8", error); }
    }

    private Node[] metadataNodes(Arena arena, long snapshot, int partition, int kind, int count,
                                 Map<Integer, Node> nodes, long[] budget) {
        bounded(count, 4096, "replica count");
        charge(budget, 8L * count);
        Node[] result = new Node[count];
        if (count == 0) return result;
        MemorySegment page = arena.allocate(ValueLayout.JAVA_INT, Math.min(1024, count));
        MemorySegment written = arena.allocate(ValueLayout.JAVA_INT);
        for (int start = 0; start < count;) {
            int capacity = Math.min(1024, count - start);
            check("metadata nodes", kr_kafka_h.kr_metadata_nodes(handle, snapshot, partition, kind, start, page, capacity, written));
            int received = pageCount(written, capacity);
            for (int i = 0; i < received; i++) {
                int id = page.getAtIndex(ValueLayout.JAVA_INT, i);
                // Metadata can reference a temporarily absent broker, as Kafka's Cluster does.
                result[start + i] = nodes.getOrDefault(id, new Node(id, "", -1));
            }
            start += received;
        }
        return result;
    }

    private static int pageCount(MemorySegment written, int capacity) {
        int count = written.get(ValueLayout.JAVA_INT, 0);
        if (count <= 0 || count > capacity) throw new ProtocolFailure("Invalid metadata page count");
        return count;
    }

    private static int bounded(int value, int limit, String field) {
        if (value < 0 || value > limit) throw new KafkaException("Native metadata " + field + " exceeds binding limit " + limit);
        return value;
    }

    private static void charge(long[] budget, long bytes) {
        budget[0] -= bytes;
        if (budget[0] < 0) throw new KafkaException("Native metadata snapshot exceeds 16 MiB binding limit");
    }

    static void validateTopicStatus(int status, int reason) {
        if (status < 0 || status > 6 || reason < 0 || reason > 16)
            throw new ProtocolFailure("Unknown native topic status/reason");
    }

    private static Uuid uuid(MemorySegment bytes) {
        var layout = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(java.nio.ByteOrder.BIG_ENDIAN);
        return new Uuid(bytes.get(layout, 0), bytes.get(layout, 8));
    }

    static long recordBytes(byte[] key, byte[] value, byte[][] headerKeys, byte[][] headerValues) {
        long bytes = Math.addExact(kr_record.sizeof(), Math.multiplyExact(kr_header.sizeof(), headerKeys.length));
        bytes = Math.addExact(bytes, key == null ? 0 : key.length);
        bytes = Math.addExact(bytes, value == null ? 0 : value.length);
        for (int i = 0; i < headerKeys.length; i++) {
            bytes = Math.addExact(bytes, headerKeys[i].length);
            bytes = Math.addExact(bytes, headerValues[i] == null ? 0 : headerValues[i].length);
        }
        return bytes;
    }

    static MemorySegment pack(MemorySegment scratch, int topic, int partition, long timestamp,
                              long token, byte[] key, byte[] value, byte[][] headerKeys, byte[][] headerValues) {
        MemorySegment record = scratch.asSlice(0, kr_record.sizeof());
        record.fill((byte) 0);
        kr_record.struct_size(record, (int) kr_record.sizeof());
        kr_record.topic(record, topic);
        kr_record.partition_hint(record, partition);
        kr_record.lane_hint(record, -1);
        kr_record.timestamp_ms(record, timestamp);
        kr_record.user_token(record, token);
        kr_record.key_is_null(record, key == null ? 1 : 0);
        kr_record.value_is_null(record, value == null ? 1 : 0);
        kr_record.header_count(record, headerKeys.length);
        long payloadOffset = kr_record.sizeof() + kr_header.sizeof() * headerKeys.length;
        payloadOffset = putBytes(scratch, payloadOffset, kr_record.key(record), key);
        payloadOffset = putBytes(scratch, payloadOffset, kr_record.value(record), value);
        if (headerKeys.length > 0) {
            MemorySegment headers = scratch.asSlice(kr_record.sizeof(), kr_header.sizeof() * headerKeys.length);
            headers.fill((byte) 0);
            kr_record.headers(record, headers);
            for (int i = 0; i < headerKeys.length; i++) {
                MemorySegment header = kr_header.asSlice(headers, i);
                kr_header.struct_size(header, (int) kr_header.sizeof());
                kr_header.value_is_null(header, headerValues[i] == null ? 1 : 0);
                payloadOffset = putBytes(scratch, payloadOffset, kr_header.key(header), headerKeys[i]);
                payloadOffset = putBytes(scratch, payloadOffset, kr_header.value(header), headerValues[i]);
            }
        }
        return record;
    }

    private static long putBytes(MemorySegment scratch, long offset, MemorySegment span, byte[] bytes) {
        int length = bytes == null ? 0 : bytes.length;
        kr_span.len(span, length);
        if (length == 0) kr_span.ptr(span, MemorySegment.NULL);
        else {
            MemorySegment target = scratch.asSlice(offset, length);
            target.copyFrom(MemorySegment.ofArray(bytes));
            kr_span.ptr(span, target);
        }
        return offset + length;
    }
}
