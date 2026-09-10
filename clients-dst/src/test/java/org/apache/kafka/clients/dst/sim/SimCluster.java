/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.dst.sim;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.LongSupplier;

/**
 * The simulated cluster's shared state: nodes, topics, partition leadership, and the
 * per-partition logs (storage outlives leadership moves, like real replicated logs).
 *
 * <p>Topic ids derive deterministically from topic creation order and the topic name — no
 * global randomness. Recreating a deleted name therefore creates a distinct log identity.
 */
public final class SimCluster {

    private final SimThreadGuard owner = new SimThreadGuard("SimCluster");

    /** Scheduler-confined notification that an append will become visible at a fixed time. */
    @FunctionalInterface
    interface AppendListener {
        void onAppend(Uuid topicId, int partition, long visibleAtMs);
    }

    /**
     * One injected partition-level produce result. UNKNOWN_PRODUCER_ID with an unknown log
     * start offset is transient; a non-negative offset may additionally model actual loss of
     * the partition's producer state.
     */
    public record ProduceError(Errors error, long logStartOffset, boolean forgetProducerState) {
        private static final ProduceError NONE = new ProduceError(Errors.NONE, -1L, false);

        public ProduceError {
            java.util.Objects.requireNonNull(error, "error");
            if (logStartOffset < -1L)
                throw new IllegalArgumentException("logStartOffset cannot be below -1: "
                    + logStartOffset);
            if (forgetProducerState && (error != Errors.UNKNOWN_PRODUCER_ID
                || logStartOffset < 0L)) {
                throw new IllegalArgumentException("Producer state loss requires "
                    + "UNKNOWN_PRODUCER_ID with a known log start offset");
            }
        }
    }

    /** A record in a simulated log; its offset is its index in the partition's log list. */
    public record StoredRecord(byte[] key, byte[] value, long timestamp, Header[] headers) {
        public StoredRecord {
            key = copy(key);
            value = copy(value);
            Header[] source = java.util.Objects.requireNonNull(headers, "headers");
            headers = new Header[source.length];
            for (int i = 0; i < source.length; i++)
                headers[i] = new RecordHeader(source[i].key(), copy(source[i].value()));
        }

        public StoredRecord(byte[] key, byte[] value) {
            this(key, value, RecordBatch.NO_TIMESTAMP, Record.EMPTY_HEADERS);
        }

        @Override
        public byte[] key() {
            return copy(key);
        }

        @Override
        public byte[] value() {
            return copy(value);
        }

        @Override
        public Header[] headers() {
            Header[] copy = new Header[headers.length];
            for (int i = 0; i < headers.length; i++)
                copy[i] = new RecordHeader(headers[i].key(), copy(headers[i].value()));
            return copy;
        }

        private static byte[] copy(byte[] bytes) {
            return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
        }
    }

    /** One visible ListOffsets timestamp match. */
    public record TimestampOffset(long offset, long timestamp) { }

    /** One logical record batch retained from a single append operation. */
    record StoredBatch(long baseOffset, int recordCount) {
        StoredBatch {
            if (baseOffset < 0L)
                throw new IllegalArgumentException("Batch base offset cannot be negative");
            if (recordCount <= 0)
                throw new IllegalArgumentException("Batch record count must be positive");
        }

        long nextOffset() {
            return Math.addExact(baseOffset, recordCount);
        }
    }

    private static final class Topic {
        final String name;
        final Uuid id;
        final Map<Integer, Integer> leaderByPartition = new LinkedHashMap<>();
        final Map<Integer, Integer> lastLeaderByPartition = new HashMap<>();
        final Map<Integer, Integer> leaderEpochByPartition = new HashMap<>();

        Topic(String name, Uuid id) {
            this.name = name;
            this.id = id;
        }
    }

    /** Name-free identity used by every partition-scoped piece of simulated broker state. */
    private record PartitionKey(Uuid topicId, int partition) { }

    private final List<Node> nodes;
    private List<Node> metadataNodes;
    /** Deterministic clock used to fence retention against the currently visible prefix. */
    private final LongSupplier nowMs;
    /** Active names retain creation order for deterministic metadata responses. */
    private final Map<String, Uuid> activeTopicIds = new LinkedHashMap<>();
    private final Map<Uuid, Topic> topicsById = new HashMap<>();
    private long nextTopicIncarnation = 1L;
    private final Map<PartitionKey, List<StoredRecord>> logs = new HashMap<>();
    /** Produce/append boundaries parallel to each logical record log. */
    private final Map<PartitionKey, List<StoredBatch>> storedBatches = new HashMap<>();
    /** Logical retention boundary; backing record indices remain stable below this offset. */
    private final Map<PartitionKey, Long> logStartOffsets = new HashMap<>();
    /** Per-offset visibility stamps, parallel to {@link #logs} (Long.MIN_VALUE = always visible). */
    private final Map<PartitionKey, List<Long>> visibleAtByOffset = new HashMap<>();
    /** Registered in broker-id order by the harness, preserving deterministic wakeup order. */
    private final NavigableMap<Integer, AppendListener> appendListeners = new TreeMap<>();
    /** Idempotent producer state, persisted independently by each (producer, partition) log. */
    private long nextProducerId = 1000L;
    private final Map<ProducerPartition, ProducerPartitionState> producerStates = new HashMap<>();

    /** One appended idempotent batch, as the broker remembers it for duplicate detection. */
    public record SequenceEntry(int baseSequence, int lastSequence, long baseOffset) { }

    private record ProducerPartition(long producerId, PartitionKey partition) { }

    private static final class ProducerPartitionState {
        private final short epoch;
        private final List<SequenceEntry> history = new ArrayList<>();

        private ProducerPartitionState(short epoch) {
            this.epoch = epoch;
        }
    }

    /** The broker's per-partition sequence cache depth — the reason for maxInFlight <= 5. */
    public static final int SEQUENCE_CACHE_DEPTH = 5;

    /** Allocate a fresh idempotent producer id at epoch 0 (InitProducerId). */
    public long allocateProducerId() {
        owner.check();
        return nextProducerId++;
    }

    /** @return the epoch persisted for this producer and partition, or -1 before its first append */
    public short currentProducerEpoch(long producerId, TopicPartition tp) {
        owner.check();
        PartitionKey partition = partitionKey(tp);
        if (partition == null)
            return RecordBatch.NO_PRODUCER_EPOCH;
        return currentProducerEpoch(producerId, partition);
    }

    short currentProducerEpoch(long producerId, Uuid topicId, int partition) {
        owner.check();
        return currentProducerEpoch(producerId, new PartitionKey(topicId, partition));
    }

    private short currentProducerEpoch(long producerId, PartitionKey partition) {
        ProducerPartitionState state = producerStates.get(
            new ProducerPartition(producerId, partition));
        return state == null ? RecordBatch.NO_PRODUCER_EPOCH : state.epoch;
    }

    /**
     * Forget one log partition's producer state, as retention can on a real broker. Other
     * partitions for the same producer id retain their epochs and sequence histories.
     */
    void forgetProducerState(long producerId, TopicPartition tp) {
        owner.check();
        producerStates.remove(new ProducerPartition(producerId, requirePartitionKey(tp)));
    }

    void forgetProducerState(long producerId, Uuid topicId, int partition) {
        owner.check();
        producerStates.remove(new ProducerPartition(producerId,
            requirePartitionKey(topicId, partition)));
    }

    /**
     * Validate an idempotent batch against the producer's sequence history for a partition.
     *
     * @return the cached entry when this batch is a duplicate of one already appended (the
     *         append must be skipped and its original base offset returned), or empty when the
     *         batch is the expected next one and should be appended
     * @throws SequenceValidationException if the batch would leave a gap, or is so old that it
     *         has fallen out of the cache — the broker's OUT_OF_ORDER_SEQUENCE_NUMBER case
     */
    public Optional<SequenceEntry> validateSequence(long producerId,
                                                    short producerEpoch,
                                                    TopicPartition tp,
                                                    int baseSequence,
                                                    int recordCount) {
        owner.check();
        return validateSequence(producerId, producerEpoch, requirePartitionKey(tp),
            baseSequence, recordCount, tp.toString());
    }

    Optional<SequenceEntry> validateSequence(long producerId,
                                             short producerEpoch,
                                             Uuid topicId,
                                             int partition,
                                             int baseSequence,
                                             int recordCount) {
        owner.check();
        PartitionKey key = requirePartitionKey(topicId, partition);
        return validateSequence(producerId, producerEpoch, key, baseSequence, recordCount,
            partitionDisplay(key));
    }

    private Optional<SequenceEntry> validateSequence(long producerId,
                                                     short producerEpoch,
                                                     PartitionKey partition,
                                                     int baseSequence,
                                                     int recordCount,
                                                     String display) {
        requirePositiveRecordCount(recordCount);
        requireValidProducerEpoch(producerEpoch);
        ProducerPartition key = new ProducerPartition(producerId, partition);
        ProducerPartitionState state = producerStates.get(key);
        if (state != null && producerEpoch < state.epoch) {
            throw new ProducerEpochValidationException("Producer " + producerId + " sent epoch "
                + producerEpoch + " for " + display + " but the partition has fenced it at epoch "
                + state.epoch);
        }
        List<SequenceEntry> history = state == null || producerEpoch > state.epoch
            ? List.of() : state.history;
        int lastSequence = DefaultRecordBatch.incrementSequence(baseSequence, recordCount - 1);
        for (SequenceEntry entry : history) {
            if (entry.baseSequence() == baseSequence && entry.lastSequence() == lastSequence)
                return Optional.of(entry); // exact retry of a batch still in the cache
        }
        int expected = history.isEmpty() ? 0 : DefaultRecordBatch.incrementSequence(
            history.get(history.size() - 1).lastSequence(), 1);
        if (baseSequence != expected)
            throw new SequenceValidationException("Producer " + producerId + " sent sequence "
                + baseSequence + " for " + display + " but expected " + expected);
        return Optional.empty();
    }

    /** Remember an appended idempotent batch, evicting beyond the cache depth. */
    public void rememberSequence(long producerId, short producerEpoch,
                                 TopicPartition tp,
                                 int baseSequence, int recordCount, long baseOffset) {
        owner.check();
        rememberSequence(producerId, producerEpoch, requirePartitionKey(tp), baseSequence,
            recordCount, baseOffset, tp.toString());
    }

    void rememberSequence(long producerId, short producerEpoch,
                          Uuid topicId, int partition,
                          int baseSequence, int recordCount, long baseOffset) {
        owner.check();
        PartitionKey key = requirePartitionKey(topicId, partition);
        rememberSequence(producerId, producerEpoch, key, baseSequence, recordCount, baseOffset,
            partitionDisplay(key));
    }

    private void rememberSequence(long producerId, short producerEpoch,
                                  PartitionKey partition,
                                  int baseSequence, int recordCount, long baseOffset,
                                  String display) {
        requirePositiveRecordCount(recordCount);
        requireValidProducerEpoch(producerEpoch);
        ProducerPartition key = new ProducerPartition(producerId, partition);
        ProducerPartitionState state = producerStates.get(key);
        if (state != null && producerEpoch < state.epoch) {
            throw new ProducerEpochValidationException("Producer " + producerId + " cannot "
                + "remember stale epoch " + producerEpoch + " for " + display
                + " after epoch " + state.epoch);
        }
        if (state == null || producerEpoch > state.epoch) {
            state = new ProducerPartitionState(producerEpoch);
            producerStates.put(key, state);
        }
        List<SequenceEntry> history = state.history;
        int lastSequence = DefaultRecordBatch.incrementSequence(baseSequence, recordCount - 1);
        history.add(new SequenceEntry(baseSequence, lastSequence, baseOffset));
        while (history.size() > SEQUENCE_CACHE_DEPTH)
            history.remove(0);
    }

    private static void requirePositiveRecordCount(int recordCount) {
        if (recordCount <= 0)
            throw new IllegalArgumentException("Record count must be positive: " + recordCount);
    }

    private static void requireValidProducerEpoch(short producerEpoch) {
        if (producerEpoch < 0)
            throw new IllegalArgumentException("Producer epoch must not be negative: "
                + producerEpoch);
    }

    /** Signals a write from an epoch already fenced by this partition's log. */
    public static final class ProducerEpochValidationException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public ProducerEpochValidationException(String message) {
            super(message);
        }
    }

    /** Signals the broker's OUT_OF_ORDER_SEQUENCE_NUMBER condition. */
    public static final class SequenceValidationException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public SequenceValidationException(String message) {
            super(message);
        }
    }
    /** Exact fetch fixtures are owner-confined like the rest of the mutable cluster state. */
    private final Map<PartitionKey, MemoryRecords> fetchRecordsOverrides = new HashMap<>();
    /** Topic-incarnation fetch compression; absent means uncompressed synthetic batches. */
    private final Map<Uuid, Compression> fetchCompressionByTopicId = new HashMap<>();
    private final Map<PartitionKey, Errors> fetchErrors = new HashMap<>();
    private final Map<PartitionKey, ProduceError> produceErrors = new HashMap<>();
    /** Remaining produce requests a bounded error injection still applies to. */
    private final Map<PartitionKey, Integer> produceErrorBudget = new HashMap<>();
    private Errors fetchTopLevelError = Errors.NONE;

    public SimCluster(int brokerCount) {
        this(brokerCount, () -> Long.MAX_VALUE);
    }

    /**
     * Construct a cluster whose retention operations observe the supplied deterministic clock.
     * The one-argument fixture treats every appended record as visible for backward compatibility;
     * the producer DST harness supplies its virtual clock.
     */
    public SimCluster(int brokerCount, org.apache.kafka.common.utils.Time time) {
        this(brokerCount, java.util.Objects.requireNonNull(time, "time")::milliseconds);
    }

    private SimCluster(int brokerCount, LongSupplier nowMs) {
        this.nowMs = nowMs;
        List<Node> configuredNodes = new ArrayList<>();
        for (int id = 1; id <= brokerCount; id++)
            configuredNodes.add(new Node(id, hostOf(id), 9092));
        this.nodes = List.copyOf(configuredNodes);
        this.metadataNodes = nodes;
    }

    public static String hostOf(int brokerId) {
        return "sim-broker-" + brokerId;
    }

    public int brokerIdFor(String host) {
        if (!host.startsWith("sim-broker-"))
            throw new IllegalArgumentException("Not a sim broker host: " + host);
        return Integer.parseInt(host.substring("sim-broker-".length()));
    }

    public List<Node> nodes() {
        return nodes;
    }

    /** Broker membership advertised by Metadata; physical simulated nodes remain available. */
    public List<Node> metadataNodes() {
        owner.check();
        return metadataNodes;
    }

    /** Model discovery and departure without changing the harness's physical network. */
    public void setMetadataBrokerIds(Set<Integer> brokerIds) {
        owner.check();
        List<Node> advertised = nodes.stream().filter(node -> brokerIds.contains(node.id())).toList();
        if (advertised.size() != brokerIds.size())
            throw new IllegalArgumentException("Metadata contains unknown simulated broker IDs");
        metadataNodes = advertised;
    }

    public String bootstrap() {
        Node first = nodes.get(0);
        return first.host() + ":" + first.port();
    }

    /**
     * Create or resize a simulated topic. Resizing an existing name preserves its incarnation,
     * matching the historical fixture contract used to model partition expansion.
     */
    public void createTopic(String name, int partitions) {
        createTopicId(name, partitions);
    }

    /**
     * Create or resize a simulated topic and return its incarnation ID. Only an explicit
     * {@link #deleteTopic(String)} followed by creation allocates a new incarnation.
     */
    public Uuid createTopicId(String name, int partitions) {
        owner.check();
        Uuid activeId = activeTopicIds.get(name);
        if (activeId != null) {
            Topic active = topicsById.get(activeId);
            if (active == null)
                throw new IllegalStateException("Active topic ID has no topic: " + activeId);
            for (int p = 0; p < partitions; p++)
                setLeader(active, p, nodes.get(p % nodes.size()).id());
            active.leaderByPartition.keySet().removeIf(partition -> partition >= partitions);
            return activeId;
        }
        long incarnation = nextTopicIncarnation++;
        if (incarnation <= 0L)
            throw new IllegalStateException("Simulated topic incarnation space exhausted");
        Topic topic = new Topic(name, deterministicTopicId(name, incarnation));
        for (int p = 0; p < partitions; p++)
            setLeader(topic, p, nodes.get(p % nodes.size()).id());
        activeTopicIds.put(name, topic.id);
        topicsById.put(topic.id, topic);
        return topic.id;
    }

    /** Delete the active incarnation and all of its partition-local broker state. */
    public Uuid deleteTopic(String name) {
        owner.check();
        Uuid topicId = activeTopicIds.remove(name);
        if (topicId == null)
            return Uuid.ZERO_UUID;
        topicsById.remove(topicId);
        logs.keySet().removeIf(key -> key.topicId().equals(topicId));
        storedBatches.keySet().removeIf(key -> key.topicId().equals(topicId));
        logStartOffsets.keySet().removeIf(key -> key.topicId().equals(topicId));
        visibleAtByOffset.keySet().removeIf(key -> key.topicId().equals(topicId));
        fetchCompressionByTopicId.remove(topicId);
        fetchRecordsOverrides.keySet().removeIf(key -> key.topicId().equals(topicId));
        fetchErrors.keySet().removeIf(key -> key.topicId().equals(topicId));
        produceErrors.keySet().removeIf(key -> key.topicId().equals(topicId));
        produceErrorBudget.keySet().removeIf(key -> key.topicId().equals(topicId));
        producerStates.keySet().removeIf(key -> key.partition().topicId().equals(topicId));
        return topicId;
    }

    public void moveLeader(TopicPartition tp, int newLeaderBrokerId) {
        owner.check();
        Topic topic = topic(tp.topic());
        if (topic == null || !topic.leaderByPartition.containsKey(tp.partition()))
            throw new IllegalArgumentException("Unknown partition " + tp);
        setLeader(topic, tp.partition(), newLeaderBrokerId);
    }

    public boolean topicExists(String name) {
        owner.check();
        return activeTopicIds.containsKey(name);
    }

    boolean topicExists(Uuid id) {
        owner.check();
        return topicsById.containsKey(id);
    }

    boolean topicMatches(Uuid id, String name) {
        owner.check();
        Topic topic = topicsById.get(id);
        return topic != null && topic.name.equals(name)
            && id.equals(activeTopicIds.get(name));
    }

    public List<String> topicNames() {
        owner.check();
        return List.copyOf(activeTopicIds.keySet());
    }

    public Uuid topicId(String name) {
        owner.check();
        return activeTopicIds.getOrDefault(name, Uuid.ZERO_UUID);
    }

    public String topicName(Uuid id) {
        owner.check();
        Topic topic = topicsById.get(id);
        return topic == null ? null : topic.name;
    }

    public int partitionCount(String name) {
        owner.check();
        Topic topic = topic(name);
        return topic == null ? 0 : topic.leaderByPartition.size();
    }

    int partitionCount(Uuid id) {
        owner.check();
        Topic topic = topicsById.get(id);
        return topic == null ? 0 : topic.leaderByPartition.size();
    }

    /** @return leader broker id, or -1 if the partition is unknown */
    public int leader(TopicPartition tp) {
        owner.check();
        Topic topic = topic(tp.topic());
        if (topic == null)
            return -1;
        Integer leader = topic.leaderByPartition.get(tp.partition());
        return leader == null ? -1 : leader;
    }

    int leader(Uuid topicId, int partition) {
        owner.check();
        Topic topic = topicsById.get(topicId);
        if (topic == null)
            return -1;
        Integer leader = topic.leaderByPartition.get(partition);
        return leader == null ? -1 : leader;
    }

    /** @return current leader epoch, or the protocol sentinel if the partition is unknown */
    public int leaderEpoch(TopicPartition tp) {
        owner.check();
        Topic topic = topic(tp.topic());
        return topic == null ? RecordBatch.NO_PARTITION_LEADER_EPOCH
            : leaderEpoch(topic, tp.partition());
    }

    int leaderEpoch(Uuid topicId, int partition) {
        owner.check();
        Topic topic = topicsById.get(topicId);
        return topic == null ? RecordBatch.NO_PARTITION_LEADER_EPOCH
            : leaderEpoch(topic, partition);
    }

    private static int leaderEpoch(Topic topic, int partition) {
        if (!topic.leaderByPartition.containsKey(partition))
            return RecordBatch.NO_PARTITION_LEADER_EPOCH;
        return topic.leaderEpochByPartition.getOrDefault(partition, 0);
    }

    /** Change leadership without ever letting an incarnation's epoch move backwards. */
    private static void setLeader(Topic topic, int partition, int leader) {
        Integer previousLeader = topic.lastLeaderByPartition.get(partition);
        if (previousLeader == null) {
            topic.leaderByPartition.put(partition, leader);
            topic.lastLeaderByPartition.put(partition, leader);
            topic.leaderEpochByPartition.putIfAbsent(partition, 0);
            return;
        }
        if (previousLeader == leader) {
            topic.leaderByPartition.put(partition, leader);
            return;
        }
        int previousEpoch = topic.leaderEpochByPartition.getOrDefault(partition, 0);
        if (previousEpoch == Integer.MAX_VALUE)
            throw new IllegalStateException("Simulated leader epoch space exhausted for topic ID "
                + topic.id + " partition " + partition);
        topic.leaderEpochByPartition.put(partition, previousEpoch + 1);
        topic.lastLeaderByPartition.put(partition, leader);
        topic.leaderByPartition.put(partition, leader);
    }

    private Topic topic(String name) {
        Uuid topicId = activeTopicIds.get(name);
        return topicId == null ? null : topicsById.get(topicId);
    }

    private PartitionKey partitionKey(TopicPartition tp) {
        Topic topic = topic(tp.topic());
        if (topic == null || !topic.leaderByPartition.containsKey(tp.partition()))
            return null;
        return new PartitionKey(topic.id, tp.partition());
    }

    private PartitionKey requirePartitionKey(TopicPartition tp) {
        PartitionKey key = partitionKey(tp);
        if (key == null)
            throw new IllegalArgumentException("Unknown partition " + tp);
        return key;
    }

    private PartitionKey requirePartitionKey(Uuid topicId, int partition) {
        Topic topic = topicsById.get(topicId);
        if (topic == null || !topic.leaderByPartition.containsKey(partition))
            throw new IllegalArgumentException("Unknown topic id partition " + topicId
                + "-" + partition);
        return new PartitionKey(topicId, partition);
    }

    private String partitionDisplay(PartitionKey key) {
        Topic topic = topicsById.get(key.topicId());
        String name = topic == null ? key.topicId().toString() : topic.name;
        return name + "-" + key.partition();
    }

    /** Append validated records, immediately visible; returns the base offset. */
    public long append(TopicPartition tp, List<StoredRecord> records) {
        owner.check();
        return append(requirePartitionKey(tp), records, Long.MIN_VALUE);
    }

    /**
     * Append validated records which become fetchable only once the virtual clock reaches
     * {@code visibleAtMs} (see {@link BrokerTimingModel#appendVisibilityDelayMs}); returns
     * the base offset. Offsets are assigned at append time regardless of visibility.
     */
    public long append(TopicPartition tp, List<StoredRecord> records, long visibleAtMs) {
        owner.check();
        return append(requirePartitionKey(tp), records, visibleAtMs);
    }

    long append(Uuid topicId, int partition, List<StoredRecord> records, long visibleAtMs) {
        owner.check();
        return append(requirePartitionKey(topicId, partition), records, visibleAtMs);
    }

    private long append(PartitionKey key, List<StoredRecord> records, long visibleAtMs) {
        List<StoredRecord> log = logs.computeIfAbsent(key, ignored -> new ArrayList<>());
        List<Long> visibility = visibleAtByOffset.computeIfAbsent(key,
            ignored -> new ArrayList<>());
        long baseOffset = log.size();
        log.addAll(records);
        if (!records.isEmpty()) {
            storedBatches.computeIfAbsent(key, ignored -> new ArrayList<>())
                .add(new StoredBatch(baseOffset, records.size()));
        }
        for (int i = 0; i < records.size(); i++)
            visibility.add(visibleAtMs);
        if (!records.isEmpty()) {
            for (AppendListener listener : appendListeners.values())
                listener.onAppend(key.topicId(), key.partition(), visibleAtMs);
        }
        return baseOffset;
    }

    /** Register a broker-local purgatory listener on the cluster's owning scheduler thread. */
    void addAppendListener(int brokerId, AppendListener listener) {
        owner.check();
        appendListeners.put(brokerId, java.util.Objects.requireNonNull(listener, "listener"));
    }

    /**
     * The partition's visible end offset (its high watermark): the end of the longest log
     * prefix whose records have all reached their visibility time. Prefix semantics keep a
     * later fast append from exposing an earlier still-replicating one.
     */
    public long visibleLogEndOffset(TopicPartition tp, long nowMs) {
        owner.check();
        PartitionKey key = partitionKey(tp);
        return key == null ? 0L : visibleLogEndOffset(key, nowMs);
    }

    long visibleLogEndOffset(Uuid topicId, int partition, long nowMs) {
        owner.check();
        return visibleLogEndOffset(new PartitionKey(topicId, partition), nowMs);
    }

    private long visibleLogEndOffset(PartitionKey key, long nowMs) {
        if (fetchRecordsOverrides.get(key) != null)
            return logEndOffset(key); // exact fetch fixtures bypass the visibility model
        List<Long> visibility = visibleAtByOffset.get(key);
        if (visibility == null)
            return 0;
        for (int offset = 0; offset < visibility.size(); offset++) {
            if (visibility.get(offset) > nowMs)
                return offset;
        }
        return visibility.size();
    }

    /** Next time the visible prefix can advance for a partition, if an append is pending. */
    OptionalLong nextVisibilityMs(Uuid topicId, int partition, long nowMs) {
        owner.check();
        PartitionKey key = new PartitionKey(topicId, partition);
        if (fetchRecordsOverrides.get(key) != null)
            return OptionalLong.empty();
        List<Long> visibility = visibleAtByOffset.get(key);
        if (visibility == null)
            return OptionalLong.empty();
        int visibleEnd = Math.toIntExact(visibleLogEndOffset(key, nowMs));
        return visibleEnd < visibility.size()
            ? OptionalLong.of(visibility.get(visibleEnd)) : OptionalLong.empty();
    }

    /**
     * Find the first retained record in the visible prefix whose timestamp reaches the target.
     * Later records whose individual visibility time has elapsed remain hidden behind an earlier
     * invisible append, exactly like {@link #visibleLogEndOffset(TopicPartition, long)}.
     */
    public Optional<TimestampOffset> offsetForTimestamp(TopicPartition tp,
                                                        long targetTimestamp,
                                                        long nowMs) {
        owner.check();
        if (targetTimestamp < 0L)
            throw new IllegalArgumentException("Target timestamp must be non-negative: "
                + targetTimestamp);
        PartitionKey key = partitionKey(tp);
        if (key == null)
            return Optional.empty();
        List<StoredRecord> log = logs.get(key);
        if (log == null)
            return Optional.empty();
        int start = Math.toIntExact(logStartOffset(key));
        int end = Math.toIntExact(Math.min(log.size(), visibleLogEndOffset(key, nowMs)));
        for (int offset = start; offset < end; offset++) {
            StoredRecord record = log.get(offset);
            if (record.timestamp() >= targetTimestamp)
                return Optional.of(new TimestampOffset(offset, record.timestamp()));
        }
        return Optional.empty();
    }

    /**
     * Advance a partition's logical retention boundary without renumbering its backing records.
     * The new start must be monotonic and may not cross records which are not visible yet.
     * Producer sequence entries wholly below the retained prefix are discarded, matching the
     * broker state loss which retention can cause.
     */
    public void advanceLogStartOffset(TopicPartition tp, long offset) {
        owner.check();
        PartitionKey key = requirePartitionKey(tp);
        long current = logStartOffset(key);
        if (offset < current) {
            throw new IllegalArgumentException("Log start offset cannot move backwards for "
                + tp + ": current=" + current + ", requested=" + offset);
        }
        long visibleEnd = visibleLogEndOffset(key, nowMs.getAsLong());
        if (offset > visibleEnd) {
            throw new IllegalArgumentException("Log start offset cannot exceed the visible end for "
                + tp + ": visibleEnd=" + visibleEnd + ", requested=" + offset);
        }
        if (offset == current)
            return;
        logStartOffsets.put(key, offset);
        discardRetainedProducerState(key, offset);
    }

    /** @return the logical first retained offset, or zero for an unknown partition */
    public long logStartOffset(TopicPartition tp) {
        owner.check();
        PartitionKey key = partitionKey(tp);
        return key == null ? 0L : logStartOffset(key);
    }

    long logStartOffset(Uuid topicId, int partition) {
        owner.check();
        return logStartOffset(new PartitionKey(topicId, partition));
    }

    private long logStartOffset(PartitionKey key) {
        return logStartOffsets.getOrDefault(key, 0L);
    }

    private void discardRetainedProducerState(PartitionKey partition, long offset) {
        List<ProducerPartition> emptyStates = new ArrayList<>();
        for (Map.Entry<ProducerPartition, ProducerPartitionState> entry :
            producerStates.entrySet()) {
            if (!entry.getKey().partition().equals(partition))
                continue;
            entry.getValue().history.removeIf(sequence -> sequence.baseOffset() < offset);
            if (entry.getValue().history.isEmpty())
                emptyStates.add(entry.getKey());
        }
        emptyStates.forEach(producerStates::remove);
    }

    /** Configure the compression used when this topic's logical log is rebuilt for Fetch. */
    public void setFetchCompression(String topic, Compression compression) {
        owner.check();
        Uuid topicId = activeTopicIds.get(topic);
        if (topicId == null)
            throw new IllegalArgumentException("Unknown topic " + topic);
        Compression configured = java.util.Objects.requireNonNull(compression, "compression");
        if (configured.type() == CompressionType.NONE)
            fetchCompressionByTopicId.remove(topicId);
        else
            fetchCompressionByTopicId.put(topicId, configured);
    }

    Compression fetchCompression(Uuid topicId) {
        owner.check();
        return fetchCompressionByTopicId.getOrDefault(topicId, Compression.NONE);
    }

    public long logEndOffset(TopicPartition tp) {
        owner.check();
        PartitionKey key = partitionKey(tp);
        return key == null ? 0L : logEndOffset(key);
    }

    long logEndOffset(Uuid topicId, int partition) {
        owner.check();
        return logEndOffset(new PartitionKey(topicId, partition));
    }

    private long logEndOffset(PartitionKey key) {
        MemoryRecords override = fetchRecordsOverrides.get(key);
        if (override != null) {
            long endOffset = 0L;
            for (RecordBatch batch : override.batches())
                endOffset = Math.max(endOffset, batch.nextOffset());
            return endOffset;
        }
        List<StoredRecord> log = logs.get(key);
        return log == null ? 0 : log.size();
    }

    public List<StoredRecord> log(TopicPartition tp) {
        owner.check();
        PartitionKey key = partitionKey(tp);
        return key == null ? List.of() : log(key.topicId(), key.partition());
    }

    List<StoredRecord> log(Uuid topicId, int partition) {
        owner.check();
        List<StoredRecord> log = logs.get(new PartitionKey(topicId, partition));
        return log == null ? List.of() : List.copyOf(log);
    }

    /** Logical append batches in base-offset order for synthetic Fetch reconstruction. */
    List<StoredBatch> storedBatches(Uuid topicId, int partition) {
        owner.check();
        List<StoredBatch> batches = storedBatches.get(new PartitionKey(topicId, partition));
        return batches == null ? List.of() : List.copyOf(batches);
    }

    /**
     * Install exact wire records for fetch tests which need batch metadata that the logical
     * record log cannot express (compaction gaps and control batches). The bytes are copied so
     * deterministic broker behavior never depends on later caller mutation.
     */
    public void setFetchRecordsOverride(TopicPartition tp, MemoryRecords records) {
        owner.check();
        ByteBuffer source = records.buffer().duplicate();
        ByteBuffer copy = ByteBuffer.allocate(source.remaining());
        copy.put(source).flip();
        MemoryRecords snapshot = MemoryRecords.readableRecords(copy);
        fetchRecordsOverrides.put(requirePartitionKey(tp), snapshot);
    }

    /** Present means this partition uses the exact-record override, even when the result is empty. */
    public Optional<MemoryRecords> fetchRecordsOverride(TopicPartition tp, long fetchOffset) {
        owner.check();
        PartitionKey key = partitionKey(tp);
        return key == null ? Optional.empty() : fetchRecordsOverride(key, fetchOffset);
    }

    Optional<MemoryRecords> fetchRecordsOverride(Uuid topicId, int partition, long fetchOffset) {
        owner.check();
        return fetchRecordsOverride(new PartitionKey(topicId, partition), fetchOffset);
    }

    private Optional<MemoryRecords> fetchRecordsOverride(PartitionKey key, long fetchOffset) {
        MemoryRecords records = fetchRecordsOverrides.get(key);
        if (records == null)
            return Optional.empty();
        for (RecordBatch batch : records.batches()) {
            if (batch.lastOffset() >= fetchOffset)
                return Optional.of(records);
        }
        return Optional.of(MemoryRecords.EMPTY);
    }

    /** Make the leader answer produce requests for this partition with the given error. */
    public void setProduceError(TopicPartition tp, Errors error) {
        owner.check();
        PartitionKey key = requirePartitionKey(tp);
        if (error == Errors.NONE)
            produceErrors.remove(key);
        else
            produceErrors.put(key, new ProduceError(error, -1L, false));
    }

    /** Fail only the next {@code count} produce requests for this partition, then heal. */
    public void failNextProduces(TopicPartition tp, Errors error, int count) {
        owner.check();
        PartitionKey key = requirePartitionKey(tp);
        produceErrors.put(key, new ProduceError(error, -1L, false));
        produceErrorBudget.put(key, count);
    }

    /**
     * Model actual partition-local producer-state loss for the next {@code count} requests.
     * Unlike a transient UNKNOWN_PRODUCER_ID whose log start offset is unknown, this response
     * gives the client the protocol evidence needed to recover its sequence identity.
     */
    public void failNextProducerStateLoss(TopicPartition tp,
                                          long logStartOffset,
                                          int count) {
        owner.check();
        PartitionKey key = requirePartitionKey(tp);
        produceErrors.put(key,
            new ProduceError(Errors.UNKNOWN_PRODUCER_ID, logStartOffset, true));
        produceErrorBudget.put(key, count);
    }

    /** Consumes one unit of any bounded produce-error budget; call once per produce request. */
    public ProduceError produceError(TopicPartition tp) {
        owner.check();
        PartitionKey key = partitionKey(tp);
        return key == null ? ProduceError.NONE : produceError(key);
    }

    ProduceError produceError(Uuid topicId, int partition) {
        owner.check();
        return produceError(new PartitionKey(topicId, partition));
    }

    private ProduceError produceError(PartitionKey key) {
        ProduceError error = produceErrors.getOrDefault(key, ProduceError.NONE);
        if (error.error() == Errors.NONE)
            return ProduceError.NONE;
        Integer remaining = produceErrorBudget.get(key);
        if (remaining == null)
            return error;
        if (remaining <= 1) {
            produceErrors.remove(key);
            produceErrorBudget.remove(key);
        } else {
            produceErrorBudget.put(key, remaining - 1);
        }
        return error;
    }

    /** Inject or clear a deterministic partition-level fetch error. */
    public void setFetchError(TopicPartition tp, Errors error) {
        owner.check();
        PartitionKey key = requirePartitionKey(tp);
        if (error == Errors.NONE)
            fetchErrors.remove(key);
        else
            fetchErrors.put(key, error);
    }

    public Errors fetchError(TopicPartition tp) {
        owner.check();
        PartitionKey key = partitionKey(tp);
        return key == null ? Errors.NONE : fetchErrors.getOrDefault(key, Errors.NONE);
    }

    Errors fetchError(Uuid topicId, int partition) {
        owner.check();
        return fetchErrors.getOrDefault(new PartitionKey(topicId, partition), Errors.NONE);
    }

    /** Inject or clear a deterministic fetch-response top-level error. */
    public void setFetchTopLevelError(Errors error) {
        owner.check();
        fetchTopLevelError = java.util.Objects.requireNonNull(error, "error");
    }

    public Errors fetchTopLevelError() {
        owner.check();
        return fetchTopLevelError;
    }

    private static Uuid deterministicTopicId(String name, long incarnation) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        long msb = 0x517b_0000_0000_0000L;
        for (byte b : bytes) {
            msb = msb * 31 + b;
        }
        // The positive, globally monotonic incarnation makes every creation unique. The high
        // bit also keeps simulated ids away from zero and Kafka's small reserved ids.
        Uuid id = new Uuid(msb | 0x4000L, Long.MIN_VALUE | incarnation);
        if (id.equals(Uuid.ZERO_UUID))
            throw new IllegalStateException("Degenerate topic id for " + name);
        return id;
    }
}
