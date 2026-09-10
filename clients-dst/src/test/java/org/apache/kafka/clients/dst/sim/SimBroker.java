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
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.LongConsumer;

/**
 * A protocol-accurate in-memory broker node for the DST harness.
 *
 * <p>Requests arrive as real wire frames produced by the real client pipeline; they are
 * parsed with the production request classes and answered with production response
 * serialization, FIFO per connection like a real broker. Fetch storage is deliberately
 * record-oriented rather than a byte-for-byte replica-log model: synthetic response batches
 * preserve logical append boundaries and base offsets while honoring approximate byte caps and
 * Fetch long-poll deadlines, but do not retain the original encoded batch bytes. Only the APIs
 * used by the current producer, assignment consumer and minimal admin paths are implemented —
 * anything else fails loudly so scope creep is visible.
 */
@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public final class SimBroker {

    /** Result of accepting one request: either ready now or parked in Fetch purgatory. */
    public sealed interface HandleResult permits Response, Parked {
        /** Immediate responses expose their framed bytes; parked fetches fail loudly here. */
        byte[] frame();
    }

    /** A serialized response frame plus how long the broker took to produce it. */
    public record Response(byte[] frame, long processingDelayMs) implements HandleResult { }

    /**
     * One already-resolved fetch parked until enough bytes are visible or its deadline fires.
     * The network owns timers and connection FIFO; the broker owns selection and session state.
     */
    public final class Parked implements HandleResult {
        private final long connectionKey;
        private final long ordinal;
        private final RequestHeader header;
        private final PendingFetch fetch;
        private final long deadlineMs;
        private final long processingDelayMs;
        private LongConsumer appendWakeup;
        private boolean active = true;

        private Parked(long connectionKey,
                       long ordinal,
                       RequestHeader header,
                       PendingFetch fetch,
                       long deadlineMs,
                       long processingDelayMs) {
            this.connectionKey = connectionKey;
            this.ordinal = ordinal;
            this.header = header;
            this.fetch = fetch;
            this.deadlineMs = deadlineMs;
            this.processingDelayMs = processingDelayMs;
        }

        @Override
        public byte[] frame() {
            throw new IllegalStateException("Fetch response is parked until t=" + deadlineMs);
        }

        public long deadlineMs() {
            owner.check();
            return deadlineMs;
        }

        public int minBytes() {
            owner.check();
            return fetch.request().minBytes();
        }

        void onAppendWakeup(LongConsumer wakeup) {
            owner.check();
            if (!active)
                throw new IllegalStateException("Parked fetch is no longer active");
            appendWakeup = java.util.Objects.requireNonNull(wakeup, "wakeup");
            signalNextVisibility();
        }

        Response reevaluate(boolean deadlineReached) {
            owner.check();
            return completeParked(this, deadlineReached);
        }

        void cancel() {
            owner.check();
            cancelParked(this);
        }

        private boolean interestedIn(Uuid topicId, int partition) {
            return fetch.session().partitions().containsKey(
                new PartitionKey(topicId, partition));
        }

        private void signalAppend(long visibleAtMs) {
            if (active && appendWakeup != null)
                appendWakeup.accept(visibleAtMs);
        }

        private void signalNextVisibility() {
            if (!active || appendWakeup == null)
                return;
            long nowMs = trace.time().milliseconds();
            long nextMs = Long.MAX_VALUE;
            for (PartitionKey key : fetch.session().partitions().keySet()) {
                java.util.OptionalLong visibility = cluster.nextVisibilityMs(
                    key.topicId(), key.partition(), nowMs);
                if (visibility.isPresent())
                    nextMs = Math.min(nextMs, visibility.getAsLong());
            }
            if (nextMs != Long.MAX_VALUE)
                appendWakeup.accept(nextMs);
        }
    }

    private final int id;
    private final SimThreadGuard owner = new SimThreadGuard("SimBroker");
    private final SimCluster cluster;
    private final SimTrace trace;
    private final BrokerTimingModel timingModel;
    private final ThrottleModel throttleModel;
    private final ProduceObserver observer;
    /** Scheduler-confined, per-API counters used by deterministic quota models. */
    private final Map<ApiKeys, Integer> requestCounts = new EnumMap<>(ApiKeys.class);
    private int produceCount = 0;
    /** Physical-connection key then admission order, both deterministic and scheduler-owned. */
    private final NavigableMap<Long, LinkedHashMap<Long, Parked>> parkedFetches = new TreeMap<>();
    private long nextParkedOrdinal;

    public SimBroker(int id, SimCluster cluster, SimTrace trace) {
        this(id, cluster, trace, BrokerTimingModel.INSTANT, ThrottleModel.NONE, ProduceObserver.NONE);
    }

    public SimBroker(int id, SimCluster cluster, SimTrace trace, BrokerTimingModel timingModel,
                     ProduceObserver observer) {
        this(id, cluster, trace, timingModel, ThrottleModel.NONE, observer);
    }

    public SimBroker(int id, SimCluster cluster, SimTrace trace, BrokerTimingModel timingModel,
                     ThrottleModel throttleModel, ProduceObserver observer) {
        this.id = id;
        this.cluster = cluster;
        this.trace = trace;
        this.timingModel = timingModel;
        this.throttleModel = throttleModel;
        this.observer = observer;
        cluster.addAppendListener(id, this::onAppend);
    }

    public int id() {
        return id;
    }

    /**
     * @param requestFrame a full request frame including the 4-byte length prefix
     * @return an immediate framed response or a Fetch parked until data/deadline completion
     */
    public HandleResult handle(byte[] requestFrame) {
        return handle(-1L, requestFrame);
    }

    /** Accept a request from one physical connection, identified by network registration. */
    HandleResult handle(long connectionKey, byte[] requestFrame) {
        owner.check();
        ByteBuffer buffer = ByteBuffer.wrap(requestFrame);
        int declaredSize = buffer.getInt();
        if (declaredSize != buffer.remaining())
            throw new IllegalStateException("Frame length prefix " + declaredSize
                + " does not match payload size " + buffer.remaining());
        RequestHeader header = RequestHeader.parse(buffer);
        AbstractRequest request = AbstractRequest.parseRequest(header.apiKey(), header.apiVersion(),
            new ByteBufferAccessor(buffer)).request;
        int requestCount = requestCounts.getOrDefault(header.apiKey(), 0) + 1;

        AbstractResponse response;
        PendingFetch pendingFetch = null;
        if (header.apiKey() == ApiKeys.FETCH) {
            FetchPreparation fetch = prepareFetch((FetchRequest) request, requestCount);
            response = fetch.response();
            pendingFetch = fetch.pending();
        } else {
            response = switch (header.apiKey()) {
                case API_VERSIONS -> handleApiVersions();
                case METADATA -> handleMetadata((MetadataRequest) request);
                case PRODUCE -> handleProduce(
                    (org.apache.kafka.common.requests.ProduceRequest) request, requestCount);
                case LIST_OFFSETS -> handleListOffsets((ListOffsetsRequest) request);
                case CREATE_TOPICS -> handleCreateTopics((CreateTopicsRequest) request);
                case INIT_PRODUCER_ID -> handleInitProducerId((InitProducerIdRequest) request);
                default -> throw new UnsupportedOperationException(
                    "SimBroker does not implement " + header.apiKey()
                        + " — extend it deliberately rather than silently");
            };
        }
        requestCounts.put(header.apiKey(), requestCount);

        if (header.apiKey() == ApiKeys.PRODUCE)
            produceCount++;
        long processingDelayMs = timingModel.processingDelayMs(header.apiKey(), produceCount);

        if (pendingFetch != null) {
            long deadlineMs = SimTime.saturatedDeadlineMs(
                trace.time().milliseconds(), pendingFetch.request().maxWait());
            Parked parked = new Parked(connectionKey, nextParkedOrdinal++, header, pendingFetch,
                deadlineMs, processingDelayMs);
            parkedFetches.computeIfAbsent(connectionKey, ignored -> new LinkedHashMap<>())
                .put(parked.ordinal, parked);
            trace.add("broker-" + id + " fetch park connection=" + connectionKey
                + " deadline=" + deadlineMs + " minBytes=" + pendingFetch.request().minBytes());
            return parked;
        }

        return serialize(response, header, processingDelayMs);
    }

    private static Response serialize(AbstractResponse response,
                                      RequestHeader header,
                                      long processingDelayMs) {
        ByteBuffer payload = RequestTestUtils.serializeResponseWithHeader(response,
            header.apiVersion(), header.correlationId());
        byte[] framed = new byte[4 + payload.remaining()];
        ByteBuffer out = ByteBuffer.wrap(framed);
        out.putInt(payload.remaining());
        out.put(payload);
        return new Response(framed, processingDelayMs);
    }

    private AbstractResponse handleApiVersions() {
        trace.add("broker-" + id + " api-versions");
        return TestUtils.defaultApiVersionsResponse(ApiMessageType.ListenerType.BROKER);
    }

    private AbstractResponse handleMetadata(MetadataRequest request) {
        MetadataResponseData data = new MetadataResponseData()
            .setClusterId("sim-cluster")
            .setControllerId(1)
            .setThrottleTimeMs(0);
        for (Node node : cluster.metadataNodes())
            data.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(node.id())
                .setHost(node.host())
                .setPort(node.port()));

        List<String> requested = new ArrayList<>();
        if (request.isAllTopics() || request.data().topics() == null) {
            for (String topic : cluster.topicNames()) {
                requested.add(topic);
                data.topics().add(metadataTopic(topic, Uuid.ZERO_UUID));
            }
        } else {
            for (var topic : request.data().topics()) {
                Uuid topicId = topic.topicId();
                if (topicId != null && !Uuid.ZERO_UUID.equals(topicId)) {
                    requested.add(topicId.toString());
                    data.topics().add(metadataTopic(null, topicId));
                } else {
                    requested.add(topic.name());
                    data.topics().add(metadataTopic(topic.name(), Uuid.ZERO_UUID));
                }
            }
        }
        trace.add("broker-" + id + " metadata topics=" + requested);
        return new org.apache.kafka.common.requests.MetadataResponse(data,
            ApiKeys.METADATA.latestVersion());
    }

    /** Resolve Metadata v12 ID queries by ID; a stale ID must never fall through to its old name. */
    private MetadataResponseData.MetadataResponseTopic metadataTopic(String requestedName,
                                                                      Uuid requestedId) {
        boolean byId = requestedId != null && !Uuid.ZERO_UUID.equals(requestedId);
        String topicName = byId ? cluster.topicName(requestedId) : requestedName;
        Uuid topicId = byId ? requestedId : cluster.topicId(requestedName);
        MetadataResponseData.MetadataResponseTopic response =
            new MetadataResponseData.MetadataResponseTopic()
                .setName(topicName)
                .setTopicId(topicId);
        if (topicName == null || Uuid.ZERO_UUID.equals(topicId)) {
            return response.setErrorCode((byId
                ? Errors.UNKNOWN_TOPIC_ID : Errors.UNKNOWN_TOPIC_OR_PARTITION).code());
        }
        response.setErrorCode(Errors.NONE.code());
        for (int partition = 0; partition < cluster.partitionCount(topicId); partition++) {
            int leader = cluster.leader(topicId, partition);
            response.partitions().add(new MetadataResponseData.MetadataResponsePartition()
                .setPartitionIndex(partition)
                .setErrorCode(Errors.NONE.code())
                .setLeaderId(leader)
                .setLeaderEpoch(cluster.leaderEpoch(topicId, partition))
                .setReplicaNodes(List.of(leader))
                .setIsrNodes(List.of(leader)));
        }
        return response;
    }

    @SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
    private AbstractResponse handleProduce(
        org.apache.kafka.common.requests.ProduceRequest request,
        int requestCount
    ) {
        ProduceResponseData responseData = new ProduceResponseData();
        int requestRecords = 0;
        int requestBytes = 0;
        for (ProduceRequestData.TopicProduceData topicData : request.data().topicData()) {
            ResolvedTopic topic = resolveTopic(topicData.topicId(), topicData.name(),
                request.version() >= 13);
            ProduceResponseData.TopicProduceResponse topicResponse =
                new ProduceResponseData.TopicProduceResponse()
                    .setName(topicData.name())
                    .setTopicId(topicData.topicId());
            for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                int partitionIndex = partitionData.index();
                TopicPartition tp = new TopicPartition(topic.name(), partitionIndex);
                ProduceResponseData.PartitionProduceResponse partitionResponse =
                    new ProduceResponseData.PartitionProduceResponse()
                        .setIndex(partitionIndex)
                        .setLogAppendTimeMs(-1);
                int leader = topic.error() == Errors.NONE
                    ? cluster.leader(topic.id(), partitionIndex) : -1;
                // A request rejected by an obsolete leader never reaches this partition's
                // append path and must not silently consume a scripted leader-side failure.
                SimCluster.ProduceError injectedError = leader == id
                    ? cluster.produceError(topic.id(), partitionIndex) : null;
                if (topic.error() != Errors.NONE) {
                    trace.add("broker-" + id + " produce " + topic.error() + " " + tp);
                    partitionResponse.setErrorCode(topic.error().code()).setBaseOffset(-1);
                } else if (leader < 0) {
                    trace.add("broker-" + id + " produce UNKNOWN_TOPIC_OR_PARTITION " + tp);
                    partitionResponse.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                        .setBaseOffset(-1);
                } else if (leader != id) {
                    trace.add("broker-" + id + " produce NOT_LEADER " + tp);
                    partitionResponse.setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code()).setBaseOffset(-1);
                } else if (injectedError != null && injectedError.error() != Errors.NONE) {
                    handleInjectedProduceError(
                        injectedError, topic.id(), partitionIndex, tp, partitionData,
                        partitionResponse);
                } else {
                    MemoryRecords records = (MemoryRecords) partitionData.records();
                    requestBytes += records.sizeInBytes();
                    List<SimCluster.StoredRecord> stored = validateAndExtract(records);
                    requestRecords += stored.size();
                    IdempotentHeader idempotent = idempotentHeader(records);
                    Optional<SimCluster.SequenceEntry> duplicate = Optional.empty();
                    if (idempotent != null) {
                        long logStartOffset = cluster.logStartOffset(
                            topic.id(), partitionIndex);
                        if (idempotent.baseSequence() != 0
                            && logStartOffset > 0L
                            && cluster.currentProducerEpoch(idempotent.producerId(),
                                topic.id(), partitionIndex)
                                == RecordBatch.NO_PRODUCER_EPOCH) {
                            trace.add("broker-" + id + " produce UNKNOWN_PRODUCER_ID " + tp
                                + " pid=" + idempotent.producerId() + " epoch="
                                + idempotent.producerEpoch() + " seq="
                                + idempotent.baseSequence());
                            partitionResponse
                                .setErrorCode(Errors.UNKNOWN_PRODUCER_ID.code())
                                .setBaseOffset(-1L)
                                .setLogStartOffset(logStartOffset);
                            topicResponse.partitionResponses().add(partitionResponse);
                            continue;
                        }
                        try {
                            duplicate = cluster.validateSequence(idempotent.producerId(),
                                idempotent.producerEpoch(), topic.id(), partitionIndex,
                                idempotent.baseSequence(), stored.size());
                        } catch (SimCluster.ProducerEpochValidationException fenced) {
                            trace.add("broker-" + id + " produce " + tp + " FENCED_EPOCH pid="
                                + idempotent.producerId() + " epoch="
                                + idempotent.producerEpoch());
                            partitionResponse.setErrorCode(Errors.INVALID_PRODUCER_EPOCH.code())
                                .setBaseOffset(-1)
                                .setErrorMessage(fenced.getMessage());
                            topicResponse.partitionResponses().add(partitionResponse);
                            continue;
                        } catch (SimCluster.SequenceValidationException outOfOrder) {
                            trace.add("broker-" + id + " produce " + tp + " OUT_OF_ORDER seq="
                                + idempotent.baseSequence() + " pid=" + idempotent.producerId()
                                + " epoch=" + idempotent.producerEpoch());
                            partitionResponse
                                .setErrorCode(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER.code())
                                .setBaseOffset(-1)
                                .setErrorMessage(outOfOrder.getMessage());
                            topicResponse.partitionResponses().add(partitionResponse);
                            continue;
                        }
                    }
                    if (duplicate.isPresent()) {
                        // The broker recognises a retried batch and returns its original offset
                        // without appending it a second time — the idempotence guarantee (D15).
                        long baseOffset = duplicate.get().baseOffset();
                        trace.add("broker-" + id + " produce " + tp + " DUPLICATE base=" + baseOffset
                            + " seq=" + idempotent.baseSequence() + " pid="
                            + idempotent.producerId() + " epoch=" + idempotent.producerEpoch());
                        partitionResponse.setErrorCode(Errors.NONE.code()).setBaseOffset(baseOffset);
                        topicResponse.partitionResponses().add(partitionResponse);
                        continue;
                    }
                    // +1: produceCount is incremented after handle(), like the throttle model.
                    long visibleAtMs = SimTime.saturatedDeadlineMs(
                        trace.time().milliseconds(),
                        timingModel.appendVisibilityDelayMs(produceCount + 1));
                    long baseOffset = cluster.append(topic.id(), partitionIndex, stored,
                        visibleAtMs);
                    if (idempotent != null)
                        cluster.rememberSequence(idempotent.producerId(),
                            idempotent.producerEpoch(), topic.id(), partitionIndex,
                            idempotent.baseSequence(), stored.size(), baseOffset);
                    if (idempotent != null) {
                        trace.add("broker-" + id + " producer-state " + tp + " pid="
                            + idempotent.producerId() + " epoch=" + idempotent.producerEpoch()
                            + " seq=" + idempotent.baseSequence());
                    }
                    trace.add("broker-" + id + " produce " + tp + " base=" + baseOffset
                        + " count=" + stored.size());
                    partitionResponse.setErrorCode(Errors.NONE.code()).setBaseOffset(baseOffset);
                }
                topicResponse.partitionResponses().add(partitionResponse);
            }
            responseData.responses().add(topicResponse);
        }
        int throttleMs = throttleModel.throttleMs(ApiKeys.PRODUCE, requestCount);
        if (throttleMs > 0) {
            responseData.setThrottleTimeMs(throttleMs);
            trace.add("broker-" + id + " throttle " + throttleMs + "ms");
        }
        observer.onProduceRequest(id, requestRecords, requestBytes, throttleMs);
        return new org.apache.kafka.common.requests.ProduceResponse(responseData);
    }

    private void handleInjectedProduceError(
        SimCluster.ProduceError injected,
        Uuid topicId,
        int partition,
        TopicPartition tp,
        ProduceRequestData.PartitionProduceData partitionData,
        ProduceResponseData.PartitionProduceResponse partitionResponse
    ) {
        Errors injectedError = injected.error();
        IdempotentHeader idempotent = idempotentHeader(
            (MemoryRecords) partitionData.records());
        if (injected.forgetProducerState() && idempotent != null)
            cluster.forgetProducerState(idempotent.producerId(), topicId, partition);
        String producerState = idempotent == null ? "" : " pid="
            + idempotent.producerId() + " epoch=" + idempotent.producerEpoch()
            + " seq=" + idempotent.baseSequence();
        trace.add("broker-" + id + " produce " + injectedError + " " + tp + producerState);
        partitionResponse.setErrorCode(injectedError.code()).setBaseOffset(-1)
            .setLogStartOffset(injected.logStartOffset());
    }

    private record ResolvedTopic(Uuid id, String name, Errors error) { }

    /** Resolve one wire topic into its active incarnation and reject stale or mismatched ids. */
    private ResolvedTopic resolveTopic(Uuid requestedId, String requestedName,
                                       boolean topicIdRequired) {
        boolean hasId = requestedId != null && !requestedId.equals(Uuid.ZERO_UUID);
        boolean hasName = requestedName != null && !requestedName.isEmpty();
        if (hasId) {
            String activeName = cluster.topicName(requestedId);
            if (activeName == null)
                return new ResolvedTopic(requestedId, requestedName, Errors.UNKNOWN_TOPIC_ID);
            if (hasName && !cluster.topicMatches(requestedId, requestedName))
                return new ResolvedTopic(requestedId, requestedName,
                    Errors.INCONSISTENT_TOPIC_ID);
            return new ResolvedTopic(requestedId, activeName, Errors.NONE);
        }
        if (topicIdRequired)
            return new ResolvedTopic(Uuid.ZERO_UUID, requestedName, Errors.UNKNOWN_TOPIC_ID);
        if (!hasName)
            return new ResolvedTopic(Uuid.ZERO_UUID, requestedName,
                Errors.UNKNOWN_TOPIC_OR_PARTITION);
        Uuid activeId = cluster.topicId(requestedName);
        if (activeId.equals(Uuid.ZERO_UUID))
            return new ResolvedTopic(activeId, requestedName,
                Errors.UNKNOWN_TOPIC_OR_PARTITION);
        if (!cluster.topicMatches(activeId, requestedName))
            return new ResolvedTopic(activeId, requestedName, Errors.INCONSISTENT_TOPIC_ID);
        return new ResolvedTopic(activeId, requestedName, Errors.NONE);
    }

    /**
     * Idempotent-only InitProducerId. Real brokers always allocate a fresh producer id at epoch
     * zero when {@code transactionalId} is null; the expected producer fields are meaningful to
     * transactional coordinator requests and cannot fence an idempotent-only producer.
     */
    private AbstractResponse handleInitProducerId(InitProducerIdRequest request) {
        if (request.data().transactionalId() != null) {
            throw new UnsupportedOperationException(
                "SimBroker does not implement transactional InitProducerId");
        }
        InitProducerIdResponseData response = new InitProducerIdResponseData();
        long producerId = cluster.allocateProducerId();
        trace.add("broker-" + id + " init-producer-id " + producerId + " epoch=0");
        return new InitProducerIdResponse(response
            .setErrorCode(Errors.NONE.code())
            .setProducerId(producerId)
            .setProducerEpoch((short) 0));
    }

    /** Name-free identity for fetch-session and response correlation. */
    private record PartitionKey(Uuid topicId, int partition) { }

    /** The request data plus the boundary name needed by pre-topic-id protocol versions. */
    private record SessionPartition(String topicName, FetchRequest.PartitionData data) { }

    /** A legacy name-only request that could not be resolved to an active topic id. */
    private record UnresolvedPartition(String topicName, int partition,
                                       FetchRequest.PartitionData data) { }

    private record CanonicalFetchData(
        LinkedHashMap<PartitionKey, SessionPartition> partitions,
        List<UnresolvedPartition> unresolved) { }

    /** Response fields cached by KIP-227 to decide whether an incremental response may omit it. */
    private record FetchResponseBaseline(long highWatermark, long logStartOffset) { }

    /** Scheduler-confined broker state for one incremental fetch session. */
    private static final class FetchSessionState {
        private final int id;
        private int expectedEpoch;
        private final boolean usesTopicIds;
        private final LinkedHashMap<PartitionKey, SessionPartition> partitions;
        private final Map<PartitionKey, FetchResponseBaseline> responseBaselines = new HashMap<>();

        private FetchSessionState(int id,
                                  boolean usesTopicIds,
                                  LinkedHashMap<PartitionKey, SessionPartition> partitions) {
            this.id = id;
            this.expectedEpoch = FetchMetadata.nextEpoch(FetchMetadata.INITIAL_EPOCH);
            this.usesTopicIds = usesTopicIds;
            this.partitions = partitions;
        }
    }

    /** Incremental fetch sessions, owned exclusively by the simulated broker scheduler. */
    private final Map<Integer, FetchSessionState> fetchSessions = new HashMap<>();
    private int nextFetchSessionId = 1;
    /** Fences a full request parked before a broker-wide session eviction. */
    private long fetchSessionGeneration;

    /**
     * Resolve the request's partition set against its session, returning the partitions to
     * answer plus the session id to report. A full request replaces the session; an incremental
     * one merges its updates and removals into the stored set.
     */
    @SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
    private FetchSessionResolution resolveFetchSession(FetchRequest request,
                                                       Map<Uuid, String> topicNames,
                                                       boolean throttled) {
        FetchMetadata metadata = request.metadata();
        CanonicalFetchData sent = canonicalFetchData(request, topicNames);
        if (metadata.isFull()) {
            if (metadata.sessionId() != FetchMetadata.INVALID_SESSION_ID) {
                FetchSessionState removed = fetchSessions.remove(metadata.sessionId());
                if (removed != null) {
                    trace.add("broker-" + id + " fetch-session close " + removed.id);
                }
            }
            // FINAL is either a legacy sessionless request (id 0) or an explicit close-only
            // request. In both cases the broker answers the supplied partitions but stores none.
            if (metadata.epoch() == FetchMetadata.FINAL_EPOCH || throttled
                || sent.partitions().isEmpty()) {
                return new FetchSessionResolution(null, sent.partitions(),
                    FetchMetadata.INVALID_SESSION_ID, Errors.NONE, sent.unresolved(), false);
            }
            LinkedHashMap<PartitionKey, SessionPartition> partitions =
                new LinkedHashMap<>(sent.partitions());
            int sessionId = nextFetchSessionId++;
            FetchSessionState candidate = new FetchSessionState(sessionId,
                request.version() >= 13, partitions);
            return new FetchSessionResolution(candidate, partitions, sessionId, Errors.NONE,
                sent.unresolved(), true);
        }
        FetchSessionState session = fetchSessions.get(metadata.sessionId());
        if (session == null) {
            trace.add("broker-" + id + " fetch-session unknown " + metadata.sessionId());
            return new FetchSessionResolution(null, new LinkedHashMap<>(),
                FetchMetadata.INVALID_SESSION_ID, Errors.FETCH_SESSION_ID_NOT_FOUND,
                List.of(), false);
        }
        if (session.expectedEpoch != metadata.epoch()) {
            trace.add("broker-" + id + " fetch-session invalid-epoch " + metadata.sessionId()
                + " expected=" + session.expectedEpoch + " received=" + metadata.epoch());
            return new FetchSessionResolution(null, new LinkedHashMap<>(), session.id,
                Errors.INVALID_FETCH_SESSION_EPOCH, List.of(), false);
        }
        boolean requestUsesTopicIds = request.version() >= 13;
        if (session.usesTopicIds != requestUsesTopicIds) {
            trace.add("broker-" + id + " fetch-session topic-id-mode " + metadata.sessionId());
            return new FetchSessionResolution(null, new LinkedHashMap<>(), session.id,
                Errors.FETCH_SESSION_TOPIC_ID_ERROR, List.of(), false);
        }
        for (Map.Entry<PartitionKey, SessionPartition> entry : sent.partitions().entrySet()) {
            if (request.version() < 13) {
                List<PartitionKey> replacedKeys = session.partitions.entrySet().stream()
                    .filter(existing ->
                        existing.getKey().partition() == entry.getKey().partition()
                            && java.util.Objects.equals(existing.getValue().topicName(),
                                entry.getValue().topicName()))
                    .map(Map.Entry::getKey)
                    .toList();
                for (PartitionKey key : replacedKeys) {
                    if (!key.equals(entry.getKey())) {
                        session.partitions.remove(key);
                        session.responseBaselines.remove(key);
                    }
                }
            }
            session.partitions.put(entry.getKey(), entry.getValue());
        }
        for (TopicIdPartition forgotten : request.forgottenTopics(topicNames)) {
            if (request.version() >= 13) {
                PartitionKey key = new PartitionKey(forgotten.topicId(), forgotten.partition());
                session.partitions.remove(key);
                session.responseBaselines.remove(key);
            } else {
                List<PartitionKey> forgottenKeys = session.partitions.entrySet().stream()
                    .filter(existing -> existing.getKey().partition() == forgotten.partition()
                        && java.util.Objects.equals(existing.getValue().topicName(),
                            forgotten.topic()))
                    .map(Map.Entry::getKey)
                    .toList();
                for (PartitionKey key : forgottenKeys) {
                    session.partitions.remove(key);
                    session.responseBaselines.remove(key);
                }
            }
        }
        session.expectedEpoch = FetchMetadata.nextEpoch(session.expectedEpoch);
        if (session.partitions.isEmpty()) {
            fetchSessions.remove(session.id);
            trace.add("broker-" + id + " fetch-session close-empty " + session.id);
            return new FetchSessionResolution(null, sent.partitions(),
                FetchMetadata.INVALID_SESSION_ID, Errors.NONE, sent.unresolved(), false);
        }
        trace.add("broker-" + id + " fetch-session incremental " + metadata.sessionId()
            + " epoch=" + metadata.epoch() + " sent=" + sent.partitions().size()
            + " partitions=" + session.partitions.size());
        return new FetchSessionResolution(session, session.partitions, metadata.sessionId(),
            Errors.NONE, sent.unresolved(), false);
    }

    private CanonicalFetchData canonicalFetchData(FetchRequest request,
                                                  Map<Uuid, String> topicNames) {
        LinkedHashMap<PartitionKey, SessionPartition> partitions = new LinkedHashMap<>();
        List<UnresolvedPartition> unresolved = new ArrayList<>();
        for (Map.Entry<TopicIdPartition, FetchRequest.PartitionData> entry :
            request.fetchData(topicNames).entrySet()) {
            TopicIdPartition requested = entry.getKey();
            Uuid topicId = requested.topicId();
            if (topicId.equals(Uuid.ZERO_UUID) && request.version() < 13) {
                topicId = cluster.topicId(requested.topic());
                if (topicId.equals(Uuid.ZERO_UUID)) {
                    unresolved.add(new UnresolvedPartition(requested.topic(),
                        requested.partition(), entry.getValue()));
                    continue;
                }
            }
            partitions.put(new PartitionKey(topicId, requested.partition()),
                new SessionPartition(requested.topic(), entry.getValue()));
        }
        return new CanonicalFetchData(partitions, List.copyOf(unresolved));
    }

    /** Forget every fetch session, as a broker does when evicting under session pressure. */
    public void evictFetchSessions() {
        owner.check();
        fetchSessions.clear();
        fetchSessionGeneration++;
        trace.add("broker-" + id + " fetch-session evict-all");
    }

    /** Scheduler-owned observability seam for deterministic session-lifecycle assertions. */
    int activeFetchSessionCount() {
        owner.check();
        return fetchSessions.size();
    }

    /** Scheduler-owned count used to assert that close and deadlines drain purgatory. */
    int activeParkedFetchCount() {
        owner.check();
        return parkedFetches.values().stream().mapToInt(Map::size).sum();
    }

    private record FetchSessionResolution(
        FetchSessionState state,
        Map<PartitionKey, SessionPartition> partitions,
        int sessionId,
        Errors error,
        List<UnresolvedPartition> unresolved,
        boolean installAfterResponse) { }

    /** Session/throttle resolution happens once, before a fetch can enter purgatory. */
    private record PendingFetch(
        FetchRequest request,
        FetchSessionResolution session,
        long sessionGeneration) { }

    private record FetchPreparation(AbstractResponse response, PendingFetch pending) { }

    private record FetchEvaluation(
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> response,
        int selectedBytes,
        boolean hasErrors,
        List<String> traceEvents) { }

    private FetchPreparation prepareFetch(FetchRequest request, int requestCount) {
        Errors topLevelError = cluster.fetchTopLevelError();
        if (topLevelError != Errors.NONE) {
            trace.add("broker-" + id + " fetch top-level " + topLevelError);
            return new FetchPreparation(FetchResponse.of(topLevelError, 0,
                FetchMetadata.INVALID_SESSION_ID, new LinkedHashMap<>(), List.of()), null);
        }
        Map<Uuid, String> topicNames = new HashMap<>();
        for (String topic : cluster.topicNames())
            topicNames.put(cluster.topicId(topic), topic);
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> response =
            new LinkedHashMap<>();
        int throttleMs = throttleModel.throttleMs(ApiKeys.FETCH, requestCount);
        FetchSessionResolution session = resolveFetchSession(request, topicNames,
            throttleMs > 0);
        if (session.error() != Errors.NONE) {
            return new FetchPreparation(FetchResponse.of(session.error(), 0,
                FetchMetadata.INVALID_SESSION_ID, response, List.of()), null);
        }
        if (throttleMs > 0) {
            trace.add("broker-" + id + " throttle FETCH " + throttleMs + "ms");
            return new FetchPreparation(FetchResponse.of(Errors.NONE, throttleMs,
                session.sessionId(), response, List.of()), null);
        }

        // The session object retains its broker-owned baseline, while the request's resolved
        // partition view is frozen so later requests cannot reshape a parked response.
        FetchSessionResolution settled = new FetchSessionResolution(session.state(),
            new LinkedHashMap<>(session.partitions()), session.sessionId(), session.error(),
            session.unresolved(), session.installAfterResponse());
        PendingFetch pending = new PendingFetch(request, settled, fetchSessionGeneration);
        FetchEvaluation evaluation = evaluateFetch(pending);
        if (shouldPark(pending, evaluation))
            return new FetchPreparation(null, pending);
        return new FetchPreparation(finishFetch(pending, evaluation), null);
    }

    /** Re-run only visibility and partition selection; session epoch and throttle stay fixed. */
    @SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
    private FetchEvaluation evaluateFetch(PendingFetch pending) {
        FetchRequest request = pending.request();
        FetchSessionResolution session = pending.session();
        long nowMs = trace.time().milliseconds();
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> response =
            new LinkedHashMap<>();
        List<String> responseTrace = new ArrayList<>();
        boolean incremental = !request.metadata().isFull();
        int responseBytes = 0;
        boolean hasErrors = !session.unresolved().isEmpty();
        for (UnresolvedPartition unresolved : session.unresolved()) {
            TopicIdPartition responseKey = new TopicIdPartition(Uuid.ZERO_UUID,
                unresolved.partition(), unresolved.topicName());
            response.put(responseKey, errorFetchPartition(unresolved.partition(),
                Errors.UNKNOWN_TOPIC_OR_PARTITION));
            responseTrace.add("broker-" + id + " fetch UNKNOWN_TOPIC_OR_PARTITION "
                + unresolved.topicName() + "-" + unresolved.partition());
        }
        for (Map.Entry<PartitionKey, SessionPartition> entry :
            session.partitions().entrySet()) {
            PartitionKey key = entry.getKey();
            SessionPartition sessionPartition = entry.getValue();
            FetchRequest.PartitionData requestData = sessionPartition.data();
            String activeName = cluster.topicName(key.topicId());
            String displayName = activeName == null ? sessionPartition.topicName() : activeName;
            TopicPartition tp = new TopicPartition(displayName, key.partition());
            TopicIdPartition responseKey = new TopicIdPartition(key.topicId(), tp);
            Errors error = Errors.NONE;
            if (activeName == null) {
                error = request.version() >= 13
                    ? Errors.UNKNOWN_TOPIC_ID : Errors.UNKNOWN_TOPIC_OR_PARTITION;
            } else if (sessionPartition.topicName() != null
                && !cluster.topicMatches(key.topicId(), sessionPartition.topicName())) {
                error = Errors.INCONSISTENT_TOPIC_ID;
            } else if (cluster.leader(key.topicId(), key.partition()) < 0) {
                error = Errors.UNKNOWN_TOPIC_OR_PARTITION;
            }
            // Consumers read only up to the visible end (the high watermark): records whose
            // append-visibility delay has not elapsed are excluded, and like a real broker a
            // fetch past the high watermark is out of range even when the log end is beyond it.
            long endOffset = error == Errors.NONE
                ? cluster.visibleLogEndOffset(key.topicId(), key.partition(), nowMs) : 0L;
            long logStartOffset = error == Errors.NONE
                ? cluster.logStartOffset(key.topicId(), key.partition()) : 0L;
            FetchResponseData.PartitionData partition = new FetchResponseData.PartitionData()
                .setPartitionIndex(key.partition())
                .setLogStartOffset(logStartOffset)
                .setHighWatermark(endOffset)
                .setLastStableOffset(endOffset);
            if (error == Errors.NONE)
                error = cluster.fetchError(key.topicId(), key.partition());
            if (error == Errors.NONE && cluster.leader(key.topicId(), key.partition()) != id)
                error = Errors.NOT_LEADER_OR_FOLLOWER;
            if (error == Errors.NONE
                && (requestData.fetchOffset < logStartOffset
                    || requestData.fetchOffset > endOffset))
                error = Errors.OFFSET_OUT_OF_RANGE;
            partition.setErrorCode(error.code());
            if (error == Errors.NONE) {
                Selection selection = selectRecords(key, requestData, endOffset,
                    responseBytes, request.maxBytes());
                if (incremental && !mustIncludeIncrementalPartition(
                    session.state(), key, partition, selection.records())) {
                    continue;
                }
                partition.setRecords(selection.records());
                responseBytes += selection.records().sizeInBytes();
                responseTrace.add("broker-" + id + " fetch " + tp + " offset="
                    + requestData.fetchOffset + " count=" + selection.count());
            } else {
                hasErrors = true;
                partition.setRecords(MemoryRecords.EMPTY);
                responseTrace.add("broker-" + id + " fetch " + error + " " + tp);
            }
            response.put(responseKey, partition);
        }
        return new FetchEvaluation(response, responseBytes, hasErrors,
            List.copyOf(responseTrace));
    }

    private static boolean shouldPark(PendingFetch pending, FetchEvaluation evaluation) {
        FetchRequest request = pending.request();
        return request.maxWait() > 0
            && !pending.session().partitions().isEmpty()
            && !evaluation.hasErrors()
            && evaluation.selectedBytes() < request.minBytes();
    }

    /** Serialize only after selection settles, so newly visible records are encoded on wakeup. */
    private AbstractResponse finishFetch(PendingFetch pending, FetchEvaluation evaluation) {
        evaluation.traceEvents().forEach(trace::add);
        updateFetchSessionAfterResponse(pending.session(), evaluation.response(),
            pending.sessionGeneration());
        return FetchResponse.of(Errors.NONE, 0, pending.session().sessionId(),
            evaluation.response(), List.of());
    }

    private Response completeParked(Parked parked, boolean deadlineReached) {
        if (!parked.active)
            return null;
        FetchEvaluation evaluation = evaluateFetch(parked.fetch);
        if (!deadlineReached && shouldPark(parked.fetch, evaluation)) {
            parked.signalNextVisibility();
            return null;
        }
        removeParked(parked);
        String reason = deadlineReached ? "deadline"
            : evaluation.hasErrors() ? "error" : "data";
        trace.add("broker-" + id + " fetch unpark connection=" + parked.connectionKey
            + " reason=" + reason + " bytes=" + evaluation.selectedBytes());
        AbstractResponse response = finishFetch(parked.fetch, evaluation);
        return serialize(response, parked.header, parked.processingDelayMs);
    }

    private void cancelParked(Parked parked) {
        if (!parked.active)
            return;
        removeParked(parked);
        trace.add("broker-" + id + " fetch cancel connection=" + parked.connectionKey);
    }

    private void removeParked(Parked parked) {
        LinkedHashMap<Long, Parked> connection = parkedFetches.get(parked.connectionKey);
        if (connection != null) {
            connection.remove(parked.ordinal);
            if (connection.isEmpty())
                parkedFetches.remove(parked.connectionKey);
        }
        parked.active = false;
        parked.appendWakeup = null;
    }

    private void onAppend(Uuid topicId, int partition, long visibleAtMs) {
        owner.check();
        for (LinkedHashMap<Long, Parked> connection : parkedFetches.values()) {
            for (Parked parked : connection.values()) {
                if (parked.interestedIn(topicId, partition))
                    parked.signalAppend(visibleAtMs);
            }
        }
    }

    @SuppressWarnings("BooleanExpressionComplexity")
    private static boolean mustIncludeIncrementalPartition(FetchSessionState session,
                                                           PartitionKey key,
                                                           FetchResponseData.PartitionData data,
                                                           MemoryRecords records) {
        FetchResponseBaseline baseline = session.responseBaselines.get(key);
        return baseline == null
            || records.sizeInBytes() > 0
            || baseline.highWatermark() != data.highWatermark()
            || baseline.logStartOffset() != data.logStartOffset()
            || data.errorCode() != Errors.NONE.code()
            || FetchResponse.isPreferredReplica(data)
            || FetchResponse.isDivergingEpoch(data);
    }

    private void updateFetchSessionAfterResponse(
        FetchSessionResolution resolution,
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> response,
        long resolvedGeneration
    ) {
        FetchSessionState state = resolution.state();
        if (state == null)
            return;
        List<PartitionKey> recordBearing = new ArrayList<>();
        for (Map.Entry<TopicIdPartition, FetchResponseData.PartitionData> entry :
            response.entrySet()) {
            TopicIdPartition idPartition = entry.getKey();
            PartitionKey key = new PartitionKey(idPartition.topicId(), idPartition.partition());
            FetchResponseData.PartitionData data = entry.getValue();
            long highWatermark = data.errorCode() == Errors.NONE.code()
                ? data.highWatermark() : FetchResponse.INVALID_HIGH_WATERMARK;
            state.responseBaselines.put(key,
                new FetchResponseBaseline(highWatermark, data.logStartOffset()));
            if (FetchResponse.recordsSize(data) > 0)
                recordBearing.add(key);
        }
        if (!resolution.installAfterResponse()) {
            for (PartitionKey key : recordBearing) {
                SessionPartition partition = state.partitions.remove(key);
                if (partition != null) {
                    state.partitions.put(key, partition);
                    trace.add("broker-" + id + " fetch-session rotate " + state.id + " "
                        + key.topicId() + "-" + key.partition());
                }
            }
        }
        if (resolution.installAfterResponse() && resolvedGeneration == fetchSessionGeneration) {
            fetchSessions.put(state.id, state);
            trace.add("broker-" + id + " fetch-session open " + state.id
                + " partitions=" + state.partitions.size());
        } else if (resolution.installAfterResponse()) {
            trace.add("broker-" + id + " fetch-session skip-open-after-eviction " + state.id);
        }
    }

    private static FetchResponseData.PartitionData errorFetchPartition(int partition,
                                                                       Errors error) {
        return new FetchResponseData.PartitionData()
            .setPartitionIndex(partition)
            .setErrorCode(error.code())
            .setLogStartOffset(0L)
            .setHighWatermark(0L)
            .setLastStableOffset(0L)
            .setRecords(MemoryRecords.EMPTY);
    }

    private record Selection(MemoryRecords records, int count) { }

    /**
     * The records to answer one partition with: an exact fixture when the test installed one,
     * otherwise whole append batches starting with the one containing the fetch offset,
     * truncated to the per-partition and whole-response byte caps (the first response batch is
     * always admitted, as a real broker does so one large batch cannot stall a consumer).
     */
    private Selection selectRecords(PartitionKey key,
                                    FetchRequest.PartitionData requestData,
                                    long endOffset,
                                    int responseBytes,
                                    int responseMaxBytes) {
        Optional<MemoryRecords> exact = cluster.fetchRecordsOverride(
            key.topicId(), key.partition(), requestData.fetchOffset);
        if (exact.isPresent()) {
            MemoryRecords records = exact.get();
            int count = 0;
            for (RecordBatch batch : records.batches()) {
                java.util.Iterator<Record> iterator = batch.iterator();
                while (iterator.hasNext()) {
                    iterator.next();
                    count++;
                }
            }
            return new Selection(records, count);
        }
        List<SimCluster.StoredRecord> log = cluster.log(key.topicId(), key.partition());
        List<MemoryRecords> selected = new ArrayList<>();
        int selectedBytes = 0;
        int selectedRecords = 0;
        for (SimCluster.StoredBatch storedBatch :
            cluster.storedBatches(key.topicId(), key.partition())) {
            if (storedBatch.nextOffset() <= requestData.fetchOffset)
                continue;
            if (storedBatch.baseOffset() >= endOffset)
                break;
            if (storedBatch.nextOffset() > endOffset) {
                throw new IllegalStateException("Visible offset " + endOffset
                    + " splits stored batch at " + storedBatch.baseOffset());
            }
            List<SimpleRecord> batchRecords = new ArrayList<>(storedBatch.recordCount());
            for (SimCluster.StoredRecord record : log.subList(
                Math.toIntExact(storedBatch.baseOffset()),
                Math.toIntExact(storedBatch.nextOffset()))) {
                batchRecords.add(new SimpleRecord(record.timestamp(), record.key(), record.value(),
                    record.headers()));
            }
            MemoryRecords candidate = MemoryRecords.withRecords(storedBatch.baseOffset(),
                cluster.fetchCompression(key.topicId()),
                batchRecords.toArray(SimpleRecord[]::new));
            boolean firstBatch = selected.isEmpty() && responseBytes == 0;
            long candidatePartitionBytes = (long) selectedBytes + candidate.sizeInBytes();
            long candidateResponseBytes = (long) responseBytes + candidatePartitionBytes;
            if (!firstBatch && (candidatePartitionBytes > requestData.maxBytes
                || candidateResponseBytes > responseMaxBytes)) {
                break;
            }
            selected.add(candidate);
            selectedBytes = Math.toIntExact(candidatePartitionBytes);
            selectedRecords += storedBatch.recordCount();
        }
        return new Selection(concatenate(selected, selectedBytes), selectedRecords);
    }

    /** Concatenate independently encoded batches without flattening their base offsets. */
    private static MemoryRecords concatenate(List<MemoryRecords> batches, int sizeInBytes) {
        if (batches.isEmpty())
            return MemoryRecords.EMPTY;
        ByteBuffer combined = ByteBuffer.allocate(sizeInBytes);
        for (MemoryRecords batch : batches)
            combined.put(batch.buffer().duplicate());
        combined.flip();
        return MemoryRecords.readableRecords(combined);
    }

    private AbstractResponse handleListOffsets(ListOffsetsRequest request) {
        ListOffsetsResponseData response = new ListOffsetsResponseData();
        for (ListOffsetsRequestData.ListOffsetsTopic topic : request.topics()) {
            ListOffsetsResponseData.ListOffsetsTopicResponse topicResponse =
                new ListOffsetsResponseData.ListOffsetsTopicResponse().setName(topic.name());
            for (ListOffsetsRequestData.ListOffsetsPartition partition : topic.partitions()) {
                TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                ListOffsetsResponseData.ListOffsetsPartitionResponse partitionResponse =
                    new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                        .setPartitionIndex(partition.partitionIndex())
                        .setLeaderEpoch(cluster.leaderEpoch(tp))
                        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP);
                if (cluster.leader(tp) != id) {
                    partitionResponse.setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
                        .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET);
                } else if (partition.timestamp() == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
                    partitionResponse.setErrorCode(Errors.NONE.code())
                        .setOffset(cluster.logStartOffset(tp));
                } else if (partition.timestamp() == ListOffsetsRequest.LATEST_TIMESTAMP) {
                    // The latest offset a consumer may read from: the visible end, not the LEO.
                    partitionResponse.setErrorCode(Errors.NONE.code())
                        .setOffset(cluster.visibleLogEndOffset(tp,
                            trace.time().milliseconds()));
                } else if (partition.timestamp() >= 0L) {
                    Optional<SimCluster.TimestampOffset> match = cluster.offsetForTimestamp(
                        tp, partition.timestamp(), trace.time().milliseconds());
                    if (match.isPresent()) {
                        partitionResponse.setErrorCode(Errors.NONE.code())
                            .setOffset(match.get().offset())
                            .setTimestamp(match.get().timestamp());
                    } else {
                        partitionResponse.setErrorCode(Errors.NONE.code())
                            .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
                            .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP);
                    }
                } else {
                    partitionResponse.setErrorCode(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT.code())
                        .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET);
                }
                topicResponse.partitions().add(partitionResponse);
            }
            response.topics().add(topicResponse);
        }
        trace.add("broker-" + id + " list-offsets");
        return new ListOffsetsResponse(response);
    }

    private AbstractResponse handleCreateTopics(CreateTopicsRequest request) {
        CreateTopicsResponseData response = new CreateTopicsResponseData();
        for (CreateTopicsRequestData.CreatableTopic topic : request.data().topics()) {
            CreateTopicsResponseData.CreatableTopicResult result =
                new CreateTopicsResponseData.CreatableTopicResult().setName(topic.name());
            if (cluster.topicExists(topic.name())) {
                result.setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code());
            } else if (topic.numPartitions() <= 0) {
                result.setErrorCode(Errors.INVALID_PARTITIONS.code());
            } else if (topic.replicationFactor() <= 0
                || topic.replicationFactor() > cluster.nodes().size()) {
                result.setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code());
            } else {
                Uuid topicId = cluster.createTopicId(topic.name(), topic.numPartitions());
                result.setErrorCode(Errors.NONE.code())
                    .setTopicId(topicId)
                    .setNumPartitions(topic.numPartitions())
                    .setReplicationFactor(topic.replicationFactor());
                trace.add("broker-" + id + " create-topic " + topic.name()
                    + " partitions=" + topic.numPartitions());
            }
            response.topics().add(result);
        }
        return new CreateTopicsResponse(response);
    }

    /** Validate batch integrity (CRC) exactly like a real broker, then copy records out. */
    private record IdempotentHeader(long producerId, short producerEpoch, int baseSequence) { }

    /**
     * The idempotent producer state stamped into the request's batch headers, or null when the
     * records carry no producer id. A produce request from this client holds exactly one batch
     * per partition, so a single header describes it.
     */
    private static IdempotentHeader idempotentHeader(MemoryRecords records) {
        for (RecordBatch batch : records.batches()) {
            if (batch.hasProducerId()) {
                return new IdempotentHeader(batch.producerId(), batch.producerEpoch(),
                    batch.baseSequence());
            }
        }
        return null;
    }

    private List<SimCluster.StoredRecord> validateAndExtract(MemoryRecords records) {
        List<SimCluster.StoredRecord> stored = new ArrayList<>();
        for (RecordBatch batch : records.batches()) {
            batch.ensureValid();
            for (Record record : batch) {
                stored.add(new SimCluster.StoredRecord(
                    Utils.toNullableArray(record.key()),
                    Utils.toNullableArray(record.value()),
                    record.timestamp(),
                    record.headers()));
            }
        }
        return stored;
    }
}
