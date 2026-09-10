package io.krkafka.producer;

import io.krkafka.ffi.kr_event;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A bounded, idempotent Kafka producer backed by kr-kafka-ffi (JDK 25).
 *
 * <p>Supports acks=all, none/zstd and the documented TLS/SASL subset. Rust owns
 * retries (at most 254 retries), batching and byte-based unkeyed routing;
 * metrics currently returns an immutable empty map. Transactions and broker
 * telemetry are unsupported. This is a supported subset of KafkaProducer.
 *
 * <p>A close duration bounds the requested delivery phase. Physical native I/O
 * retirement and user plugin/callback code may extend elapsed close time.
 * Callback send may proceed only if admission can complete immediately; callbacks
 * must not wait for pending deliveries. Supplied serializers are closed by this
 * producer but are not configured, matching KafkaProducer ownership.
 *
 * <p>The binding bounds pending records, topics, flushes, scratch bytes and
 * concurrent serialization. A serializer's arbitrary internal allocations are
 * outside those bounds. Valid serialized payload retained by concurrent sends
 * is bounded by serializationConcurrency times max.request.size. Producers with
 * interceptors additionally retain bounded copied headers until acknowledgement;
 * serialized key/value arrays are never retained for delivery.
 */
public final class KrKafkaProducer<K, V> implements Producer<K, V> {
    private static final System.Logger LOG = System.getLogger(KrKafkaProducer.class.getName());
    private static final long RECHECK_NANOS = 1_000_000;
    enum State { OPEN, FAILED, CLOSING, DESTROYING, DESTROYED }

    private final ProducerSettings settings;
    private final ReentrantLock callGate = new ReentrantLock();
    private final Condition changed = callGate.newCondition();
    private final PendingTable pending;
    private final ScratchPool scratch;
    private final HeaderContextPool headerContexts;
    private final Map<String, Topic> topics = new HashMap<>();
    private final Map<Integer, Topic> topicHandles = new HashMap<>();
    private final Map<Long, FlushWait> flushes = new HashMap<>();
    private final ArrayList<AutoCloseable> ownedPlugins = new ArrayList<>();
    private final ArrayList<ProducerInterceptor<K, V>> interceptors = new ArrayList<>();
    private final CountDownLatch teardown = new CountDownLatch(1);
    private final ThreadLocal<Integer> operationDepth = ThreadLocal.withInitial(() -> 0);
    private final ThreadLocal<Boolean> callbackContext = ThreadLocal.withInitial(() -> false);
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final Partitioner partitioner;
    private final NativeAccess nativeAccess;
    private final Thread poller;
    private State state = State.OPEN;
    private KafkaException failure;
    private int activeOperations;
    private long closeStarted;
    private long closeBudget = Long.MAX_VALUE;
    private boolean nativeCloseSent;
    private boolean normalClosed;
    private boolean aborted;
    private boolean ownerClosed;

    private static final class Topic {
        final String name;
        final int handle;
        boolean ready;
        Uuid id;
        KafkaException failure;
        boolean retirementRequested;
        boolean retirementObserved;
        long lastSnapshot;
        Topic(String name, int handle) { this.name = name; this.handle = handle; }
    }

    private static final class FlushWait {
        boolean done;
        KafkaException failure;
    }

    /** Monotonic budget excludes user code, includes all binding-controlled waits. */
    private static final class Budget {
        long remaining;
        long last = System.nanoTime();
        Budget(long nanos) { remaining = nanos; }
        long remaining() {
            long now = System.nanoTime();
            long elapsed = Math.max(0, now - last);
            remaining = Math.max(0, remaining - elapsed);
            last = now;
            return remaining;
        }
        void beforeUserCode() { remaining(); }
        void afterUserCode() { last = System.nanoTime(); }
    }

    public KrKafkaProducer(Map<String, ?> configs) { this(configs, null, null); }
    public KrKafkaProducer(Properties configs) { this(properties(configs), null, null); }
    public KrKafkaProducer(Properties configs, Serializer<K> key, Serializer<V> value) {
        this(properties(configs), key, value);
    }
    public KrKafkaProducer(Map<String, ?> configs, Serializer<K> key, Serializer<V> value) {
        this(new ProducerSettings(configs), key, value, null);
    }

    KrKafkaProducer(ProducerSettings settings, Serializer<K> key, Serializer<V> value, NativeAccess injected) {
        this.settings = settings;
        this.pending = new PendingTable(settings.recordDescriptors);
        this.scratch = new ScratchPool(settings.scratchBytes, settings.scratchCheckouts);
        this.headerContexts = new HeaderContextPool(settings.interceptorHeaderBytes);
        NativeAccess created = injected;
        try {
            keySerializer = serializer(key, "key.serializer", true);
            valueSerializer = serializer(value, "value.serializer", false);
            for (Object configured : pluginList(settings.originals.get("interceptor.classes"))) {
                @SuppressWarnings("unchecked")
                ProducerInterceptor<K, V> interceptor = (ProducerInterceptor<K, V>) instantiate(configured, ProducerInterceptor.class);
                own(interceptor);
                interceptor.configure(settings.originals);
                interceptors.add(interceptor);
            }
            Object configuredPartitioner = settings.originals.get("partitioner.class");
            partitioner = configuredPartitioner == null ? null : instantiate(configuredPartitioner, Partitioner.class);
            if (partitioner != null) { own(partitioner); partitioner.configure(settings.originals); }
            if (created == null) created = NativeAccess.create(settings);
            nativeAccess = created;
            poller = Thread.ofPlatform().daemon(true).name("kr-kafka-poller").unstarted(this::pollLoop);
            poller.start();
        } catch (Throwable error) {
            if (created != null) {
                try { created.destroy(); } catch (Throwable cleanup) { error.addSuppressed(cleanup); }
            }
            closePlugins();
            scratch.close();
            throw error;
        }
    }

    private static Map<String, Object> properties(Properties properties) {
        Map<String, Object> result = new HashMap<>();
        properties.forEach((key, value) -> result.put((String) key, value));
        return result;
    }

    private void own(AutoCloseable plugin) {
        for (AutoCloseable existing : ownedPlugins) if (existing == plugin) return;
        ownedPlugins.add(plugin);
    }

    @SuppressWarnings("unchecked")
    private <T> Serializer<T> serializer(Serializer<T> supplied, String setting, boolean key) {
        if (supplied != null) { own(supplied); return supplied; }
        Object configured = settings.originals.get(setting);
        if (configured == null) throw new org.apache.kafka.common.config.ConfigException("Missing " + setting);
        Serializer<T> serializer = (Serializer<T>) instantiate(configured, Serializer.class);
        own(serializer);
        serializer.configure(settings.originals, key);
        return serializer;
    }

    private static List<?> pluginList(Object configured) {
        if (configured == null) return List.of();
        if (configured instanceof List<?> list) return list;
        if (configured instanceof String string) return string.isBlank() ? List.of() : List.of(string.split("\\s*,\\s*"));
        return List.of(configured);
    }

    private static <T> T instantiate(Object configured, Class<T> expected) {
        try {
            Class<?> type = configured instanceof Class<?> c ? c : Class.forName(configured.toString(), true,
                    Thread.currentThread().getContextClassLoader());
            return expected.cast(type.getDeclaredConstructor().newInstance());
        } catch (ReflectiveOperationException | ClassCastException error) {
            throw new org.apache.kafka.common.config.ConfigException("Cannot instantiate " + expected.getSimpleName() + ": " + configured);
        }
    }

    @Override public Future<RecordMetadata> send(ProducerRecord<K, V> record) { return send(record, null); }

    @Override public Future<RecordMetadata> send(ProducerRecord<K, V> original, Callback callback) {
        Objects.requireNonNull(original, "record");
        Budget budget = new Budget(settings.maxBlockNanos);
        ProducerRecord<K, V> record = original;
        boolean active = false;
        try {
            beginOperation(budget);
            active = true;
            budget.beforeUserCode();
            byte[] key;
            byte[] value;
            try {
                for (ProducerInterceptor<K, V> interceptor : interceptors) {
                    try {
                        ProducerRecord<K, V> next = interceptor.onSend(record);
                        if (next != null) record = next;
                    } catch (Throwable error) { pluginFailure(error); }
                }
                validateTopic(record.topic());
                key = serialize(keySerializer, record.topic(), record.headers(), record.key());
                value = serialize(valueSerializer, record.topic(), record.headers(), record.value());
                if (record.headers() instanceof RecordHeaders headers) headers.setReadOnly();
            } finally { budget.afterUserCode(); }

            if (record.partition() != null && record.partition() < 0)
                throw new IllegalArgumentException("partition must be nonnegative");
            Header[] headers = record.headers().toArray();
            if (headers.length > settings.maxHeaderCount) throw new InvalidRecordException("Too many record headers");
            byte[][] headerKeys = new byte[headers.length][];
            byte[][] headerValues = new byte[headers.length][];
            long headerContextBytes = 0;
            long framedBytes = 97L + (key == null ? 0 : key.length) + (value == null ? 0 : value.length);
            if (framedBytes > settings.maxRequestSize) throw new RecordTooLargeException("Serialized record exceeds max.request.size");
            for (int i = 0; i < headers.length; i++) {
                if (headers[i].key() == null) throw new IllegalArgumentException("header key must not be null");
                if (headers[i].key().length() > settings.maxRequestSize)
                    throw new RecordTooLargeException("Header key exceeds max.request.size");
                headerKeys[i] = headerUtf8(headers[i].key());
                headerValues[i] = headers[i].value();
                headerContextBytes += 2L * headers[i].key().length() + (headerValues[i] == null ? 0 : headerValues[i].length);
                framedBytes += 10L + headerKeys[i].length + (headerValues[i] == null ? 0 : headerValues[i].length);
                if (framedBytes > settings.maxRequestSize) throw new RecordTooLargeException("Serialized headers exceed max.request.size");
            }
            if (!interceptors.isEmpty() && headerContextBytes > settings.interceptorHeaderBytes)
                throw new RecordTooLargeException("Record headers exceed kr.interceptor.header.bytes");
            long bytes = NativeAccess.recordBytes(key, value, headerKeys, headerValues);
            // Native scratch includes C descriptors in addition to the wire framing.
            if (bytes > settings.scratchBytes)
                throw new RecordTooLargeException("Serialized record and framing exceed configured record/scratch limit");
            Topic topic = awaitTopic(record.topic(), budget);
            int partition = record.partition() == null ? -1 : record.partition();
            if (partition < 0 && partitioner != null) {
                Cluster cluster = metadata(topic, budget).cluster();
                budget.beforeUserCode();
                try { partition = partitioner.partition(record.topic(), record.key(), key, record.value(), value, cluster); }
                finally { budget.afterUserCode(); }
                if (partition < 0 || cluster.partition(new TopicPartition(record.topic(), partition)) == null)
                    throw new IllegalArgumentException("partitioner returned an invalid partition: " + partition);
            }
            long timestamp = record.timestamp() == null ? System.currentTimeMillis() : record.timestamp();
            DeliveryFuture future = new DeliveryFuture(poller);
            return admit(topic, partition, timestamp, key, value, headerKeys, headerValues, bytes,
                    headerContextBytes, callback, future, budget);
        } catch (SerializationException | IllegalArgumentException | IllegalStateException | InterruptException error) {
            if (active) acknowledgeRejected(record, error);
            throw error;
        } catch (KafkaException error) {
            fenceNativeFailure(error);
            return rejected(record, callback, error, active);
        } finally {
            if (active) endOperation();
        }
    }

    private static <T> byte[] serialize(Serializer<T> serializer, String topic,
                                      org.apache.kafka.common.header.Headers headers, T value) {
        try { return serializer.serialize(topic, headers, value); }
        catch (SerializationException error) { throw error; }
        catch (ClassCastException error) { throw new SerializationException("Serializer cannot accept the record type", error); }
    }

    private Future<RecordMetadata> admit(Topic topic, int partition, long timestamp,
            byte[] key, byte[] value, byte[][] headerKeys, byte[][] headerValues, long bytes,
            long headerContextBytes, Callback callback, DeliveryFuture future, Budget budget) {
        callGate.lock();
        try {
            while (true) {
                requireOpen();
                if (topic.failure != null) throw topic.failure;
                PendingTable.Entry entry = pending.reserve(future, callback, topic.name, topic.handle,
                        topic.id, key == null ? -1 : key.length, value == null ? -1 : value.length, timestamp);
                if (entry == null) { awaitProgress(budget, false); continue; }
                ScratchPool.Slab slab = null;
                boolean accepted = false;
                try {
                    slab = scratch.acquire(bytes);
                    if (slab == null) {
                        releaseEntry(entry);
                        entry = null;
                        awaitProgress(budget, false);
                        continue;
                    }
                    if (!interceptors.isEmpty()) {
                        entry.acknowledgementHeaders = headerContexts.acquire(headerKeys, headerValues, headerContextBytes);
                        if (entry.acknowledgementHeaders == null) {
                            // Release all other admission credits before parking.
                            scratch.release(slab);
                            slab = null;
                            releaseEntry(entry);
                            entry = null;
                            awaitProgress(budget, false);
                            continue;
                        }
                    }
                    MemorySegment packed = NativeAccess.pack(slab.memory, topic.handle, partition,
                            timestamp, entry.token(), key, value, headerKeys, headerValues);
                    nativeAccess.submit(packed);
                    pending.accept(entry);
                    accepted = true;
                    LockSupport.unpark(poller);
                    return future;
                } catch (NativeAccess.CallFailure error) {
                    if (error.code == NativeAccess.FAILED) fail(error);
                    if (error.code != NativeAccess.EXHAUSTED && error.code != NativeAccess.NOT_READY) throw error;
                } finally {
                    if (slab != null) scratch.release(slab);
                    if (!accepted && entry != null) releaseEntry(entry);
                    changed.signalAll();
                }
                awaitProgress(budget, false);
            }
        } finally { callGate.unlock(); }
    }

    private void releaseEntry(PendingTable.Entry entry) {
        headerContexts.release(entry.acknowledgementHeaders);
        pending.release(entry);
    }

    private void beginOperation(Budget budget) {
        callGate.lock();
        try {
            requireOpen();
            while (activeOperations >= settings.serializationConcurrency) {
                awaitProgress(budget, false);
                requireOpen();
            }
            activeOperations++;
            operationDepth.set(operationDepth.get() + 1);
        } finally { callGate.unlock(); }
    }

    private void endOperation() {
        callGate.lock();
        try {
            activeOperations--;
            int depth = operationDepth.get() - 1;
            if (depth == 0) operationDepth.remove(); else operationDepth.set(depth);
            changed.signalAll(); LockSupport.unpark(poller);
        }
        finally { callGate.unlock(); }
    }

    private void requireOpen() {
        if (state == State.FAILED) throw failure;
        if (state != State.OPEN) throw new IllegalStateException("Producer is closed");
    }

    private void awaitProgress(Budget budget, boolean metadata) {
        long remaining = budget.remaining();
        if (Thread.currentThread() == poller || callbackContext.get() || remaining == 0) {
            if (metadata) throw new TimeoutException("Metadata unavailable within max.block.ms");
            throw new BufferExhaustedException("Producer admission capacity unavailable within max.block.ms");
        }
        try { changed.awaitNanos(Math.min(remaining, RECHECK_NANOS)); }
        catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new InterruptException(error); }
    }

    private static void validateTopic(String name) {
        if (name == null || name.isEmpty() || name.equals(".") || name.equals("..") || name.length() > 249)
            throw new org.apache.kafka.common.errors.InvalidTopicException("Invalid topic name");
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-'))
                throw new org.apache.kafka.common.errors.InvalidTopicException("Invalid topic name");
        }
    }

    private static byte[] headerUtf8(String key) {
        try {
            java.nio.ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                    .encode(java.nio.CharBuffer.wrap(key));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (java.nio.charset.CharacterCodingException error) {
            throw new IllegalArgumentException("Header key is not valid Unicode", error);
        }
    }

    private Topic awaitTopic(String name, Budget budget) {
        callGate.lock();
        try {
            while (true) {
                requireOpen();
                Topic topic = topics.get(name);
                if (topic == null) {
                    if (topics.size() >= settings.maxOpenTopics) { awaitProgress(budget, true); continue; }
                    try {
                        int handle = nativeAccess.topicOpen(name);
                        topic = new Topic(name, handle);
                        if (topicHandles.putIfAbsent(handle, topic) != null)
                            throw new NativeAccess.ProtocolFailure("Native topic handle reused before retirement");
                        topics.put(name, topic);
                        LockSupport.unpark(poller);
                    } catch (NativeAccess.CallFailure error) {
                        if (error.code == NativeAccess.FAILED) fail(error);
                        if (error.code != NativeAccess.EXHAUSTED && error.code != NativeAccess.NOT_READY) throw error;
                        awaitProgress(budget, true);
                        continue;
                    }
                }
                NativeAccess.TopicStatus status = nativeAccess.topicStatus(topic.handle);
                if (status.status() == 5) {
                    // The poller retires this Java entry after one further empty
                    // drain so already-published topic events cannot race removal.
                    if (topic.failure != null) throw topic.failure;
                    throw new NativeAccess.CallFailure("topic retired", NativeAccess.CLOSED);
                }
                if (status.status() == 2 || status.status() == 3)
                    topic.failure = new NativeDeliveryException(1, status.reason(), 0);
                if (topic.failure != null) {
                    if (!topic.retirementRequested) {
                        try { nativeAccess.topicClose(topic.handle); topic.retirementRequested = true; }
                        catch (NativeAccess.CallFailure error) {
                            if (error.code != NativeAccess.EXHAUSTED && error.code != NativeAccess.CLOSED) throw error;
                        }
                    }
                    throw topic.failure;
                }
                if (status.status() == 1) {
                    if (status.topicId().equals(Uuid.ZERO_UUID) || topic.id != null && !topic.id.equals(status.topicId()))
                        throw new NativeAccess.ProtocolFailure("Native topic handle changed identity without retirement");
                    topic.id = status.topicId();
                    topic.ready = true;
                    return topic;
                }
                awaitProgress(budget, true);
            }
        } finally { callGate.unlock(); }
    }

    private NativeAccess.Metadata metadata(Topic topic, Budget budget) {
        callGate.lock();
        try {
            if (topic.lastSnapshot != 0 && System.nanoTime() - topic.lastSnapshot >= settings.metadataMaxAgeNanos) {
                nativeAccess.refresh(topic.handle);
                topic.lastSnapshot = 0;
            }
            while (true) {
                requireOpen();
                if (topic.failure != null) throw topic.failure;
                try {
                    NativeAccess.Metadata snapshot = nativeAccess.metadata(topic.handle, topic.name);
                    if (!topic.id.equals(snapshot.cluster().topicId(topic.name)))
                        throw new NativeAccess.ProtocolFailure("Metadata UUID differs from its topic handle");
                    topic.lastSnapshot = System.nanoTime();
                    return snapshot;
                }
                catch (NativeAccess.CallFailure error) {
                    if (error.code == NativeAccess.FAILED) fail(error);
                    if (error.code != NativeAccess.NOT_READY && error.code != NativeAccess.EXHAUSTED) throw error;
                    awaitProgress(budget, true);
                }
            }
        } finally { callGate.unlock(); }
    }

    @Override public List<PartitionInfo> partitionsFor(String topic) {
        validateTopic(topic);
        Budget budget = new Budget(settings.maxBlockNanos);
        beginOperation(budget);
        try { return List.copyOf(metadata(awaitTopic(topic, budget), budget).cluster().partitionsForTopic(topic)); }
        catch (KafkaException error) { fenceNativeFailure(error); throw error; }
        finally { endOperation(); }
    }

    private Future<RecordMetadata> rejected(ProducerRecord<K, V> record, Callback callback, KafkaException error, boolean notifyInterceptors) {
        DeliveryFuture future = new DeliveryFuture(poller);
        if (notifyInterceptors) acknowledgeRejected(record, error);
        try { invokeCallback(callback, null, error); }
        catch (Throwable callbackError) { pluginFailure(callbackError); }
        finally { future.complete(null, error); }
        return future;
    }

    private void acknowledgeRejected(ProducerRecord<K, V> record, Exception error) {
        if (interceptors.isEmpty()) return;
        // Kafka's synchronous rejection borrows the input values. The producer
        // retains none after this invocation; structural mutations are forbidden.
        RecordHeaders headers = record.headers() instanceof RecordHeaders existing
                ? existing : new RecordHeaders(record.headers());
        headers.setReadOnly();
        acknowledge(failedMetadata(record.topic(), record.partition() == null ? -1 : record.partition()), error, headers);
    }

    private static RecordMetadata failedMetadata(String topic, int partition) {
        return new RecordMetadata(new TopicPartition(topic, partition), -1, -1, -1, -1, -1);
    }

    private void acknowledge(RecordMetadata metadata, Exception error, Headers headers) {
        boolean previous = callbackContext.get();
        callbackContext.set(true);
        try {
            for (ProducerInterceptor<K, V> interceptor : interceptors) {
                try { interceptor.onAcknowledgement(metadata, error, headers); }
                catch (Throwable pluginError) { pluginFailure(pluginError); }
            }
        } finally { callbackContext.set(previous); }
    }

    private void invokeCallback(Callback callback, RecordMetadata metadata, Exception error) {
        boolean previous = callbackContext.get();
        callbackContext.set(true);
        try { if (callback != null) callback.onCompletion(metadata, error); }
        finally { callbackContext.set(previous); }
    }

    private static void pluginFailure(Throwable error) {
        LOG.log(System.Logger.Level.WARNING, "Producer plugin/callback failed", error);
    }

    @Override public void flush() {
        if (Thread.currentThread() == poller || callbackContext.get()) throw new IllegalStateException("flush is not allowed from a producer callback");
        Budget budget = new Budget(Long.MAX_VALUE);
        callGate.lock();
        try {
            FlushWait wait;
            while (true) {
                requireOpen();
                if (flushes.size() == settings.maxFlushes) { awaitProgress(budget, false); continue; }
                try {
                    long token = nativeAccess.flush();
                    wait = new FlushWait();
                    if (flushes.putIfAbsent(token, wait) != null)
                        throw new NativeAccess.ProtocolFailure("Native flush token reused");
                    LockSupport.unpark(poller);
                    break;
                } catch (NativeAccess.CallFailure error) {
                    if (error.code == NativeAccess.FAILED) fail(error);
                    if (error.code != NativeAccess.EXHAUSTED) throw error;
                    awaitProgress(budget, false);
                }
            }
            while (!wait.done) {
                if (state == State.CLOSING || state == State.DESTROYING || state == State.DESTROYED)
                    throw new IllegalStateException("Producer closed while flush was pending");
                awaitProgress(budget, false);
            }
            if (wait.failure != null) throw wait.failure;
        } catch (KafkaException error) {
            fenceNativeFailure(error);
            throw error;
        } finally { callGate.unlock(); }
    }

    @Override public void close() { closeNanos(Long.MAX_VALUE); }
    @Override public void close(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) throw new IllegalArgumentException("close timeout must be nonnegative");
        long nanos;
        try { nanos = timeout.toNanos(); } catch (ArithmeticException overflow) { nanos = Long.MAX_VALUE; }
        closeNanos(nanos);
    }

    private void closeNanos(long nanos) {
        boolean callback = Thread.currentThread() == poller || callbackContext.get() || operationDepth.get() > 0;
        callGate.lock();
        try {
            if (state == State.OPEN || state == State.FAILED) {
                state = State.CLOSING;
                closeStarted = System.nanoTime();
                closeBudget = callback ? 0 : nanos;
            } else if (callback && state == State.CLOSING && !nativeCloseSent) closeBudget = 0;
            changed.signalAll();
            LockSupport.unpark(poller);
        } finally { callGate.unlock(); }
        if (callback) return;
        try { teardown.await(); }
        catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new InterruptException(error); }
    }

    private void fail(KafkaException cause) {
        if (failure == null) failure = cause;
        if (state == State.OPEN) state = State.FAILED;
        changed.signalAll();
    }

    private void fenceNativeFailure(KafkaException error) {
        if (error instanceof NativeAccess.ProtocolFailure ||
                error instanceof NativeAccess.CallFailure nativeFailure && nativeFailure.code == NativeAccess.FAILED) {
            callGate.lock();
            try { fail(error); } finally { callGate.unlock(); }
        }
    }

    private void pollLoop() {
        int capacity = Math.min(1024, settings.maxCompletionsPerPoll);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment eventBuffer = kr_event.allocateArray(capacity, arena);
            long park = 1_000;
            while (true) {
                NativeAccess.Event[] events = null;
                boolean destroy = false;
                callGate.lock();
                try {
                    if (state == State.CLOSING && activeOperations == 0 && !nativeCloseSent && !aborted && !normalClosed) {
                        long elapsed = Math.max(0, System.nanoTime() - closeStarted);
                        long remaining = Math.max(0, closeBudget - elapsed);
                        long millis = remaining / 1_000_000;
                        try { nativeAccess.close(millis); nativeCloseSent = true; }
                        catch (NativeAccess.CallFailure error) {
                            if (error.code != NativeAccess.EXHAUSTED && error.code != NativeAccess.CLOSED && error.code != NativeAccess.FAILED)
                                fail(error);
                        }
                    }
                    boolean terminalBeforeDrain = normalClosed || aborted || ownerClosed;
                    events = nativeAccess.poll(eventBuffer, capacity);
                    retireTopics(events.length == 0);
                    int owner = nativeAccess.ownerStatus();
                    if (owner == 2) { aborted = true; fail(new KafkaException("Native owner aborted")); }
                    if (owner == 1) {
                        ownerClosed = true;
                        if (state == State.OPEN) fail(new KafkaException("Native owner closed before Java teardown"));
                    }
                    // Owner status is sampled AFTER drain; a terminal status can race with
                    // event publication. Always perform another empty drain before teardown.
                    if (state == State.CLOSING && activeOperations == 0 && events.length == 0 && terminalBeforeDrain) {
                        state = State.DESTROYING;
                        destroy = true;
                    }
                    changed.signalAll();
                } catch (Throwable error) {
                    fail(error instanceof KafkaException k ? k : new KafkaException("Native binding protocol failure", error));
                    // A bad event stream requires exclusive destruction to establish the
                    // release boundary; accepted records remain unknown until it returns.
                    if (state == State.CLOSING && activeOperations == 0) { state = State.DESTROYING; destroy = true; }
                } finally { callGate.unlock(); }
                if (destroy) { destroyAndFinish(); return; }
                if (events != null) for (NativeAccess.Event event : events) {
                    dispatch(event);
                    // This platform thread belongs to the producer. A callback's
                    // interrupt must not poison the next callback or cause an idle spin.
                    Thread.interrupted();
                }
                if (events == null || events.length == 0) {
                    Thread.interrupted();
                    LockSupport.parkNanos(this, park);
                    park = Math.min(RECHECK_NANOS, park * 2);
                } else park = 1_000;
            }
        } catch (Throwable error) {
            callGate.lock();
            try { fail(new KafkaException("Producer poller failed", error)); }
            finally { callGate.unlock(); }
            // Keep a teardown owner alive even after a Java event-buffer failure.
            while (true) {
                callGate.lock();
                try {
                    if (state == State.CLOSING && activeOperations == 0) { state = State.DESTROYING; break; }
                    try { changed.awaitNanos(RECHECK_NANOS); } catch (InterruptedException ignored) { }
                } finally { callGate.unlock(); }
            }
            destroyAndFinish();
        }
    }

    /** callGate held. Retired identifiers stay published until queued events drain. */
    private void retireTopics(boolean drainedEmpty) {
        var iterator = topics.values().iterator();
        while (iterator.hasNext()) {
            Topic topic = iterator.next();
            if (topic.retirementObserved && drainedEmpty) {
                iterator.remove();
                topicHandles.remove(topic.handle, topic);
                continue;
            }
            if (topic.failure == null) continue;
            if (!topic.retirementRequested) {
                try { nativeAccess.topicClose(topic.handle); topic.retirementRequested = true; }
                catch (NativeAccess.CallFailure error) {
                    if (error.code == NativeAccess.CLOSED) topic.retirementRequested = true;
                    else if (error.code != NativeAccess.EXHAUSTED && error.code != NativeAccess.FAILED) throw error;
                }
            }
            if (nativeAccess.topicStatus(topic.handle).status() == 5) topic.retirementObserved = true;
        }
    }

    private void dispatch(NativeAccess.Event event) {
        try {
            validateEvent(event);
            if (event.kind() == 1) { delivery(event); return; }
            callGate.lock();
            try {
                switch (event.kind()) {
                    case 3 -> {
                        FlushWait wait = flushes.remove(event.token());
                        if (wait == null) throw new IllegalStateException("Unknown or duplicate flush fence");
                        wait.done = true;
                    }
                    case 4, 5 -> {
                        Topic topic = topicHandles.get(event.topic());
                        if (topic == null) throw new IllegalStateException("Event for unpublished topic handle");
                        if (event.kind() == 4) {
                            if (topic.id != null && !topic.id.equals(event.topicId()))
                                throw new NativeAccess.ProtocolFailure("Topic-ready identity differs from its published handle");
                            topic.id = event.topicId();
                            topic.ready = true;
                        }
                        else topic.failure = new NativeDeliveryException(1, event.reason(), event.attempts());
                    }
                    case 6 -> normalClosed = true;
                    case 7 -> fail(new NativeDeliveryException(1, event.reason(), event.attempts()));
                    default -> throw new IllegalStateException("Unexpected native event kind " + event.kind());
                }
                changed.signalAll();
            } finally { callGate.unlock(); }
        } catch (Throwable error) {
            callGate.lock();
            try { fail(error instanceof KafkaException k ? k : new KafkaException("Native event protocol failure", error)); }
            finally { callGate.unlock(); }
        }
    }

    private void delivery(NativeAccess.Event event) {
        PendingTable.Entry entry;
        RecordMetadata metadata;
        Exception error;
        callGate.lock();
        try {
            entry = pending.accepted(event.userToken());
            if (entry.topicHandle != event.topic()) throw new IllegalStateException("Delivery topic differs from admission");
            if (!entry.topicId.equals(event.topicId())) throw new NativeAccess.ProtocolFailure("Delivery UUID differs from admission");
            error = outcome(event);
            metadata = error == null ? new RecordMetadata(new TopicPartition(entry.topic, event.partition()),
                    event.hasOffset() ? event.offset() : -1, 0,
                    event.hasTimestamp() ? event.timestamp() : entry.timestamp, entry.keySize, entry.valueSize) : null;
            pending.terminal(event.userToken());
        } finally { callGate.unlock(); }
        try {
            acknowledge(metadata == null ? failedMetadata(entry.topic, event.partition()) : metadata, error,
                    entry.acknowledgementHeaders == null ? null : entry.acknowledgementHeaders.headers());
            invokeCallback(entry.callback, metadata, error);
        } catch (Throwable callbackError) { pluginFailure(callbackError); }
        finally {
            entry.future.complete(metadata, error);
            callGate.lock();
            try { releaseEntry(entry); changed.signalAll(); }
            finally { callGate.unlock(); }
        }
    }

    static Exception outcome(NativeAccess.Event event) {
        if (event.reason() < 0 || event.reason() > 16 || event.outcome() < 0 || event.outcome() > 2)
            throw new IllegalStateException("Unknown native delivery outcome/reason");
        if (event.outcome() == 0) {
            if (event.reason() != 0 || event.partition() < 0 || event.hasOffset() && event.offset() < 0)
                throw new IllegalStateException("Malformed native acknowledgement");
            return null;
        }
        NativeDeliveryException diagnostic = new NativeDeliveryException(event.outcome(), event.reason(), event.attempts());
        if (event.outcome() == 2) {
            DeliveryUnknownException result = new DeliveryUnknownException(diagnostic.getMessage(), event.reason(), event.attempts());
            result.addSuppressed(diagnostic);
            return result;
        }
        KafkaException result = switch (event.reason()) {
            case 1 -> new TimeoutException(diagnostic.getMessage());
            case 6 -> new RecordTooLargeException(diagnostic.getMessage());
            case 7 -> new InvalidRecordException(diagnostic.getMessage());
            case 9 -> new ProducerFencedException(diagnostic.getMessage());
            case 11 -> new NetworkException(diagnostic.getMessage());
            case 15 -> new BufferExhaustedException(diagnostic.getMessage());
            case 16 -> new AuthenticationException(diagnostic.getMessage());
            default -> diagnostic;
        };
        if (result != diagnostic) result.addSuppressed(diagnostic);
        return result;
    }

    static void validateEvent(NativeAccess.Event event) {
        if (event.kind() < 1 || event.kind() > 7 || event.kind() == 2)
            throw new NativeAccess.ProtocolFailure("Unexpected native event kind " + event.kind());
        if (event.reason() < 0 || event.reason() > 16 || event.outcome() < 0 || event.outcome() > 2)
            throw new NativeAccess.ProtocolFailure("Unknown native event outcome/reason");
        if (event.attempts() < 0 || event.attempts() > 255)
            throw new NativeAccess.ProtocolFailure("Native event attempts exceed the configured ABI range");
        if (event.kind() == 1) {
            if (event.token() == 0 || event.userToken() == 0 || event.topicId().equals(Uuid.ZERO_UUID))
                throw new NativeAccess.ProtocolFailure("Delivery omitted its record or topic identity");
        } else {
            if (event.outcome() != 0 || event.hasOffset() || event.hasTimestamp())
                throw new NativeAccess.ProtocolFailure("Non-delivery event contains a delivery outcome");
            if (event.kind() == 3 && event.token() == 0)
                throw new NativeAccess.ProtocolFailure("Flush event omitted its fence token");
            if (event.kind() == 4 && (event.count() <= 0 || event.reason() != 0 || event.topicId().equals(Uuid.ZERO_UUID)))
                throw new NativeAccess.ProtocolFailure("Malformed topic-ready event");
            if ((event.kind() == 5 || event.kind() == 7) && event.reason() == 0)
                throw new NativeAccess.ProtocolFailure("Native failure event omitted its reason");
        }
    }

    private void destroyAndFinish() {
        Throwable destroyFailure = null;
        // DESTROYING excludes all other calls. Do not hold callGate across join.
        try { nativeAccess.destroy(); }
        catch (Throwable error) { destroyFailure = error; }
        ArrayList<PendingTable.Entry> missing = new ArrayList<>();
        callGate.lock();
        try {
            for (PendingTable.Entry entry : pending.entries()) {
                if (entry.state == PendingTable.State.ACCEPTED || entry.state == PendingTable.State.TERMINAL_DISPATCH) {
                    entry.state = PendingTable.State.TERMINAL_DISPATCH;
                    missing.add(entry);
                }
            }
            KafkaException unmatched = new KafkaException("Native termination left an unmatched flush fence", destroyFailure);
            for (FlushWait wait : flushes.values()) { wait.failure = unmatched; wait.done = true; }
            flushes.clear();
        } finally { callGate.unlock(); }
        for (PendingTable.Entry entry : missing) {
            DeliveryUnknownException unknown = new DeliveryUnknownException("Native termination omitted an accepted record outcome", 12, 0);
            if (destroyFailure != null) unknown.addSuppressed(destroyFailure);
            try {
                acknowledge(failedMetadata(entry.topic, -1), unknown,
                        entry.acknowledgementHeaders == null ? null : entry.acknowledgementHeaders.headers());
                invokeCallback(entry.callback, null, unknown);
            } catch (Throwable error) { pluginFailure(error); }
            finally {
                entry.future.complete(null, unknown);
                callGate.lock();
                try { releaseEntry(entry); } finally { callGate.unlock(); }
            }
        }
        try { scratch.close(); }
        finally {
            closePlugins();
            callGate.lock();
            try { state = State.DESTROYED; changed.signalAll(); }
            finally { callGate.unlock(); teardown.countDown(); }
        }
    }

    private void closePlugins() {
        for (int i = ownedPlugins.size() - 1; i >= 0; i--) {
            try { ownedPlugins.get(i).close(); }
            catch (Throwable error) { pluginFailure(error); }
        }
        ownedPlugins.clear();
    }

    @Override public Map<MetricName, ? extends Metric> metrics() { return Map.of(); }
    /** Configured native/Java pool limits; excludes user allocations and is not total resident memory. */
    public Map<String, Long> resourceBudget() { return settings.resourceBudget(); }
    @Override public void registerMetricForSubscription(KafkaMetric metric) { }
    @Override public void unregisterMetricFromSubscription(KafkaMetric metric) { }
    @Override public void initTransactions() { throw unsupported(); }
    @Override public void beginTransaction() { throw unsupported(); }
    @Override public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata group) { throw unsupported(); }
    @Override public void commitTransaction() { throw unsupported(); }
    @Override public void abortTransaction() { throw unsupported(); }
    @Override public Uuid clientInstanceId(Duration timeout) { throw unsupported(); }
    private static UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException("Transactions and broker telemetry are not supported by kr-kafka");
    }
}
