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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.BootstrapConfiguration;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.HostResolver;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.SeededRecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.ExponentialBackoff;
import org.apache.kafka.common.utils.internals.LogContext;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

/**
 * Experimental: builds a classic {@link KafkaProducer} without its I/O thread so a deterministic
 * simulation can drive {@link Sender#runOnce()} itself. Mirrors the private wiring in the
 * production constructor; keep it in sync by hand.
 */
public final class ClassicProducerSimFactory {

    public record Handle<K, V>(KafkaProducer<K, V> producer,
                               Sender sender,
                               RecordAccumulator accumulator,
                               NetworkClient client,
                               ProducerMetadata metadata) { }

    private ClassicProducerSimFactory() { }

    public static <K, V> Handle<K, V> create(Map<String, Object> configs,
                                             Serializer<K> keySerializer,
                                             Serializer<V> valueSerializer,
                                             Selectable selector,
                                             HostResolver hostResolver,
                                             Time time,
                                             long seed) {
        ProducerConfig config = new ProducerConfig(
            ProducerConfig.appendSerializerToConfig(configs, keySerializer, valueSerializer));
        // Independent streams per consumer so one component's draws never shift another's.
        Random jitterRandom = new Random(seed ^ 0x4A4954544552L); // "JITTER"
        Random partitionRandom = new Random(seed ^ 0x5041525449L); // "PARTI"
        String clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
        LogContext logContext = new LogContext("[SimProducer clientId=" + clientId + "] ");
        Metrics metrics = new Metrics(new MetricConfig(), List.of(), time);
        long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
        long retryBackoffMaxMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);

        ProducerMetadata metadata = new ProducerMetadata(retryBackoffMs, retryBackoffMaxMs,
            config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG),
            config.getLong(ProducerConfig.METADATA_MAX_IDLE_CONFIG),
            logContext, new ClusterResourceListeners()) {
            private final Random topologyRandom = new Random(seed ^ 0x544F504F4CL);
            private Cluster previous;

            @Override
            public Cluster fetch() {
                Cluster cluster = super.fetch();
                if (cluster != previous) {
                    // Cluster shuffles nodes with a process-global RNG. The simulation owner
                    // selects a seeded order before publishing each snapshot to the client.
                    List<Node> nodes = new ArrayList<>(cluster.nodes());
                    nodes.sort(Comparator.comparingInt(Node::id));
                    Collections.shuffle(nodes, topologyRandom);
                    writeField(cluster, "nodes", Collections.unmodifiableList(nodes));
                    previous = cluster;
                }
                return cluster;
            }

            @Override
            public synchronized void awaitUpdate(int lastVersion, Timer timer) throws InterruptedException {
                // The production wait parks a thread. Let the injected clock drive the sender
                // and broker events while retaining the metadata version/error/close checks.
                timer.update();
                long nowMs = time.milliseconds();
                long deadlineMs = timer.remainingMs() > Long.MAX_VALUE - nowMs
                    ? Long.MAX_VALUE : nowMs + timer.remainingMs();
                time.waitObject(this, () -> {
                    maybeThrowFatalException();
                    return updateVersion() > lastVersion || isClosed();
                }, deadlineMs);
                if (isClosed())
                    throw new KafkaException("Requested metadata update after close");
            }
        };
        seedBackoff(metadata, "refreshBackoff", jitterRandom);
        // Sim hosts never resolve through DNS, so bootstrap with unresolved addresses.
        metadata.bootstrap(config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).stream()
            .map(url -> {
                int colon = url.lastIndexOf(':');
                return InetSocketAddress.createUnresolved(url.substring(0, colon),
                    Integer.parseInt(url.substring(colon + 1)));
            }).toList());

        ApiVersions apiVersions = new ApiVersions();
        TransactionManager transactionManager = null;
        if (config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)) {
            transactionManager = new TransactionManager(logContext,
                config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG),
                config.getInt(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG),
                retryBackoffMs, apiVersions, metadata,
                config.getBoolean(ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG));
        }

        int batchSize = Math.max(1, config.getInt(ProducerConfig.BATCH_SIZE_CONFIG));
        int lingerMs = (int) Math.min(config.getLong(ProducerConfig.LINGER_MS_CONFIG), Integer.MAX_VALUE);
        int requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        int deliveryTimeoutMs = Math.max(config.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG),
            lingerMs + requestTimeoutMs);
        CompressionType compressionType = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        Compression compression = switch (compressionType) {
            case ZSTD -> Compression.zstd().level(config.getInt(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG)).build();
            case GZIP -> Compression.gzip().level(config.getInt(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG)).build();
            case LZ4 -> Compression.lz4().level(config.getInt(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG)).build();
            default -> Compression.of(compressionType).build();
        };
        RecordAccumulator.PartitionerConfig partitionerConfig = new RecordAccumulator.PartitionerConfig(
            config.getBoolean(ProducerConfig.PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG),
            config.getLong(ProducerConfig.PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG),
            config.getBoolean(ProducerConfig.PARTITIONER_RACK_AWARE_CONFIG),
            config.getString(ProducerConfig.CLIENT_RACK_CONFIG));
        String metricGroup = KafkaProducer.PRODUCER_METRIC_GROUP_NAME;
        RecordAccumulator accumulator = new SeededRecordAccumulator(logContext, batchSize, compression,
            lingerMs, retryBackoffMs, retryBackoffMaxMs, deliveryTimeoutMs, partitionerConfig,
            metrics, metricGroup, time, transactionManager,
            new BufferPool(config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG), batchSize, metrics,
                time, metricGroup),
            partitionRandom);
        seedBackoff(accumulator, "retryBackoff", jitterRandom);
        // ProducerBatch has identity hash codes. Select one stable legal order for
        // otherwise unordered forced-close callbacks in the single-owner simulation.
        writeField(readField(accumulator, "incomplete"), "incomplete", new LinkedHashSet<>());

        int maxInFlight = config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        ProducerMetrics metricsRegistry = new ProducerMetrics(metrics);
        NetworkClient client = new NetworkClient(null, metadata, selector, clientId, maxInFlight,
            config.getLong(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG),
            config.getLong(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
            config.getInt(ProducerConfig.SEND_BUFFER_CONFIG),
            config.getInt(ProducerConfig.RECEIVE_BUFFER_CONFIG),
            requestTimeoutMs,
            config.getLong(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
            config.getLong(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
            time, true, apiVersions, Sender.throttleTimeSensor(metricsRegistry.senderMetrics),
            logContext, hostResolver, null, Long.MAX_VALUE,
            MetadataRecoveryStrategy.forName(config.getString(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG)),
            BootstrapConfiguration.DISABLED,
            false);
        seedLeastLoadedNodeOffset(client, seed);
        Object connectionStates = readField(client, "connectionStates");
        seedBackoff(connectionStates, "reconnectBackoff", jitterRandom);
        seedBackoff(connectionStates, "connectionSetupTimeout", jitterRandom);

        Sender sender = new Sender(logContext, client, metadata, accumulator, maxInFlight == 1,
            config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
            Short.parseShort(config.getString(ProducerConfig.ACKS_CONFIG)),
            config.getInt(ProducerConfig.RETRIES_CONFIG),
            metricsRegistry.senderMetrics, time, requestTimeoutMs, retryBackoffMs, transactionManager);

        KafkaProducer<K, V> producer = new KafkaProducer<>(config, logContext, metrics, keySerializer,
            valueSerializer, metadata, accumulator, transactionManager, sender,
            new ProducerInterceptors<>(List.of(), metrics), null, time, null, Optional.empty());
        return new Handle<>(producer, sender, accumulator, client, metadata);
    }

    /** NetworkClient picks its least-loaded-node scan offset from an unseeded Random. */
    private static void seedLeastLoadedNodeOffset(NetworkClient client, long seed) {
        writeField(client, "randOffset", new Random(seed));
    }

    /**
     * Every {@link ExponentialBackoff} in the classic client draws jitter from
     * {@code ThreadLocalRandom}. Swap the instance for one with the same parameters and a
     * seeded source; reflection because the owners build them privately.
     */
    private static void seedBackoff(Object owner, String fieldName, Random random) {
        ExponentialBackoff original = (ExponentialBackoff) readField(owner, fieldName);
        writeField(owner, fieldName, new SeededBackoff(original, random));
    }

    private static Object readField(Object owner, String fieldName) {
        try {
            Field field = declaredField(owner.getClass(), fieldName);
            field.setAccessible(true);
            return field.get(owner);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot read " + fieldName, e);
        }
    }

    private static void writeField(Object owner, String fieldName, Object value) {
        try {
            Field field = declaredField(owner.getClass(), fieldName);
            field.setAccessible(true);
            field.set(owner, value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot write " + fieldName, e);
        }
    }

    private static Field declaredField(Class<?> type, String fieldName) throws NoSuchFieldException {
        for (Class<?> c = type; c != null; c = c.getSuperclass()) {
            try {
                return c.getDeclaredField(fieldName);
            } catch (NoSuchFieldException ignored) {
                // keep walking up
            }
        }
        throw new NoSuchFieldException(type.getName() + "." + fieldName);
    }

    /** Mirrors {@link ExponentialBackoff#backoff(long)} with a seeded jitter draw. */
    static final class SeededBackoff extends ExponentialBackoff {
        private final long initialInterval;
        private final int multiplier;
        private final long maxInterval;
        private final double jitter;
        private final double expMax;
        private final Random random;

        SeededBackoff(ExponentialBackoff original, Random random) {
            this((long) readField(original, "initialInterval"), (int) readField(original, "multiplier"),
                (long) readField(original, "maxInterval"), (double) readField(original, "jitter"), random);
        }

        SeededBackoff(long initialInterval, int multiplier, long maxInterval, double jitter, Random random) {
            super(initialInterval, multiplier, maxInterval, jitter);
            this.initialInterval = Math.min(maxInterval, initialInterval);
            this.multiplier = multiplier;
            this.maxInterval = maxInterval;
            this.jitter = jitter;
            this.expMax = maxInterval > initialInterval
                ? Math.log(maxInterval / (double) Math.max(initialInterval, 1)) / Math.log(multiplier) : 0;
            this.random = random;
        }

        @Override
        public long backoff(long attempts) {
            if (expMax == 0)
                return initialInterval;
            double exp = Math.min(attempts, expMax);
            double term = initialInterval * Math.pow(multiplier, exp);
            double randomFactor = jitter < Double.MIN_NORMAL ? 1.0
                : random.nextDouble(1 - jitter, 1 + jitter);
            return Math.min((long) (randomFactor * term), maxInterval);
        }
    }
}
