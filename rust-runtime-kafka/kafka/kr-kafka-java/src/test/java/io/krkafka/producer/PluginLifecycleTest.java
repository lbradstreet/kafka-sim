package io.krkafka.producer;

import static org.junit.jupiter.api.Assertions.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(20)
class PluginLifecycleTest {
    private static final List<String> calls = java.util.Collections.synchronizedList(new ArrayList<>());
    private static final AtomicInteger partitionCalls = new AtomicInteger();
    private static final AtomicReference<Cluster> partitionCluster = new AtomicReference<>();

    @BeforeEach void reset() { calls.clear(); partitionCalls.set(0); partitionCluster.set(null); }

    public static final class HeaderSerializer implements Serializer<byte[]> {
        boolean key;
        @Override public void configure(Map<String, ?> config, boolean isKey) { key = isKey; calls.add("configure:" + key); }
        @Override public byte[] serialize(String topic, byte[] data) { throw new AssertionError("header-aware overload required"); }
        @Override public byte[] serialize(String topic, Headers headers, byte[] data) {
            if (headers.lastHeader("intercepted") == null) throw new AssertionError("interceptor must precede serialization");
            calls.add("serialize:" + key + ":" + (data == null));
            return data == null ? new byte[0] : data;
        }
        @Override public void close() { calls.add("close-serializer:" + key); }
    }

    public static class TrackingInterceptor implements ProducerInterceptor<byte[], byte[]> {
        @Override public void configure(Map<String, ?> config) { calls.add("configure-interceptor"); }
        @Override public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) {
            calls.add("intercept");
            record.headers().add("intercepted", new byte[0]);
            return record;
        }
        @Override public void onAcknowledgement(RecordMetadata metadata, Exception error) { calls.add("acknowledge"); }
        @Override public void close() { calls.add("close-interceptor"); }
    }

    public static final class FailingInterceptor extends TrackingInterceptor {
        @Override public void configure(Map<String, ?> config) { super.configure(config); throw new IllegalArgumentException("plugin configuration failure"); }
    }

    public static final class SnapshotPartitioner implements Partitioner {
        @Override public void configure(Map<String, ?> config) { calls.add("configure-partitioner"); }
        @Override public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            partitionCalls.incrementAndGet();
            partitionCluster.set(cluster);
            return 1;
        }
        @Override public void close() { calls.add("close-partitioner"); }
    }

    @Test void configuredPluginsHaveKafkaRolesHeaderAwareNullConversionAndFailureOrdering() throws Exception {
        try (var owner = new NativeOwnerTest.Owner(0, null, null, Map.of(
                "key.serializer", HeaderSerializer.class, "value.serializer", HeaderSerializer.class,
                "interceptor.classes", List.of(TrackingInterceptor.class)))) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("topic", null, null);
            var future = owner.producer.send(record, (metadata, error) -> calls.add("callback"));
            assertTrue(future.isDone());
            assertThrows(ExecutionException.class, future::get);
            assertEquals(List.of("configure:true", "configure:false", "configure-interceptor",
                    "intercept", "serialize:true:true", "serialize:false:true", "acknowledge", "callback"), calls);
            assertThrows(IllegalStateException.class, () -> record.headers().add("late", new byte[0]));
        }
        assertEquals(List.of("close-interceptor", "close-serializer:false", "close-serializer:true"), calls.subList(calls.size() - 3, calls.size()));
    }

    @Test void partialPluginConstructionClosesEveryOwnedInstance() {
        assertThrows(IllegalArgumentException.class, () -> new NativeOwnerTest.Owner(0, null, null, Map.of(
                "key.serializer", HeaderSerializer.class, "value.serializer", HeaderSerializer.class,
                "interceptor.classes", List.of(FailingInterceptor.class))));
        assertEquals(List.of("configure:true", "configure:false", "configure-interceptor",
                "close-interceptor", "close-serializer:false", "close-serializer:true"), calls);
    }

    @Test void customPartitionerReceivesCurrentClusterAndExplicitPartitionTakesPrecedence() throws Exception {
        try (var owner = new NativeOwnerTest.Owner(10_000, new ByteArraySerializer(), new ByteArraySerializer(),
                Map.of("partitioner.class", SnapshotPartitioner.class))) {
            owner.initialize("topic", 2, 8);
            owner.producer.send(new ProducerRecord<>("topic", new byte[]{1}));
            assertEquals(1, partitionCalls.get());
            Cluster cluster = partitionCluster.get();
            assertEquals(3, cluster.nodes().size());
            assertEquals(2, cluster.partitionsForTopic("topic").size());
            assertEquals("broker-1", cluster.partitionsForTopic("topic").get(1).leader().host());
            assertEquals(new org.apache.kafka.common.Uuid(0x0808080808080808L, 0x0808080808080808L), cluster.topicId("topic"));
            owner.producer.send(new ProducerRecord<>("topic", 0, null, new byte[]{2}));
            assertEquals(1, partitionCalls.get());
        }
        assertEquals(List.of("configure-partitioner", "close-partitioner"), calls);
    }
}
