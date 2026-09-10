package io.krkafka.producer;

import static io.krkafka.producer.NativeOwnerTest.*;
import static org.junit.jupiter.api.Assertions.*;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class PayloadReclamationTest {
    @Test void serializedKeyAndValueCanBeCollectedWhileTheirAcceptedFutureIsPending() throws Exception {
        for (boolean withInterceptors : new boolean[]{false, true}) {
            var outputs = new CopyOnWriteArrayList<WeakReference<byte[]>>();
            Serializer<byte[]> serializer = new Serializer<>() {
                @Override public byte[] serialize(String topic, byte[] value) { throw new AssertionError("header overload"); }
                @Override public byte[] serialize(String topic, Headers headers, byte[] value) {
                    byte[] output = new byte[1024];
                    outputs.add(new WeakReference<>(output));
                    return output;
                }
            };
            Map<String, Object> plugins = withInterceptors
                    ? Map.of("interceptor.classes", List.of(HeaderAcknowledgementTest.HeaderInterceptor.class)) : Map.of();
            try (Owner owner = new Owner(10_000, serializer, serializer, plugins)) {
                owner.initialize("topic", 1, 1);
                AtomicReference<Future<RecordMetadata>> result = new AtomicReference<>();
                // Retire the sending stack before requesting collection. The
                // producer, serializers and delivery future all remain strongly live.
                Thread.ofPlatform().start(() -> {
                    var record = new ProducerRecord<byte[], byte[]>("topic", null, null);
                    record.headers().add("context", new byte[]{1});
                    result.set(owner.producer.send(record));
                }).join();
                assertNotNull(result.get());
                assertFalse(result.get().isDone());
                assertEquals(2, outputs.size());
                await(() -> {
                    System.gc();
                    return outputs.stream().allMatch(reference -> reference.refersTo(null));
                });
                assertFalse(result.get().isDone(), "collection must precede the native terminal outcome");
                owner.gate.lock();
                try {
                    assertEquals(1, ((PendingTable) field(owner.producer, "pending")).used());
                    assertEquals(0, ((ScratchPool) field(owner.producer, "scratch")).active());
                } finally { owner.gate.unlock(); }
            }
        }
    }
}
