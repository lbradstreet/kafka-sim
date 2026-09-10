package io.krkafka.producer;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Pins local Kafka 4.3 behavior without requiring a broker or imitating its implementation. */
@Timeout(20)
class KafkaSurfaceParityTest {
    static final class TrackingSerializer extends ByteArraySerializer {
        final AtomicInteger configurations = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        @Override public void configure(Map<String, ?> config, boolean key) { configurations.incrementAndGet(); }
        @Override public void close() { closes.incrementAndGet(); }
    }

    @Test void suppliedSerializerOwnershipAndZeroMetadataBudgetMatchKafka43() throws Exception {
        for (boolean binding : new boolean[]{false, true}) {
            var key = new TrackingSerializer();
            var value = new TrackingSerializer();
            NativeOwnerTest.Owner owner = binding ? new NativeOwnerTest.Owner(0, key, value) : null;
            Producer<byte[], byte[]> producer = binding ? owner.producer : new KafkaProducer<>(
                    Map.of("bootstrap.servers", "127.0.0.1:1", "max.block.ms", 0), key, value);
            try {
                assertEquals(0, key.configurations.get());
                assertEquals(0, value.configurations.get());
                AtomicReference<Thread> callbackThread = new AtomicReference<>();
                AtomicReference<Exception> callbackError = new AtomicReference<>();
                var delivery = producer.send(new ProducerRecord<>("not-resolved", new byte[0]), (metadata, error) -> {
                    callbackThread.set(Thread.currentThread());
                    callbackError.set(error);
                });
                assertSame(Thread.currentThread(), callbackThread.get());
                assertInstanceOf(TimeoutException.class, callbackError.get());
                assertTrue(delivery.isDone());
                assertFalse(delivery.cancel(true));
                assertInstanceOf(TimeoutException.class, assertThrows(ExecutionException.class, delivery::get).getCause());
            } finally {
                if (owner != null) owner.close(); else producer.close(Duration.ZERO);
            }
            assertEquals(1, key.closes.get());
            assertEquals(1, value.closes.get());
            assertThrows(IllegalStateException.class, () -> producer.send(new ProducerRecord<>("closed", new byte[0])));
        }
    }
}
