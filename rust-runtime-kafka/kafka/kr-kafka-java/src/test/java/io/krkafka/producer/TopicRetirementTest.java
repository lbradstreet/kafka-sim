package io.krkafka.producer;

import static org.junit.jupiter.api.Assertions.*;
import static io.krkafka.producer.NativeOwnerTest.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class TopicRetirementTest {
    @Test void failedHandleRetiresBeforeSameNameReopensAndOldDeliveryKeepsItsUuid() throws Exception {
        try (Owner owner = new Owner(10_000, new ByteArraySerializer(), new ByteArraySerializer(), Map.of("kr.max.open.topics", 1))) {
            owner.initialize("topic", 2, 1);
            int oldHandle = owner.openedTopic("topic");
            AtomicInteger callbacks = new AtomicInteger();
            Future<RecordMetadata> old = owner.producer.send(new ProducerRecord<>("topic", 0, null, new byte[]{1}),
                    (metadata, error) -> callbacks.incrementAndGet());
            owner.gate.lock();
            try {
                PendingTable pending = (PendingTable) field(owner.producer, "pending");
                PendingTable.Entry entry = java.util.Arrays.stream(pending.entries())
                        .filter(slot -> slot.state == PendingTable.State.ACCEPTED).findFirst().orElseThrow();
                assertEquals(new Uuid(0x0101010101010101L, 0x0101010101010101L), entry.topicId);
            } finally { owner.gate.unlock(); }
            // A successful response for a changed UUID must delete the old identity,
            // rather than silently updating its accepted records to the replacement.
            owner.metadata("topic", 2, 2);
            Throwable cause = assertThrows(ExecutionException.class, () -> old.get(10, TimeUnit.SECONDS)).getCause();
            NativeDeliveryException deleted = assertInstanceOf(NativeDeliveryException.class, cause);
            assertEquals(3, deleted.reason());
            assertEquals(1, callbacks.get());
            FutureTask<List<PartitionInfo>> reopen = new FutureTask<>(() -> {
                while (true) {
                    try { return owner.producer.partitionsFor("topic"); }
                    catch (NativeDeliveryException retirementInProgress) { Thread.yield(); }
                }
            });
            Thread.ofVirtual().start(reopen);
            await(() -> {
                int next = owner.openedTopic("topic");
                return next >= 0 && next != oldHandle;
            });
            int newHandle = owner.openedTopic("topic");
            owner.metadata("topic", 3, 2);
            assertEquals(3, reopen.get(10, TimeUnit.SECONDS).size());
            owner.gate.lock();
            try {
                assertEquals(5, owner.access.topicStatus(oldHandle).status());
                assertEquals(new Uuid(0x0202020202020202L, 0x0202020202020202L), owner.access.metadata(newHandle, "topic").cluster().topicId("topic"));
                assertEquals(1, ((Map<?, ?>) field(owner.producer, "topics")).size());
                assertEquals(1, ((Map<?, ?>) field(owner.producer, "topicHandles")).size());
                assertEquals(KrKafkaProducer.State.OPEN, field(owner.producer, "state"));
            } finally { owner.gate.unlock(); }
            Future<RecordMetadata> replacement = owner.producer.send(new ProducerRecord<>("topic", 2, null, new byte[]{2}),
                    (metadata, error) -> callbacks.incrementAndGet());
            owner.producer.close(Duration.ZERO);
            assertThrows(ExecutionException.class, replacement::get);
            assertEquals(2, callbacks.get());
            assertEquals(0, ((PendingTable) field(owner.producer, "pending")).used());
        }
    }
}
