package io.krkafka.producer;

import static io.krkafka.producer.NativeOwnerTest.*;
import static org.junit.jupiter.api.Assertions.*;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(20)
class HeaderAcknowledgementTest {
    record Acknowledgement(RecordMetadata metadata, Exception error, Headers headers) { }
    private static final List<Acknowledgement> acknowledgements = new CopyOnWriteArrayList<>();
    private static final AtomicInteger legacy = new AtomicInteger();

    public static final class HeaderInterceptor implements ProducerInterceptor<byte[], byte[]> {
        @Override public void configure(Map<String, ?> properties) { }
        @Override public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) { return record; }
        @Override public void onAcknowledgement(RecordMetadata metadata, Exception error) { legacy.incrementAndGet(); }
        @Override public void onAcknowledgement(RecordMetadata metadata, Exception error, Headers headers) {
            acknowledgements.add(new Acknowledgement(metadata, error, headers));
        }
        @Override public void close() { }
    }

    @BeforeEach void reset() { acknowledgements.clear(); legacy.set(0); }

    private static ProducerRecord<byte[], byte[]> record(byte[] value) {
        return new ProducerRecord<>("topic", 0, 1L, null, new byte[]{9}, List.of(
                new RecordHeader("dup", null), new RecordHeader("dup", new byte[0]),
                new RecordHeader("dup", value), new RecordHeader("", new byte[0])));
    }

    private static void assertHeaders(Headers headers, byte first) {
        var rows = headers.toArray();
        assertEquals(4, rows.length);
        assertEquals(List.of("dup", "dup", "dup", ""), java.util.Arrays.stream(rows).map(h -> h.key()).toList());
        assertNull(rows[0].value());
        assertArrayEquals(new byte[0], rows[1].value());
        assertArrayEquals(new byte[]{first, 2}, rows[2].value());
        assertArrayEquals(new byte[0], rows[3].value());
        assertThrows(IllegalStateException.class, () -> headers.add("late", new byte[0]));
    }

    private static long retained(Owner owner) {
        owner.gate.lock();
        try { return ((HeaderContextPool) field(owner.producer, "headerContexts")).used(); }
        finally { owner.gate.unlock(); }
    }

    private static void cancel(Owner owner, MemorySegment cursor) {
        await(() -> {
            owner.gate.lock();
            try {
                int code = invoke("kr_test_cancel_since", FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS, ValueLayout.ADDRESS), owner.access.handle, cursor);
                assertTrue(code == 0 || code == NativeAccess.EXHAUSTED);
                return code == 0;
            } finally { owner.gate.unlock(); }
        });
    }

    @Test void acceptedHeadersAreIsolatedAndRejectedHeadersUseTheKafka43OverloadWithoutLeakingCredit() throws Exception {
        try (Owner owner = new Owner(0, new ByteArraySerializer(), new ByteArraySerializer(), Map.of(
                "interceptor.classes", List.of(HeaderInterceptor.class), "kr.interceptor.header.bytes", 20,
                "kr.record.descriptors", 2, "kr.scratch.bytes", 512)); Arena arena = Arena.ofConfined()) {
            // Establish readiness without consuming an interceptor/send context.
            assertThrows(org.apache.kafka.common.errors.TimeoutException.class, () -> owner.producer.partitionsFor("topic"));
            owner.metadata("topic", 1, 1);
            owner.hook("kr_test_resume");
            owner.ready("topic", 1);
            assertEquals(20L, owner.producer.resourceBudget().get("java.interceptor.header.bytes"));
            var cursor = arena.allocate(ValueLayout.JAVA_LONG);
            for (int iteration = 0; iteration < 32; iteration++) {
                byte[] acceptedValue = {1, 2};
                var accepted = owner.producer.send(record(acceptedValue));
                assertFalse(accepted.isDone());
                assertEquals(20, retained(owner));
                acceptedValue[0] = 99;
                int offset = acknowledgements.size();
                var rejected = owner.producer.send(record(new byte[]{7, 2}));
                assertInstanceOf(BufferExhaustedException.class,
                        assertThrows(ExecutionException.class, rejected::get).getCause());
                assertEquals(offset + 1, acknowledgements.size());
                var rejection = acknowledgements.get(offset);
                assertEquals("topic", rejection.metadata().topic());
                assertEquals(0, rejection.metadata().partition());
                assertFalse(rejection.metadata().hasOffset());
                assertHeaders(rejection.headers(), (byte) 7);
                assertEquals(20, retained(owner));
                cancel(owner, cursor);
                assertThrows(ExecutionException.class, () -> accepted.get(10, TimeUnit.SECONDS));
                assertEquals(offset + 2, acknowledgements.size());
                assertHeaders(acknowledgements.get(offset + 1).headers(), (byte) 1);
                await(() -> retained(owner) == 0);
                owner.gate.lock();
                try {
                    PendingTable table = (PendingTable) field(owner.producer, "pending");
                    assertEquals(0, table.used());
                    for (var entry : table.entries()) assertNull(entry.acknowledgementHeaders);
                    assertEquals(0, ((ScratchPool) field(owner.producer, "scratch")).active());
                } finally { owner.gate.unlock(); }
            }
            assertEquals(64, acknowledgements.size());
            assertEquals(0, legacy.get());
        }
    }

    @Test void oversizeAndSerializationRejectionsAcknowledgeHeadersSynchronouslyAndAbortReleasesAcceptedContext() throws Exception {
        try (Owner owner = new Owner(10_000, new ByteArraySerializer(), new ByteArraySerializer(), Map.of(
                "interceptor.classes", List.of(HeaderInterceptor.class), "kr.interceptor.header.bytes", 20))) {
            owner.initialize("topic", 1, 1);
            var oversized = record(new byte[]{1, 2, 3});
            var rejected = owner.producer.send(oversized);
            assertInstanceOf(RecordTooLargeException.class, assertThrows(ExecutionException.class, rejected::get).getCause());
            assertEquals(3, acknowledgements.getFirst().headers().lastHeader("dup").value().length);
            assertEquals(0, retained(owner));
            var accepted = owner.producer.send(record(new byte[]{1, 2}));
            assertEquals(20, retained(owner));
            owner.hook("kr_test_abort");
            assertThrows(ExecutionException.class, () -> accepted.get(10, TimeUnit.SECONDS));
            owner.producer.close();
            assertEquals(0, retained(owner));
            assertHeaders(acknowledgements.getLast().headers(), (byte) 1);
        }
        acknowledgements.clear();
        var serializer = new ByteArraySerializer() {
            @Override public byte[] serialize(String topic, Headers headers, byte[] value) {
                throw new org.apache.kafka.common.errors.SerializationException("fixture");
            }
        };
        try (Owner owner = new Owner(0, serializer, new ByteArraySerializer(), Map.of(
                "interceptor.classes", List.of(HeaderInterceptor.class)))) {
            assertThrows(org.apache.kafka.common.errors.SerializationException.class,
                    () -> owner.producer.send(record(new byte[]{1, 2})));
            assertEquals(1, acknowledgements.size());
            assertHeaders(acknowledgements.getFirst().headers(), (byte) 1);
            assertEquals(0, retained(owner));
        }
    }

    @Test void producersWithoutInterceptorsDoNotRetainHeadersOrConsumeTheirBudget() throws Exception {
        try (Owner owner = new Owner(10_000, new ByteArraySerializer(), new ByteArraySerializer(),
                Map.of("kr.interceptor.header.bytes", 0))) {
            owner.initialize("topic", 1, 1);
            var accepted = owner.producer.send(record(new byte[]{1, 2}));
            assertFalse(accepted.isDone());
            assertEquals(0, retained(owner));
            owner.gate.lock();
            try {
                for (var entry : ((PendingTable) field(owner.producer, "pending")).entries())
                    assertNull(entry.acknowledgementHeaders);
            } finally { owner.gate.unlock(); }
        }
    }
}
