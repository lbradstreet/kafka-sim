package io.krkafka.producer;

import static org.junit.jupiter.api.Assertions.*;
import io.krkafka.loader.NativeLibrary;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Uses the real native actor and its paused connector, never a mock producer. */
@Timeout(20)
class NativeOwnerTest {
    static final class Owner implements AutoCloseable {
        final NativeAccess access;
        final KrKafkaProducer<byte[], byte[]> producer;
        final ReentrantLock gate;

        Owner(long maxBlockMs) { this(maxBlockMs, new ByteArraySerializer(), new ByteArraySerializer()); }
        Owner(long maxBlockMs, Serializer<byte[]> key, Serializer<byte[]> value) {
            this(maxBlockMs, key, value, Map.of());
        }
        Owner(long maxBlockMs, Serializer<byte[]> key, Serializer<byte[]> value, Map<String, Object> overrides) {
            var config = new java.util.HashMap<String, Object>(Map.of("max.block.ms", maxBlockMs,
                    "bootstrap.servers", "localhost:9092", "kr.record.descriptors", 8, "kr.max.open.topics", 4,
                    "kr.serialization.concurrency", 4, "kr.scratch.checkouts", 2));
            config.putAll(overrides);
            var settings = new ProducerSettings(config);
            NativeLibrary.load();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment out = arena.allocate(ValueLayout.ADDRESS);
                assertEquals(0, invoke("kr_test_producer_create", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS), out));
                access = new NativeAccess(out.get(ValueLayout.ADDRESS, 0));
            }
            producer = new KrKafkaProducer<>(settings, key, value, access);
            gate = (ReentrantLock) field(producer, "callGate");
        }

        void hook(String name) {
            gate.lock();
            try {
                String state = field(producer, "state").toString();
                if (state.equals("DESTROYING") || state.equals("DESTROYED")) return;
                assertEquals(0, invoke(name, FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS), access.handle));
            } finally { gate.unlock(); }
        }

        int openedTopic(String name) {
            gate.lock();
            try {
                Object topic = ((Map<?, ?>) field(producer, "topics")).get(name);
                return topic == null ? -1 : (int) field(topic, "handle");
            } finally { gate.unlock(); }
        }

        void metadata(String name, int count, int identity) {
            int topic = openedTopic(name);
            assertTrue(topic >= 0);
            gate.lock();
            try {
                assertEquals(0, invoke("kr_test_metadata", FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT),
                        access.handle, topic, count, identity));
            } finally { gate.unlock(); }
        }

        List<PartitionInfo> ready(String name, int count) {
            AtomicReference<List<PartitionInfo>> result = new AtomicReference<>();
            await(() -> {
                try {
                    List<PartitionInfo> rows = producer.partitionsFor(name);
                    if (rows.size() != count) return false;
                    result.set(rows);
                    return true;
                } catch (TimeoutException notYetReady) { return false; }
            });
            return result.get();
        }

        void initialize(String name, int count, int identity) throws Exception {
            FutureTask<List<PartitionInfo>> waiting = new FutureTask<>(() -> producer.partitionsFor(name));
            Thread.ofPlatform().start(waiting);
            await(() -> openedTopic(name) >= 0);
            metadata(name, count, identity);
            hook("kr_test_resume");
            assertEquals(count, waiting.get(10, TimeUnit.SECONDS).size());
        }

        @Override public void close() {
            hook("kr_test_resume");
            producer.close(Duration.ZERO);
        }
    }

    static int invoke(String symbol, FunctionDescriptor descriptor, Object... args) {
        MethodHandle handle = Linker.nativeLinker().downcallHandle(SymbolLookup.loaderLookup().findOrThrow(symbol), descriptor);
        try { return (int) handle.invokeWithArguments(args); }
        catch (Throwable error) { throw new AssertionError("Test native call failed: " + symbol, error); }
    }

    static Object field(Object object, String name) {
        try {
            Field field = object.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return field.get(object);
        } catch (ReflectiveOperationException error) { throw new AssertionError(error); }
    }

    static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) fail("Observable native condition did not become true");
            Thread.yield();
        }
    }

    @Test void zeroTimeoutIsFailedFutureAndCallbackCloseDoesNotDeadlock() throws Exception {
        try (Owner owner = new Owner(0)) {
            AtomicInteger callbacks = new AtomicInteger();
            AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
            Future<RecordMetadata> result = owner.producer.send(new ProducerRecord<>("topic", new byte[]{1}), (metadata, error) -> {
                try {
                    callbacks.incrementAndGet();
                    assertNull(metadata);
                    assertInstanceOf(TimeoutException.class, error);
                    assertFalse(owner.gate.isHeldByCurrentThread());
                    assertThrows(IllegalStateException.class, owner.producer::flush);
                    owner.producer.close(Duration.ZERO);
                } catch (Throwable assertion) { callbackFailure.set(assertion); }
            });
            assertEquals(1, callbacks.get());
            assertNull(callbackFailure.get());
            assertInstanceOf(TimeoutException.class, assertThrows(ExecutionException.class, result::get).getCause());
            assertFalse(result.cancel(true));
            assertThrows(IllegalStateException.class, () -> owner.producer.send(new ProducerRecord<>("topic", new byte[0])));
        }
    }

    @Test void realMetadataHasLeadersReplicasRackIdentityAndExpansion() throws Exception {
        try (Owner owner = new Owner(0)) {
            var initial = owner.producer.send(new ProducerRecord<>("topic", new byte[0]));
            assertInstanceOf(TimeoutException.class, assertThrows(ExecutionException.class, initial::get).getCause());
            owner.metadata("topic", 2, 7);
            owner.hook("kr_test_resume");
            List<PartitionInfo> rows = owner.ready("topic", 2);
            assertEquals("broker-0", rows.get(0).leader().host());
            assertEquals("rack-1", rows.get(1).leader().rack());
            assertEquals(3, rows.get(0).replicas().length);
            assertEquals(2, rows.get(0).inSyncReplicas().length);
            assertEquals(1, rows.get(0).offlineReplicas().length);
            assertThrows(UnsupportedOperationException.class, () -> rows.clear());
            owner.metadata("topic", 4, 7);
            assertEquals(4, owner.ready("topic", 4).size());
            assertEquals(2, rows.size(), "previous snapshot remains stable");
        }
    }

    @Test void acceptedRecordsCompleteOnceBeforeFlushAndTeardownDespiteCallbackThrows() throws Exception {
        try (Owner owner = new Owner(10_000)) {
            owner.initialize("topic", 1, 9);
            ArrayList<Future<RecordMetadata>> futures = new ArrayList<>();
            List<Integer> callbacks = java.util.Collections.synchronizedList(new ArrayList<>());
            for (int i = 0; i < 8; i++) {
                int index = i;
                futures.add(owner.producer.send(new ProducerRecord<>("topic", 0, null, new byte[]{(byte) i}), (metadata, error) -> {
                    callbacks.add(index);
                    if (index == 3) throw new IllegalStateException("intentional callback failure");
                }));
            }
            // The paused/unavailable connector has no broker success path. Native close
            // establishes each actual delivery outcome before publishing CLOSED.
            owner.producer.close(Duration.ZERO);
            assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7), callbacks);
            for (Future<RecordMetadata> future : futures) {
                assertTrue(future.isDone());
                assertThrows(ExecutionException.class, future::get);
            }
            assertEquals(0, ((PendingTable) field(owner.producer, "pending")).used());
            assertEquals("DESTROYED", field(owner.producer, "state").toString());
        }
    }

    @Test void abortedOwnerHasNoClosedEventAndStillCompletesAcceptedRecords() throws Exception {
        try (Owner owner = new Owner(10_000)) {
            owner.initialize("topic", 1, 1);
            Future<RecordMetadata> result = owner.producer.send(new ProducerRecord<>("topic", new byte[]{3}));
            owner.hook("kr_test_abort");
            await(() -> {
                owner.gate.lock();
                try { return owner.access.ownerStatus() == 2; }
                finally { owner.gate.unlock(); }
            });
            owner.producer.close(Duration.ZERO);
            assertTrue(result.isDone());
            assertThrows(ExecutionException.class, result::get);
            assertFalse((boolean) field(owner.producer, "normalClosed"));
            assertEquals("DESTROYED", field(owner.producer, "state").toString());
        }
    }

    @Test void interruptedMetadataWaitDoesNotAcceptOrAbandonTeardown() throws Exception {
        try (Owner owner = new Owner(60_000)) {
            FutureTask<Void> send = new FutureTask<>(() -> {
                assertThrows(InterruptException.class, () -> owner.producer.send(new ProducerRecord<>("topic", new byte[]{3})));
                assertTrue(Thread.currentThread().isInterrupted());
                return null;
            });
            Thread sender = Thread.ofPlatform().start(send);
            await(() -> owner.openedTopic("topic") >= 0);
            sender.interrupt();
            send.get(10, TimeUnit.SECONDS);
            owner.gate.lock();
            try { assertEquals(0, ((PendingTable) field(owner.producer, "pending")).used()); }
            finally { owner.gate.unlock(); }
        }
    }

    @Test void flushRegistrationIsAtomicAndAbandonedWaiterRetainsItsFence() throws Exception {
        try (Owner owner = new Owner(0)) {
            FutureTask<Void> waiting = new FutureTask<>(() -> {
                assertThrows(InterruptException.class, owner.producer::flush);
                return null;
            });
            Thread caller = Thread.ofPlatform().start(waiting);
            await(() -> {
                owner.gate.lock();
                try { return ((Map<?, ?>) field(owner.producer, "flushes")).size() == 1; }
                finally { owner.gate.unlock(); }
            });
            caller.interrupt();
            waiting.get(10, TimeUnit.SECONDS);
            owner.gate.lock();
            try { assertEquals(1, ((Map<?, ?>) field(owner.producer, "flushes")).size()); }
            finally { owner.gate.unlock(); }
            owner.hook("kr_test_resume");
            owner.producer.flush();
            owner.gate.lock();
            try { assertTrue(((Map<?, ?>) field(owner.producer, "flushes")).isEmpty()); }
            finally { owner.gate.unlock(); }
        }
    }

    @Test void concurrentFlushAndCloseCallersShareBoundedProgress() throws Exception {
        try (Owner owner = new Owner(0)) {
            ArrayList<FutureTask<Void>> calls = new ArrayList<>();
            for (int i = 0; i < 24; i++) {
                FutureTask<Void> call = new FutureTask<>(() -> { owner.producer.flush(); return null; });
                calls.add(call);
                (i % 2 == 0 ? Thread.ofPlatform() : Thread.ofVirtual()).start(call);
            }
            await(() -> {
                owner.gate.lock();
                try { return !((Map<?, ?>) field(owner.producer, "flushes")).isEmpty(); }
                finally { owner.gate.unlock(); }
            });
            owner.hook("kr_test_resume");
            for (FutureTask<Void> call : calls) call.get(10, TimeUnit.SECONDS);
            calls.clear();
            for (int i = 0; i < 12; i++) {
                FutureTask<Void> call = new FutureTask<>(() -> { owner.producer.close(Duration.ZERO); return null; });
                calls.add(call);
                Thread.ofVirtual().start(call);
            }
            for (FutureTask<Void> call : calls) call.get(10, TimeUnit.SECONDS);
            assertEquals("DESTROYED", field(owner.producer, "state").toString());
        }
    }

    @Test void gracefulAndHugeDurationCloseHaveCheckedNativeMillisecondConversion() {
        try (Owner owner = new Owner(0)) {
            owner.hook("kr_test_resume");
            owner.producer.close();
            assertEquals("DESTROYED", field(owner.producer, "state").toString());
        }
        try (Owner owner = new Owner(0)) {
            assertThrows(IllegalArgumentException.class, () -> owner.producer.close(Duration.ofNanos(-1)));
            owner.hook("kr_test_resume");
            owner.producer.close(Duration.ofSeconds(Long.MAX_VALUE));
            assertEquals("DESTROYED", field(owner.producer, "state").toString());
            assertThrows(UnsupportedOperationException.class, () -> owner.producer.resourceBudget().clear());
        }
    }

    @Test void closeFencesAnAlreadySerializingCallerAndClosesSuppliedPluginsOnce() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger closes = new AtomicInteger();
        Serializer<byte[]> serializer = new Serializer<>() {
            @Override public byte[] serialize(String topic, byte[] value) {
                entered.countDown();
                try { release.await(); } catch (InterruptedException error) { throw new InterruptException(error); }
                return value;
            }
            @Override public void close() { closes.incrementAndGet(); }
        };
        try (Owner owner = new Owner(0, serializer, serializer)) {
            FutureTask<Void> send = new FutureTask<>(() -> {
                assertThrows(IllegalStateException.class, () -> owner.producer.send(new ProducerRecord<>("topic", new byte[0])));
                return null;
            });
            Thread.ofVirtual().start(send);
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            owner.hook("kr_test_resume");
            FutureTask<Void> closing = new FutureTask<>(() -> { owner.producer.close(Duration.ZERO); return null; });
            Thread.ofPlatform().start(closing);
            await(() -> {
                owner.gate.lock();
                try { return field(owner.producer, "state").toString().equals("CLOSING"); }
                finally { owner.gate.unlock(); }
            });
            assertEquals(0, closes.get());
            release.countDown();
            send.get(10, TimeUnit.SECONDS);
            closing.get(10, TimeUnit.SECONDS);
            assertEquals(1, closes.get(), "the same supplied instance is closed once");
        } finally { release.countDown(); }
    }
}
