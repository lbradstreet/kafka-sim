package io.krkafka.producer;

import static org.junit.jupiter.api.Assertions.*;
import static io.krkafka.producer.NativeOwnerTest.*;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(90)
class ProducerStressTest {
    private static final FunctionDescriptor CANCEL = FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS);

    private static void assertBounds(Owner owner) {
        // Owner.gate is held by every observer, just as by production accounting.
        PendingTable pending = (PendingTable) field(owner.producer, "pending");
        ScratchPool scratch = (ScratchPool) field(owner.producer, "scratch");
        assertTrue(pending.used() >= 0 && pending.used() <= 2);
        assertTrue(scratch.allocatedBytes() >= 0 && scratch.allocatedBytes() <= 512);
        assertTrue(scratch.active() >= 0 && scratch.active() <= 1);
        int active = (int) field(owner.producer, "activeOperations");
        assertTrue(active >= 0 && active <= 3);
        assertTrue(((Map<?, ?>) field(owner.producer, "topics")).size() <= 2);
        assertTrue(((Map<?, ?>) field(owner.producer, "flushes")).size() <= 1);
        int obligations = 0;
        for (PendingTable.Entry entry : pending.entries()) {
            if (entry.state != PendingTable.State.FREE && entry.state != PendingTable.State.RETIRED) obligations++;
        }
        assertEquals(obligations, pending.used());
    }

    @Test void seededPlatformAndVirtualSendersStayBoundedAndMakeProgress() throws Exception {
        for (long seed : new long[]{7, 113, 991}) runWorkload(seed);
    }

    private static void runWorkload(long seed) throws Exception {
        int callers = 8;
        int perCaller = 160;
        int primaryCount = callers * perCaller;
        int reentries = (primaryCount + 16) / 17;
        int total = primaryCount + reentries;
        AtomicIntegerArray callbacks = new AtomicIntegerArray(total);
        CountDownLatch completed = new CountDownLatch(total);
        ConcurrentHashMap<Integer, Future<RecordMetadata>> futures = new ConcurrentHashMap<>();
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger peakPending = new AtomicInteger();
        try (Owner owner = new Owner(30_000, new ByteArraySerializer(), new ByteArraySerializer(), Map.of(
                "kr.record.descriptors", 2, "kr.max.open.topics", 2, "kr.scratch.bytes", 512,
                "kr.scratch.checkouts", 1, "kr.serialization.concurrency", 3,
                "kr.max.flushes", 1, "kr.max.completions.per.poll", 1,
                "max.request.size", 1024, "batch.size", 64))) {
            owner.initialize("first", 2, 1);
            owner.initialize("second", 2, 2);
            CountDownLatch start = new CountDownLatch(1);
            ArrayList<FutureTask<Void>> workers = new ArrayList<>();
            FutureTask<Void> cancellation = new FutureTask<>(() -> {
                try (Arena arena = Arena.ofConfined()) {
                    MemorySegment cursor = arena.allocate(ValueLayout.JAVA_LONG);
                    Condition changed = (Condition) field(owner.producer, "changed");
                    while (!stop.get()) {
                        owner.gate.lock();
                        try {
                            assertBounds(owner);
                            peakPending.accumulateAndGet(((PendingTable) field(owner.producer, "pending")).used(), Math::max);
                            int code = invoke("kr_test_cancel_since", CANCEL, owner.access.handle, cursor);
                            assertTrue(code == 0 || code == NativeAccess.EXHAUSTED, "cancel control error=" + code);
                            // A condition recheck handles native control-credit progress even
                            // when no Java event exists yet. No sleep determines correctness.
                            changed.awaitNanos(TimeUnit.MILLISECONDS.toNanos(1));
                        } finally { owner.gate.unlock(); }
                    }
                }
                return null;
            });
            Thread canceller = Thread.ofPlatform().unstarted(cancellation);
            try {
                for (int caller = 0; caller < callers; caller++) {
                    int callerId = caller;
                    FutureTask<Void> worker = new FutureTask<>(() -> {
                        Random random = new Random(seed * 31 + callerId);
                        start.await();
                        for (int item = 0; item < perCaller; item++) {
                            int id = callerId * perCaller + item;
                            String topic = random.nextBoolean() ? "first" : "second";
                            byte[] value = new byte[random.nextInt(201)];
                            random.nextBytes(value);
                            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, random.nextInt(2), null, value);
                            if (random.nextBoolean()) record.headers().add("h", null).add("h", new byte[0]);
                            Future<RecordMetadata> result = owner.producer.send(record, (metadata, error) -> {
                                try {
                                    assertEquals(1, callbacks.incrementAndGet(id), "duplicate callback " + id);
                                    assertFalse(owner.gate.isHeldByCurrentThread());
                                    assertNotNull(error, "the controlled fixture has no broker acknowledgement path");
                                    if (id % 17 == 0) {
                                        int nested = primaryCount + id / 17;
                                        Future<RecordMetadata> next = owner.producer.send(new ProducerRecord<>(topic, new byte[]{1}), (m, e) -> {
                                            try {
                                                assertEquals(1, callbacks.incrementAndGet(nested));
                                                assertNotNull(e);
                                            } catch (Throwable failure) { callbackFailure.compareAndSet(null, failure); }
                                            finally { completed.countDown(); }
                                        });
                                        assertNull(futures.putIfAbsent(nested, next));
                                    }
                                } catch (Throwable failure) { callbackFailure.compareAndSet(null, failure); }
                                finally { completed.countDown(); }
                            });
                            assertNull(futures.putIfAbsent(id, result));
                            assertFalse(result.cancel(random.nextBoolean()));
                            if (item % 31 == 0) {
                                Thread.currentThread().interrupt();
                                assertThrows(InterruptedException.class, result::get);
                                assertFalse(Thread.interrupted());
                            }
                        }
                        return null;
                    });
                    workers.add(worker);
                    (caller % 2 == 0 ? Thread.ofPlatform() : Thread.ofVirtual()).start(worker);
                }
                start.countDown();
                await(() -> {
                    owner.gate.lock();
                    try {
                        assertBounds(owner);
                        return ((PendingTable) field(owner.producer, "pending")).used() == 2 &&
                                (int) field(owner.producer, "activeOperations") == 3;
                    } finally { owner.gate.unlock(); }
                });
                canceller.start();
                for (int caller = 0; caller < 4; caller++) {
                    FutureTask<Void> flush = new FutureTask<>(() -> {
                        for (int i = 0; i < 40; i++) owner.producer.flush();
                        return null;
                    });
                    workers.add(flush);
                    (caller % 2 == 0 ? Thread.ofPlatform() : Thread.ofVirtual()).start(flush);
                }
                for (FutureTask<Void> worker : workers) worker.get(40, TimeUnit.SECONDS);
                assertTrue(completed.await(40, TimeUnit.SECONDS), "seed=" + seed + " callbacks missing=" + completed.getCount());
                owner.producer.flush();
                assertNull(callbackFailure.get());
                assertEquals(total, futures.size());
                for (int id = 0; id < total; id++) {
                    assertEquals(1, callbacks.get(id), "seed=" + seed + " id=" + id);
                    Future<RecordMetadata> result = futures.get(id);
                    assertTrue(result.isDone());
                    assertThrows(ExecutionException.class, result::get);
                }
                owner.gate.lock();
                try {
                    assertBounds(owner);
                    assertEquals(0, ((PendingTable) field(owner.producer, "pending")).used());
                    assertEquals(0, ((ScratchPool) field(owner.producer, "scratch")).active());
                    assertEquals(0, (int) field(owner.producer, "activeOperations"));
                    assertTrue(((Map<?, ?>) field(owner.producer, "flushes")).isEmpty());
                    assertEquals(KrKafkaProducer.State.OPEN, field(owner.producer, "state"));
                } finally { owner.gate.unlock(); }
                assertEquals(2, peakPending.get());
            } finally {
                stop.set(true);
                owner.gate.lock();
                try { ((Condition) field(owner.producer, "changed")).signalAll(); }
                finally { owner.gate.unlock(); }
                if (canceller.getState() != Thread.State.NEW) cancellation.get(10, TimeUnit.SECONDS);
                else start.countDown();
            }
        }
    }

    @Test void interruptingSerializerAndClosingWaiterNeverAbandonsTeardown() throws Exception {
        for (int iteration = 0; iteration < 16; iteration++) {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger pluginCloses = new AtomicInteger();
            Serializer<byte[]> serializer = new Serializer<>() {
                @Override public byte[] serialize(String topic, byte[] value) {
                    entered.countDown();
                    try { release.await(); }
                    catch (InterruptedException interrupted) { throw new InterruptException(interrupted); }
                    return value;
                }
                @Override public void close() { pluginCloses.incrementAndGet(); }
            };
            try (Owner owner = new Owner(30_000, serializer, serializer, Map.of("kr.serialization.concurrency", 1))) {
                FutureTask<Void> sending = new FutureTask<>(() -> {
                    assertThrows(InterruptException.class, () -> owner.producer.send(new ProducerRecord<>("topic", new byte[0])));
                    return null;
                });
                Thread sender = (iteration % 2 == 0 ? Thread.ofVirtual() : Thread.ofPlatform()).start(sending);
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                owner.hook("kr_test_resume");
                FutureTask<Void> closing = new FutureTask<>(() -> {
                    assertThrows(InterruptException.class, () -> owner.producer.close(Duration.ZERO));
                    assertTrue(Thread.currentThread().isInterrupted());
                    return null;
                });
                Thread closer = Thread.ofPlatform().start(closing);
                await(() -> {
                    owner.gate.lock();
                    try { return field(owner.producer, "state") == KrKafkaProducer.State.CLOSING; }
                    finally { owner.gate.unlock(); }
                });
                closer.interrupt();
                closing.get(10, TimeUnit.SECONDS);
                assertEquals(0, pluginCloses.get());
                sender.interrupt();
                sending.get(10, TimeUnit.SECONDS);
                owner.producer.close(Duration.ZERO);
                assertEquals(1, pluginCloses.get());
                assertEquals(0, ((PendingTable) field(owner.producer, "pending")).used());
                assertEquals(0, (int) field(owner.producer, "activeOperations"));
                assertEquals(KrKafkaProducer.State.DESTROYED, field(owner.producer, "state"));
            } finally { release.countDown(); }
        }
    }
}
