package io.krkafka.producer;

import static org.junit.jupiter.api.Assertions.*;
import static io.krkafka.producer.NativeOwnerTest.*;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Native return-point barriers force publication races; no timing sleeps or mocks. */
@Timeout(20)
class ControlledConcurrencyTest {
    private static final FunctionDescriptor CONTROL = FunctionDescriptor.of(ValueLayout.JAVA_INT,
            ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT);
    private static final FunctionDescriptor ONE = FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS);

    private static void arm(Owner owner, int operation, int flags) {
        owner.gate.lock();
        try { assertEquals(0, invoke("kr_test_arm_call", CONTROL, owner.access.handle, operation, flags)); }
        finally { owner.gate.unlock(); }
    }

    private static void published(Owner owner) {
        // These test-only observer calls explicitly preserve operation diagnostics
        // and must be outside callGate while the real downcall is paused within it.
        assertEquals(0, invoke("kr_test_wait_call", CONTROL, owner.access.handle, 4, 10_000));
    }

    private static void release(Owner owner) { assertEquals(0, invoke("kr_test_release_call", ONE, owner.access.handle)); }

    private static void assertGateHeldByOther(Owner owner) {
        boolean acquired = owner.gate.tryLock();
        if (acquired) owner.gate.unlock();
        assertFalse(acquired, "the production gate covers native return and Java publication");
    }

    @Test void deliveryPublishedBeforeSubmitReturnCannotBeatPendingPublication() throws Exception {
        try (Owner owner = new Owner(10_000)) {
            owner.initialize("topic", 1, 1);
            AtomicInteger callbacks = new AtomicInteger();
            arm(owner, 1, 7);
            FutureTask<Future<RecordMetadata>> send = new FutureTask<>(() -> owner.producer.send(
                    new ProducerRecord<>("topic", new byte[]{1}), (metadata, error) -> callbacks.incrementAndGet()));
            Thread.ofPlatform().start(send);
            try {
                published(owner);
                assertFalse(send.isDone());
                assertEquals(0, callbacks.get());
                assertGateHeldByOther(owner);
            } finally { release(owner); }
            Future<RecordMetadata> result = send.get(10, TimeUnit.SECONDS);
            assertThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS));
            assertEquals(1, callbacks.get());
            owner.gate.lock();
            try { assertEquals(0, ((PendingTable) field(owner.producer, "pending")).used()); }
            finally { owner.gate.unlock(); }
        }
    }

    @Test void flushPublishedBeforeReturnCannotBeatFenceRegistration() throws Exception {
        try (Owner owner = new Owner(0)) {
            owner.hook("kr_test_resume");
            arm(owner, 2, 6);
            FutureTask<Void> flush = new FutureTask<>(() -> { owner.producer.flush(); return null; });
            Thread.ofVirtual().start(flush);
            try {
                published(owner);
                assertFalse(flush.isDone());
                assertGateHeldByOther(owner);
            } finally { release(owner); }
            flush.get(10, TimeUnit.SECONDS);
            owner.gate.lock();
            try { assertTrue(((Map<?, ?>) field(owner.producer, "flushes")).isEmpty()); }
            finally { owner.gate.unlock(); }
        }
    }

    @Test void rejectedSubmitDiagnosticsCannotBeOverwrittenByConcurrentMetadataOrPoll() throws Exception {
        try (Owner owner = new Owner(0, new ByteArraySerializer(), new ByteArraySerializer(),
                Map.of("kr.record.descriptors", 16))) {
            owner.producer.send(new ProducerRecord<>("topic", new byte[0]));
            owner.metadata("topic", 1, 1);
            owner.hook("kr_test_resume");
            owner.ready("topic", 1);
            AtomicInteger accepted = new AtomicInteger();
            await(() -> {
                Future<RecordMetadata> result = owner.producer.send(new ProducerRecord<>("topic", new byte[]{1}));
                if (!result.isDone()) accepted.incrementAndGet();
                return accepted.get() == 8;
            });
            arm(owner, 1, 4);
            FutureTask<Future<RecordMetadata>> send = new FutureTask<>(() -> owner.producer.send(new ProducerRecord<>("topic", new byte[]{2})));
            Thread.ofPlatform().start(send);
            FutureTask<Void> metadata = new FutureTask<>(() -> { owner.producer.partitionsFor("topic"); return null; });
            Thread contender = Thread.ofPlatform().unstarted(metadata);
            try {
                assertEquals(0, invoke("kr_test_wait_call", CONTROL, owner.access.handle, 2, 10_000));
                contender.start();
                await(() -> owner.gate.hasQueuedThread(contender));
                assertFalse(metadata.isDone());
            } finally { release(owner); }
            Future<RecordMetadata> result = send.get(10, TimeUnit.SECONDS);
            assertInstanceOf(BufferExhaustedException.class, assertThrows(ExecutionException.class, result::get).getCause());
            metadata.get(10, TimeUnit.SECONDS);
        }
    }

    @Test void duplicateAndUnknownGenerationEventsFenceAdmissionWithoutRepeatingCallbacks() throws Exception {
        for (long token : new long[]{0, 0x7fff_ffff_0000_0000L, 0x1_ffff_ffffL}) {
            try (Owner owner = new Owner(10_000)) {
                owner.initialize("topic", 1, 1);
                AtomicInteger callbacks = new AtomicInteger();
                arm(owner, 1, 3);
                Future<RecordMetadata> result = owner.producer.send(new ProducerRecord<>("topic", new byte[]{1}),
                        (metadata, error) -> callbacks.incrementAndGet());
                assertThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS));
                owner.gate.lock();
                try {
                    assertEquals(0, invoke("kr_test_replay_delivery", FunctionDescriptor.of(ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS, ValueLayout.JAVA_LONG), owner.access.handle, token));
                } finally { owner.gate.unlock(); }
                await(() -> {
                    owner.gate.lock();
                    try { return field(owner.producer, "state").toString().equals("FAILED"); }
                    finally { owner.gate.unlock(); }
                });
                assertEquals(1, callbacks.get());
                Future<RecordMetadata> rejected = owner.producer.send(new ProducerRecord<>("topic", new byte[]{2}));
                assertThrows(ExecutionException.class, rejected::get);
            }
        }
    }

    @Test void slowCallbackDelaysFutureAndCloseUntilCallbackUnwinds() throws Exception {
        try (Owner owner = new Owner(10_000)) {
            owner.initialize("topic", 1, 1);
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch continueCallback = new CountDownLatch(1);
            arm(owner, 1, 3);
            Future<RecordMetadata> result = owner.producer.send(new ProducerRecord<>("topic", new byte[]{1}), (metadata, error) -> {
                owner.producer.close(Duration.ZERO);
                entered.countDown();
                try { continueCallback.await(); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            });
            try {
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                assertFalse(result.isDone());
                FutureTask<Void> close = new FutureTask<>(() -> { owner.producer.close(Duration.ZERO); return null; });
                Thread closer = Thread.ofPlatform().start(close);
                await(() -> closer.getState() == Thread.State.WAITING);
                assertFalse(close.isDone());
                continueCallback.countDown();
                close.get(10, TimeUnit.SECONDS);
                assertTrue(result.isDone());
                assertEquals("DESTROYED", field(owner.producer, "state").toString());
            } finally { continueCallback.countDown(); }
        }
    }

    @Test void publishedFlushFenceStillWaitsForItsCoveredSlowCallbackAndFuture() throws Exception {
        try (Owner owner = new Owner(10_000)) {
            owner.initialize("topic", 1, 1);
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch continueCallback = new CountDownLatch(1);
            arm(owner, 1, 3);
            Future<RecordMetadata> result = owner.producer.send(new ProducerRecord<>("topic", new byte[]{1}), (metadata, error) -> {
                entered.countDown();
                try { continueCallback.await(); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            });
            try {
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                arm(owner, 2, 6);
                FutureTask<Void> flush = new FutureTask<>(() -> { owner.producer.flush(); return null; });
                Thread.ofVirtual().start(flush);
                try { published(owner); }
                finally { release(owner); }
                await(() -> {
                    owner.gate.lock();
                    try { return ((Map<?, ?>) field(owner.producer, "flushes")).size() == 1; }
                    finally { owner.gate.unlock(); }
                });
                // The real native fence is already application-queued and the
                // Java waiter registered. Only the covered callback blocks it.
                assertFalse(result.isDone());
                assertFalse(flush.isDone());
                continueCallback.countDown();
                flush.get(10, TimeUnit.SECONDS);
                assertTrue(result.isDone());
            } finally { continueCallback.countDown(); }
        }
    }

    @Test void corruptedJavaIdentityFencesAdmissionButDoesNotConsumeTheNativeObligation() throws Exception {
        try (Owner owner = new Owner(10_000)) {
            owner.initialize("topic", 1, 1);
            AtomicInteger callbacks = new AtomicInteger();
            Future<RecordMetadata> result = owner.producer.send(new ProducerRecord<>("topic", new byte[]{1}),
                    (metadata, error) -> callbacks.incrementAndGet());
            PendingTable.Entry entry;
            Uuid original;
            owner.gate.lock();
            try (Arena arena = Arena.ofConfined()) {
                entry = java.util.Arrays.stream(((PendingTable) field(owner.producer, "pending")).entries())
                        .filter(slot -> slot.state == PendingTable.State.ACCEPTED).findFirst().orElseThrow();
                original = entry.topicId;
                entry.topicId = new Uuid(7, 8);
                MemorySegment cursor = arena.allocate(ValueLayout.JAVA_LONG);
                assertEquals(0, invoke("kr_test_cancel_since", FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS, ValueLayout.ADDRESS), owner.access.handle, cursor));
            } finally { owner.gate.unlock(); }
            await(() -> {
                owner.gate.lock();
                try { return field(owner.producer, "state") == KrKafkaProducer.State.FAILED; }
                finally { owner.gate.unlock(); }
            });
            assertFalse(result.isDone());
            assertEquals(0, callbacks.get());
            owner.gate.lock();
            try {
                assertEquals(PendingTable.State.ACCEPTED, entry.state);
                assertInstanceOf(NativeAccess.ProtocolFailure.class, field(owner.producer, "failure"));
                entry.topicId = original;
                assertEquals(0, invoke("kr_test_replay_delivery", FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS, ValueLayout.JAVA_LONG), owner.access.handle, 0L));
            } finally { owner.gate.unlock(); }
            // The same real native terminal event can still settle the accepted
            // record after correlation is repaired; it was not replaced with a guess.
            Throwable cause = assertThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS)).getCause();
            assertEquals(2, assertInstanceOf(NativeDeliveryException.class, cause).reason());
            assertEquals(1, callbacks.get());
        }
    }

    @Test void interruptingCallbackDoesNotPoisonSubsequentPollerWork() throws Exception {
        for (int iteration = 0; iteration < 8; iteration++) {
            try (Owner owner = new Owner(10_000)) {
                owner.initialize("topic", 1, 1);
                CountDownLatch entered = new CountDownLatch(1);
                CountDownLatch blocker = new CountDownLatch(1);
                AtomicInteger interruptions = new AtomicInteger();
                arm(owner, 1, 3);
                Future<RecordMetadata> result = owner.producer.send(new ProducerRecord<>("topic", new byte[]{1}), (metadata, error) -> {
                    entered.countDown();
                    try { blocker.await(); }
                    catch (InterruptedException interrupted) {
                        interruptions.incrementAndGet();
                        Thread.currentThread().interrupt();
                    }
                });
                try {
                    assertTrue(entered.await(10, TimeUnit.SECONDS));
                    Thread poller = (Thread) field(owner.producer, "poller");
                    poller.interrupt();
                    assertThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS));
                    owner.producer.flush();
                    assertEquals(1, interruptions.get());
                    assertFalse(poller.isInterrupted());
                    assertEquals(KrKafkaProducer.State.OPEN, field(owner.producer, "state"));
                } finally { blocker.countDown(); }
            }
        }
    }
}
