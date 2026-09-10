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
package org.apache.kafka.clients.dst;

import org.apache.kafka.clients.dst.sim.ClassicSimSelector;
import org.apache.kafka.clients.dst.sim.FaultInjector;
import org.apache.kafka.clients.dst.sim.FaultInjector.FaultProfile;
import org.apache.kafka.clients.dst.sim.SimBroker;
import org.apache.kafka.clients.dst.sim.SimCluster;
import org.apache.kafka.clients.dst.sim.SimNetwork;
import org.apache.kafka.clients.dst.sim.SimScheduler;
import org.apache.kafka.clients.dst.sim.SimTrace;
import org.apache.kafka.clients.producer.ClassicProducerSimFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.SenderPump;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Experimental: the classic {@link KafkaProducer} driven under the deterministic simulation.
 * The I/O thread is replaced by a scheduler-paced {@code Sender.runOnce()} pump and the clock
 * pumps the scheduler whenever the producer would block on metadata.
 */
public class ClassicProducerDstTest {

    private static final String TOPIC = "classic-dst";
    private static final int PARTITIONS = 3;
    private static final long MAX_VIRTUAL_TIME_MS = Duration.ofMinutes(30).toMillis();
    /**
     * Disconnects and delay only. The sim's frame-drop faults lose one request on an open
     * connection, which TCP cannot do; the classic NetworkClient relies on in-order responses
     * and would report a correlation-id mismatch.
     */
    private static final FaultProfile LOSSY = new FaultProfile(0, 0, 0.2, 20);

    /** A MockTime whose blocking waits run the simulation instead of parking the thread. */
    static final class PumpingClock extends MockTime {
        SimScheduler scheduler;

        PumpingClock() {
            super(0L, 0L, 0L);
        }

        @Override
        public void waitObject(Object obj, Supplier<Boolean> condition, long deadlineMs) {
            while (milliseconds() < deadlineMs && !condition.get()) {
                try {
                    scheduler.runUntil(() -> condition.get() || milliseconds() >= deadlineMs, deadlineMs);
                } catch (IllegalStateException e) {
                    if (!ClassicSimSelector.isIdleOrPastCap(e))
                        throw e;
                    sleep(Math.max(0, deadlineMs - milliseconds()));
                }
            }
            if (!condition.get())
                throw new TimeoutException("Condition not satisfied before deadline");
        }
    }

    static final class Harness implements AutoCloseable {
        final PumpingClock time = new PumpingClock();
        final SimTrace trace = new SimTrace(time);
        final SimScheduler scheduler = new SimScheduler(time);
        final SimCluster cluster;
        final FaultInjector faults;
        final SimNetwork network;
        final long seed;
        ClassicSimSelector selector;
        ClassicProducerSimFactory.Handle<String, String> handle;
        private ScheduledFuture<?> pumpTask;
        private long pumpDueMs;
        private boolean pumping;
        private boolean wakeupPending;
        private boolean stopped;

        Harness(long seed, FaultProfile profile, int brokerCount) {
            this.seed = seed;
            time.scheduler = scheduler;
            cluster = new SimCluster(brokerCount, time);
            faults = new FaultInjector(seed, profile, trace);
            network = new SimNetwork(scheduler, faults, trace);
            cluster.nodes().forEach(node -> network.addBroker(new SimBroker(node.id(), cluster, trace)));
        }

        Map<String, Object> baseConfig() {
            Map<String, Object> config = new HashMap<>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrap());
            config.put(ProducerConfig.CLIENT_ID_CONFIG, "classic-dst");
            config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
            config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 50L);
            // Small batches mean many produce requests, so per-request faults actually fire.
            config.put(ProducerConfig.BATCH_SIZE_CONFIG, 512);
            config.put(ProducerConfig.LINGER_MS_CONFIG, 5L);
            config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
            config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60_000L);
            return config;
        }

        KafkaProducer<String, String> startProducer(Map<String, Object> config) {
            selector = new ClassicSimSelector(network, cluster, scheduler, trace);
            selector.onWakeup(() -> schedulePump(0));
            handle = ClassicProducerSimFactory.create(config, new StringSerializer(),
                new StringSerializer(), selector, Harness::resolveSimHost, time, seed);
            schedulePump(0);
            return handle.producer();
        }

        private static InetAddress[] resolveSimHost(String host) throws UnknownHostException {
            return new InetAddress[] {InetAddress.getByAddress(host, new byte[] {127, 0, 0, 1})};
        }

        private void schedulePump(long delayMs) {
            if (stopped)
                return;
            if (pumping) {
                wakeupPending = true;
                return;
            }
            // Never zero: a self-rescheduling zero-delay task would starve every timer.
            long delay = Math.max(1, delayMs);
            long dueMs = time.milliseconds() + delay;
            if (pumpTask != null && !pumpTask.isDone()) {
                if (pumpDueMs <= dueMs)
                    return;
                pumpTask.cancel(false);
            }
            pumpDueMs = dueMs;
            pumpTask = scheduler.clientWorkScheduler().schedule(this::pump, delay, TimeUnit.MILLISECONDS);
        }

        private void pump() {
            if (stopped)
                return;
            pumping = true;
            wakeupPending = false;
            try {
                SenderPump.runOnce(handle.sender());
            } finally {
                pumping = false;
            }
            // poll() already consumed the sender's wait, so the next iteration is due at once.
            schedulePump(1);
        }

        void runUntil(BooleanSupplier condition) {
            scheduler.runUntil(condition, MAX_VIRTUAL_TIME_MS);
        }

        void runFor(long ms) {
            long until = time.milliseconds() + ms;
            scheduler.schedule(() -> { }, ms, TimeUnit.MILLISECONDS);
            runUntil(() -> time.milliseconds() >= until);
        }

        /** Drain like the I/O thread would after initiateClose, then tear down without joining. */
        void closeProducer() {
            handle.sender().initiateClose();
            runUntil(() -> !handle.accumulator().hasUndrained()
                && handle.client().inFlightRequestCount() == 0);
            stopped = true;
            if (pumpTask != null)
                pumpTask.cancel(false);
            handle.client().close();
            handle.producer().close(Duration.ZERO);
        }

        Map<Integer, List<String>> storedValues() {
            Map<Integer, List<String>> stored = new HashMap<>();
            for (int p = 0; p < PARTITIONS; p++) {
                List<String> values = new ArrayList<>();
                for (SimCluster.StoredRecord record : cluster.log(new TopicPartition(TOPIC, p)))
                    values.add(new String(record.value(), StandardCharsets.UTF_8));
                stored.put(p, values);
            }
            return stored;
        }

        @Override
        public void close() {
            stopped = true;
            scheduler.shutdownNow();
        }
    }

    private record RunResult(List<String> trace, List<Integer> succeeded, List<Integer> failed,
                             Map<Integer, List<String>> stored, int faultsInjected) { }

    private RunResult run(long seed, FaultProfile profile, int records, Map<String, Object> overrides) {
        return run(seed, profile, records, overrides, true);
    }

    private RunResult run(long seed, FaultProfile profile, int records, Map<String, Object> overrides,
                          boolean explicitPartitions) {
        try (Harness harness = new Harness(seed, profile, 2)) {
            harness.cluster.createTopic(TOPIC, PARTITIONS);
            Map<String, Object> config = harness.baseConfig();
            config.putAll(overrides);
            KafkaProducer<String, String> producer = harness.startProducer(config);

            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < records; i++) {
                Integer partition = explicitPartitions ? i % PARTITIONS : null;
                futures.add(producer.send(new ProducerRecord<>(TOPIC, partition, "key-" + i, "value-" + i)));
            }
            harness.runUntil(() -> futures.stream().allMatch(Future::isDone));
            harness.closeProducer();

            List<Integer> succeeded = new ArrayList<>();
            List<Integer> failed = new ArrayList<>();
            for (int i = 0; i < futures.size(); i++) {
                try {
                    futures.get(i).get();
                    succeeded.add(i);
                } catch (Exception e) {
                    failed.add(i);
                }
            }
            return new RunResult(harness.trace.events(), succeeded, failed,
                harness.storedValues(), harness.faults.faultsInjected());
        }
    }

    private static List<String> expectedValues(int partition, int records) {
        List<String> expected = new ArrayList<>();
        for (int i = partition; i < records; i += PARTITIONS)
            expected.add("value-" + i);
        return expected;
    }

    @Test
    public void testQuietNetworkStoresEveryRecordInOrder() {
        int records = 200;
        RunResult result = run(1L, FaultProfile.NONE, records, Map.of());
        assertEquals(List.of(), result.failed());
        assertEquals(records, result.succeeded().size());
        for (int p = 0; p < PARTITIONS; p++)
            assertEquals(expectedValues(p, records), result.stored().get(p), "partition " + p);
    }

    @ParameterizedTest
    @ValueSource(longs = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L})
    public void testLossyNetworkWithIdempotenceIsExactlyOnceAndOrdered(long seed) {
        int records = 300;
        RunResult result = run(seed, LOSSY, records, Map.of(
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1_000));
        assertTrue(result.faultsInjected() > 0, "scenario must actually inject faults");
        assertEquals(List.of(), result.failed(), "idempotent producer retries through faults");
        assertEquals(records, result.succeeded().size());
        for (int p = 0; p < PARTITIONS; p++)
            assertEquals(expectedValues(p, records), result.stored().get(p), "partition " + p);
    }

    @ParameterizedTest
    @ValueSource(longs = {11L, 12L, 13L})
    public void testLossyNetworkWithoutIdempotenceNeverLosesAnAcknowledgedRecord(long seed) {
        int records = 300;
        RunResult result = run(seed, LOSSY, records, Map.of(
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false,
            ProducerConfig.ACKS_CONFIG, "1",
            ProducerConfig.RETRIES_CONFIG, 20,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1_000));
        assertTrue(result.faultsInjected() > 0);
        assertEquals(records, result.succeeded().size() + result.failed().size());
        // Without idempotence a lost acknowledgement may duplicate, but acknowledged data is present.
        for (int i : result.succeeded()) {
            assertTrue(result.stored().get(i % PARTITIONS).contains("value-" + i),
                "acknowledged record " + i + " missing from partition " + i % PARTITIONS);
        }
    }

    @Test
    public void testLeaderIsolationRecoversWithoutLossOrDuplication() {
        int records = 120;
        try (Harness harness = new Harness(99L, FaultProfile.NONE, 2)) {
            harness.cluster.createTopic(TOPIC, PARTITIONS);
            Map<String, Object> config = harness.baseConfig();
            config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1_000);
            KafkaProducer<String, String> producer = harness.startProducer(config);

            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < records / 2; i++)
                futures.add(producer.send(new ProducerRecord<>(TOPIC, i % PARTITIONS, null, "value-" + i)));
            harness.runUntil(() -> futures.stream().allMatch(Future::isDone));
            long connectsBeforeIsolation = connectCount(harness);

            harness.network.isolateBroker(1, harness.time.milliseconds() + 5_000);
            for (int i = records / 2; i < records; i++)
                futures.add(producer.send(new ProducerRecord<>(TOPIC, i % PARTITIONS, null, "value-" + i)));
            harness.runUntil(() -> futures.stream().allMatch(Future::isDone));
            harness.closeProducer();

            for (Future<RecordMetadata> future : futures)
                assertTrue(future.isDone() && !future.isCancelled());
            for (int p = 0; p < PARTITIONS; p++)
                assertEquals(expectedValues(p, records), harness.storedValues().get(p), "partition " + p);
            assertTrue(harness.trace.events().stream().anyMatch(e -> e.contains("network isolate-close")),
                "isolation must kill a live connection");
            assertTrue(connectCount(harness) > connectsBeforeIsolation, "producer must reconnect");
        }
    }

    private static long connectCount(Harness harness) {
        return harness.trace.events().stream().filter(e -> e.contains("classic connect")).count();
    }

    @Test
    public void testQuietRunReplaysIdentically() {
        RunResult first = run(5L, FaultProfile.NONE, 60, Map.of());
        RunResult second = run(5L, FaultProfile.NONE, 60, Map.of());
        assertEquals(first.trace(), second.trace());
        assertEquals(first.stored(), second.stored());
    }

    @Test
    public void testStickyPartitionerRunReplaysIdentically() {
        Map<String, Object> unkeyedFriendly = Map.of(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG, true);
        RunResult first = run(8L, LOSSY, 200, unkeyedFriendly, false);
        RunResult second = run(8L, LOSSY, 200, unkeyedFriendly, false);
        assertTrue(first.faultsInjected() > 0);
        assertEquals(List.of(), first.failed());
        long partitionsUsed = first.stored().values().stream().filter(values -> !values.isEmpty()).count();
        assertTrue(partitionsUsed > 1, "sticky partitioner should rotate, used " + partitionsUsed);
        assertEquals(first.trace(), second.trace());
        assertEquals(first.stored(), second.stored());
    }

    @Test
    public void testLossyRunReplaysIdentically() {
        RunResult first = run(6L, LOSSY, 200, Map.of());
        RunResult second = run(6L, LOSSY, 200, Map.of());
        assertTrue(first.faultsInjected() > 0);
        assertEquals(first.trace(), second.trace());
        assertEquals(first.stored(), second.stored());
    }
}
