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
package org.apache.kafka.clients.dst.sim;

import org.apache.kafka.clients.dst.sim.FaultInjector.FaultPhase;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FaultInjectorTest {

    @Test
    public void testMaximumExtraDelayDoesNotOverflowTheInclusiveRandomBound() {
        SimTrace trace = new SimTrace(new MockTime(0, 0L, 0L));
        FaultInjector faults = new FaultInjector(1L,
            new FaultInjector.FaultProfile(0, 0, 0, Long.MAX_VALUE), trace);

        for (int i = 0; i < 100; i++) {
            long delayMs = faults.extraDelayMs();
            assertTrue(delayMs >= 0, "the inclusive maximum bound must produce a valid delay");
        }
    }

    @Test
    public void testNegativeExtraDelayBoundIsRejected() {
        assertThrows(IllegalArgumentException.class,
            () -> new FaultInjector.FaultProfile(0, 0, 0, -1));
    }

    @Test
    public void testScriptedRequestDropMatchesPhaseApiAndConnectionExactlyOnce() {
        SimTrace trace = new SimTrace(new MockTime(0, 0L, 0L));
        FaultInjector faults = new FaultInjector(1L, FaultInjector.FaultProfile.NONE, trace);
        faults.dropNext(FaultPhase.BEFORE_BROKER_HANDLE, ApiKeys.PRODUCE, "1#0");

        assertFalse(faults.dropRequest("1#0", ApiKeys.METADATA));
        assertFalse(faults.dropRequest("2#0", ApiKeys.PRODUCE));
        assertFalse(faults.dropResponse("1#0", ApiKeys.PRODUCE));
        assertTrue(faults.dropRequest("1#0", ApiKeys.PRODUCE));
        assertFalse(faults.dropRequest("1#0", ApiKeys.PRODUCE));

        assertEquals(1, faults.faultsInjected());
        assertEquals(List.of("t=0 fault scripted-drop phase=BEFORE_BROKER_HANDLE "
            + "api=PRODUCE conn=1#0"), trace.events());
    }

    @Test
    public void testRepeatedScriptedResponseDropsConsumeTheirExactBudget() {
        SimTrace trace = new SimTrace(new MockTime(0, 0L, 0L));
        FaultInjector faults = new FaultInjector(1L, FaultInjector.FaultProfile.NONE, trace);
        faults.dropNext(FaultPhase.AFTER_BROKER_HANDLE_BEFORE_RESPONSE, ApiKeys.FETCH, "1#0");
        faults.dropNext(FaultPhase.AFTER_BROKER_HANDLE_BEFORE_RESPONSE, ApiKeys.FETCH, "1#0");

        assertTrue(faults.dropResponse("1#0", ApiKeys.FETCH));
        assertTrue(faults.dropResponse("1#0", ApiKeys.FETCH));
        assertFalse(faults.dropResponse("1#0", ApiKeys.FETCH));

        assertEquals(2, faults.faultsInjected());
        assertEquals(2, trace.events().size());
        assertTrue(trace.events().stream().allMatch(event -> event.contains(
            "phase=AFTER_BROKER_HANDLE_BEFORE_RESPONSE api=FETCH conn=1#0")));
    }

    @Test
    public void testConcurrentArmingPublishesExactBudgetToSchedulerOwner() throws Exception {
        SimTrace trace = new SimTrace(new MockTime(0, 0L, 0L));
        FaultInjector faults = new FaultInjector(1L, FaultInjector.FaultProfile.NONE, trace);
        int faultsPerArmer = 500;
        int totalFaults = faultsPerArmer * 2;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            Future<?> firstArmer = threads.submit(() -> {
                await(start);
                for (int i = 0; i < faultsPerArmer; i++) {
                    faults.dropNext(FaultPhase.AFTER_BROKER_HANDLE_BEFORE_RESPONSE,
                        ApiKeys.PRODUCE, "1#0");
                }
            });
            Future<?> secondArmer = threads.submit(() -> {
                await(start);
                for (int i = 0; i < faultsPerArmer; i++) {
                    faults.dropNext(FaultPhase.AFTER_BROKER_HANDLE_BEFORE_RESPONSE,
                        ApiKeys.PRODUCE, "1#0");
                }
            });

            start.countDown();
            firstArmer.get(10, TimeUnit.SECONDS);
            secondArmer.get(10, TimeUnit.SECONDS);

            // Arming is the intentional cross-thread seam. Decisions and trace mutation remain
            // on the simulator owner thread.
            for (int i = 0; i < totalFaults; i++)
                assertTrue(faults.dropResponse("1#0", ApiKeys.PRODUCE));
            assertFalse(faults.dropResponse("1#0", ApiKeys.PRODUCE));
            assertEquals(totalFaults, faults.faultsInjected());
            assertEquals(totalFaults, trace.events().size());
        } finally {
            threads.shutdownNow();
            assertTrue(threads.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testForeignDecisionsRejectBeforeConsumingBudgetsOrRandomness() throws Exception {
        SimTrace trace = new SimTrace(new MockTime(0, 0L, 0L));
        FaultInjector faults = new FaultInjector(1L, FaultInjector.FaultProfile.NONE, trace);
        faults.failNextConnect("1#0");
        faults.dropNext(FaultPhase.BEFORE_BROKER_HANDLE, ApiKeys.PRODUCE, "1#0");
        faults.dropNext(FaultPhase.AFTER_BROKER_HANDLE_BEFORE_RESPONSE,
            ApiKeys.PRODUCE, "1#0");

        assertOwnershipFailure(() -> faults.failConnect("1#0"));
        assertOwnershipFailure(() -> faults.dropRequest("1#0", ApiKeys.PRODUCE));
        assertOwnershipFailure(() -> faults.dropResponse("1#0", ApiKeys.PRODUCE));
        assertOwnershipFailure(() -> faults.recordForced("isolation-test"));

        assertEquals(0, faults.faultsInjected());
        assertEquals(List.of(), trace.events());
        assertTrue(faults.failConnect("1#0"));
        assertFalse(faults.failConnect("1#0"));
        assertTrue(faults.dropRequest("1#0", ApiKeys.PRODUCE));
        assertFalse(faults.dropRequest("1#0", ApiKeys.PRODUCE));
        assertTrue(faults.dropResponse("1#0", ApiKeys.PRODUCE));
        assertFalse(faults.dropResponse("1#0", ApiKeys.PRODUCE));
        assertEquals(3, faults.faultsInjected(),
            "foreign decisions must leave all three one-shot budgets for the owner");

        FaultInjector.FaultProfile randomProfile =
            new FaultInjector.FaultProfile(0.5, 0.5, 0.5, Long.MAX_VALUE);
        FaultInjector probed = new FaultInjector(73L, randomProfile,
            new SimTrace(new MockTime(0, 0L, 0L)));
        FaultInjector control = new FaultInjector(73L, randomProfile,
            new SimTrace(new MockTime(0, 0L, 0L)));

        assertOwnershipFailure(() -> probed.dropRequest("random"));
        assertOwnershipFailure(() -> probed.dropResponse("random"));
        assertOwnershipFailure(() -> probed.disconnect("random"));
        assertOwnershipFailure(probed::extraDelayMs);
        Throwable observationFailure = runOnOtherThread(probed::faultsInjected);
        assertNull(observationFailure,
            "the atomic fault count remains an intentional cross-thread observation seam");

        for (int i = 0; i < 8; i++) {
            assertEquals(control.extraDelayMs(), probed.extraDelayMs(),
                "a rejected decision must not advance the seeded RNG");
        }
    }

    private static void assertOwnershipFailure(Runnable action) throws InterruptedException {
        Throwable thrown = runOnOtherThread(action);
        assertNotNull(thrown, "fault decisions must reject a foreign-thread call");
        IllegalStateException violation = assertInstanceOf(IllegalStateException.class, thrown);
        assertTrue(violation.getMessage().startsWith(
            "FaultInjector thread ownership violation: owner='"));
    }

    private static Throwable runOnOtherThread(Runnable action) throws InterruptedException {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                action.run();
            } catch (Throwable thrown) {
                failure.set(thrown);
            }
        }, "foreign-FaultInjector");
        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(10));
        assertFalse(thread.isAlive(), "foreign-thread ownership probe must finish");
        return failure.get();
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS))
                throw new AssertionError("timed out waiting for concurrent fault actors");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for concurrent fault actors", e);
        }
    }
}
