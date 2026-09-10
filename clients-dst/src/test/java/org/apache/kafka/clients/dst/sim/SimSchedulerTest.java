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

import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimSchedulerTest {

    @Test
    public void testSameTimeLivelockHitsTaskBudgetWithoutDiscardingOwnership() {
        for (boolean currentTickOnly : List.of(false, true)) {
            MockTime time = new MockTime(0, 0L, 0L);
            SimScheduler scheduler = new SimScheduler(time);
            AtomicLong executions = new AtomicLong();
            Runnable[] loop = new Runnable[1];
            loop[0] = () -> {
                executions.incrementAndGet();
                scheduler.execute(loop[0]);
            };
            scheduler.execute(loop[0]);
            IllegalStateException failure = assertThrows(IllegalStateException.class, () -> {
                if (currentTickOnly)
                    scheduler.runCurrent(3);
                else
                    scheduler.runUntil(() -> false, 10L, 3);
            });
            assertTrue(failure.getMessage().contains("task budget 3"));
            assertEquals(3L, executions.get());
            assertEquals(0L, time.milliseconds());
            assertTrue(scheduler.hasPending(), "budget failure must retain the unexecuted task");
            scheduler.shutdownNow();
            assertFalse(scheduler.hasPending());
        }
    }

    @Test
    public void testTaskBudgetAllowsExactBoundaryAndRejectsInvalidLimits() {
        MockTime time = new MockTime(0, 0L, 0L);
        SimScheduler scheduler = new SimScheduler(time);
        AtomicBoolean ran = new AtomicBoolean();
        scheduler.execute(() -> {
            ran.set(true);
        });
        assertThrows(IllegalArgumentException.class, () -> scheduler.runCurrent(0));
        assertThrows(IllegalArgumentException.class, () -> scheduler.runUntil(ran::get, 0L, -1));
        scheduler.runUntil(ran::get, 0L, 1);
        assertTrue(ran.get());
        assertFalse(scheduler.hasPending());
    }

    @Test
    public void testEqualDeadlineOrderRemainsFifoAcrossLongSequenceBoundary() {
        MockTime time = new MockTime(0, 0L, 0L);
        SimScheduler scheduler = new SimScheduler(time,
            BigInteger.valueOf(Long.MAX_VALUE).subtract(BigInteger.ONE));
        List<Integer> events = new ArrayList<>();

        for (int ordinal = 0; ordinal < 4; ordinal++) {
            int event = ordinal;
            scheduler.schedule(() -> {
                events.add(event);
            }, 10, TimeUnit.MILLISECONDS);
        }

        scheduler.runUntil(() -> events.size() == 4, 10);
        assertEquals(List.of(0, 1, 2, 3), events);
        assertFalse(scheduler.hasPending());
    }

    @Test
    public void testClientAndEnvironmentTasksShareScenarioOrder() {
        MockTime time = new MockTime(0, 0L, 0L);
        SimScheduler scheduler = new SimScheduler(time);
        List<String> events = new ArrayList<>();

        scheduler.schedule(() -> {
            events.add("environment-1");
        }, 1, TimeUnit.MILLISECONDS);
        scheduler.clientWorkScheduler().schedule(() -> {
            events.add("client");
        }, 1, TimeUnit.MILLISECONDS);
        scheduler.schedule(() -> {
            events.add("environment-2");
        }, 1, TimeUnit.MILLISECONDS);

        scheduler.runUntil(() -> events.size() == 3, 1);
        assertEquals(List.of("environment-1", "client", "environment-2"), events);
    }

    @Test
    public void testSealedClientDrainSkipsEarlierEnvironmentWork() {
        MockTime time = new MockTime(0, 0L, 0L);
        SimScheduler scheduler = new SimScheduler(time);
        AtomicBoolean environmentRan = new AtomicBoolean();
        AtomicBoolean clientRan = new AtomicBoolean();
        scheduler.schedule(() -> environmentRan.set(true), 1, TimeUnit.DAYS);
        scheduler.clientWorkScheduler().schedule(
            () -> clientRan.set(true), 2, TimeUnit.DAYS);

        scheduler.shutdown();
        assertTrue(scheduler.runNextClientWork());

        assertTrue(clientRan.get());
        assertFalse(environmentRan.get());
        assertFalse(scheduler.hasPendingClientWork());
        assertTrue(scheduler.hasPending(), "the environment task remains for explicit discard");
        assertEquals(TimeUnit.DAYS.toMillis(2), time.milliseconds());
        scheduler.shutdownNow();
        assertFalse(scheduler.hasPending());
    }

    @Test
    public void testVirtualTimeCapFailureDoesNotDropNextTask() {
        MockTime time = new MockTime(0, 0L, 0L);
        SimScheduler scheduler = new SimScheduler(time);
        AtomicBoolean ran = new AtomicBoolean();
        ScheduledFuture<?> task = scheduler.schedule(
            () -> ran.set(true), 10, TimeUnit.MILLISECONDS);

        assertThrows(IllegalStateException.class,
            () -> scheduler.runUntil(ran::get, 5));

        assertEquals(0L, time.milliseconds());
        assertFalse(ran.get());
        assertFalse(task.isDone(), "the rejected advance must not forge task completion");
        assertTrue(scheduler.hasPending(), "the rejected advance must retain task ownership");

        scheduler.runUntil(ran::get, 10);
        assertTrue(ran.get());
        assertTrue(task.isDone());
        assertFalse(scheduler.hasPending());
    }

    @Test
    public void testPositiveDelayConversionAndAdditionSaturateAtMax() {
        long startedMs = Long.MAX_VALUE - 5;
        MockTime time = new MockTime(0, startedMs, 0);
        SimScheduler scheduler = new SimScheduler(time);
        List<String> events = new ArrayList<>();

        ScheduledFuture<?> converted = scheduler.schedule(
            () -> {
                events.add("converted@" + time.milliseconds());
            },
            Long.MAX_VALUE, TimeUnit.DAYS);
        ScheduledFuture<?> added = scheduler.schedule(
            () -> {
                events.add("added@" + time.milliseconds());
            }, 10, TimeUnit.MILLISECONDS);

        assertEquals(5, converted.getDelay(TimeUnit.MILLISECONDS));
        assertEquals(5, added.getDelay(TimeUnit.MILLISECONDS));
        scheduler.runCurrent();
        assertEquals(List.of(), events, "overflow must not make a future task runnable now");
        assertEquals(startedMs, time.milliseconds(), "the virtual clock must not travel backward");

        scheduler.runUntil(() -> events.size() == 2, Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, time.milliseconds());
        assertEquals(List.of(
            "converted@" + Long.MAX_VALUE,
            "added@" + Long.MAX_VALUE), events,
            "equal saturated deadlines must retain submission order");
        assertFalse(scheduler.hasPending());
    }

    @Test
    public void testNegativeOverflowingUnitConversionIsImmediateWithoutTimeTravel() {
        long startedMs = Long.MAX_VALUE - 5;
        MockTime time = new MockTime(0, startedMs, 0);
        SimScheduler scheduler = new SimScheduler(time);
        AtomicLong ranAtMs = new AtomicLong(Long.MIN_VALUE);

        ScheduledFuture<?> task = scheduler.schedule(
            () -> ranAtMs.set(time.milliseconds()), Long.MIN_VALUE, TimeUnit.DAYS);

        assertEquals(0, task.getDelay(TimeUnit.MILLISECONDS));
        scheduler.runCurrent();
        assertEquals(startedMs, ranAtMs.get());
        assertEquals(startedMs, time.milliseconds());
        assertFalse(scheduler.hasPending());
    }
}
