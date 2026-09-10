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

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Deterministic virtual-time scheduler: the single task queue of the DST harness.
 *
 * <p>Tasks execute in {@code (dueTime, submissionOrder)} order on the caller's thread;
 * {@link #runUntil} advances the {@link MockTime} clock to each task's due time. Everything
 * in a simulation — drain ticks, request timeouts, network deliveries, retries — flows
 * through this queue, so a run is a pure function of (scenario, seed).
 *
 * <p>Single-threaded by design: all simulation work happens inline on the thread which creates
 * the scheduler. Calls from another real thread fail before observing or mutating scheduler
 * state.
 */
public final class SimScheduler extends AbstractExecutorService implements ScheduledExecutorService {

    private static final int DEFAULT_TASK_BUDGET = 1_000_000;
    private final SimThreadGuard owner = new SimThreadGuard("SimScheduler");
    private final MockTime time;
    private final PriorityQueue<SimTask> tasks = new PriorityQueue<>();
    private final ScheduledExecutorService clientWorkScheduler = new ClientWorkScheduler();
    private BigInteger sequence;
    private boolean shutdown = false;

    public SimScheduler(MockTime time) {
        this(time, BigInteger.ZERO);
    }

    /** Seeded only for deterministic boundary tests of equal-deadline submission order. */
    SimScheduler(MockTime time, BigInteger initialSequence) {
        this.time = time;
        this.sequence = Objects.requireNonNull(initialSequence, "initialSequence");
        if (initialSequence.signum() < 0)
            throw new IllegalArgumentException("Initial sequence must not be negative");
    }

    public MockTime time() {
        owner.check();
        return time;
    }

    /**
     * Scheduler view which tags tasks as client-owned rather than simulated-environment work.
     * Both domains retain the same virtual clock and total execution order during a scenario;
     * the tag only lets teardown retire client ownership without executing delayed wire faults or
     * scenario assertions.
     */
    public ScheduledExecutorService clientWorkScheduler() {
        owner.check();
        return clientWorkScheduler;
    }

    /**
     * Run tasks (advancing virtual time) until the condition holds, the queue empties, or
     * the virtual-time cap is exceeded (which throws — a simulated hang).
     */
    public void runUntil(BooleanSupplier condition, long maxVirtualTimeMs) {
        runUntil(condition, maxVirtualTimeMs, DEFAULT_TASK_BUDGET);
    }

    /** Bound queue work as well as time, so zero-delay livelocks fail reproducibly. */
    public void runUntil(BooleanSupplier condition, long maxVirtualTimeMs, int maxTasks) {
        owner.check();
        requireTaskBudget(maxTasks);
        int processed = 0;
        while (!condition.getAsBoolean()) {
            SimTask task = tasks.peek();
            if (task == null)
                throw new IllegalStateException(
                    "Simulation is idle at t=" + time.milliseconds() + " but the condition never held");
            checkTaskBudget(processed++, maxTasks);
            if (task.cancelled) {
                tasks.poll();
                continue;
            }
            long nowMs = time.milliseconds();
            if (task.dueMs > nowMs) {
                if (task.dueMs > maxVirtualTimeMs)
                    throw new IllegalStateException("Virtual-time cap " + maxVirtualTimeMs
                        + "ms exceeded at t=" + nowMs + " — simulated hang");
                time.sleep(SimTime.delayUntilMs(nowMs, task.dueMs));
            }
            tasks.poll();
            task.run();
        }
    }

    public boolean hasPending() {
        owner.check();
        return tasks.stream().anyMatch(t -> !t.cancelled);
    }

    /** True when an admitted client-work task has not run or been cancelled. */
    public boolean hasPendingClientWork() {
        owner.check();
        return tasks.stream().anyMatch(t -> t.clientWork && !t.cancelled);
    }

    /**
     * Run the earliest admitted client-work task, advancing virtual time if necessary while
     * deliberately leaving environment tasks untouched. Intended only for sealed teardown.
     */
    public boolean runNextClientWork() {
        owner.check();
        if (!shutdown)
            throw new IllegalStateException("Client-work teardown requires a sealed scheduler");
        SimTask next = null;
        for (SimTask task : tasks) {
            if (!task.clientWork || task.cancelled)
                continue;
            if (next == null || task.compareTo(next) < 0)
                next = task;
        }
        if (next == null)
            return false;
        tasks.remove(next);
        long nowMs = time.milliseconds();
        if (next.dueMs > nowMs)
            time.sleep(SimTime.delayUntilMs(nowMs, next.dueMs));
        next.run();
        return true;
    }

    /**
     * Run every task due at the current virtual time, including same-time tasks transitively
     * submitted by those tasks, without advancing the clock to a future timer.
     */
    public void runCurrent() {
        runCurrent(DEFAULT_TASK_BUDGET);
    }

    /** Run the current tick with an explicit queue-work budget. */
    public void runCurrent(int maxTasks) {
        owner.check();
        requireTaskBudget(maxTasks);
        int processed = 0;
        long nowMs = time.milliseconds();
        while (true) {
            SimTask task = tasks.peek();
            if (task == null || task.dueMs > nowMs)
                return;
            checkTaskBudget(processed++, maxTasks);
            tasks.poll();
            if (!task.cancelled)
                task.run();
        }
    }

    private static void requireTaskBudget(int maxTasks) {
        if (maxTasks < 1)
            throw new IllegalArgumentException("Simulation task budget must be positive: " + maxTasks);
    }

    private void checkTaskBudget(int processed, int maxTasks) {
        if (processed >= maxTasks) {
            throw new IllegalStateException("Simulation task budget " + maxTasks
                + " exceeded at t=" + time.milliseconds() + " with " + tasks.size()
                + " queued tasks — possible same-time livelock");
        }
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return schedule(command, delay, unit, false);
    }

    private ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit,
                                        boolean clientWork) {
        owner.check();
        if (shutdown)
            throw new java.util.concurrent.RejectedExecutionException("SimScheduler is shut down");
        long dueMs = SimTime.saturatedDeadlineMs(time.milliseconds(), delay, unit);
        SimTask task = new SimTask(dueMs, sequence, command, clientWork);
        sequence = sequence.add(BigInteger.ONE);
        tasks.add(task);
        return task;
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        owner.check();
        throw new UnsupportedOperationException("Callable scheduling is not used by the v2 stack");
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        owner.check();
        throw new UnsupportedOperationException("Fixed-rate scheduling is not used by the v2 stack");
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        owner.check();
        throw new UnsupportedOperationException("Fixed-delay scheduling is not used by the v2 stack");
    }

    @Override
    public void execute(Runnable command) {
        owner.check();
        schedule(command, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        owner.check();
        shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
        owner.check();
        shutdown = true;
        tasks.clear();
        return List.of();
    }

    @Override
    public boolean isShutdown() {
        owner.check();
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        owner.check();
        return shutdown && tasks.isEmpty();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        owner.check();
        return true;
    }

    private final class SimTask implements ScheduledFuture<Object>, Runnable, Comparable<Delayed> {
        final long dueMs;
        final BigInteger seq;
        final Runnable command;
        final boolean clientWork;
        boolean cancelled = false;
        boolean done = false;

        SimTask(long dueMs, BigInteger seq, Runnable command, boolean clientWork) {
            this.dueMs = dueMs;
            this.seq = seq;
            this.command = command;
            this.clientWork = clientWork;
        }

        @Override
        public void run() {
            owner.check();
            done = true;
            command.run();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            owner.check();
            return unit.convert(
                SimTime.differenceMs(dueMs, time.milliseconds()), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            owner.check();
            if (other instanceof SimTask task) {
                int byTime = Long.compare(dueMs, task.dueMs);
                return byTime != 0 ? byTime : seq.compareTo(task.seq);
            }
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            owner.check();
            if (done || cancelled)
                return false;
            cancelled = true;
            // Mirror ScheduledThreadPoolExecutor's remove-on-cancel policy. A lazy tombstone
            // would retain the command and everything it captures until virtual time reached
            // its original due date, making long-deadline simulations grow without bound.
            tasks.remove(this);
            return true;
        }

        @Override
        public boolean isCancelled() {
            owner.check();
            return cancelled;
        }

        @Override
        public boolean isDone() {
            owner.check();
            return done || cancelled;
        }

        @Override
        public Object get() {
            owner.check();
            throw new UnsupportedOperationException("SimTask results are not observable");
        }

        @Override
        public Object get(long timeout, TimeUnit unit) {
            owner.check();
            throw new UnsupportedOperationException("SimTask results are not observable");
        }
    }

    private final class ClientWorkScheduler extends AbstractExecutorService
            implements ScheduledExecutorService {

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return SimScheduler.this.schedule(command, delay, unit, true);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            return SimScheduler.this.schedule(callable, delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
            Runnable command,
            long initialDelay,
            long period,
            TimeUnit unit
        ) {
            return SimScheduler.this.scheduleAtFixedRate(command, initialDelay, period, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command,
            long initialDelay,
            long delay,
            TimeUnit unit
        ) {
            return SimScheduler.this.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }

        @Override
        public void execute(Runnable command) {
            schedule(command, 0L, TimeUnit.MILLISECONDS);
        }

        @Override
        public void shutdown() {
            SimScheduler.this.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return SimScheduler.this.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return SimScheduler.this.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return SimScheduler.this.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return SimScheduler.this.awaitTermination(timeout, unit);
        }
    }
}
