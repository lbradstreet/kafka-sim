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

import org.apache.kafka.common.protocol.ApiKeys;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Buggify-style fault injection, driven by a single seeded {@link Random} so every fault
 * sequence reproduces from the scenario seed. Faults are injected only at the
 * simulated-network layer — never above it — so the transport's FIFO correlation contract
 * is genuinely exercised, not silently violated.
 */
public final class FaultInjector {

    private final SimThreadGuard owner = new SimThreadGuard("FaultInjector");

    /**
     * A deterministic frame-delivery cut point. For a successful produce,
     * {@link FaultPhase#AFTER_BROKER_HANDLE_BEFORE_RESPONSE} is after the broker append and
     * before any response is scheduled back to the client.
     */
    public enum FaultPhase {
        BEFORE_BROKER_HANDLE,
        AFTER_BROKER_HANDLE_BEFORE_RESPONSE
    }

    /**
     * @param dropRequestProbability  chance a client→broker frame vanishes (client times out)
     * @param dropResponseProbability chance a broker→client frame vanishes
     * @param disconnectProbability   chance a frame event instead kills the connection
     * @param maxExtraDelayMs         uniform extra delivery latency, order-preserving per connection
     */
    public record FaultProfile(double dropRequestProbability,
                               double dropResponseProbability,
                               double disconnectProbability,
                               long maxExtraDelayMs) {

        public static final FaultProfile NONE = new FaultProfile(0, 0, 0, 0);

        public FaultProfile {
            if (maxExtraDelayMs < 0)
                throw new IllegalArgumentException("maxExtraDelayMs must be non-negative");
        }

        public boolean quiet() {
            return dropRequestProbability == 0 && dropResponseProbability == 0
                && disconnectProbability == 0 && maxExtraDelayMs == 0;
        }
    }

    private final Random random;
    private final FaultProfile profile;
    private final SimTrace trace;
    private final AtomicReference<Map<String, Integer>> scriptedConnectFailures =
        new AtomicReference<>(Map.of());
    /**
     * Immutable budgets may be armed from any thread. Fault decisions and trace mutation remain
     * confined to the sim scheduler; the atomic snapshot makes publication linearizable with it.
     */
    private final AtomicReference<Map<ScriptedFrameFault, Integer>> scriptedFrameDrops =
        new AtomicReference<>(Map.of());
    /** Scheduler-written but safely observable by scenario threads. */
    private final AtomicInteger faultsInjected = new AtomicInteger();

    private record ScriptedFrameFault(FaultPhase phase, ApiKeys apiKey, String connectionId) { }

    public FaultInjector(long seed, FaultProfile profile, SimTrace trace) {
        this.random = new Random(seed);
        this.profile = profile;
        this.trace = trace;
    }

    public boolean dropRequest(String connectionId) {
        owner.check();
        return fire(profile.dropRequestProbability(), "drop-request conn=" + connectionId);
    }

    boolean dropRequest(String connectionId, ApiKeys apiKey) {
        owner.check();
        return fireScripted(FaultPhase.BEFORE_BROKER_HANDLE, apiKey, connectionId)
            || dropRequest(connectionId);
    }

    public boolean dropResponse(String connectionId) {
        owner.check();
        return fire(profile.dropResponseProbability(), "drop-response conn=" + connectionId);
    }

    boolean dropResponse(String connectionId, ApiKeys apiKey) {
        owner.check();
        return fireScripted(FaultPhase.AFTER_BROKER_HANDLE_BEFORE_RESPONSE, apiKey, connectionId)
            || dropResponse(connectionId);
    }

    public boolean disconnect(String connectionId) {
        owner.check();
        return fire(profile.disconnectProbability(), "disconnect conn=" + connectionId);
    }

    /** Fail the next simulated connection attempt with this transport-scoped id. */
    public void failNextConnect(String connectionId) {
        while (true) {
            Map<String, Integer> current = scriptedConnectFailures.get();
            Map<String, Integer> updated = new HashMap<>(current);
            updated.merge(connectionId, 1, Integer::sum);
            if (scriptedConnectFailures.compareAndSet(current, Map.copyOf(updated)))
                return;
        }
    }

    /**
     * Drop one frame at an exact protocol phase. Repeated calls add to the fault budget.
     * Matching includes the connection and API key, so unrelated negotiation or metadata
     * traffic cannot consume a produce fault.
     */
    public void dropNext(FaultPhase phase, ApiKeys apiKey, String connectionId) {
        ScriptedFrameFault fault = new ScriptedFrameFault(
            Objects.requireNonNull(phase, "phase"),
            Objects.requireNonNull(apiKey, "apiKey"),
            Objects.requireNonNull(connectionId, "connectionId"));
        while (true) {
            Map<ScriptedFrameFault, Integer> current = scriptedFrameDrops.get();
            Map<ScriptedFrameFault, Integer> updated = new HashMap<>(current);
            updated.merge(fault, 1, Integer::sum);
            if (scriptedFrameDrops.compareAndSet(current, Map.copyOf(updated)))
                return;
        }
    }

    boolean failConnect(String connectionId) {
        owner.check();
        while (true) {
            Map<String, Integer> current = scriptedConnectFailures.get();
            Integer remaining = current.get(connectionId);
            if (remaining == null || remaining == 0)
                return false;
            Map<String, Integer> updated = new HashMap<>(current);
            if (remaining == 1)
                updated.remove(connectionId);
            else
                updated.put(connectionId, remaining - 1);
            if (scriptedConnectFailures.compareAndSet(current, Map.copyOf(updated))) {
                faultsInjected.incrementAndGet();
                trace.add("fault connect conn=" + connectionId);
                return true;
            }
        }
    }

    public long extraDelayMs() {
        owner.check();
        if (profile.maxExtraDelayMs() == 0)
            return 0;
        if (profile.maxExtraDelayMs() == Long.MAX_VALUE)
            return random.nextLong() & Long.MAX_VALUE;
        return random.nextLong(profile.maxExtraDelayMs() + 1);
    }

    public int faultsInjected() {
        return faultsInjected.get();
    }

    /** Record a deterministic fault imposed by a stateful network disruption. */
    void recordForced(String event) {
        owner.check();
        faultsInjected.incrementAndGet();
        trace.add("fault " + event);
    }

    private boolean fireScripted(FaultPhase phase, ApiKeys apiKey, String connectionId) {
        owner.check();
        ScriptedFrameFault fault = new ScriptedFrameFault(phase, apiKey, connectionId);
        while (true) {
            Map<ScriptedFrameFault, Integer> current = scriptedFrameDrops.get();
            Integer remaining = current.get(fault);
            if (remaining == null || remaining == 0)
                return false;
            Map<ScriptedFrameFault, Integer> updated = new HashMap<>(current);
            if (remaining == 1)
                updated.remove(fault);
            else
                updated.put(fault, remaining - 1);
            if (scriptedFrameDrops.compareAndSet(current, Map.copyOf(updated))) {
                faultsInjected.incrementAndGet();
                trace.add("fault scripted-drop phase=" + phase + " api=" + apiKey
                    + " conn=" + connectionId);
                return true;
            }
        }
    }

    private boolean fire(double probability, String event) {
        owner.check();
        if (probability > 0 && random.nextDouble() < probability) {
            faultsInjected.incrementAndGet();
            trace.add("fault " + event);
            return true;
        }
        return false;
    }
}
