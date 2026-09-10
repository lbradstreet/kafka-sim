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
import org.apache.kafka.common.requests.RequestHeader;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.LinkedHashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * The simulated wire: delivers frames between client channels and {@link SimBroker}s as
 * scheduled virtual-time events, applying {@link FaultInjector} decisions at each hop.
 *
 * <p>Delivery is order-preserving per connection and direction (extra fault delay never
 * reorders frames on one connection) — the FIFO property real TCP provides and the
 * transport's correlation layer depends on (D8). A parked Fetch retains its response-order
 * slot, so later responses on that physical connection observe broker-purgatory head-of-line
 * blocking. Faults are therefore drops, delays and disconnects, never reorderings or
 * duplications. The thread which creates the network owns all endpoint and delivery state.
 */
public final class SimNetwork {

    public static final long BASE_LATENCY_MS = 2;

    /** One client request from wire admission until response delivery, drop, or close. */
    private static final class Exchange {
        final ApiKeys apiKey;
        ScheduledFuture<?> requestDelivery;
        ScheduledFuture<?> deadlineWakeup;
        ScheduledFuture<?> dataWakeup;
        ScheduledFuture<?> responseDelivery;
        long dataWakeupAtMs = Long.MAX_VALUE;
        long responseEligibleAtMs;
        SimBroker.Parked parked;
        SimBroker.Response response;
        boolean ready;
        boolean observerReleased;

        Exchange(ApiKeys apiKey) {
            this.apiKey = apiKey;
        }
    }

    /** One live client connection attached to a broker. */
    public static final class Endpoint {
        final long registrationId;
        final String connectionId;
        final SimChannel channel;
        final SimBroker broker;
        final ArrayDeque<Exchange> brokerResponses = new ArrayDeque<>();
        final Set<Exchange> activeExchanges = new LinkedHashSet<>();
        long nextClientToBrokerMs;
        long nextBrokerToClientMs;
        volatile boolean open = true;
        final AtomicBoolean released = new AtomicBoolean();

        Endpoint(long registrationId, String connectionId, SimChannel channel, SimBroker broker) {
            this.registrationId = registrationId;
            this.connectionId = connectionId;
            this.channel = channel;
            this.broker = broker;
        }
    }

    /** One generation-fenced broker isolation window. */
    private static final class Isolation {
        final long generation;
        final long untilMs;
        ScheduledFuture<?> liftTask;

        Isolation(long generation, long untilMs) {
            this.generation = generation;
            this.untilMs = untilMs;
        }
    }

    private final SimThreadGuard owner = new SimThreadGuard("SimNetwork");
    private final SimScheduler scheduler;
    private final FaultInjector faults;
    private final SimTrace trace;
    private final ProduceObserver observer;
    /** Broker-wide actions iterate by broker id, independent of registration order. */
    private final NavigableMap<Integer, SimBroker> brokers = new TreeMap<>();
    /** Live endpoints in stable physical-registration order. */
    private final NavigableMap<Long, Endpoint> endpoints = new TreeMap<>();
    /** Active outages in broker-id order. */
    private final NavigableMap<Integer, Isolation> isolations = new TreeMap<>();
    private final AtomicInteger activeEndpoints = new AtomicInteger();
    private final AtomicInteger registeredEndpoints = new AtomicInteger();
    private final AtomicInteger releasedEndpoints = new AtomicInteger();
    private final AtomicInteger isolationDroppedFrames = new AtomicInteger();
    private long nextEndpointRegistrationId;
    private long nextIsolationGeneration;

    public SimNetwork(SimScheduler scheduler, FaultInjector faults, SimTrace trace) {
        this(scheduler, faults, trace, ProduceObserver.NONE);
    }

    public SimNetwork(SimScheduler scheduler, FaultInjector faults, SimTrace trace,
                      ProduceObserver observer) {
        this.scheduler = scheduler;
        this.faults = faults;
        this.trace = trace;
        this.observer = observer;
    }

    /** Evict every broker's fetch sessions, as brokers do under session-cache pressure. */
    public void evictFetchSessions() {
        owner.check();
        brokers.values().forEach(SimBroker::evictFetchSessions);
    }

    /** Number of requests currently owned by broker Fetch purgatory. */
    public int activeParkedFetchCount() {
        owner.check();
        return brokers.values().stream().mapToInt(SimBroker::activeParkedFetchCount).sum();
    }

    public void addBroker(SimBroker broker) {
        owner.check();
        brokers.put(broker.id(), broker);
    }

    public SimBroker broker(int id) {
        owner.check();
        return brokers.get(id);
    }

    /** Number of registered client endpoints whose channel has not physically closed. */
    public int activeEndpointCount() {
        owner.check();
        return activeEndpoints.get();
    }

    /** Total endpoint registrations, for exactly-once ownership assertions. */
    public int endpointRegistrationCount() {
        owner.check();
        return registeredEndpoints.get();
    }

    /** Total endpoint releases, for exactly-once ownership assertions. */
    public int endpointReleaseCount() {
        owner.check();
        return releasedEndpoints.get();
    }

    /** Number of in-flight request/response frames discarded by broker isolation. */
    public int isolationDroppedFrameCount() {
        owner.check();
        return isolationDroppedFrames.get();
    }

    /**
     * Make one broker unreachable through {@code untilMs}. Re-isolating an already isolated
     * broker can only extend its window. Existing connections are killed in registration order.
     */
    public void isolateBroker(int brokerId, long untilMs) {
        owner.check();
        SimBroker broker = requireBroker(brokerId);
        long nowMs = scheduler.time().milliseconds();
        if (untilMs < nowMs) {
            throw new IllegalArgumentException("Isolation deadline " + untilMs
                + " precedes current time " + nowMs);
        }
        Isolation previous = isolations.get(brokerId);
        long effectiveUntilMs = previous == null
            ? untilMs : Math.max(previous.untilMs, untilMs);
        if (previous != null)
            cancel(previous.liftTask);
        Isolation isolation = new Isolation(nextIsolationGeneration++, effectiveUntilMs);
        isolations.put(brokerId, isolation);
        trace.add("network isolate broker-" + broker.id() + " until=" + effectiveUntilMs);
        for (Endpoint endpoint : java.util.List.copyOf(endpoints.values())) {
            if (endpoint.broker.id() == brokerId)
                isolateEndpoint(endpoint);
        }
        isolation.liftTask = scheduleAt(effectiveUntilMs,
            () -> liftIsolation(brokerId, isolation.generation));
    }

    /** Simulate a state-preserving broker restart with an empty Fetch-session cache. */
    public void restartBroker(int brokerId, long downForMs) {
        owner.check();
        if (downForMs < 0L)
            throw new IllegalArgumentException("downForMs must be non-negative");
        SimBroker broker = requireBroker(brokerId);
        isolateBroker(brokerId, SimTime.saturatedDeadlineMs(
            scheduler.time().milliseconds(), downForMs));
        broker.evictFetchSessions();
    }

    boolean failConnect(String connectionId, int brokerId) {
        owner.check();
        if (faults.failConnect(connectionId))
            return true;
        if (!isolations.containsKey(brokerId))
            return false;
        faults.recordForced("isolation-connect broker-" + brokerId + " conn=" + connectionId);
        return true;
    }

    public Endpoint register(String connectionId, SimChannel channel, SimBroker broker) {
        owner.check();
        Endpoint endpoint = new Endpoint(nextEndpointRegistrationId++, connectionId,
            channel, broker);
        endpoints.put(endpoint.registrationId, endpoint);
        activeEndpoints.incrementAndGet();
        registeredEndpoints.incrementAndGet();
        try {
            channel.closeFuture().addListener(f -> releaseEndpoint(endpoint));
            channel.outboundSink(frame -> clientFrame(endpoint, frame));
            return endpoint;
        } catch (RuntimeException | Error failure) {
            releaseEndpoint(endpoint);
            throw failure;
        }
    }

    private void releaseEndpoint(Endpoint endpoint) {
        owner.check();
        endpoint.open = false;
        endpoints.remove(endpoint.registrationId, endpoint);
        for (Exchange exchange : java.util.List.copyOf(endpoint.activeExchanges))
            cancelExchange(endpoint, exchange);
        endpoint.brokerResponses.clear();
        if (endpoint.released.compareAndSet(false, true)) {
            activeEndpoints.decrementAndGet();
            releasedEndpoints.incrementAndGet();
        }
    }

    /** Client → broker hop; called synchronously from the channel flush. */
    private void clientFrame(Endpoint endpoint, ByteBuf frame) {
        owner.check();
        byte[] bytes = new byte[frame.readableBytes()];
        frame.readBytes(bytes);
        frame.release();
        if (!endpoint.open)
            return;
        // The frame is now on the wire (in flight) until its response is delivered.
        observer.onFrameSent(endpoint.connectionId);
        ApiKeys apiKey = apiKey(bytes);
        Exchange exchange = new Exchange(apiKey);
        endpoint.activeExchanges.add(exchange);
        if (faults.dropRequest(endpoint.connectionId, apiKey)) {
            releaseExchange(endpoint, exchange); // never answered; clear observer ownership
            return; // the client's request timeout handles it
        }
        if (faults.disconnect(endpoint.connectionId)) {
            releaseExchange(endpoint, exchange);
            scheduler.execute(() -> closeEndpoint(endpoint));
            return;
        }
        long deliverAt = orderPreservingDeliveryTime(endpoint.nextClientToBrokerMs);
        endpoint.nextClientToBrokerMs = deliverAt;
        exchange.requestDelivery = scheduleAt(deliverAt,
            () -> deliverToBroker(endpoint, exchange, bytes));
    }

    private void deliverToBroker(Endpoint endpoint, Exchange exchange, byte[] requestFrame) {
        owner.check();
        exchange.requestDelivery = null;
        if (!endpoint.open) {
            releaseExchange(endpoint, exchange);
            return;
        }
        endpoint.brokerResponses.addLast(exchange);
        try {
            SimBroker.HandleResult result = endpoint.broker.handle(
                endpoint.registrationId, requestFrame);
            if (result instanceof SimBroker.Response response) {
                responseReady(endpoint, exchange, response);
            } else if (result instanceof SimBroker.Parked parked) {
                parkFetch(endpoint, exchange, parked);
            } else {
                throw new IllegalStateException("Unknown broker handle result " + result);
            }
        } catch (RuntimeException | Error failure) {
            cancelExchange(endpoint, exchange);
            throw failure;
        }
    }

    private void parkFetch(Endpoint endpoint, Exchange exchange, SimBroker.Parked parked) {
        owner.check();
        exchange.parked = parked;
        exchange.deadlineWakeup = scheduleAt(parked.deadlineMs(),
            () -> reevaluateParked(endpoint, exchange, true));
        parked.onAppendWakeup(visibleAtMs -> scheduleDataWakeup(
            endpoint, exchange, visibleAtMs));
    }

    private void scheduleDataWakeup(Endpoint endpoint, Exchange exchange, long visibleAtMs) {
        owner.check();
        if (!endpoint.open || exchange.parked == null)
            return;
        long wakeupAtMs = Math.max(scheduler.time().milliseconds(), visibleAtMs);
        if (exchange.dataWakeup != null && exchange.dataWakeupAtMs <= wakeupAtMs)
            return;
        cancel(exchange.dataWakeup);
        exchange.dataWakeupAtMs = wakeupAtMs;
        exchange.dataWakeup = scheduleAt(wakeupAtMs, () -> {
            if (exchange.dataWakeupAtMs != wakeupAtMs)
                return;
            exchange.dataWakeup = null;
            exchange.dataWakeupAtMs = Long.MAX_VALUE;
            reevaluateParked(endpoint, exchange, false);
        });
    }

    private void reevaluateParked(Endpoint endpoint, Exchange exchange,
                                  boolean deadlineReached) {
        owner.check();
        if (deadlineReached)
            exchange.deadlineWakeup = null;
        if (!endpoint.open || exchange.parked == null)
            return;
        SimBroker.Response response = exchange.parked.reevaluate(deadlineReached);
        if (response == null)
            return;
        exchange.parked = null;
        cancel(exchange.deadlineWakeup);
        exchange.deadlineWakeup = null;
        cancel(exchange.dataWakeup);
        exchange.dataWakeup = null;
        exchange.dataWakeupAtMs = Long.MAX_VALUE;
        responseReady(endpoint, exchange, response);
    }

    private void responseReady(Endpoint endpoint, Exchange exchange,
                               SimBroker.Response response) {
        owner.check();
        if (!endpoint.open || !endpoint.activeExchanges.contains(exchange))
            return;
        exchange.response = response;
        exchange.ready = true;
        if (faults.dropResponse(endpoint.connectionId, exchange.apiKey)) {
            endpoint.brokerResponses.remove(exchange);
            releaseExchange(endpoint, exchange);
            drainBrokerResponses(endpoint);
            return;
        }
        exchange.responseEligibleAtMs = SimTime.saturatedDeadlineMs(
            scheduler.time().milliseconds(), BASE_LATENCY_MS, faults.extraDelayMs(),
            response.processingDelayMs());
        drainBrokerResponses(endpoint);
    }

    /** Release ready responses strictly in request-arrival order, modeling connection HOL. */
    private void drainBrokerResponses(Endpoint endpoint) {
        owner.check();
        while (endpoint.open) {
            Exchange exchange = endpoint.brokerResponses.peekFirst();
            if (exchange == null || !exchange.ready)
                return;
            endpoint.brokerResponses.removeFirst();
            long deliverAt = Math.max(exchange.responseEligibleAtMs,
                endpoint.nextBrokerToClientMs);
            deliverAt = Math.max(deliverAt, scheduler.time().milliseconds());
            endpoint.nextBrokerToClientMs = deliverAt;
            exchange.responseDelivery = scheduleAt(deliverAt,
                () -> deliverToClient(endpoint, exchange));
        }
    }

    private void deliverToClient(Endpoint endpoint, Exchange exchange) {
        owner.check();
        exchange.responseDelivery = null;
        releaseExchange(endpoint, exchange);
        if (!endpoint.open || !endpoint.channel.isActive())
            return;
        endpoint.channel.writeInbound(Unpooled.wrappedBuffer(exchange.response.frame()));
        endpoint.channel.runPendingTasks();
    }

    private void cancelExchange(Endpoint endpoint, Exchange exchange) {
        cancel(exchange.requestDelivery);
        cancel(exchange.deadlineWakeup);
        cancel(exchange.dataWakeup);
        cancel(exchange.responseDelivery);
        if (exchange.parked != null) {
            exchange.parked.cancel();
            exchange.parked = null;
        }
        endpoint.brokerResponses.remove(exchange);
        releaseExchange(endpoint, exchange);
    }

    private void releaseExchange(Endpoint endpoint, Exchange exchange) {
        endpoint.activeExchanges.remove(exchange);
        if (!exchange.observerReleased) {
            exchange.observerReleased = true;
            observer.onResponseDelivered(endpoint.connectionId);
        }
    }

    private static void cancel(ScheduledFuture<?> task) {
        if (task != null)
            task.cancel(false);
    }

    private static ApiKeys apiKey(byte[] requestFrame) {
        ByteBuffer buffer = ByteBuffer.wrap(requestFrame);
        int declaredSize = buffer.getInt();
        if (declaredSize != buffer.remaining())
            throw new IllegalStateException("Frame length prefix " + declaredSize
                + " does not match payload size " + buffer.remaining());
        return RequestHeader.parse(buffer).apiKey();
    }

    private void closeEndpoint(Endpoint endpoint) {
        owner.check();
        if (!endpoint.open)
            return;
        endpoint.open = false;
        trace.add("network close conn=" + endpoint.connectionId);
        endpoint.channel.close();
        endpoint.channel.runPendingTasks();
    }

    private SimBroker requireBroker(int brokerId) {
        SimBroker broker = brokers.get(brokerId);
        if (broker == null)
            throw new IllegalArgumentException("Unknown simulated broker " + brokerId);
        return broker;
    }

    private void isolateEndpoint(Endpoint endpoint) {
        for (Exchange exchange : java.util.List.copyOf(endpoint.activeExchanges)) {
            isolationDroppedFrames.incrementAndGet();
            faults.recordForced("isolation-drop broker-" + endpoint.broker.id()
                + " api=" + exchange.apiKey + " conn=" + endpoint.connectionId);
        }
        trace.add("network isolate-close broker-" + endpoint.broker.id()
            + " conn=" + endpoint.connectionId);
        closeEndpoint(endpoint);
    }

    private void liftIsolation(int brokerId, long generation) {
        owner.check();
        Isolation isolation = isolations.get(brokerId);
        if (isolation == null || isolation.generation != generation)
            return;
        isolations.remove(brokerId);
        isolation.liftTask = null;
        trace.add("network restore broker-" + brokerId);
    }

    private long orderPreservingDeliveryTime(long previousDeliveryMs) {
        owner.check();
        long candidate = SimTime.saturatedDeadlineMs(scheduler.time().milliseconds(),
            BASE_LATENCY_MS, faults.extraDelayMs());
        return Math.max(candidate, previousDeliveryMs);
    }

    private ScheduledFuture<?> scheduleAt(long timeMs, Runnable task) {
        owner.check();
        return scheduler.schedule(task,
            SimTime.delayUntilMs(scheduler.time().milliseconds(), timeMs),
            TimeUnit.MILLISECONDS);
    }
}
