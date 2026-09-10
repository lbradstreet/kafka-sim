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

import java.util.HashMap;
import java.util.Map;

/**
 * Structured observation of produce behavior in a simulation, so scenarios can assert on
 * batching and pipelining outcomes directly rather than scraping the {@link SimTrace} text.
 *
 * <p>Populated and queried on its creation thread — no synchronization needed. Records what
 * the broker sees (per-request record counts and byte sizes, per-broker request distribution)
 * plus the on-the-wire in-flight high-water mark tracked by {@link SimNetwork}. The shared
 * immutable {@link #NONE} observer remains safe to use from any thread.
 */
public final class ProduceObserver {

    /** A shared no-op instance for scenarios that don't observe. */
    public static final ProduceObserver NONE = new ProduceObserver(false);

    private final SimThreadGuard owner;

    private int produceRequests;
    private long totalRecords;
    private int maxRecordsPerRequest;
    private final Map<Integer, Integer> requestsPerBroker = new HashMap<>();
    private final Map<Integer, Long> recordsPerBroker = new HashMap<>();

    private int throttledResponses;
    private int maxThrottleMs;

    private final Map<String, Integer> inFlightPerConnection = new HashMap<>();
    private int maxInFlight;

    public ProduceObserver() {
        this(true);
    }

    private ProduceObserver(boolean enforceOwnership) {
        owner = enforceOwnership ? new SimThreadGuard("ProduceObserver") : null;
    }

    // ----------------------------------------------------------------- broker-side hooks

    void onProduceRequest(int brokerId, int records, int bytes, int throttleMs) {
        if (this == NONE)
            return;
        owner.check();
        produceRequests++;
        totalRecords += records;
        maxRecordsPerRequest = Math.max(maxRecordsPerRequest, records);
        requestsPerBroker.merge(brokerId, 1, Integer::sum);
        recordsPerBroker.merge(brokerId, (long) records, Long::sum);
        if (throttleMs > 0) {
            throttledResponses++;
            maxThrottleMs = Math.max(maxThrottleMs, throttleMs);
        }
    }

    // ----------------------------------------------------------------- wire in-flight hooks

    /**
     * A frame left the client and is now in flight (no response yet). In fault-free
     * scenarios sends and responses balance exactly, so the running count is the true
     * on-the-wire depth; the maximum reflects how deep the pipeline actually filled.
     */
    void onFrameSent(String connectionId) {
        if (this == NONE)
            return;
        owner.check();
        int inFlight = inFlightPerConnection.merge(connectionId, 1, Integer::sum);
        maxInFlight = Math.max(maxInFlight, inFlight);
    }

    void onResponseDelivered(String connectionId) {
        if (this == NONE)
            return;
        owner.check();
        inFlightPerConnection.computeIfPresent(connectionId, (id, n) -> n > 0 ? n - 1 : 0);
    }

    // ----------------------------------------------------------------- queries

    public int produceRequests() {
        checkOwner();
        return produceRequests;
    }

    public long totalRecords() {
        checkOwner();
        return totalRecords;
    }

    public int maxRecordsPerRequest() {
        checkOwner();
        return maxRecordsPerRequest;
    }

    public int requestsToBroker(int brokerId) {
        checkOwner();
        return requestsPerBroker.getOrDefault(brokerId, 0);
    }

    public long recordsToBroker(int brokerId) {
        checkOwner();
        return recordsPerBroker.getOrDefault(brokerId, 0L);
    }

    /** The deepest the on-the-wire pipeline filled on any single connection. */
    public int maxInFlight() {
        checkOwner();
        return maxInFlight;
    }

    /** How many produce responses reported a non-zero {@code throttle_time_ms} (KIP-219). */
    public int throttledResponses() {
        checkOwner();
        return throttledResponses;
    }

    public int maxThrottleMs() {
        checkOwner();
        return maxThrottleMs;
    }

    private void checkOwner() {
        if (owner != null)
            owner.check();
    }
}
