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

import java.util.Random;

/**
 * How long a {@link SimBroker} takes to process a request before its response is put on the
 * wire — the seam for simulating different produce dynamics (a busy broker fills the client's
 * in-flight window, which drives batching and pipelining behavior). The delay is added on top
 * of network latency and, because responses stay FIFO per connection, it models head-of-line
 * processing time realistically.
 *
 * <p>Deterministic: any randomness comes from a scenario-seeded {@link Random} so a run
 * reproduces exactly.
 */
@FunctionalInterface
public interface BrokerTimingModel {

    /** No processing delay — the default; existing scenarios are unaffected. */
    BrokerTimingModel INSTANT = (apiKey, produceCount) -> 0L;

    /**
     * @param apiKey       the request being processed
     * @param produceCount how many produce requests this broker has handled so far (1-based
     *                     for the current one), enabling "slow after N" profiles
     * @return processing delay in virtual milliseconds
     */
    long processingDelayMs(ApiKeys apiKey, int produceCount);

    /**
     * How long after a produce request arrives its records stay invisible to fetches and
     * latest-offset queries — the append/replication half of produce latency, advancing the
     * partition's visible end (high watermark) only once it elapses. Independent of
     * {@link #processingDelayMs}, which delays only the producer's ack: a realistic broker
     * has ack latency of at least the visibility latency, but the two knobs are deliberately
     * free so acks-ahead-of-visibility and visibility-ahead-of-acks are both expressible.
     *
     * @param produceCount as for {@link #processingDelayMs}
     * @return append-visibility delay in virtual milliseconds; default 0 (visible on arrival)
     */
    default long appendVisibilityDelayMs(int produceCount) {
        return 0L;
    }

    /** Constant per-produce ack latency; append visibility and metadata/ApiVersions stay instant. */
    static BrokerTimingModel constantProduceLatency(long ms) {
        return (apiKey, n) -> apiKey == ApiKeys.PRODUCE ? ms : 0L;
    }

    /** Constant processing latency for one API — e.g. slow metadata responses. */
    static BrokerTimingModel constantLatency(ApiKeys api, long ms) {
        return (apiKey, n) -> apiKey == api ? ms : 0L;
    }

    /** Compose two models by summing their processing and append-visibility delays. */
    default BrokerTimingModel plus(BrokerTimingModel other) {
        BrokerTimingModel self = this;
        return new BrokerTimingModel() {
            @Override
            public long processingDelayMs(ApiKeys apiKey, int produceCount) {
                return SimTime.saturatedDelayMs(
                    self.processingDelayMs(apiKey, produceCount),
                    other.processingDelayMs(apiKey, produceCount));
            }

            @Override
            public long appendVisibilityDelayMs(int produceCount) {
                return SimTime.saturatedDelayMs(
                    self.appendVisibilityDelayMs(produceCount),
                    other.appendVisibilityDelayMs(produceCount));
            }
        };
    }

    /** Constant produce latency split into its two halves: append visibility and ack delay. */
    static BrokerTimingModel produceLatency(long appendVisibilityMs, long ackProcessingMs) {
        return new BrokerTimingModel() {
            @Override
            public long processingDelayMs(ApiKeys apiKey, int produceCount) {
                return apiKey == ApiKeys.PRODUCE ? ackProcessingMs : 0L;
            }

            @Override
            public long appendVisibilityDelayMs(int produceCount) {
                return appendVisibilityMs;
            }
        };
    }

    /** Fast until the broker has handled {@code threshold} produce requests, then slow. */
    static BrokerTimingModel slowAfter(int threshold, long normalMs, long slowMs) {
        return (apiKey, n) -> apiKey == ApiKeys.PRODUCE ? (n > threshold ? slowMs : normalMs) : 0L;
    }

    /** Uniform seeded jitter in {@code [minMs, maxMs]} per produce request. */
    static BrokerTimingModel seededJitter(long seed, long minMs, long maxMs) {
        Random random = new Random(seed);
        return (apiKey, n) -> apiKey == ApiKeys.PRODUCE
            ? minMs + (long) (random.nextDouble() * (maxMs - minMs))
            : 0L;
    }
}
