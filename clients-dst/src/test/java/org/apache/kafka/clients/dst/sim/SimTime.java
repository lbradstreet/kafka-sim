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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** The single conversion and saturation policy for simulated virtual-time deadlines. */
final class SimTime {

    private SimTime() {
    }

    /**
     * Convert a relative delay to the simulator's millisecond resolution and derive its
     * absolute deadline. Positive conversion/addition overflow saturates at
     * {@link Long#MAX_VALUE}; a non-positive converted delay is due at {@code nowMs}.
     */
    static long saturatedDeadlineMs(long nowMs, long delay, TimeUnit unit) {
        return saturatedDeadlineMs(
            nowMs, Objects.requireNonNull(unit, "unit").toMillis(delay));
    }

    /**
     * Add non-negative delay components to a virtual timestamp. Every non-positive component
     * contributes zero and every positive addition saturates, so the result never precedes
     * {@code nowMs} because of invalid input or signed overflow.
     */
    static long saturatedDeadlineMs(long nowMs, long... delayComponentsMs) {
        long deadlineMs = nowMs;
        for (long delayMs : delayComponentsMs) {
            if (delayMs > 0)
                deadlineMs = addPositiveSaturated(deadlineMs, delayMs);
        }
        return deadlineMs;
    }

    /** Compose two duration values under the same non-negative saturated-delay policy. */
    static long saturatedDelayMs(long firstMs, long secondMs) {
        return saturatedDeadlineMs(0L, firstMs, secondMs);
    }

    /** Return a non-negative, saturated relative delay for an absolute deadline. */
    static long delayUntilMs(long nowMs, long deadlineMs) {
        return deadlineMs <= nowMs ? 0L : subtractSaturated(deadlineMs, nowMs);
    }

    /** Return a signed, saturated difference for {@link java.util.concurrent.Delayed}. */
    static long differenceMs(long deadlineMs, long nowMs) {
        return subtractSaturated(deadlineMs, nowMs);
    }

    private static long addPositiveSaturated(long value, long positiveDelta) {
        return value > Long.MAX_VALUE - positiveDelta
            ? Long.MAX_VALUE : value + positiveDelta;
    }

    private static long subtractSaturated(long left, long right) {
        if (right > 0 && left < Long.MIN_VALUE + right)
            return Long.MIN_VALUE;
        if (right < 0 && left > Long.MAX_VALUE + right)
            return Long.MAX_VALUE;
        return left - right;
    }
}
