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

/** Fail-fast creation-thread confinement for mutable deterministic simulator components. */
final class SimThreadGuard {

    private final String component;
    private final Thread owner = Thread.currentThread();

    SimThreadGuard(String component) {
        this.component = Objects.requireNonNull(component, "component");
    }

    void check() {
        Thread caller = Thread.currentThread();
        if (caller != owner) {
            throw new IllegalStateException(component + " thread ownership violation: owner="
                + describe(owner) + ", caller=" + describe(caller));
        }
    }

    private static String describe(Thread thread) {
        return "'" + thread.getName() + "'#" + thread.getId();
    }
}
