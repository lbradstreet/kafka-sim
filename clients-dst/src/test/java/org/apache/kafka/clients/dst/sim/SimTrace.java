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

import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.List;

/**
 * Ordered, virtual-time-stamped event log of a simulation run.
 *
 * <p>Two runs of the same scenario with the same seed must produce identical traces — the
 * determinism check. The creation thread owns both appends and reads,
 * preventing partial or nondeterministically ordered observations.
 */
public final class SimTrace {

    private final SimThreadGuard owner = new SimThreadGuard("SimTrace");
    private final Time time;
    private final List<String> events = new ArrayList<>();

    public SimTrace(Time time) {
        this.time = time;
    }

    /** The virtual clock of the run — the sim brokers' time source for visibility stamps. */
    public Time time() {
        owner.check();
        return time;
    }

    public void add(String event) {
        owner.check();
        events.add("t=" + time.milliseconds() + " " + event);
    }

    public List<String> events() {
        owner.check();
        return List.copyOf(events);
    }
}
