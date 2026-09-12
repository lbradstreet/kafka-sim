<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Deterministic simulation tests for the Java producer

For the branch goals, Rust architecture and published behavior comparisons, start
with the [repository overview](../README.md).

This test-only module runs the existing `KafkaProducer`, `Sender`, accumulator and
`NetworkClient` against a virtual clock, seeded network faults and in-memory brokers.
It has no production sources and is not published.

Run the producer scenarios:

```shell
./gradlew :clients-dst:test --tests org.apache.kafka.clients.dst.ClassicProducerDstTest
```

Run all producer and simulator checks:

```shell
./gradlew :clients-dst:check :clients-dst:spotlessCheck
```

The producer tests cover ordered delivery on a quiet network, exactly-once delivery
with idempotence through disconnects and delays, preservation of acknowledged records
without idempotence, broker isolation and reconnect, and identical replay of traces
and stored records. Replay also covers unkeyed sticky partitioning. Parameterized
scenarios use fixed seeds declared in `ClassicProducerDstTest`.

`ClassicProducerSimFactory` wires the existing producer without starting an I/O
thread. `SenderPump` exposes `Sender.runOnce()` to the virtual scheduler;
`ClassicSimSelector` adapts `Selectable` to the simulated network. The factory injects
seeded streams for node selection, retry and reconnect jitter, and sticky partitioning.
The test metadata adapter and clock advance the simulation during metadata waits.
These test adapters must stay in sync with the production constructor and wait semantics.

The shared broker, network, scheduler, timing, throttle and log fixtures live under
`org.apache.kafka.clients.dst.sim`. Netty is used only for test buffers and embedded
channels. The extraction includes the simulator's fault, scheduling, network and
producer-idempotence regression tests. The shared broker retains its protocol handlers,
including metadata, produce, fetch and topic creation; these are simulated broker
infrastructure, not additional client implementations.

## Scope and limitations

The classic producer scenarios use disconnect and delay faults. Dropping one frame on
an otherwise open TCP connection does not represent the classic client's ordered wire
semantics. Transactions are outside this suite because the simulator does not implement
those broker APIs. Blocking `Future.get()` before completion or `flush()` can deadlock
the single simulation thread; scenarios drive the scheduler before observing futures.
The factory currently uses the full-buffer accumulator strategy.

## Java and Rust comparison campaigns

The `classicScenarios` source set contains the JDK 25 foreign-function bridge,
shared workload driver, and selector used to compare this checkout's Java
producer with the Rust producer. Both adapters use the Rust simulated broker,
network, workload definitions, and virtual clock. The driver verifies complete
terminal populations and broker-log invariants, then repeats every execution
and requires an identical replay.

With JDK 25 on `JAVA_HOME` and `PATH`, run from `rust-runtime-kafka`:

```shell
python3 -B scripts/run-classic-scenarios.py --out target/classic-scenarios/test
python3 -B scripts/run-classic-matrix.py --out target/classic-scenarios/full-review
```

The scripts default to this repository's `:clients-dst:classicScenarios` task.
The ordinary producer simulator tests remain Java 17 sources. Compression
levels in scenario configurations are applied to the actual Java accumulator.
See [the comparison guide](../rust-runtime-kafka/kafka/kr-kafka-experiments/CLASSIC_COMPARISON.md)
for profiles, exact replay, bounded evidence archiving, and HTML exports.

The simulated cluster shares logical partition logs. It does not model replica storage
divergence, OS socket scheduling or crash durability.

## Extraction provenance

The classic producer harness and its required shared simulator were extracted from
`dst-classic-producer-sim` at `2519fbb6e49`, including the producer work introduced in
`3666721b852` and `167e9772f47`. Packages and build wiring were moved into this module,
and test adapters were updated for the current mainline APIs. No redesigned Java client
or production Netty transport is included.
