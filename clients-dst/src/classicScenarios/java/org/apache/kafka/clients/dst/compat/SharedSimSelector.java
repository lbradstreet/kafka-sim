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
package org.apache.kafka.clients.dst.compat;

import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.requests.ByteBufferChannel;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Real classic NetworkClient frames over the Rust simulator's bounded byte streams. */
final class SharedSimSelector implements Selectable {
    private final SharedScenarioRunner run;
    private final Map<String, Long> ids = new LinkedHashMap<>();
    private final Map<Long, String> names = new HashMap<>();
    private final Map<String, ArrayDeque<NetworkSend>> sends = new HashMap<>();
    private final Set<String> ready = new HashSet<>();
    private final Set<String> muted = new HashSet<>();
    private final List<NetworkSend> completedSends = new ArrayList<>();
    private final List<NetworkReceive> receives = new ArrayList<>();
    private final List<String> connected = new ArrayList<>();
    private final Map<String, ChannelState> disconnected = new LinkedHashMap<>();
    private final List<JsonNode> pending = new ArrayList<>();
    private long nextId = 1;

    SharedSimSelector(SharedScenarioRunner run) {
        this.run = run;
    }

    void event(JsonNode event) {
        pending.add(event);
    }

    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        String host = address.getHostString();
        JsonNode broker = null;
        for (JsonNode b : run.manifest.get("brokers")) {
            if (b.get("host").asText().equals(host))
                broker = b;
        }
        if (broker == null)
            throw new IOException("Unknown simulation endpoint " + host);
        long wireId = nextId++;
        ids.put(id, wireId);
        names.put(wireId, id);
        run.bridge.call("connect", "id", wireId, "broker", broker.get("id").intValue(),
            "timeout_ns", run.manifest.get("producer").get("request_timeout").longValue());
    }

    @Override
    public void send(NetworkSend send) {
        Long id = ids.get(send.destinationId());
        if (id == null || !ready.contains(send.destinationId()))
            throw new IllegalStateException("Send on disconnected channel");
        ByteBuffer frame = ByteBufferChannel.toBuffer(send);
        List<Integer> bytes = new ArrayList<>(frame.remaining());
        while (frame.hasRemaining())
            bytes.add(Byte.toUnsignedInt(frame.get()));
        sends.computeIfAbsent(send.destinationId(), key -> new ArrayDeque<>()).add(send);
        run.bridge.call("write", "id", id, "bytes", bytes);
        run.trace("dispatch", "connection", id, "bytes", bytes.size());
    }

    @Override
    public void poll(long timeout) {
        completedSends.clear();
        receives.clear();
        connected.clear();
        disconnected.clear();
        run.supply();
        if (pending.isEmpty()) {
            long deadline = run.now + Math.min(1000, Math.max(1, timeout)) * 1_000_000L;
            run.advance(Math.min(deadline, run.nextSourceDeadline()));
        }
        List<JsonNode> held = new ArrayList<>();
        for (JsonNode event : pending) {
            long id = event.get("id").longValue();
            String name = names.get(id);
            if (name == null || !Long.valueOf(id).equals(ids.get(name)))
                continue;
            switch (event.get("kind").asText()) {
                case "connected" -> {
                    ready.add(name);
                    connected.add(name);
                }
                case "sent" -> {
                    ArrayDeque<NetworkSend> queue = sends.get(name);
                    if (queue != null && !queue.isEmpty())
                        completedSends.add(queue.remove());
                }
                case "receive" -> {
                    if (muted.contains(name)) {
                        held.add(event);
                    } else {
                        byte[] bytes = ScenarioBridge.bytes(event.get("bytes"));
                        receives.add(new NetworkReceive(name, ByteBuffer.wrap(bytes, 4, bytes.length - 4).slice()));
                    }
                }
                case "disconnected" -> {
                    ready.remove(name);
                    disconnected.put(name, ChannelState.READY);
                }
                default -> throw new IllegalStateException("Unknown wire event " + event);
            }
        }
        pending.clear();
        pending.addAll(held);
    }

    @Override
    public void close(String id) {
        Long wireId = ids.remove(id);
        ready.remove(id);
        muted.remove(id);
        sends.remove(id);
        if (wireId != null) {
            names.remove(wireId);
            run.bridge.call("disconnect", "id", wireId);
        }
    }

    @Override
    public void close() {
        for (String id : List.copyOf(ids.keySet()))
            close(id);
    }

    @Override public void wakeup() { }
    @Override
    public List<NetworkSend> completedSends() {
        return completedSends;
    }
    @Override
    public Collection<NetworkReceive> completedReceives() {
        return receives;
    }
    @Override
    public Map<String, ChannelState> disconnected() {
        return disconnected;
    }
    @Override
    public List<String> connected() {
        return connected;
    }
    @Override
    public void mute(String id) {
        muted.add(id);
    }
    @Override
    public void unmute(String id) {
        muted.remove(id);
    }
    @Override
    public void muteAll() {
        muted.addAll(ready);
    }
    @Override
    public void unmuteAll() {
        muted.clear();
    }
    @Override
    public boolean isChannelReady(String id) {
        return ready.contains(id);
    }
}
