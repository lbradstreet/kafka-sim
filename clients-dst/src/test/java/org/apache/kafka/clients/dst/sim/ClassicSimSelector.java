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

import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.requests.ByteBufferChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Experimental: a {@link Selectable} that plugs the classic {@code NetworkClient} into the
 * deterministic {@link SimNetwork}. Lives in this package only to reach package-private
 * connect-fault seams. Every call must come from the sim owner thread.
 */
public final class ClassicSimSelector implements Selectable {

    private final SimNetwork network;
    private final SimCluster cluster;
    private final SimScheduler scheduler;
    private final SimTrace trace;
    private final Map<String, SimChannel> channels = new LinkedHashMap<>();
    private final Set<String> muted = new HashSet<>();
    private final List<String> pendingConnected = new ArrayList<>();
    private final Map<String, ChannelState> pendingDisconnected = new LinkedHashMap<>();
    private List<NetworkSend> pendingSends = new ArrayList<>();
    private List<NetworkSend> completedSends = new ArrayList<>();
    private final List<NetworkReceive> completedReceives = new ArrayList<>();
    private List<String> connected = new ArrayList<>();
    private Map<String, ChannelState> disconnected = new LinkedHashMap<>();
    private Runnable wakeupListener = () -> { };
    private long lastPollTimeoutMs = -1;
    private boolean closed;

    public ClassicSimSelector(SimNetwork network, SimCluster cluster, SimScheduler scheduler,
                              SimTrace trace) {
        this.network = network;
        this.cluster = cluster;
        this.scheduler = scheduler;
        this.trace = trace;
    }

    public void onWakeup(Runnable listener) {
        this.wakeupListener = listener;
    }

    /** The timeout the last {@link #poll(long)} was asked to wait for, for pacing the sender. */
    public long lastPollTimeoutMs() {
        return lastPollTimeoutMs;
    }

    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize,
                        int receiveBufferSize) throws IOException {
        int brokerId;
        try {
            brokerId = cluster.brokerIdFor(address.getHostString());
        } catch (IllegalArgumentException e) {
            throw new IOException(e);
        }
        SimBroker broker = network.broker(brokerId);
        if (broker == null)
            throw new IOException("No sim broker " + brokerId);
        trace.add("classic connect " + id + " -> broker-" + brokerId);
        scheduler.schedule(() -> {
            if (closed)
                return;
            if (network.failConnect(id, brokerId)) {
                pendingDisconnected.put(id, ChannelState.NOT_CONNECTED);
                return;
            }
            SimChannel channel = new SimChannel();
            network.register(id, channel, broker);
            channels.put(id, channel);
            pendingConnected.add(id);
        }, SimNetwork.BASE_LATENCY_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void wakeup() {
        wakeupListener.run();
    }

    @Override
    public void close() {
        closed = true;
        for (String id : new ArrayList<>(channels.keySet()))
            close(id);
    }

    @Override
    public void close(String id) {
        SimChannel channel = channels.remove(id);
        muted.remove(id);
        if (channel != null && channel.isOpen()) {
            channel.close();
            channel.runPendingTasks();
        }
    }

    @Override
    public void send(NetworkSend send) {
        String id = send.destinationId();
        SimChannel channel = channels.get(id);
        if (channel == null)
            throw new IllegalStateException("No open channel " + id);
        if (!channel.isOpen()) {
            pendingDisconnected.put(id, ChannelState.FAILED_SEND);
            return;
        }
        ByteBuffer frame = ByteBufferChannel.toBuffer(send);
        channel.writeOutbound(Unpooled.wrappedBuffer(frame));
        pendingSends.add(send);
    }

    /**
     * Block like a real selector: advance the simulation until this selector has something to
     * report or {@code timeout} virtual milliseconds pass. The classic client contains loops
     * (for example {@code NetworkClientUtils.awaitReady}) which only terminate if poll lets
     * time move.
     */
    @Override
    public void poll(long timeout) {
        lastPollTimeoutMs = timeout;
        if (timeout > 0 && !hasReadyEvents()) {
            long deadlineMs = scheduler.time().milliseconds() + timeout;
            try {
                scheduler.runUntil(() -> hasReadyEvents()
                    || scheduler.time().milliseconds() >= deadlineMs, deadlineMs);
            } catch (IllegalStateException e) {
                if (!isIdleOrPastCap(e))
                    throw e;
                scheduler.time().sleep(Math.max(0, deadlineMs - scheduler.time().milliseconds()));
            }
        }
        completedSends = pendingSends;
        pendingSends = new ArrayList<>();
        completedReceives.clear();
        connected = new ArrayList<>(pendingConnected);
        pendingConnected.clear();
        disconnected = new LinkedHashMap<>(pendingDisconnected);
        pendingDisconnected.clear();
        Iterator<Map.Entry<String, SimChannel>> it = channels.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, SimChannel> entry = it.next();
            String id = entry.getKey();
            SimChannel channel = entry.getValue();
            if (!muted.contains(id)) {
                ByteBuf buf;
                while ((buf = channel.readInbound()) != null) {
                    byte[] frame = new byte[buf.readableBytes()];
                    buf.readBytes(frame);
                    buf.release();
                    // NetworkReceive payloads exclude the 4-byte length prefix.
                    completedReceives.add(new NetworkReceive(id,
                        ByteBuffer.wrap(frame, 4, frame.length - 4).slice()));
                }
            }
            if (!channel.isOpen()) {
                disconnected.putIfAbsent(id, ChannelState.READY);
                it.remove();
            }
        }
    }

    /** The scheduler reports both an empty queue and a next task past the cap as ISE. */
    public static boolean isIdleOrPastCap(IllegalStateException e) {
        String message = String.valueOf(e.getMessage());
        return message.startsWith("Simulation is idle") || message.startsWith("Virtual-time cap");
    }

    private boolean hasReadyEvents() {
        if (!pendingConnected.isEmpty() || !pendingDisconnected.isEmpty())
            return true;
        for (Map.Entry<String, SimChannel> entry : channels.entrySet()) {
            SimChannel channel = entry.getValue();
            if (!channel.isOpen())
                return true;
            if (!muted.contains(entry.getKey()) && !channel.inboundMessages().isEmpty())
                return true;
        }
        return false;
    }

    @Override
    public List<NetworkSend> completedSends() {
        return completedSends;
    }

    @Override
    public Collection<NetworkReceive> completedReceives() {
        return completedReceives;
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
        muted.addAll(channels.keySet());
    }

    @Override
    public void unmuteAll() {
        muted.clear();
    }

    @Override
    public boolean isChannelReady(String id) {
        SimChannel channel = channels.get(id);
        return channel != null && channel.isOpen();
    }
}
