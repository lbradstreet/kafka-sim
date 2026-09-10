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

import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * An {@link EmbeddedChannel} whose flushed outbound frames are delivered to the simulated
 * network instead of an outbound queue. {@link ClassicSimSelector} bridges the existing
 * client's selector interface to these in-memory channels; the sim replaces only the wire.
 *
 * <p>{@code EmbeddedChannel} executes all channel work inline on the calling thread, which
 * is exactly the determinism the harness needs. The simulation-supported pipeline, I/O, task,
 * and lifecycle entry points are confined to the thread which creates the channel.
 */
public final class SimChannel extends EmbeddedChannel {

    private final SimThreadGuard owner;
    private Consumer<ByteBuf> outboundSink;

    public SimChannel() {
        super();
        owner = new SimThreadGuard("SimChannel");
    }

    /** Set after pipeline construction; frames flushed before this are dropped loudly. */
    public void outboundSink(Consumer<ByteBuf> sink) {
        checkOwner();
        this.outboundSink = sink;
    }

    @Override
    public boolean isOpen() {
        checkOwner();
        return super.isOpen();
    }

    @Override
    public boolean isActive() {
        checkOwner();
        return super.isActive();
    }

    @Override
    public boolean isRegistered() {
        checkOwner();
        return super.isRegistered();
    }

    @Override
    public ChannelPipeline pipeline() {
        checkOwner();
        return super.pipeline();
    }

    @Override
    public ByteBufAllocator alloc() {
        checkOwner();
        return super.alloc();
    }

    @Override
    public EventLoop eventLoop() {
        checkOwner();
        return super.eventLoop();
    }

    @Override
    public ChannelFuture closeFuture() {
        checkOwner();
        return super.closeFuture();
    }

    @Override
    public ChannelPromise newPromise() {
        checkOwner();
        return super.newPromise();
    }

    @Override
    public <T> T readInbound() {
        checkOwner();
        return super.readInbound();
    }

    @Override
    public <T> T readOutbound() {
        checkOwner();
        return super.readOutbound();
    }

    @Override
    public boolean writeInbound(Object... messages) {
        checkOwner();
        return super.writeInbound(messages);
    }

    @Override
    public SimChannel flushInbound() {
        checkOwner();
        super.flushInbound();
        return this;
    }

    @Override
    public boolean writeOutbound(Object... messages) {
        checkOwner();
        return super.writeOutbound(messages);
    }

    @Override
    public SimChannel flushOutbound() {
        checkOwner();
        super.flushOutbound();
        return this;
    }

    @Override
    public boolean finishAndReleaseAll() {
        checkOwner();
        return super.finishAndReleaseAll();
    }

    @Override
    public SimChannel flush() {
        checkOwner();
        super.flush();
        return this;
    }

    @Override
    public SimChannel read() {
        checkOwner();
        super.read();
        return this;
    }

    @Override
    public ChannelFuture write(Object message) {
        checkOwner();
        return super.write(message);
    }

    @Override
    public ChannelFuture write(Object message, ChannelPromise promise) {
        checkOwner();
        return super.write(message, promise);
    }

    @Override
    public ChannelFuture writeAndFlush(Object message) {
        checkOwner();
        return super.writeAndFlush(message);
    }

    @Override
    public ChannelFuture writeAndFlush(Object message, ChannelPromise promise) {
        checkOwner();
        return super.writeAndFlush(message, promise);
    }

    @Override
    public void runPendingTasks() {
        checkOwner();
        super.runPendingTasks();
    }

    @Override
    public boolean hasPendingTasks() {
        checkOwner();
        return super.hasPendingTasks();
    }

    @Override
    public long runScheduledPendingTasks() {
        checkOwner();
        return super.runScheduledPendingTasks();
    }

    @Override
    public void checkException() {
        checkOwner();
        super.checkException();
    }

    @Override
    protected void handleOutboundMessage(Object message) {
        checkOwner();
        if (outboundSink != null && message instanceof ByteBuf frame)
            outboundSink.accept(frame);
        else
            super.handleOutboundMessage(message);
    }

    @Override
    protected void handleInboundMessage(Object message) {
        checkOwner();
        super.handleInboundMessage(message);
    }

    @Override
    protected void doClose() throws Exception {
        checkOwner();
        super.doClose();
    }

    private void checkOwner() {
        // EmbeddedChannel's superclass constructor invokes overridable accessors before this
        // subclass can publish its guard. The channel is not externally reachable then.
        if (owner != null)
            owner.check();
    }
}
