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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ByteBufferChannel;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class SimNetworkTest {

    private static final String TOPIC = "time-boundary";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    @Test
    public void testNetworkDeliverySaturatesAtMaxWithoutFiringEarly() throws Exception {
        long startedMs = Long.MAX_VALUE - 1;
        MockTime time = new MockTime(0, startedMs, 0);
        SimScheduler scheduler = new SimScheduler(time);
        SimTrace trace = new SimTrace(time);
        FaultInjector faults = new FaultInjector(1L, FaultInjector.FaultProfile.NONE, trace);
        SimCluster cluster = new SimCluster(1);
        SimBroker broker = new SimBroker(1, cluster, trace);
        SimNetwork network = new SimNetwork(scheduler, faults, trace);
        network.addBroker(broker);
        AtomicLong deliveredAtMs = new AtomicLong(Long.MIN_VALUE);
        SimChannel channel = new SimChannel();
        channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext context, Object message) {
                ReferenceCountUtil.release(message);
                deliveredAtMs.set(time.milliseconds());
            }
        });
        network.register("1#0", channel, broker);

        short version = ApiKeys.API_VERSIONS.oldestVersion();
        ApiVersionsRequest request = new ApiVersionsRequest.Builder(version).build(version);
        channel.writeAndFlush(Unpooled.wrappedBuffer(requestFrame(
            request, ApiKeys.API_VERSIONS, version)));

        scheduler.runCurrent();
        assertEquals(Long.MIN_VALUE, deliveredAtMs.get(),
            "overflow must not deliver a request and response at the current time");
        assertEquals(startedMs, time.milliseconds());

        scheduler.runUntil(() -> deliveredAtMs.get() != Long.MIN_VALUE, Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, time.milliseconds());
        assertEquals(Long.MAX_VALUE, deliveredAtMs.get());
        assertEquals(List.of("t=" + Long.MAX_VALUE + " broker-1 api-versions"), trace.events());
        assertFalse(scheduler.hasPending());
        assertFalse(channel.finishAndReleaseAll(), "the simulated wire must retain no frames");
    }

    @Test
    public void testAppendVisibilitySaturatesAtMaxInsteadOfWrappingIntoThePast()
        throws Exception {
        long startedMs = Long.MAX_VALUE - 1;
        MockTime time = new MockTime(0, startedMs, 0);
        SimTrace trace = new SimTrace(time);
        SimCluster cluster = new SimCluster(1);
        cluster.createTopic(TOPIC, 1);
        SimBroker broker = new SimBroker(1, cluster, trace,
            BrokerTimingModel.produceLatency(2, 0), ProduceObserver.NONE);

        short version = 12;
        broker.handle(produceFrame(version));

        assertEquals(1, cluster.logEndOffset(PARTITION), "the record must be appended");
        assertEquals(0, cluster.visibleLogEndOffset(PARTITION, time.milliseconds()),
            "the saturated visibility deadline must not become immediately visible");
        time.sleep(1);
        assertEquals(Long.MAX_VALUE, time.milliseconds());
        assertEquals(1, cluster.visibleLogEndOffset(PARTITION, time.milliseconds()));
    }

    @Test
    public void testFetchSessionEvictionTraceIsIndependentOfBrokerInsertionOrder() {
        List<String> expected = List.of(
            "t=0 broker-1 fetch-session evict-all",
            "t=0 broker-17 fetch-session evict-all");

        assertEquals(expected, fetchSessionEvictionTrace(1, 17));
        assertEquals(expected, fetchSessionEvictionTrace(17, 1));
    }

    @Test
    public void testComposedBrokerDelaysSaturate() {
        BrokerTimingModel first = BrokerTimingModel.produceLatency(
            Long.MAX_VALUE, Long.MAX_VALUE);
        BrokerTimingModel second = BrokerTimingModel.produceLatency(1, 1);
        BrokerTimingModel composed = first.plus(second);

        assertEquals(Long.MAX_VALUE, composed.processingDelayMs(ApiKeys.PRODUCE, 1));
        assertEquals(Long.MAX_VALUE, composed.appendVisibilityDelayMs(1));
    }

    private static List<String> fetchSessionEvictionTrace(int firstBroker, int secondBroker) {
        MockTime time = new MockTime(0, 0, 0);
        SimScheduler scheduler = new SimScheduler(time);
        SimTrace trace = new SimTrace(time);
        SimCluster cluster = new SimCluster(17);
        SimNetwork network = new SimNetwork(
            scheduler, new FaultInjector(1L, FaultInjector.FaultProfile.NONE, trace), trace);
        network.addBroker(new SimBroker(firstBroker, cluster, trace));
        network.addBroker(new SimBroker(secondBroker, cluster, trace));

        network.evictFetchSessions();
        return trace.events();
    }

    private static byte[] produceFrame(short version) throws IOException {
        MemoryRecords records = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord(0L, "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8)));
        ProduceRequestData.TopicProduceDataCollection topics =
            new ProduceRequestData.TopicProduceDataCollection();
        topics.add(new ProduceRequestData.TopicProduceData()
            .setName(TOPIC)
            .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData()
                .setIndex(PARTITION.partition())
                .setRecords(records))));
        ProduceRequest request = new ProduceRequest.Builder(version, version,
            new ProduceRequestData()
                .setAcks((short) 1)
                .setTimeoutMs(1_000)
                .setTopicData(topics)).build(version);
        return requestFrame(request, ApiKeys.PRODUCE, version);
    }

    private static byte[] requestFrame(AbstractRequest request, ApiKeys apiKey, short version)
        throws IOException {
        RequestHeader header = new RequestHeader(apiKey, version, "sim-test", 1);
        Send send = request.toSend(header);
        ByteBuffer buffer = ByteBufferChannel.toBuffer(send);
        byte[] frame = new byte[buffer.remaining()];
        buffer.get(frame);
        return frame;
    }
}
