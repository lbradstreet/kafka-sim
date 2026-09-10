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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimBrokerIdempotenceTest {

    @Test
    public void testStaleLeaderRequestsDoNotConsumeScriptedProduceFailures() {
        SimCluster cluster = new SimCluster(2);
        TopicPartition partition = new TopicPartition("fault-budget", 0);
        Uuid topicId = cluster.createTopicId(partition.topic(), 1);
        SimTrace trace = new SimTrace(new MockTime(0, 0L, 0L));
        SimBroker oldLeader = new SimBroker(1, cluster, trace);
        SimBroker newLeader = new SimBroker(2, cluster, trace);
        long producerId = cluster.allocateProducerId();
        cluster.moveLeader(partition, 2);
        cluster.failNextProduces(partition, Errors.NOT_ENOUGH_REPLICAS, 1);

        for (int request = 0; request < 2; request++) {
            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code(),
                produce(oldLeader, topicId, producerId, request).errorCode());
        }
        assertEquals(Errors.NOT_ENOUGH_REPLICAS.code(),
            produce(newLeader, topicId, producerId, 2).errorCode(),
            "the fault budget must survive requests rejected by an obsolete leader");
        assertTrue(cluster.log(partition).isEmpty());
        ProduceResponseData.PartitionProduceResponse accepted = produce(newLeader, topicId, producerId, 3);
        assertEquals(Errors.NONE.code(), accepted.errorCode(), "the scripted fault must fire exactly once");
        assertEquals(0L, accepted.baseOffset());
        assertEquals(1, cluster.log(partition).size());
        assertEquals(1, trace.events().stream()
            .filter(event -> event.contains("produce NOT_ENOUGH_REPLICAS ")).count());
    }

    @Test
    public void testNullTransactionalIdAlwaysAllocatesAFreshProducerId() {
        SimCluster cluster = new SimCluster(1);
        SimTrace trace = new SimTrace(new MockTime(0, 0L, 0L));
        SimBroker broker = new SimBroker(1, cluster, trace);

        InitProducerIdResponse first = init(broker, RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH, 1);
        InitProducerIdResponse second = init(broker, first.data().producerId(),
            first.data().producerEpoch(), 2);

        assertEquals((short) 0, first.data().producerEpoch());
        assertEquals((short) 0, second.data().producerEpoch());
        assertNotEquals(first.data().producerId(), second.data().producerId(),
            "expected identity fields must not turn a null-transactional-id request into a bump");
        assertEquals(2, trace.events().stream()
            .filter(event -> event.contains("init-producer-id"))
            .count());
    }

    private static ProduceResponseData.PartitionProduceResponse produce(SimBroker broker,
                                                                        Uuid topicId,
                                                                        long producerId,
                                                                        int correlationId) {
        short version = 13;
        ProduceRequestData.TopicProduceDataCollection topics = new ProduceRequestData.TopicProduceDataCollection();
        topics.add(new ProduceRequestData.TopicProduceData().setTopicId(topicId)
            .setPartitionData(List.of(new ProduceRequestData.PartitionProduceData().setIndex(0)
                .setRecords(MemoryRecords.withIdempotentRecords(Compression.NONE, producerId,
                    (short) 0, 0, new SimpleRecord(new byte[] {1}))))));
        ProduceRequest request = new ProduceRequest.Builder(version, version, new ProduceRequestData()
            .setAcks((short) -1).setTimeoutMs(1_000).setTopicData(topics)).build(version);
        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, version, "fault-budget", correlationId);
        ByteBuffer payload = request.serializeWithHeader(header);
        ByteBuffer framed = ByteBuffer.allocate(Integer.BYTES + payload.remaining());
        framed.putInt(payload.remaining()).put(payload);
        ByteBuffer responseFrame = ByteBuffer.wrap(broker.handle(framed.array()).frame());
        assertEquals(responseFrame.getInt(), responseFrame.remaining());
        ProduceResponse response = (ProduceResponse) AbstractResponse.parseResponse(responseFrame, header);
        return response.data().responses().iterator().next().partitionResponses().get(0);
    }

    private static InitProducerIdResponse init(SimBroker broker, long producerId,
                                               short producerEpoch, int correlationId) {
        short version = ApiKeys.INIT_PRODUCER_ID.latestVersion();
        InitProducerIdRequest request = new InitProducerIdRequest.Builder(
            new InitProducerIdRequestData()
                .setTransactionalId(null)
                .setTransactionTimeoutMs(Integer.MAX_VALUE)
                .setProducerId(producerId)
                .setProducerEpoch(producerEpoch))
            .build(version);
        RequestHeader header = new RequestHeader(ApiKeys.INIT_PRODUCER_ID, version,
            "sim-idempotence", correlationId);
        ByteBuffer payload = request.serializeWithHeader(header);
        ByteBuffer framed = ByteBuffer.allocate(Integer.BYTES + payload.remaining());
        framed.putInt(payload.remaining());
        framed.put(payload);

        ByteBuffer responseFrame = ByteBuffer.wrap(broker.handle(framed.array()).frame());
        int responseSize = responseFrame.getInt();
        assertEquals(responseSize, responseFrame.remaining());
        AbstractResponse response = AbstractResponse.parseResponse(responseFrame, header);
        return (InitProducerIdResponse) response;
    }
}
