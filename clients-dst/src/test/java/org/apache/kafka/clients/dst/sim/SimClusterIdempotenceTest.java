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
import org.apache.kafka.common.record.internal.RecordBatch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimClusterIdempotenceTest {

    private static final TopicPartition PARTITION = new TopicPartition("wrap", 0);
    private static final TopicPartition OTHER_PARTITION = new TopicPartition("wrap", 1);

    @Test
    public void testSequenceHistoryWrapsFromMaxValueToZero() {
        SimCluster cluster = cluster();
        long producerId = cluster.allocateProducerId();

        cluster.rememberSequence(
            producerId, (short) 0, PARTITION, Integer.MAX_VALUE - 1, 2, 10L);

        SimCluster.SequenceEntry duplicate = cluster.validateSequence(
            producerId, (short) 0, PARTITION, Integer.MAX_VALUE - 1, 2).orElseThrow();
        assertEquals(Integer.MAX_VALUE, duplicate.lastSequence());
        assertTrue(cluster.validateSequence(producerId, (short) 0, PARTITION, 0, 1).isEmpty());

        cluster.rememberSequence(producerId, (short) 0, PARTITION, 0, 1, 12L);
        assertTrue(cluster.validateSequence(producerId, (short) 0, PARTITION, 1, 1).isEmpty());
    }

    @Test
    public void testBatchLastSequenceCanWrapWithinTheBatch() {
        SimCluster cluster = cluster();
        long producerId = cluster.allocateProducerId();

        cluster.rememberSequence(
            producerId, (short) 0, PARTITION, Integer.MAX_VALUE, 2, 10L);

        SimCluster.SequenceEntry duplicate = cluster.validateSequence(
            producerId, (short) 0, PARTITION, Integer.MAX_VALUE, 2).orElseThrow();
        assertEquals(0, duplicate.lastSequence());
        assertTrue(cluster.validateSequence(producerId, (short) 0, PARTITION, 1, 1).isEmpty());
    }

    @Test
    public void testSequenceValidationRejectsInvalidRecordCounts() {
        SimCluster cluster = cluster();
        long producerId = cluster.allocateProducerId();

        assertThrows(IllegalArgumentException.class,
            () -> cluster.validateSequence(producerId, (short) 0, PARTITION, 0, 0));
        assertThrows(IllegalArgumentException.class,
            () -> cluster.rememberSequence(producerId, (short) 0, PARTITION, 0, -1, 0L));
    }

    @Test
    public void testDuplicateRequiresExactSequenceRange() {
        SimCluster cluster = cluster();
        long producerId = cluster.allocateProducerId();
        cluster.rememberSequence(producerId, (short) 0, PARTITION, 0, 1, 17L);

        assertThrows(SimCluster.SequenceValidationException.class,
            () -> cluster.validateSequence(producerId, (short) 0, PARTITION, 0, 2),
            "the same base with a different last sequence is not the cached batch");

        SimCluster.SequenceEntry exact = cluster.validateSequence(
            producerId, (short) 0, PARTITION, 0, 1).orElseThrow();
        assertEquals(0, exact.lastSequence());
        assertEquals(17L, exact.baseOffset());
    }

    @Test
    public void testHigherEpochResetsAndFencesOnlyItsPartition() {
        SimCluster cluster = cluster();
        long producerId = cluster.allocateProducerId();
        cluster.rememberSequence(producerId, (short) 0, PARTITION, 0, 1, 0L);
        cluster.rememberSequence(producerId, (short) 0, OTHER_PARTITION, 0, 1, 0L);

        assertTrue(cluster.validateSequence(
            producerId, (short) 1, PARTITION, 0, 1).isEmpty());
        cluster.rememberSequence(producerId, (short) 1, PARTITION, 0, 1, 1L);

        assertEquals((short) 1, cluster.currentProducerEpoch(producerId, PARTITION));
        assertEquals((short) 0, cluster.currentProducerEpoch(producerId, OTHER_PARTITION));
        assertThrows(SimCluster.ProducerEpochValidationException.class,
            () -> cluster.validateSequence(producerId, (short) 0, PARTITION, 1, 1));
        assertEquals(0L, cluster.validateSequence(
            producerId, (short) 0, OTHER_PARTITION, 0, 1).orElseThrow().baseOffset());
        assertTrue(cluster.validateSequence(
            producerId, (short) 0, OTHER_PARTITION, 1, 1).isEmpty());
    }

    @Test
    public void testRejectedHigherEpochDoesNotFenceTheCurrentEpoch() {
        SimCluster cluster = cluster();
        long producerId = cluster.allocateProducerId();
        cluster.rememberSequence(producerId, (short) 0, PARTITION, 0, 1, 0L);

        assertThrows(SimCluster.SequenceValidationException.class,
            () -> cluster.validateSequence(producerId, (short) 1, PARTITION, 1, 1));
        assertEquals((short) 0, cluster.currentProducerEpoch(producerId, PARTITION));
        assertTrue(cluster.validateSequence(
            producerId, (short) 0, PARTITION, 1, 1).isEmpty());
    }

    @Test
    public void testForgettingProducerStateIsPartitionScoped() {
        SimCluster cluster = cluster();
        long producerId = cluster.allocateProducerId();
        cluster.rememberSequence(producerId, (short) 0, PARTITION, 0, 1, 0L);
        cluster.rememberSequence(producerId, (short) 0, OTHER_PARTITION, 0, 1, 0L);

        cluster.forgetProducerState(producerId, PARTITION);

        assertEquals(RecordBatch.NO_PRODUCER_EPOCH,
            cluster.currentProducerEpoch(producerId, PARTITION));
        assertEquals((short) 0, cluster.currentProducerEpoch(producerId, OTHER_PARTITION));
        assertTrue(cluster.validateSequence(
            producerId, (short) 1, PARTITION, 0, 1).isEmpty());
        assertEquals(0L, cluster.validateSequence(
            producerId, (short) 0, OTHER_PARTITION, 0, 1).orElseThrow().baseOffset());
    }

    private static SimCluster cluster() {
        SimCluster cluster = new SimCluster(1);
        cluster.createTopic(PARTITION.topic(), 2);
        return cluster;
    }
}
