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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.ClassicProducerSimFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class ClassicProducerCompressionTest {
    static Stream<Arguments> configuredLevels() {
        return Stream.of(
            Arguments.of("zstd", ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG, 1, Compression.zstd().level(1).build()),
            Arguments.of("gzip", ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG, 1, Compression.gzip().level(1).build()),
            Arguments.of("lz4", ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG, 9, Compression.lz4().level(9).build())
        );
    }

    @ParameterizedTest
    @MethodSource("configuredLevels")
    void accumulatorUsesConfiguredCompressionLevel(String codec, String levelConfig, int level, Compression expected) {
        Map<String, Object> config = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker.invalid:9092",
            ProducerConfig.COMPRESSION_TYPE_CONFIG, codec,
            levelConfig, level
        );
        var handle = ClassicProducerSimFactory.create(config, new ByteArraySerializer(), new ByteArraySerializer(),
            mock(Selectable.class), host -> new InetAddress[] {InetAddress.getLoopbackAddress()}, new MockTime(), 0);
        try {
            assertEquals(expected, handle.accumulator().compression,
                "The simulation must use the requested codec level, rather than the codec default");
        } finally {
            handle.client().close();
            handle.producer().close(Duration.ZERO);
        }
    }
}
