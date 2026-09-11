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

import org.apache.kafka.clients.producer.ClassicProducerSimFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.SenderPump;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.internal.CompressionRatioEstimator;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.MockTime;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

/** Executes every Rust catalogue entry against the real classic producer or the native actor. */
public final class SharedScenarioRunner {
    private static final long MS = 1_000_000;
    final ScenarioBridge bridge;
    final JsonNode manifest;
    final JsonNode initialization;
    final boolean nativeAdapter;
    long now;
    long pollDeadline;
    private final long cap;
    private final Path sidecars;
    private final boolean streaming;
    private JsonGenerator traceWriter;
    private int traceCount;
    private final List<Map<String, Object>> trace = new ArrayList<>();
    private final List<Source> sources = new ArrayList<>();
    private final Map<Long, Integer> owners = new HashMap<>();
    private final Map<Long, Integer> accepted = new LinkedHashMap<>();
    private final Map<Long, Map<String, Object>> deliveries = new LinkedHashMap<>();
    private final List<Map<String, Object>> pendingDeliveries = new ArrayList<>();
    private final Set<Integer> retiredTopics = new HashSet<>();
    private final List<JsonNode> controls = new ArrayList<>();
    private final Clock clock = new Clock();
    private SharedSimSelector selector;
    private ClassicProducerSimFactory.Handle<byte[], byte[]> classic;
    private int controlCount;
    private long closeAt = Long.MAX_VALUE;
    private long closeDeadline = Long.MAX_VALUE;
    private long offered;
    private long refused;
    private boolean sending;
    private boolean classicStopped;
    private Exception immediateFailure;

    private final class Clock extends MockTime {
        Clock() {
            super(0, 0, 0);
        }
        @Override
        public long milliseconds() {
            return now / MS;
        }
        @Override
        public long nanoseconds() {
            return now;
        }
        @Override
        public void sleep(long ms) {
            long end = now + ms * MS;
            while (now < end) {
                supply();
                advance(Math.min(end, nextSourceDeadline()));
            }
        }
    }

    private static final class Source {
        final int number;
        final JsonNode template;
        final String kind;
        final long start;
        final long end;
        final long rate;
        final long count;
        final long depth;
        int index;
        int outstanding;
        long retryAt;
        long candidateDue;
        JsonNode candidate;
        boolean offeredCandidate;
        long offers;
        long accepts;
        long refusals;

        Source(int number, JsonNode node) {
            this.number = number;
            template = node.get("template");
            kind = node.get("shape").fieldNames().next();
            JsonNode shape = node.get("shape").get(kind);
            start = shape.get("start_ns").longValue();
            end = shape.path("end_ns").asLong(Long.MAX_VALUE);
            rate = shape.path("rate_per_s").asLong(0);
            count = kind.equals("OpenLoop") ? ((end - start) * rate + 999_999_999) / 1_000_000_000
                : shape.path("count").asLong(shape.path("max_offers").asLong());
            depth = shape.path("outstanding").asLong(Long.MAX_VALUE);
            retryAt = start;
        }

        long due(long now) {
            if (index == count || (!kind.equals("OpenLoop") && now >= end) || outstanding >= depth)
                return Long.MAX_VALUE;
            return Math.max(retryAt, start + (rate == 0 ? 0 : index * 1_000_000_000L / rate));
        }

        boolean done(long now) {
            return index == count || (!kind.equals("OpenLoop") && now >= end);
        }
    }

    private SharedScenarioRunner(ScenarioBridge bridge, JsonNode initialization, boolean nativeAdapter, long seed, Path sidecars) {
        this.bridge = bridge;
        this.initialization = initialization;
        this.manifest = initialization.get("manifest");
        this.nativeAdapter = nativeAdapter;
        this.sidecars = sidecars;
        streaming = manifest.get("limits").get("records").longValue() > 16_384;
        if (streaming) {
            try {
                Files.createDirectories(sidecars);
                traceWriter = ScenarioBridge.JSON.getFactory().createGenerator(sidecars.resolve("external.json").toFile(), com.fasterxml.jackson.core.JsonEncoding.UTF8);
                traceWriter.writeStartArray();
            } catch (IOException e) {
                throw new IllegalStateException("Open streamed trace", e);
            }
        }
        cap = manifest.get("limits").get("elapsed_ns").longValue();
        int i = 0;
        for (JsonNode load : manifest.get("experiment").get("loads"))
            sources.add(new Source(i++, load));
        if (!nativeAdapter) {
            for (JsonNode topic : manifest.get("topics"))
                CompressionRatioEstimator.resetEstimation(topic.get("name").asText());
            selector = new SharedSimSelector(this);
            classic = ClassicProducerSimFactory.create(classicConfig(), new ByteArraySerializer(),
                new ByteArraySerializer(), selector,
                host -> new InetAddress[] {InetAddress.getByAddress(host, new byte[] {127, 0, 0, 1})}, clock, seed);
        }
    }

    private Map<String, Object> classicConfig() {
        JsonNode p = manifest.get("producer");
        Map<String, Object> config = new LinkedHashMap<>();
        List<String> bootstrap = new ArrayList<>();
        for (JsonNode broker : p.get("bootstrap"))
            bootstrap.add(broker.get(0).asText() + ":" + broker.get(1).asInt());
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "classic-shared-sim");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, p.get("max_in_flight_per_connection").intValue());
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, ms(p, "request_timeout"));
        config.put(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, (long) ms(p, "request_timeout"));
        config.put(ProducerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, (long) ms(p, "request_timeout"));
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ms(p, "delivery_timeout"));
        config.put(ProducerConfig.LINGER_MS_CONFIG, (long) ms(p, "linger_max"));
        config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, (long) ms(p, "retry_backoff_min"));
        config.put(ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, (long) ms(p, "retry_backoff_max"));
        config.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, (long) ms(p, "retry_backoff_min"));
        config.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, (long) ms(p, "retry_backoff_max"));
        config.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, (long) ms(p, "metadata_max_age"));
        config.put(ProducerConfig.RETRIES_CONFIG, p.get("max_attempts").intValue() - 1);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, p.get("batch_target_bytes").intValue());
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, p.get("request_hard_bytes").intValue());
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, p.get("input_bytes").longValue());
        // A synchronous admission attempt must not move the immutable open-loop source clock.
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 0L);
        JsonNode compression = p.get("compression");
        if (compression.isObject()) {
            config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
            config.put(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG, compression.get("Zstd").get("level").intValue());
        } else {
            config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        }
        return config;
    }

    private static int ms(JsonNode node, String field) {
        return Math.toIntExact((node.get(field).longValue() + MS - 1) / MS);
    }

    void trace(String kind, Object... fields) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("at_ns", now);
        event.put("kind", kind);
        for (int i = 0; i < fields.length; i += 2)
            event.put((String) fields[i], fields[i + 1]);
        if (streaming) {
            try {
                traceWriter.writeObject(event);
            } catch (IOException e) {
                throw new IllegalStateException("Write streamed trace", e);
            }
        } else {
            trace.add(event);
        }
        if (++traceCount > manifest.get("limits").get("history_events").intValue())
            throw new IllegalStateException("External history bound exceeded");
    }

    void advance(long until) {
        JsonNode result = bridge.call("advance", "until_ns", Math.max(now, Math.min(cap, until)));
        now = result.get("now_ns").longValue();
        for (JsonNode event : result.get("events")) {
            if (event.get("kind").asText().equals("control"))
                controls.add(event);
            else if (selector != null)
                selector.event(event);
            else
                throw new IllegalStateException("Wire completion for native adapter");
        }
    }

    private boolean paused() {
        for (JsonNode pause : manifest.get("experiment").get("polling_pauses")) {
            if (pause.get("start_ns").longValue() <= now && now < pause.get("end_ns").longValue())
                return true;
        }
        return false;
    }

    private int partitions(int topic) {
        if (nativeAdapter)
            return bridge.call("metadata", "topic", topic).get("partitions").intValue();
        Integer count = classic.metadata().fetch().partitionCountForTopic(manifest.get("topics").get(topic).get("name").asText());
        return count == null ? 0 : count;
    }

    private void offer(Source source) {
        int topic = source.template.get("topic").intValue();
        if (!source.offeredCandidate) {
            source.offeredCandidate = true;
            source.candidateDue = source.rate == 0 ? now : source.start + source.index * 1_000_000_000L / source.rate;
            source.offers++;
            offered++;
            trace("offer", "load", source.number, "id", source.template.get("first_id").longValue() + source.index,
                "due_ns", source.candidateDue);
        }
        int partitions = partitions(topic);
        if (partitions == 0)
            partitions = manifest.get("topics").get(topic).get("leaders").size();
        source.candidate = bridge.call("record", "load", source.number, "index", source.index, "partitions", partitions);
        JsonNode record = source.candidate;
        long id = record.get("id").longValue();
        String error = submit(record, topic);
        boolean admitted = error == null;
        if (admitted)
            error = "";
        trace("admission", "id", id, "accepted", admitted, "error", error);
        if (admitted) {
            if (accepted.put(id, accepted.size()) != null)
                throw new IllegalStateException("Duplicate admission " + id);
            owners.put(id, source.number);
            source.accepts++;
            source.outstanding++;
            source.index++;
            source.offeredCandidate = false;
        } else if (source.rate == 0 && !error.contains("Closed") && !error.contains("Failed") && !error.contains("Retired")) {
            source.retryAt = now + manifest.get("driver").get("admission_retry_delay_ns").longValue();
        } else {
            refused++;
            source.refusals++;
            source.index++;
            source.offeredCandidate = false;
            trace("refused", "id", id, "error", error);
        }
    }

    private String submit(JsonNode record, int topic) {
        long id = record.get("id").longValue();
        if (nativeAdapter) {
            JsonNode result = bridge.call("accept", "id", id);
            return result.get("accepted").booleanValue() ? null : result.get("error").asText();
        }
        if (retiredTopics.contains(topic))
            return "TopicRetiredByWorkload";
        List<Header> headers = new ArrayList<>();
        for (JsonNode h : record.get("headers"))
            headers.add(new RecordHeader(h.get("key").asText(), ScenarioBridge.bytes(h.get("value"))));
        ProducerRecord<byte[], byte[]> input = new ProducerRecord<>(manifest.get("topics").get(topic).get("name").asText(),
            record.get("key_routed").booleanValue() ? null : record.get("partition").intValue(),
            record.get("timestamp_ms").longValue(), ScenarioBridge.bytes(record.get("key")),
            ScenarioBridge.bytes(record.get("value")), headers);
        sending = true;
        immediateFailure = null;
        try {
            Future<RecordMetadata> future = classic.producer().send(input,
                (metadata, exception) -> callback(id, metadata, exception));
            if (future.isCancelled())
                throw new IllegalStateException("Unexpected cancelled send");
            if (immediateFailure != null)
                return immediateFailure.getClass().getName();
        } catch (org.apache.kafka.common.KafkaException e) {
            return e.getClass().getName();
        } finally {
            sending = false;
        }
        bridge.call("accept", "id", id);
        return null;
    }

    private void callback(long id, RecordMetadata metadata, Exception exception) {
        if (sending && exception != null) {
            immediateFailure = exception;
            return;
        }
        Map<String, Object> delivery = new LinkedHashMap<>();
        delivery.put("id", id);
        delivery.put("callback_ns", now);
        delivery.put("success", exception == null);
        delivery.put("partition", metadata == null ? -1 : metadata.partition());
        delivery.put("offset", metadata == null || !metadata.hasOffset() ? null : metadata.offset());
        delivery.put("error", exception == null ? "" : exception.getClass().getName());
        pendingDeliveries.add(delivery);
        trace("callback", "id", id, "success", exception == null);
    }

    private void consume() {
        if (paused())
            return;
        if (nativeAdapter) {
            for (JsonNode event : bridge.call("poll")) {
                if (event.get("kind").asText().equals("delivery")) {
                    Map<String, Object> delivery = new LinkedHashMap<>();
                    delivery.put("id", event.get("id").longValue());
                    delivery.put("at_ns", now);
                    delivery.put("success", event.get("outcome").intValue() == 0);
                    delivery.put("partition", event.get("partition").intValue());
                    delivery.put("offset", event.get("offset").isNull() ? null : event.get("offset").longValue());
                    delivery.put("outcome", event.get("outcome").intValue());
                    delivery.put("reason", event.get("reason").intValue());
                    delivery.put("attempts", event.get("attempts").intValue());
                    pendingDeliveries.add(delivery);
                } else {
                    trace("native-event", "event", event.get("event").asText());
                }
            }
        }
        for (Map<String, Object> delivery : pendingDeliveries) {
            // Compare application consumption in both adapters; classic callbacks
            // may have completed much earlier while consumption was paused.
            delivery.put("at_ns", now);
            long id = (long) delivery.get("id");
            Integer owner = owners.get(id);
            if (owner == null || deliveries.put(id, delivery) != null)
                throw new IllegalStateException("Unknown/duplicate terminal delivery " + id);
            sources.get(owner).outstanding--;
            trace("consumed", "id", id);
        }
        pendingDeliveries.clear();
    }

    private void controls() {
        for (JsonNode event : controls) {
            controlCount++;
            JsonNode action = event.get("action");
            String kind = action.isTextual() ? action.asText() : action.fieldNames().next();
            JsonNode body = action.isTextual() ? action : action.get(kind);
            trace("control", "action", action, "scheduled_ns", event.get("at_ns").longValue());
            switch (kind) {
                case "Close" -> {
                    closeAt = event.get("at_ns").longValue();
                    closeDeadline = closeAt + body.get("deadline_ns").longValue();
                    if (!nativeAdapter)
                        classic.sender().initiateClose();
                }
                case "CloseTopic" -> retiredTopics.add(body.get("topic").intValue());
                case "OpenTopic" -> {
                    retiredTopics.remove(body.get("topic").intValue());
                    if (!nativeAdapter)
                        classic.metadata().requestUpdate(false);
                }
                case "Flush" -> {
                    if (!nativeAdapter)
                        classic.accumulator().beginFlush();
                }
                default -> { }
            }
        }
        controls.clear();
    }

    void supply() {
        controls();
        consume();
        if (now < closeAt) {
            for (int work = 0; work < 64; work++) {
                Source next = null;
                for (Source source : sources) {
                    if (source.due(now) <= now && (next == null || source.due(now) < next.due(now)))
                        next = source;
                }
                if (next == null)
                    break;
                offer(next);
            }
        }
    }

    long nextSourceDeadline() {
        long deadline = Math.min(cap, now + MS);
        for (Source source : sources) {
            if (now < closeAt) {
                long due = source.due(now);
                if (due > now)
                    deadline = Math.min(deadline, due);
            }
        }
        return deadline;
    }

    private void pump() {
        if (classicStopped)
            return;
        try {
            SenderPump.runOnce(classic.sender());
        } catch (org.apache.kafka.common.requests.CorrelationIdMismatchException e) {
            // Sender.run catches Exception around runOnce and continues. Keep
            // the error visible when the original broker-hook Drop model creates
            // a hole in a TCP response sequence.
            trace("sender-error", "error", e.getClass().getName(), "message", e.getMessage());
        }
    }

    private boolean sourcesFinished() {
        return (now >= closeAt || sources.stream().allMatch(s -> s.done(now)))
            && controlCount == manifest.get("experiment").get("scheduled_actions").size();
    }

    private void closeIfExpired() {
        if (now >= closeDeadline && !nativeAdapter) {
            classic.sender().forceClose();
            classic.sender().run();
            classicStopped = true;
            closeDeadline = Long.MAX_VALUE;
        }
    }

    private void driveOneTurn(long settleEnd) {
        long next = Math.min(now + MS, Math.min(settleEnd, closeDeadline));
        if (now < closeAt) {
            for (Source source : sources) {
                long due = source.due(now);
                if (due > now)
                    next = Math.min(next, due);
                if (source.end > now && source.end != Long.MAX_VALUE)
                    next = Math.min(next, source.end);
            }
        }
        pollDeadline = Math.min(cap, next);
        long before = now;
        if (nativeAdapter)
            advance(pollDeadline);
        else
            pump();
        if (now == before)
            advance(Math.min(pollDeadline, now + 1000));
    }

    private void drive() {
        long settleEnd = Long.MAX_VALUE;
        long turns = 0;
        while (true) {
            if (++turns > 5_000_000 || now >= cap)
                throw new IllegalStateException("Simulation bound at " + now + ", accepted=" + accepted.size()
                    + ", delivered=" + deliveries.size());
            controls();
            consume();
            boolean sourcesFinished = sourcesFinished();
            if (sourcesFinished && settleEnd == Long.MAX_VALUE)
                settleEnd = Math.min(cap, now + manifest.get("experiment").get("settle_timeout_ns").longValue());
            if (sourcesFinished && deliveries.size() == accepted.size())
                break;
            if (now >= settleEnd)
                throw new IllegalStateException("Unsettled deliveries at deadline");
            closeIfExpired();
            supply();
            driveOneTurn(settleEnd);
        }
    }

    private JsonNode execute() {
        drive();
        // The source reservation envelope includes cancelled IDs; do not report them as offers.
        for (Source source : sources) {
            if (source.offeredCandidate) {
                refused++;
                source.refusals++;
                source.offeredCandidate = false;
            }
        }
        closeTrace();
        JsonNode evidence = null;
        List<String> checks;
        if (streaming) {
            bridge.call("export", "path", sidecars.resolve("environment.json").toString());
            checks = checkStreamedLog();
        } else {
            evidence = bridge.call("evidence");
            checks = check(evidence);
        }
        ObjectNode report = ScenarioBridge.JSON.createObjectNode();
        report.put("schema", "kr-classic-comparison/v1");
        report.put("adapter", nativeAdapter ? "native-panama-sim" : "classic");
        report.set("manifest", manifest);
        report.set("profile", initialization.get("profile"));
        report.set("original_manifest", initialization.get("original_manifest"));
        report.set("adjustments", initialization.get("adjustments"));
        report.set("compatibility", ScenarioBridge.JSON.valueToTree(Map.of(
            "shared", List.of("record generator", "broker model", "network byte streams", "fault engine", "nanosecond fault/control schedule", "Java source driver"),
            "differences", List.of("classic timers use millisecond precision", "classic max.block.ms=0; unresolved metadata is an admission refusal",
                "buffer.memory and native input credits account different allocations", "classic has no descriptor, delivery-event, or wire-credit pool",
                "classic has no topic-handle identity fence", "batch/request size packing and retry jitter algorithms differ",
                "native Panama adapter exercises ProducerClient in simulation, not the production KrKafkaProducer binding"))));
        report.set("classic_config", ScenarioBridge.JSON.valueToTree(classicConfig()));
        report.put("offered", offered);
        report.put("accepted", accepted.size());
        report.put("refused", refused);
        report.put("acked", deliveries.values().stream().filter(d -> Boolean.TRUE.equals(d.get("success"))).count());
        report.put("failed", deliveries.values().stream().filter(d -> Boolean.FALSE.equals(d.get("success"))).count());
        report.set("checks", ScenarioBridge.JSON.valueToTree(checks));
        if (!streaming)
            report.set("deliveries", ScenarioBridge.JSON.valueToTree(deliveries.values()));
        report.set("source_evidence", ScenarioBridge.JSON.valueToTree(sources.stream().map(s -> Map.of(
            "load", s.number, "offered", s.offers, "accepted", s.accepts, "refused", s.refusals,
            "reserved", s.count, "cancelled", s.count - s.offers)).toList()));
        if (streaming) {
            try {
                ScenarioBridge.JSON.writeValue(sidecars.resolve("deliveries.json").toFile(), deliveries.values());
            } catch (IOException e) {
                throw new IllegalStateException("Write deliveries", e);
            }
            report.set("artifacts", ScenarioBridge.JSON.valueToTree(Map.of(
                "external_history", artifact(sidecars.resolve("external.json")),
                "environment", artifact(sidecars.resolve("environment.json")),
                "deliveries", artifact(sidecars.resolve("deliveries.json")))));
        } else {
            report.set("external_history", ScenarioBridge.JSON.valueToTree(trace));
            report.set("environment", evidence);
        }
        if (!nativeAdapter) {
            if (!classicStopped)
                classic.client().close();
            classic.producer().close(Duration.ZERO);
        } else if (closeAt == Long.MAX_VALUE) {
            bridge.call("close", "timeout_ns", manifest.get("experiment").get("close_timeout_ns").longValue());
        }
        report.set("teardown", bridge.call("shutdown"));
        return report;
    }

    private void persistFailure(RuntimeException error) {
        closeTrace();
        try {
            Files.createDirectories(sidecars);
            ScenarioBridge.JSON.writeValue(sidecars.resolve("failure.json").toFile(), Map.of(
                "error", error.toString(), "initialization", initialization, "at_ns", now,
                "offered", offered, "accepted", accepted.size(), "delivered", deliveries.size()));
            if (!streaming)
                ScenarioBridge.JSON.writeValue(sidecars.resolve("external.json").toFile(), trace);
            bridge.call("export_history", "path", sidecars.resolve("environment-history.json").toAbsolutePath().toString());
        } catch (IOException | RuntimeException diagnosticsError) {
            error.addSuppressed(diagnosticsError);
        }
    }

    private void closeTrace() {
        if (traceWriter != null) {
            try {
                traceWriter.writeEndArray();
                traceWriter.close();
                traceWriter = null;
            } catch (IOException e) {
                throw new IllegalStateException("Close streamed trace", e);
            }
        }
    }

    private static Map<String, String> artifact(Path path) {
        try (InputStream input = Files.newInputStream(path)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[65536];
            for (int n; (n = input.read(buffer)) != -1;)
                digest.update(buffer, 0, n);
            return Map.of("path", path.toAbsolutePath().toString(), "sha256", HexFormat.of().formatHex(digest.digest()));
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Hash complete evidence", e);
        }
    }

    private final class LogCheck {
        private final Set<Long> stored = new HashSet<>();
        private final Map<String, Integer> order = new HashMap<>();
        private long foundAcks;

        void record(JsonNode record) {
            long id = record.get("id").longValue();
            Integer ordinal = accepted.get(id);
            if (ordinal == null || !stored.add(id))
                throw new IllegalStateException("Unknown/duplicate stored ID " + id);
            String partition = record.get("topic_id").toString() + ":" + record.get("partition");
            Integer previous = order.put(partition, ordinal);
            if (previous != null && previous >= ordinal)
                throw new IllegalStateException("Stored partition order differs from admission order " + id);
            Map<String, Object> delivery = deliveries.get(id);
            if (Boolean.TRUE.equals(delivery.get("success"))) {
                checkOffset(record, delivery);
                foundAcks++;
            }
        }

        List<String> finish() {
            if (offered != accepted.size() + refused || accepted.size() != deliveries.size())
                throw new IllegalStateException("Terminal population does not reconcile");
            if (foundAcks != deliveries.values().stream().filter(d -> Boolean.TRUE.equals(d.get("success"))).count())
                throw new IllegalStateException("Acknowledged record absent from broker log");
            for (Source source : sources) {
                if (source.start < closeAt && source.offers == 0)
                    throw new IllegalStateException("Vacuous load " + source.number);
            }
            if (controlCount != manifest.get("experiment").get("scheduled_actions").size())
                throw new IllegalStateException("Missing scheduled control");
            return List.of("terminal population accounting", "acknowledged payloads/offsets match independent broker log",
                "no duplicate stored IDs", "partition order matches admission", "every active load has offers", "all scheduled controls executed");
        }
    }

    private static void checkOffset(JsonNode record, Map<String, Object> delivery) {
        if (record.get("partition").intValue() != (int) delivery.get("partition") || delivery.get("offset") == null
            || record.get("offset").longValue() != (long) delivery.get("offset"))
            throw new IllegalStateException("Wrong acknowledgment route/offset " + record.get("id"));
    }

    private List<String> checkStreamedLog() {
        LogCheck check = new LogCheck();
        try (JsonParser parser = ScenarioBridge.JSON.getFactory().createParser(sidecars.resolve("environment.json").toFile())) {
            if (parser.nextToken() != JsonToken.START_OBJECT)
                throw new IllegalStateException("Environment object");
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String field = parser.currentName();
                parser.nextToken();
                if (!field.equals("log")) {
                    parser.skipChildren();
                    continue;
                }
                if (parser.currentToken() != JsonToken.START_ARRAY)
                    throw new IllegalStateException("Broker log array");
                while (parser.nextToken() != JsonToken.END_ARRAY)
                    check.record(ScenarioBridge.JSON.readTree(parser));
                return check.finish();
            }
            throw new IllegalStateException("Missing broker log");
        } catch (IOException e) {
            throw new IllegalStateException("Validate streamed broker log", e);
        }
    }

    private static JsonNode replayValue(JsonNode report) {
        ObjectNode value = report.deepCopy();
        if (value.has("artifacts"))
            value.get("artifacts").forEach(artifact -> ((ObjectNode) artifact).remove("path"));
        return value;
    }

    private List<String> check(JsonNode evidence) {
        LogCheck check = new LogCheck();
        for (JsonNode record : evidence.get("log"))
            check.record(record);
        return check.finish();
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i += 2)
            options.put(args[i], args[i + 1]);
        Path library = Path.of(options.getOrDefault("--library", System.getProperty("kr.sim.library", ""))).toAbsolutePath();
        Path out = Path.of(options.getOrDefault("--out", "build/classic-scenarios"));
        Files.createDirectories(out);
        String adapter = options.getOrDefault("--adapter", "classic");
        String size = options.getOrDefault("--size", "test");
        String profile = options.getOrDefault("--profile", "original");
        long seed = Long.parseUnsignedLong(options.getOrDefault("--seed", "0"));
        boolean replay = Boolean.parseBoolean(options.getOrDefault("--replay", "true"));
        int passed = 0;
        List<String> failures = new ArrayList<>();
        try (ScenarioBridge bridge = new ScenarioBridge(library)) {
            JsonNode catalogue = bridge.call("catalogue");
            for (JsonNode entry : catalogue) {
                String scenario = entry.get("scenario").asText();
                String variant = entry.get("variant").asText();
                if (!scenario.matches(options.getOrDefault("--scenario", ".*")) || !variant.matches(options.getOrDefault("--variant", ".*")))
                    continue;
                String name = scenario + "--" + variant + "--" + adapter + "--" + profile + "--" + size + "--" + Long.toUnsignedString(seed);
                try {
                    JsonNode first = run(bridge, scenario, variant, size, seed, adapter, profile, out.resolve(name + ".first"));
                    if (replay) {
                        JsonNode second = run(bridge, scenario, variant, size, seed, adapter, profile, out.resolve(name + ".replay"));
                        if (!replayValue(first).equals(replayValue(second))) {
                            ScenarioBridge.JSON.writeValue(out.resolve(name + ".first.json").toFile(), first);
                            ScenarioBridge.JSON.writeValue(out.resolve(name + ".second.json").toFile(), second);
                            throw new IllegalStateException("Deterministic replay diverged");
                        }
                    }
                    ((ObjectNode) first).put("replay_verified", replay);
                    ScenarioBridge.JSON.writeValue(out.resolve(name + ".json").toFile(), first);
                    System.out.println("PASS " + name + " offered=" + first.get("offered") + " acked=" + first.get("acked") + " failed=" + first.get("failed"));
                    passed++;
                } catch (RuntimeException error) {
                    error.printStackTrace();
                    failures.add(name + ": " + error);
                    Files.writeString(out.resolve(name + ".failure.txt"), error.toString());
                    bridge.call("destroy");
                }
            }
        }
        ScenarioBridge.JSON.writeValue(out.resolve("summary-" + adapter + "-" + profile + "-" + size + ".json").toFile(), Map.of("passed", passed, "failures", failures));
        if (passed == 0 || !failures.isEmpty())
            throw new IllegalStateException("passed=" + passed + ", failures=" + failures.size());
    }

    private static JsonNode run(ScenarioBridge bridge, String scenario, String variant, String size, long seed, String adapter, String profile, Path sidecars) {
        JsonNode initialization = bridge.call("init", "scenario", scenario, "variant", variant,
            "size", size, "seed", Long.toUnsignedString(seed), "adapter", adapter, "profile", profile);
        SharedScenarioRunner runner = new SharedScenarioRunner(bridge, initialization, adapter.equals("native"), seed, sidecars);
        try {
            return runner.execute();
        } catch (RuntimeException error) {
            runner.persistFailure(error);
            throw error;
        } finally {
            runner.closeTrace();
            bridge.call("destroy");
        }
    }
}
