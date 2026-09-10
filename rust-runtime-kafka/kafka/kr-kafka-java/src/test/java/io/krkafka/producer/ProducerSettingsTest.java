package io.krkafka.producer;

import io.krkafka.ffi.*;
import io.krkafka.loader.NativeLibrary;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ProducerSettingsTest {
    private Map<String, Object> config(Object... pairs) {
        Map<String, Object> result = new HashMap<>();
        result.put("bootstrap.servers", "localhost:9092");
        for (int i = 0; i < pairs.length; i += 2) result.put((String) pairs[i], pairs[i + 1]);
        return result;
    }

    @Test void defaultsAndCoupledNativeBounds() {
        NativeLibrary.load();
        ProducerSettings settings = new ProducerSettings(config());
        try (Arena arena = Arena.ofConfined()) {
            var nativeConfig = settings.nativeConfig(arena);
            assertEquals(kr_producer_config.sizeof(), kr_producer_config.struct_size(nativeConfig));
            assertEquals(255, kr_producer_config.max_attempts(nativeConfig));
            assertEquals(5_000_000L, kr_producer_config.linger_max_ns(nativeConfig));
            assertEquals(0, kr_producer_config.compression(nativeConfig));
            assertEquals(0, kr_producer_config.batch_target_mode(nativeConfig));
            assertEquals(1, kr_producer_config.transport(nativeConfig));
            assertEquals(33_554_432, kr_producer_config.input_bytes(nativeConfig));
            int payload = kr_producer_config.batch_hard_bytes(nativeConfig);
            assertEquals(1_048_576 - "kr-kafka".length() - 381, payload);
            assertTrue(1 + (payload + kr_producer_config.output_chunk_bytes(nativeConfig) - 1)
                / kr_producer_config.output_chunk_bytes(nativeConfig) <= 3);
            assertEquals(settings.recordDescriptors, kr_producer_config.delivery_event_capacity(nativeConfig));
            assertEquals(33_554_432L, settings.resourceBudget().get("native.input.pool.bytes"));
            assertEquals(4 * 1_048_576L, settings.resourceBudget().get("java.scratch.pool.bytes"));
            assertEquals(4 * 1_048_576L, settings.resourceBudget().get("java.interceptor.header.bytes"));
            assertThrows(UnsupportedOperationException.class, () -> settings.resourceBudget().clear());
        }
    }

    @Test void requestSizesPreserveBatchAndChunkRelationship() {
        NativeLibrary.load();
        for (int request : new int[] {512, 1024, 32_768, 1_048_576, 8_388_608}) {
            ProducerSettings settings = new ProducerSettings(config("max.request.size", request, "batch.size", 64));
            try (Arena arena = Arena.ofConfined()) {
                var c = settings.nativeConfig(arena);
                long payload = kr_producer_config.batch_hard_bytes(c);
                long chunk = kr_producer_config.output_chunk_bytes(c);
                assertTrue(payload > 64 && chunk >= 61 && chunk <= payload + 61);
                assertTrue(1 + (payload + chunk - 1) / chunk <= 3);
                assertTrue(kr_producer_config.compressed_bytes(c) >= payload + 61);
                assertTrue(kr_producer_config.progressive_threshold(c) <= payload);
            }
        }
    }

    @Test void batchTargetModeDefaultsToWireAndRawRemainsExplicit() {
        NativeLibrary.load();
        for (String mode : new String[] {"estimated-wire", "raw"}) {
            try (Arena arena = Arena.ofConfined()) {
                var settings = new ProducerSettings(config("kr.batch.target.mode", mode));
                assertEquals(mode.equals("raw") ? 1 : 0,
                    kr_producer_config.batch_target_mode(settings.nativeConfig(arena)));
            }
        }
        assertThrows(ConfigException.class,
            () -> new ProducerSettings(config("kr.batch.target.mode", "unknown")));
    }

    @Test void unsupportedKnownAndUnknownNativeOptionsFail() {
        for (String key : new String[] {"transactional.id", "transaction.timeout.ms", "partitioner.ignore.keys",
            "partitioner.adaptive.partitioning.enable", "ssl.keystore.location", "sasl.login.callback.handler.class",
            "ssl.protocol", "reconnect.backoff.ms", "kr.input_bytes", "kr.tls.server.name", "enable.metrics.push"}) {
            assertThrows(ConfigException.class, () -> new ProducerSettings(config(key, "x")), key);
        }
        assertDoesNotThrow(() -> new ProducerSettings(config("my.serializer.option", "allowed")));
    }

    @Test void requestBatchingPolicyHasIndependentExplicitOptions() {
        NativeLibrary.load();
        String[] policies = {"sealed", "single-partition", "broker-ready"};
        for (int i = 0; i < policies.length; i++) {
            try (Arena arena = Arena.ofConfined()) {
                var settings = new ProducerSettings(config("kr.request.batching.policy", policies[i]));
                assertEquals(i, kr_producer_config.request_batching_policy(settings.nativeConfig(arena)));
            }
        }
        assertThrows(ConfigException.class,
            () -> new ProducerSettings(config("kr.request.batching.policy", "unknown")));
    }

    @Test void numericAndProfileErrorsAreNeverSilentlyTruncated() {
        Object[][] bad = {{"acks", "1"}, {"enable.idempotence", false}, {"retries", 0}, {"retries", 255},
            {"retries", Integer.MAX_VALUE}, {"retries", 1.5}, {"batch.size", 0},
            {"max.block.ms", Long.MAX_VALUE}, {"max.block.ms", -1}, {"linger.ms", Long.MAX_VALUE},
            {"max.in.flight.requests.per.connection", 6}, {"compression.type", "gzip"},
            {"compression.zstd.level", -5}, {"compression.zstd.level", 4}, {"security.protocol", "SASL_PLAINTEXT"},
            {"kr.input.mode", "native"}, {"kr.transport", "auto"}, {"kr.scratch.bytes", 0},
            {"kr.interceptor.header.bytes", -1}, {"kr.interceptor.header.bytes", Long.MAX_VALUE},
            {"ssl.endpoint.identification.algorithm", ""}, {"delivery.timeout.ms", 30_000},
            {"retry.backoff.ms", 1001}, {"batch.size", 1_048_576}};
        for (Object[] pair : bad) assertThrows(ConfigException.class,
            () -> new ProducerSettings(config(pair[0], pair[1])), pair[0].toString());
        assertEquals(0, new ProducerSettings(config("max.block.ms", 0)).maxBlockNanos);
    }

    @Test void endpointAndUtf8Validation() {
        assertDoesNotThrow(() -> new ProducerSettings(config("bootstrap.servers", "[::1]:9092, example.com:9093")));
        for (String value : new String[] {"", "::1:9092", "[::1]", "x:0", "x:65536", "x:1,", "x/y:9092", "\0x:1"})
            assertThrows(ConfigException.class, () -> new ProducerSettings(config("bootstrap.servers", value)), value);
        assertThrows(ConfigException.class, () -> new ProducerSettings(config("client.id", "\ud800")));
        assertThrows(ConfigException.class, () -> new ProducerSettings(config("client.id", "é".repeat(16_384))));
    }

    @Test void staticJaasGrammarAndSecretsStayRedacted() {
        String jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"SECRET\";";
        assertDoesNotThrow(() -> new ProducerSettings(config("security.protocol", "SASL_SSL", "sasl.mechanism", "PLAIN", "sasl.jaas.config", jaas)));
        for (String value : new String[] {jaas + jaas, jaas.replace("required", "optional"),
            jaas.replace(";", " custom=\"SECRET\";"), jaas.replace("PlainLoginModule", "EvilLoginModule"),
            jaas.replace("username=\"alice\"", "username=alice"), jaas.replace("password=", "username=")}) {
            var error = assertThrows(ConfigException.class, () -> new ProducerSettings(config(
                "security.protocol", "SASL_SSL", "sasl.mechanism", "PLAIN", "sasl.jaas.config", value)));
            assertFalse(error.toString().contains("SECRET"));
        }
        assertThrows(ConfigException.class, () -> new ProducerSettings(config("security.protocol", "SASL_SSL",
            "sasl.mechanism", "PLAIN", "sasl.jaas.config", jaas, "kr.sasl.username", "conflict")));
        assertThrows(ConfigException.class, () -> new ProducerSettings(config("security.protocol", "SASL_SSL")));
    }

    @Test void javaDefaultTrustIsExplicitDerAndNeverNativeSystemRoots() {
        NativeLibrary.load();
        var settings = new ProducerSettings(config("security.protocol", "SSL"));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment c = settings.nativeConfig(arena);
            assertEquals(0, kr_producer_config.tls_system_roots(c));
            assertTrue(kr_producer_config.tls_root_count(c) > 0);
            assertEquals(0, kr_span.len(kr_producer_config.tls_server_name(c)));
            var first = kr_span.asSlice(kr_producer_config.tls_roots(c), 0);
            assertEquals(0x30, Byte.toUnsignedInt(kr_span.ptr(first).get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0)));
        }
    }

    @Test void restrictedPemJksAndPkcs12StoresContainOnlySuppliedRoots() throws Exception {
        NativeLibrary.load();
        var factory = javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
        factory.init((java.security.KeyStore) null);
        var manager = (javax.net.ssl.X509TrustManager) factory.getTrustManagers()[0];
        var certificate = manager.getAcceptedIssuers()[0];
        byte[] der = certificate.getEncoded();
        String pem = "-----BEGIN CERTIFICATE-----\n" + java.util.Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(der)
            + "\n-----END CERTIFICATE-----\n";
        var directory = java.nio.file.Files.createTempDirectory("kr-java-trust-");
        try {
            for (String type : new String[] {"PEM", "JKS", "PKCS12"}) {
                var path = directory.resolve("trust." + type);
                Map<String, Object> config = config("security.protocol", "SSL", "ssl.truststore.type", type,
                    "ssl.truststore.location", path.toString());
                if (type.equals("PEM")) java.nio.file.Files.writeString(path, pem);
                else {
                    var store = java.security.KeyStore.getInstance(type);
                    store.load(null, null); store.setCertificateEntry("only-root", certificate);
                    try (var output = java.nio.file.Files.newOutputStream(path)) { store.store(output, "fixture-password".toCharArray()); }
                    config.put("ssl.truststore.password", "fixture-password");
                }
                var settings = new ProducerSettings(config);
                try (Arena arena = Arena.ofConfined()) {
                    var c = settings.nativeConfig(arena);
                    assertEquals(0, kr_producer_config.tls_system_roots(c));
                    assertEquals(1, kr_producer_config.tls_root_count(c));
                    var root = kr_span.asSlice(kr_producer_config.tls_roots(c), 0);
                    assertArrayEquals(der, kr_span.ptr(root).asSlice(0, kr_span.len(root)).toArray(java.lang.foreign.ValueLayout.JAVA_BYTE));
                }
                java.nio.file.Files.delete(path);
            }
            assertDoesNotThrow(() -> new ProducerSettings(config("security.protocol", "SSL", "ssl.truststore.type", "PEM",
                "ssl.truststore.certificates", new org.apache.kafka.common.config.types.Password(pem))));
            assertThrows(ConfigException.class, () -> new ProducerSettings(config("security.protocol", "SSL",
                "ssl.truststore.type", "PEM", "ssl.truststore.certificates", pem, "ssl.truststore.password", "secret")));
        } finally { java.nio.file.Files.delete(directory); }
    }
}
