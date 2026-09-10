package io.krkafka.producer;

import io.krkafka.ffi.kr_broker;
import io.krkafka.ffi.kr_producer_config;
import io.krkafka.ffi.kr_span;
import static io.krkafka.ffi.kr_kafka_h.kr_producer_config_init;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

/** Explicit supported profile; no reflective mapping of arbitrary native fields. */
final class ProducerSettings {
    private static final Set<String> SUPPORTED = Set.of(
        "bootstrap.servers", "client.id", "acks", "enable.idempotence",
        "max.in.flight.requests.per.connection", "linger.ms", "batch.size", "max.request.size",
        "buffer.memory", "delivery.timeout.ms", "request.timeout.ms", "retry.backoff.ms",
        "retry.backoff.max.ms", "retries", "metadata.max.age.ms", "max.block.ms",
        "compression.type", "compression.zstd.level", "partitioner.class", "security.protocol",
        "sasl.mechanism", "sasl.jaas.config", "ssl.truststore.location", "ssl.truststore.password",
        "ssl.truststore.type", "ssl.truststore.certificates", "ssl.endpoint.identification.algorithm",
        "key.serializer", "value.serializer", "interceptor.classes");
    private static final Set<String> EXTENSIONS = Set.of(
        "kr.input.mode", "kr.transport", "kr.batch.target.mode", "kr.request.batching.policy", "kr.max.open.topics", "kr.record.descriptors",
        "kr.scratch.bytes", "kr.scratch.checkouts", "kr.serialization.concurrency", "kr.max.flushes",
        "kr.interceptor.header.bytes",
        "kr.max.header.count", "kr.max.completions.per.poll", "kr.sasl.username", "kr.sasl.password");
    final Map<String, Object> originals;
    final long maxBlockNanos, metadataMaxAgeNanos, scratchBytes, interceptorHeaderBytes;
    final int recordDescriptors, maxOpenTopics, maxCompletionsPerPoll, maxHeaderCount;
    final int maxRequestSize, scratchCheckouts, serializationConcurrency, maxFlushes;
    private final int requestBatchingPolicy, batchTargetMode, batchSize, retries, inFlight, compressionLevel, transport, security, mechanism;
    private final long inputBytes, deliveryNanos, requestNanos, lingerNanos, backoffNanos, backoffMaxNanos;
    private final byte[] clientId;
    private final List<Endpoint> bootstrap;
    private final String username, password;
    private final boolean compressed;
    private final List<byte[]> roots;
    private volatile Map<String, Long> memoryBudget = Map.of();
    private record Endpoint(byte[] host, int port) {}

    ProducerSettings(Map<String, ?> properties) {
        if (properties == null) throw new IllegalArgumentException("properties must not be null");
        Map<String, Object> copy = new LinkedHashMap<>();
        properties.forEach((key, value) -> {
            if (key == null) throw new ConfigException("Null configuration key");
            copy.put(key, value);
        });
        originals = Collections.unmodifiableMap(copy);
        for (String key : copy.keySet()) {
            if (key.startsWith("kr.") && !EXTENSIONS.contains(key)) throw invalid(key, "unknown native extension");
            if (!SUPPORTED.contains(key) && (ProducerConfig.configNames().contains(key)
                || key.startsWith("ssl.") || key.startsWith("sasl."))) {
                throw invalid(key, "unsupported by the native producer profile");
            }
        }
        String acks = text("acks", "all");
        if (!acks.equals("all") && !acks.equals("-1")) throw invalid("acks", "requires all/-1");
        if (!bool("enable.idempotence", true)) throw invalid("enable.idempotence", "must be true");
        if (!text("kr.input.mode", "copy").equals("copy")) throw invalid("kr.input.mode", "only copy is enabled");
        batchTargetMode = switch (text("kr.batch.target.mode", "estimated-wire")) {
            case "estimated-wire" -> 0;
            case "raw" -> 1;
            default -> throw invalid("kr.batch.target.mode", "requires estimated-wire or raw");
        };
        requestBatchingPolicy = switch (text("kr.request.batching.policy", "sealed")) {
            case "sealed" -> 0;
            case "single-partition" -> 1;
            case "broker-ready" -> 2;
            default -> throw invalid("kr.request.batching.policy", "requires sealed, single-partition or broker-ready");
        };
        inFlight = integer("max.in.flight.requests.per.connection", 5, 1, 5);
        maxBlockNanos = nanos("max.block.ms", 60_000, 0);
        metadataMaxAgeNanos = nanos("metadata.max.age.ms", 300_000, 1);
        deliveryNanos = nanos("delivery.timeout.ms", 120_000, 1);
        requestNanos = nanos("request.timeout.ms", 30_000, 1);
        if (requestNanos / 1_000_000 > Integer.MAX_VALUE) throw invalid("request.timeout.ms", "exceeds wire limit");
        lingerNanos = nanos("linger.ms", 5, 0);
        if (deliveryNanos < requestNanos || deliveryNanos - requestNanos < lingerNanos)
            throw invalid("delivery.timeout.ms", "must cover request.timeout.ms plus linger.ms");
        backoffNanos = nanos("retry.backoff.ms", 100, 0);
        backoffMaxNanos = nanos("retry.backoff.max.ms", 1_000, 0);
        if (backoffNanos > backoffMaxNanos) throw invalid("retry.backoff.ms", "exceeds retry.backoff.max.ms");
        retries = integer("retries", 254, 1, 254);
        maxRequestSize = integer("max.request.size", 1_048_576, 512, Integer.MAX_VALUE - 8);
        batchSize = integer("batch.size", 16_384, 1, maxRequestSize);
        inputBytes = number("buffer.memory", 33_554_432, 1, Long.MAX_VALUE);
        clientId = utf8(text("client.id", "kr-kafka"), "client.id", 32_767);
        if (batchSize > maxRequestSize - clientId.length - 381)
            throw invalid("batch.size", "exceeds the native request payload after framing");
        bootstrap = endpoints(value("bootstrap.servers", List.of()));
        String codec = text("compression.type", "none");
        if (!codec.equals("none") && !codec.equals("zstd")) throw invalid("compression.type", "only none and zstd are supported");
        compressed = codec.equals("zstd");
        compressionLevel = integer("compression.zstd.level", 3, 1, 3);
        transport = switch (text("kr.transport", "readiness")) {
            case "readiness" -> 1;
            case "uring" -> 0;
            default -> throw invalid("kr.transport", "requires readiness or uring; automatic fallback is unavailable");
        };
        recordDescriptors = integer("kr.record.descriptors", 65_536, 1, 1_048_576);
        maxOpenTopics = integer("kr.max.open.topics", 1024, 1, 65_536);
        maxHeaderCount = integer("kr.max.header.count", 1024, 0, 65_536);
        maxCompletionsPerPoll = integer("kr.max.completions.per.poll", 128, 1, 1024);
        scratchBytes = number("kr.scratch.bytes", 4 * 1_048_576, 128, Integer.MAX_VALUE);
        interceptorHeaderBytes = number("kr.interceptor.header.bytes", 4 * 1_048_576, 0, Integer.MAX_VALUE);
        scratchCheckouts = integer("kr.scratch.checkouts", 64, 1, 4096);
        serializationConcurrency = integer("kr.serialization.concurrency", 64, 1, 4096);
        maxFlushes = integer("kr.max.flushes", 64, 1, 4096);
        security = switch (text("security.protocol", "PLAINTEXT")) {
            case "PLAINTEXT" -> 0;
            case "SSL" -> 1;
            case "SASL_SSL" -> 2;
            default -> throw invalid("security.protocol", "requires PLAINTEXT, SSL or SASL_SSL");
        };
        if (!text("ssl.endpoint.identification.algorithm", "https").equalsIgnoreCase("https"))
            throw invalid("ssl.endpoint.identification.algorithm", "hostname verification must be https");
        if (security != 2 && copy.keySet().stream().anyMatch(k -> k.startsWith("sasl.") || k.startsWith("kr.sasl.")))
            throw invalid("security.protocol", "SASL settings require SASL_SSL");
        if (security == 0 && copy.keySet().stream().anyMatch(k -> k.startsWith("ssl.")))
            throw invalid("security.protocol", "TLS settings require SSL or SASL_SSL");
        mechanism = security == 2 ? switch (text("sasl.mechanism", "GSSAPI")) {
            case "PLAIN" -> 0;
            case "SCRAM-SHA-256" -> 1;
            case "SCRAM-SHA-512" -> 2;
            default -> throw invalid("sasl.mechanism", "requires PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512");
        } : 0;
        String[] credentials = security == 2 ? credentials() : new String[] {"", ""};
        username = credentials[0]; password = credentials[1];
        roots = security == 0 ? List.of() : trustRoots();
    }

    MemorySegment nativeConfig(Arena arena) {
        MemorySegment config = kr_producer_config.allocate(arena);
        int code = kr_producer_config_init(config, (int) kr_producer_config.sizeof());
        if (code != 0) throw new ConfigException("Native configuration initialization failed: " + code);
        kr_producer_config.client_id(config).copyFrom(span(arena, clientId));
        MemorySegment brokers = kr_broker.allocateArray(bootstrap.size(), arena);
        for (int i = 0; i < bootstrap.size(); i++) {
            MemorySegment broker = kr_broker.asSlice(brokers, i);
            kr_broker.struct_size(broker, (int) kr_broker.sizeof());
            kr_broker.host(broker).copyFrom(span(arena, bootstrap.get(i).host));
            kr_broker.port(broker, bootstrap.get(i).port);
        }
        kr_producer_config.bootstrap(config, brokers);
        kr_producer_config.bootstrap_count(config, bootstrap.size());
        kr_producer_config.max_in_flight_per_connection(config, inFlight);
        kr_producer_config.delivery_timeout_ns(config, deliveryNanos);
        kr_producer_config.request_timeout_ns(config, requestNanos);
        kr_producer_config.linger_max_ns(config, lingerNanos);
        kr_producer_config.metadata_max_age_ns(config, metadataMaxAgeNanos);
        kr_producer_config.retry_backoff_min_ns(config, backoffNanos);
        kr_producer_config.retry_backoff_max_ns(config, backoffMaxNanos);
        kr_producer_config.max_attempts(config, retries + 1);
        kr_producer_config.input_bytes(config, inputBytes);
        int payload = maxRequestSize - clientId.length - 381;
        kr_producer_config.batch_target_bytes(config, batchSize);
        kr_producer_config.batch_target_mode(config, batchTargetMode);
        kr_producer_config.request_batching_policy(config, requestBatchingPolicy);
        kr_producer_config.batch_hard_bytes(config, payload);
        kr_producer_config.request_target_bytes(config, maxRequestSize);
        kr_producer_config.request_hard_bytes(config, maxRequestSize);
        kr_producer_config.output_chunk_bytes(config, Math.max(61, (payload + 1) / 2));
        kr_producer_config.progressive_threshold(config, Math.min(16_384, payload));
        kr_producer_config.compressed_bytes(config, Math.max(kr_producer_config.compressed_bytes(config), (long) payload + 61));
        kr_producer_config.record_descriptors(config, recordDescriptors);
        kr_producer_config.delivery_event_capacity(config, recordDescriptors);
        kr_producer_config.pending_records_per_topic(config, recordDescriptors);
        kr_producer_config.max_open_topics(config, maxOpenTopics);
        kr_producer_config.max_header_count(config, maxHeaderCount);
        kr_producer_config.max_completions_per_poll(config, maxCompletionsPerPoll);
        kr_producer_config.compression(config, compressed ? 1 : 0);
        kr_producer_config.compression_level(config, compressionLevel);
        kr_producer_config.transport(config, transport);
        kr_producer_config.partitioner(config, originals.get("partitioner.class") == null ? 0 : 1);
        kr_producer_config.security(config, security);
        kr_producer_config.sasl_mechanism(config, mechanism);
        kr_producer_config.tls_system_roots(config, 0);
        // Always verify the actual broker endpoint. No bootstrap/global override.
        kr_producer_config.tls_server_name(config).fill((byte) 0);
        if (!roots.isEmpty()) {
            MemorySegment certs = kr_span.allocateArray(roots.size(), arena);
            for (int i = 0; i < roots.size(); i++) kr_span.asSlice(certs, i).copyFrom(span(arena, roots.get(i)));
            kr_producer_config.tls_roots(config, certs);
            kr_producer_config.tls_root_count(config, roots.size());
        }
        kr_producer_config.username(config).copyFrom(span(arena, utf8(username, "SASL username", 4096)));
        kr_producer_config.password(config).copyFrom(span(arena, utf8(password, "SASL password", 4096)));
        Map<String, Long> budget = new LinkedHashMap<>();
        budget.put("native.input.pool.bytes", inputBytes);
        budget.put("native.compressed.pool.bytes", kr_producer_config.compressed_bytes(config));
        budget.put("native.control.pool.bytes", kr_producer_config.control_reserve_bytes(config));
        budget.put("native.codec.workspace.bytes.per.context", kr_producer_config.codec_workspace_bytes(config));
        budget.put("native.codec.contexts", (long) kr_producer_config.codec_contexts(config));
        budget.put("native.connection.staging.bytes", (long) kr_producer_config.staging_bytes_per_connection(config));
        budget.put("native.connection.receive.bytes", (long) kr_producer_config.rx_bytes_per_connection(config));
        budget.put("native.max.connections", (long) kr_producer_config.lanes(config) * kr_producer_config.brokers_max(config) + 2);
        memoryBudget = Map.copyOf(budget);
        return config;
    }

    /** Configured independent budgets, not a claim about total resident memory. */
    Map<String, Long> resourceBudget() {
        Map<String, Long> budget = new LinkedHashMap<>(memoryBudget);
        budget.put("java.interceptor.header.bytes", interceptorHeaderBytes);
        budget.put("java.scratch.pool.bytes", scratchBytes);
        budget.put("java.scratch.checkouts", (long) scratchCheckouts);
        budget.put("java.active.serializations", (long) serializationConcurrency);
        budget.put("java.serialized.payload.bound.bytes", (long) serializationConcurrency * maxRequestSize);
        budget.put("java.pending.records", (long) recordDescriptors);
        budget.put("java.topic.entries", (long) maxOpenTopics);
        budget.put("java.flush.entries", (long) maxFlushes);
        return Map.copyOf(budget);
    }

    static MemorySegment span(Arena arena, byte[] bytes) {
        MemorySegment span = kr_span.allocate(arena);
        if (bytes.length > 0) {
            MemorySegment data = arena.allocate(bytes.length);
            data.copyFrom(MemorySegment.ofArray(bytes));
            kr_span.ptr(span, data);
        }
        kr_span.len(span, bytes.length);
        return span;
    }

    static byte[] utf8(String text, String key, int maximum) {
        if (text.indexOf('\0') >= 0) throw invalid(key, "NUL is unsupported");
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(text));
            if (encoded.remaining() > maximum) throw invalid(key, "UTF-8 byte limit exceeded");
            byte[] result = new byte[encoded.remaining()]; encoded.get(result); return result;
        } catch (CharacterCodingException error) { throw invalid(key, "invalid Unicode"); }
    }

    private List<Endpoint> endpoints(Object configured) {
        List<?> entries = configured instanceof List<?> list ? list : Arrays.asList(configured.toString().split(",", -1));
        if (entries.isEmpty() || entries.size() > 64) throw invalid("bootstrap.servers", "requires 1..64 endpoints");
        List<Endpoint> result = new ArrayList<>();
        for (Object entry : entries) {
            if (!(entry instanceof String)) throw invalid("bootstrap.servers", "endpoint must be a string");
            if (((String) entry).indexOf('\0') >= 0) throw invalid("bootstrap.servers", "NUL is unsupported");
            String endpoint = ((String) entry).strip();
            String host, port;
            if (endpoint.startsWith("[")) {
                int end = endpoint.indexOf(']');
                if (end < 2 || end + 1 >= endpoint.length() || endpoint.charAt(end + 1) != ':')
                    throw invalid("bootstrap.servers", "invalid bracketed IPv6 endpoint");
                host = endpoint.substring(1, end); port = endpoint.substring(end + 2);
                if (host.indexOf(':') < 0 || !host.matches("[0-9A-Fa-f:.]+"))
                    throw invalid("bootstrap.servers", "invalid IPv6 host");
                try { java.net.InetAddress.getByName(host); }
                catch (java.net.UnknownHostException error) { throw invalid("bootstrap.servers", "invalid IPv6 literal"); }
            } else {
                int colon = endpoint.indexOf(':');
                if (colon <= 0 || colon != endpoint.lastIndexOf(':')) throw invalid("bootstrap.servers", "requires host:port or [IPv6]:port");
                host = endpoint.substring(0, colon); port = endpoint.substring(colon + 1);
                if (host.chars().anyMatch(c -> Character.isWhitespace(c) || "/[]".indexOf(c) >= 0))
                    throw invalid("bootstrap.servers", "invalid host");
            }
            int number;
            if (!port.matches("[0-9]{1,5}")) throw invalid("bootstrap.servers", "invalid port");
            try { number = Integer.parseInt(port); }
            catch (NumberFormatException error) { throw invalid("bootstrap.servers", "invalid port"); }
            if (number < 1 || number > 65535) throw invalid("bootstrap.servers", "port outside 1..65535");
            result.add(new Endpoint(utf8(host, "bootstrap.servers", 253), number));
        }
        return List.copyOf(result);
    }

    private String[] credentials() {
        if (originals.containsKey("sasl.jaas.config")) {
            if (originals.containsKey("kr.sasl.username") || originals.containsKey("kr.sasl.password"))
                throw invalid("sasl.jaas.config", "conflicts with kr.sasl credentials");
            String module = mechanism == 0 ? "org.apache.kafka.common.security.plain.PlainLoginModule"
                : "org.apache.kafka.common.security.scram.ScramLoginModule";
            String jaas = text("sasl.jaas.config", "");
            Matcher outer = Pattern.compile("\\s*" + Pattern.quote(module) + "\\s+required\\s+(.+);\\s*", Pattern.DOTALL).matcher(jaas);
            if (!outer.matches()) throw invalid("sasl.jaas.config", "requires one supported static login module with required control flag");
            String options = outer.group(1);
            Matcher match = Pattern.compile("\\G\\s*(username|password)\\s*=\\s*\"((?:[^\"\\\\]|\\\\[\"\\\\])*)\"\\s*").matcher(options);
            Map<String, String> parsed = new LinkedHashMap<>();
            int end = 0;
            while (match.find()) {
                if (parsed.put(match.group(1), match.group(2).replace("\\\"", "\"").replace("\\\\", "\\")) != null)
                    throw invalid("sasl.jaas.config", "duplicate credential");
                end = match.end();
            }
            if (end != options.length() || parsed.size() != 2) throw invalid("sasl.jaas.config", "only quoted username and password options are supported");
            return checkedCredentials(parsed.get("username"), parsed.get("password"));
        }
        if (!originals.containsKey("kr.sasl.username") || !originals.containsKey("kr.sasl.password"))
            throw invalid("sasl.jaas.config", "static username and password are required");
        return checkedCredentials(text("kr.sasl.username", ""), text("kr.sasl.password", ""));
    }

    private String[] checkedCredentials(String user, String secret) {
        if (utf8(user, "SASL username", 4096).length == 0) throw invalid("SASL username", "must not be empty");
        utf8(secret, "SASL password", 4096);
        return new String[] {user, secret};
    }

    private List<byte[]> trustRoots() {
        char[] storePassword = text("ssl.truststore.password", "").toCharArray();
        try {
            List<Certificate> certificates = new ArrayList<>();
            String type = text("ssl.truststore.type", "JKS");
            if (!Set.of("PEM", "JKS", "PKCS12").contains(type)) throw invalid("ssl.truststore.type", "requires PEM, JKS or PKCS12");
            Object location = originals.get("ssl.truststore.location");
            Object inline = originals.get("ssl.truststore.certificates");
            if (inline != null && (location != null || !type.equals("PEM"))) throw invalid("ssl.truststore.certificates", "requires PEM without a truststore location");
            if (type.equals("PEM") && originals.containsKey("ssl.truststore.password")) throw invalid("ssl.truststore.password", "PEM truststores have no password");
            if (type.equals("PEM")) {
                byte[] pem;
                if (inline != null) pem = text("ssl.truststore.certificates", "").getBytes(StandardCharsets.US_ASCII);
                else if (location != null) pem = boundedFile(Path.of(location.toString()));
                else throw invalid("ssl.truststore.type", "PEM requires certificates or a location");
                certificates.addAll(CertificateFactory.getInstance("X.509").generateCertificates(new ByteArrayInputStream(pem)));
            } else if (location != null) {
                KeyStore store = KeyStore.getInstance(type);
                try (InputStream input = new ByteArrayInputStream(boundedFile(Path.of(location.toString())))) {
                    store.load(input, originals.containsKey("ssl.truststore.password") ? storePassword : null);
                }
                var aliases = store.aliases();
                while (aliases.hasMoreElements()) {
                    Certificate cert = store.getCertificate(aliases.nextElement());
                    if (cert != null) certificates.add(cert);
                }
            } else {
                if (originals.containsKey("ssl.truststore.password")) throw invalid("ssl.truststore.password", "requires a truststore location");
                TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                factory.init((KeyStore) null);
                for (var manager : factory.getTrustManagers()) if (manager instanceof X509TrustManager x509)
                    certificates.addAll(Arrays.asList(x509.getAcceptedIssuers()));
            }
            if (certificates.isEmpty() || certificates.size() > 1024) throw invalid("ssl.truststore", "requires 1..1024 certificates");
            List<byte[]> result = new ArrayList<>();
            long bytes = 0;
            for (Certificate certificate : certificates) {
                byte[] der = certificate.getEncoded(); bytes += der.length;
                if (der.length > 65_536 || bytes > 4 * 1_048_576) throw invalid("ssl.truststore", "certificate budget exceeded");
                result.add(der);
            }
            return List.copyOf(result);
        } catch (ConfigException error) { throw error; }
        catch (Exception error) {
            // Keystore/provider exception messages can contain filenames or credentials.
            throw invalid("ssl.truststore", "could not load X.509 trust certificates (" + error.getClass().getSimpleName() + ")");
        } finally { Arrays.fill(storePassword, '\0'); }
    }

    private static byte[] boundedFile(Path path) throws Exception {
        try (InputStream input = Files.newInputStream(path)) {
            byte[] bytes = input.readNBytes(4 * 1_048_576 + 1);
            if (bytes.length > 4 * 1_048_576) throw invalid("ssl.truststore", "file exceeds 4 MiB");
            return bytes;
        }
    }

    private Object value(String key, Object fallback) { Object value = originals.get(key); return value == null ? fallback : value; }
    private String text(String key, String fallback) {
        Object value = value(key, fallback);
        return value instanceof Password secret ? secret.value() : value.toString();
    }
    private boolean bool(String key, boolean fallback) {
        String value = text(key, Boolean.toString(fallback));
        if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) throw invalid(key, "requires a boolean");
        return Boolean.parseBoolean(value);
    }
    private int integer(String key, int fallback, int min, int max) { return (int) number(key, fallback, min, max); }
    private long number(String key, long fallback, long min, long max) {
        long result;
        try { result = Long.parseLong(text(key, Long.toString(fallback))); }
        catch (NumberFormatException error) { throw invalid(key, "requires an integer"); }
        if (result < min || result > max) throw invalid(key, "outside supported range " + min + ".." + max);
        return result;
    }
    private long nanos(String key, long fallback, long min) { return number(key, fallback, min, Long.MAX_VALUE / 1_000_000) * 1_000_000; }
    private static ConfigException invalid(String key, String why) { return new ConfigException(key + ": " + why); }
}
