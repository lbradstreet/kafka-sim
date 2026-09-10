import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.AppInfoParser;

/** No throughput throttle tied to completions, no send/get or retry-on-full loop. */
public final class JavaOpenLoop {
    static final class Histogram {
        final long[] bins = new long[1025];
        long count, maximum;
        double total;
        synchronized void record(long value) {
            if (value < 0) throw new IllegalArgumentException("negative elapsed time");
            int exponent = 63 - Long.numberOfLeadingZeros(value);
            int index = value < 16 ? (int)value : 16 + (exponent - 4) * 16 + (int)((value - (1L << exponent)) >>> (exponent - 4));
            bins[index]++; count++; total += value; maximum = Math.max(maximum, value);
        }
        long quantile(int fraction) {
            long rank = (count * fraction + 999) / 1000, sum = 0;
            if (rank == 0) return 0;
            for (int index = 0; index < bins.length; index++) {
                sum += bins[index];
                if (sum >= rank) {
                    if (index < 16) return index;
                    int exponent = 4 + (index - 16) / 16, sub = (index - 16) % 16;
                    return Math.min(maximum, (1L << exponent) + (sub + 1L) * (1L << (exponent - 4)) - 1);
                }
            }
            throw new AssertionError("histogram count mismatch");
        }
        synchronized Map<String, Object> result() {
            return Map.of("count", count, "mean_ns", count == 0 ? 0 : total / count,
                "p50_ns_upper", quantile(500), "p99_ns_upper", quantile(990), "p999_ns_upper", quantile(999), "max_ns", maximum);
        }
    }
    static final class Counts {
        long accepted, acked, failed, completed;
        final Map<String, Long> rejected = new LinkedHashMap<>();
        synchronized void reject(Exception error) { rejected.merge(error.getClass().getSimpleName(), 1L, Long::sum); }
        synchronized void accept() { accepted++; }
        synchronized void delivered(Exception error) { completed++; if (error == null) acked++; else failed++; }
    }
    static long due(long index, long rate) {
        // Bounded profile limits index to 1e8, so multiplication cannot overflow.
        return index * 1_000_000_000L / rate;
    }
    static byte[][] corpus(int size, long seed, boolean incompressible) {
        if (size < 1 || size > 1024 * 1024) throw new IllegalArgumentException("payload bound");
        byte[][] result = new byte[Math.min(1024, Math.max(1, 16 * 1024 * 1024 / size))][size];
        long state = seed;
        for (byte[] row : result) for (int i = 0; i < size; i++) {
            state = state * 6364136223846793005L + 1442695040888963407L;
            row[i] = incompressible ? (byte)(state >>> 56) : (byte)'a';
        }
        return result;
    }
    static String quote(String value) {
        StringBuilder out = new StringBuilder("\"");
        for (char ch : value.toCharArray()) {
            if (ch == '"' || ch == '\\') out.append('\\').append(ch);
            else if (ch < 32) out.append(String.format("\\u%04x", (int)ch));
            else out.append(ch);
        }
        return out.append('"').toString();
    }
    static String json(Object value) {
        if (value == null) return "null";
        if (value instanceof String) return quote((String)value);
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof Map<?, ?> map) {
            StringBuilder out = new StringBuilder("{");
            for (var entry : map.entrySet()) {
                if (out.length() > 1) out.append(',');
                out.append(quote(entry.getKey().toString())).append(':').append(json(entry.getValue()));
            }
            return out.append('}').toString();
        }
        throw new IllegalArgumentException("unsupported JSON value");
    }
    static String requiredEnv(String key) {
        String value = System.getenv(key);
        if (value == null) throw new IllegalArgumentException("required credential environment variable is absent");
        return value;
    }
    static String jaasQuote(String value) { return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\""; }
    static long allocated(com.sun.management.ThreadMXBean threads) {
        if (!threads.isThreadAllocatedMemorySupported()) return -1;
        if (!threads.isThreadAllocatedMemoryEnabled()) threads.setThreadAllocatedMemoryEnabled(true);
        return threads.getTotalThreadAllocatedBytes();
    }
    static ProducerRecord<byte[], byte[]> record(String topic, String routing, int partitions, byte[][] payloads, long index) {
        Integer partition = switch (routing) {
            case "hot" -> 0;
            case "many" -> (int)(index % partitions);
            case "skewed", "unkeyed" -> null;
            default -> throw new IllegalArgumentException("unsupported routing profile");
        };
        byte[] key = routing.equals("skewed") ? ByteBuffer.allocate(8).putLong(index % 10 == 0 ? index : 0).array() : null;
        return new ProducerRecord<>(topic, partition, 0L, key, payloads[(int)(index % payloads.length)]);
    }
    static void warmup(Producer<byte[], byte[]> producer, String topic, String routing, int partitions,
                       byte[][] payloads, int records, long budgetNanos) throws Exception {
        long start = System.nanoTime();
        for (int index = 0; index < records; index++) {
            if (System.nanoTime() - start >= budgetNanos)
                throw new java.util.concurrent.TimeoutException("The complete warmup phase exceeded its delivery-timeout budget");
            var delivery = producer.send(record(topic, routing, partitions, payloads, index));
            long remaining = budgetNanos - (System.nanoTime() - start);
            if (remaining <= 0)
                throw new java.util.concurrent.TimeoutException("The complete warmup phase exceeded its delivery-timeout budget");
            delivery.get(remaining, java.util.concurrent.TimeUnit.NANOSECONDS);
        }
        // Every accepted warmup future has completed. No additional unbounded
        // flush wait is necessary before starting the measurement interval.
    }
    @SuppressWarnings("unchecked")
    static Producer<byte[], byte[]> producer(String name, Properties properties) throws Exception {
        if (name.equals("org.apache.kafka.clients.producer.KafkaProducer")) return new KafkaProducer<>(properties);
        if (!name.equals("io.krkafka.producer.KrKafkaProducer")) throw new IllegalArgumentException("unsupported producer_class");
        // Keep this driver compilable against the pinned Kafka jar alone. Native
        // runs add the binding jar and native-access option at launch.
        try {
            return (Producer<byte[], byte[]>)Class.forName(name).getConstructor(Properties.class).newInstance(properties);
        } catch (java.lang.reflect.InvocationTargetException failure) {
            if (failure.getCause() instanceof Exception cause) throw cause;
            throw failure;
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("--self-test")) {
            byte[] expected = {(byte)108, (byte)130, (byte)165, (byte)98, (byte)203, (byte)128, (byte)141, (byte)16};
            if (!java.util.Arrays.equals(corpus(8, 1, true)[0], expected)) throw new AssertionError("payload fixture");
            if (due(999, 3) != 333_000_000_000L) throw new AssertionError("fixed schedule");
            Histogram h = new Histogram(); for (int i = 0; i < 10000; i++) h.record(i);
            if (h.quantile(990) < 9899 || h.quantile(990) > 9999) throw new AssertionError("quantile");
            byte[][] payloads = corpus(8, 1, true);
            if (record("topic", "hot", 16, payloads, 9).partition() != 0) throw new AssertionError("hot routing");
            if (record("topic", "many", 16, payloads, 19).partition() != 3) throw new AssertionError("many routing");
            if (record("topic", "unkeyed", 16, payloads, 9).partition() != null) throw new AssertionError("unkeyed routing");
            if (ByteBuffer.wrap(record("topic", "skewed", 16, payloads, 10).key()).getLong() != 10 ||
                ByteBuffer.wrap(record("topic", "skewed", 16, payloads, 11).key()).getLong() != 0) throw new AssertionError("skewed routing");
            try (var reference = new org.apache.kafka.clients.producer.MockProducer<byte[], byte[]>(true, null,
                    new org.apache.kafka.common.serialization.ByteArraySerializer(), new org.apache.kafka.common.serialization.ByteArraySerializer())) {
                warmup(reference, "topic", "hot", 16, payloads, 8, 1_000_000_000L);
                if (reference.history().stream().anyMatch(record -> record.partition() != 0) || reference.history().size() != 8)
                    throw new AssertionError("warmup must use the measured route");
                try { warmup(reference, "topic", "hot", 16, payloads, 1, 0); throw new AssertionError("warmup budget"); }
                catch (java.util.concurrent.TimeoutException expectedTimeout) { }
                if (reference.history().size() != 8) throw new AssertionError("expired warmup must not submit");
            }
            System.out.println("Java open-loop primitive fixtures passed"); return;
        }
        if (args.length != 2) throw new IllegalArgumentException("usage: JavaOpenLoop PROFILE.properties RESULT.json");
        Properties profile = new Properties();
        try (var input = Files.newBufferedReader(Path.of(args[0]), StandardCharsets.UTF_8)) { profile.load(input); }
        long rate = Long.parseLong(profile.getProperty("rate")), records = Long.parseLong(profile.getProperty("records"));
        if (rate < 1 || rate > 1_000_000_000L || records < 1 || records > 100_000_000L) throw new IllegalArgumentException("load bound");
        String topic = profile.getProperty("topic"), routing = profile.getProperty("routing");
        int partitions = Integer.parseInt(profile.getProperty("partitions"));
        int size = Integer.parseInt(profile.getProperty("record_bytes"));
        int warmupRecords = Integer.parseInt(profile.getProperty("warmup_records", "0"));
        if (warmupRecords < 0 || warmupRecords > 100_000) throw new IllegalArgumentException("warmup record bound");
        long deliveryMs = Long.parseLong(profile.getProperty("delivery_timeout_ms"));
        Properties props = new Properties();
        for (String key : profile.stringPropertyNames()) if (key.startsWith("producer.")) props.put(key.substring(9), profile.getProperty(key));
        String security = profile.getProperty("security");
        if (!security.equals("plaintext") && !security.equals("tls")) {
            String mechanism = props.getProperty("sasl.mechanism");
            String module = mechanism.equals("PLAIN") ? "org.apache.kafka.common.security.plain.PlainLoginModule" : "org.apache.kafka.common.security.scram.ScramLoginModule";
            props.put("sasl.jaas.config", module + " required username=" + jaasQuote(requiredEnv(profile.getProperty("username_env"))) + " password=" + jaasQuote(requiredEnv(profile.getProperty("password_env"))) + ";");
        }
        byte[][] payloads = corpus(size, Long.parseUnsignedLong(profile.getProperty("seed")), profile.getProperty("pattern").equals("incompressible"));
        Counts counts = new Counts();
        Histogram late = new Histogram(), admission = new Histogram(), offerDelivery = new Histogram(), submitDelivery = new Histogram(), ackDelivery = new Histogram();
        Histogram offeredAdmission = new Histogram();
        OperatingSystemMXBean os = (OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean)ManagementFactory.getThreadMXBean();
        Map<String, Object> result = new LinkedHashMap<>();
        String producerClass = profile.getProperty("producer_class", "org.apache.kafka.clients.producer.KafkaProducer");
        boolean nativeBinding = producerClass.equals("io.krkafka.producer.KrKafkaProducer");
        try (Producer<byte[], byte[]> producer = producer(producerClass, props)) {
            long readyBy = System.nanoTime() + deliveryMs * 1_000_000L;
            while (true) {
                try {
                    if (producer.partitionsFor(topic).size() != partitions) throw new IllegalStateException("partition count differs from profile");
                    break;
                } catch (org.apache.kafka.common.errors.TimeoutException error) {
                    if (System.nanoTime() >= readyBy) throw error;
                    LockSupport.parkNanos(1_000_000);
                }
            }
            // Explicit startup/JIT warmup is outside every reported interval.
            // Serial completion here is not part of the open-loop workload.
            long warmupBudget = warmupRecords == 0 ? 0 : Math.multiplyExact(deliveryMs, 1_000_000L);
            long warmupStart = System.nanoTime();
            warmup(producer, topic, routing, partitions, payloads, warmupRecords, warmupBudget);
            long warmupElapsed = System.nanoTime() - warmupStart;
            long heapStart = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
            long allocationStart = allocated(threads);
            long cpuStart = os.getProcessCpuTime(), start = System.nanoTime();
            Thread offerThread = Thread.currentThread();
            for (long index = 0; index < records; index++) {
                final long planned = due(index, rate);
                long remaining;
                while ((remaining = planned - (System.nanoTime() - start)) > 0) LockSupport.parkNanos(Math.min(100_000, remaining));
                ProducerRecord<byte[], byte[]> record = record(topic, routing, partitions, payloads, index);
                final long submitted = System.nanoTime() - start;
                AtomicBoolean rejectedInline = new AtomicBoolean();
                late.record(submitted - planned);
                try {
                    producer.send(record, (metadata, error) -> {
                        // Pinned KafkaProducer.doSend invokes its ApiException
                        // callback inline for pre-admission failure (including full).
                        if (Thread.currentThread() == offerThread && error != null) {
                            rejectedInline.set(true); counts.reject(error); return;
                        }
                        long now = System.nanoTime() - start, elapsed = now - planned;
                        offerDelivery.record(elapsed); submitDelivery.record(now - submitted);
                        counts.delivered(error); if (error == null) ackDelivery.record(elapsed);
                    });
                    if (!rejectedInline.get()) { counts.accept(); offeredAdmission.record(System.nanoTime() - start - planned); }
                } catch (org.apache.kafka.common.KafkaException error) { counts.reject(error); }
                admission.record(System.nanoTime() - start - submitted);
            }
            long offeredElapsed = System.nanoTime() - start;
            producer.close(Duration.ofMillis(deliveryMs));
            long elapsed = System.nanoTime() - start, cpu = os.getProcessCpuTime() - cpuStart;
            long allocationEnd = allocated(threads);
            Long allocatedBytes = allocationStart < 0 || allocationEnd < allocationStart ? null : allocationEnd - allocationStart;
            result.put("schema", "kr-kafka-open-loop/v1"); result.put("implementation", nativeBinding ? "kr-kafka-java" : "kafka-java");
            result.put("producer_class", producerClass);
            result.put("warmup_records", warmupRecords);
            result.put("warmup_budget_ns", warmupBudget); result.put("warmup_elapsed_ns", warmupElapsed);
            result.put("heap_used_before_bytes", heapStart);
            result.put("heap_used_after_bytes", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
            result.put("java_allocated_bytes", allocatedBytes);
            result.put("java_allocation_bytes_per_second", allocatedBytes == null ? null : allocatedBytes * 1e9 / Math.max(1, elapsed));
            if (nativeBinding) result.put("configured_resource_budgets", producer.getClass().getMethod("resourceBudget").invoke(producer));
            result.put("native_version", nativeBinding ? "ABI2" : AppInfoParser.getVersion());
            result.put("native_commit", nativeBinding ? System.getProperty("kr.kafka.build.commit", "unrecorded") : AppInfoParser.getCommitId());
            result.put("complete", counts.completed == counts.accepted);
            result.put("offered", records); result.put("accepted", counts.accepted); result.put("rejected", records - counts.accepted);
            result.put("rejections_by_reason", counts.rejected); result.put("acked", counts.acked); result.put("failed_unclassified", counts.failed);
            result.put("unresolved", counts.accepted - counts.completed); result.put("offered_elapsed_ns", offeredElapsed); result.put("delivery_elapsed_ns", elapsed);
            result.put("acked_records_per_second", counts.acked * 1e9 / Math.max(1, elapsed)); result.put("acked_raw_bytes_per_second", counts.acked * (double)size * 1e9 / Math.max(1, elapsed));
            result.put("process_cpu_ns", cpu); result.put("cpu_ns_per_ack", counts.acked == 0 ? null : (double)cpu / counts.acked);
            result.put("scheduler_lateness", late.result()); result.put("admission_call", admission.result());
            result.put("offered_to_admission", offeredAdmission.result());
            result.put("offered_to_delivery", offerDelivery.result()); result.put("submit_to_delivery", submitDelivery.result()); result.put("offered_to_ack", ackDelivery.result());
            result.put("corpus_bytes", (long)payloads.length * size);
            result.put("unavailable_metrics", "not_written_vs_unknown,wire_bytes,copies,native_allocation_counts,codec_memory,per_poll_work");
        }
        Files.writeString(Path.of(args[1]), json(result), StandardCharsets.UTF_8);
        if (!Boolean.TRUE.equals(result.get("complete"))) System.exit(1);
    }
}
