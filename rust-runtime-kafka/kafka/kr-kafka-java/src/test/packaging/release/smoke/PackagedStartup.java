package release.smoke;

import io.krkafka.producer.KrKafkaProducer;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Separate consumer module: exercises the public facade and implicit jar extraction. */
public final class PackagedStartup {
    public static void main(String[] args) {
        if (System.getProperty("kr.kafka.library") != null) {
            throw new AssertionError("Packaged verification requires implicit native extraction");
        }
        String source = KrKafkaProducer.class.getProtectionDomain().getCodeSource().getLocation().toString();
        if (!source.endsWith("-linux.jar")) throw new AssertionError("Expected release jar, got " + source);
        var producer = new KrKafkaProducer<byte[], byte[]>(
            Map.of("bootstrap.servers", "127.0.0.1:1"),
            new ByteArraySerializer(), new ByteArraySerializer());
        producer.close(Duration.ZERO);
        System.out.println("PACKAGED_OK arch=" + System.getProperty("os.arch")
            + " module=" + KrKafkaProducer.class.getModule().getName());
    }
}
