/* Synthetic zstd-jni close-path probe, not a Kafka or CPU benchmark.
 * Run with JDK 25 and the frozen Kafka checkout's zstd-jni 1.5.6-10 jar.
 */
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

class CompressionCloseProbe {
    static byte[] compress(byte[] input, boolean buffered) throws Exception {
        var destination = new ByteArrayOutputStream();
        var codec = new ZstdOutputStreamNoFinalizer(destination, RecyclingBufferPool.INSTANCE, 1);
        // Kafka's wrapper drains its input buffer and calls codec.flush() before
        // codec.close(). Direct close sends the remaining input to frame end.
        try (OutputStream stream = buffered ? new BufferedOutputStream(codec, 16 * 1024) : codec) {
            for (int offset = 0; offset < input.length; offset += 16 * 1024)
                stream.write(input, offset, Math.min(16 * 1024, input.length - offset));
        }
        byte[] compressed = destination.toByteArray();
        if (!Arrays.equals(input, Zstd.decompress(compressed, input.length)))
            throw new IllegalStateException("Round-trip mismatch");
        return compressed;
    }

    public static void main(String[] args) throws Exception {
        byte[] unit = "event=payment region=ap-southeast-1 status=accepted\n"
            .getBytes(StandardCharsets.US_ASCII);
        System.out.println("input_bytes,buffered_close_bytes,direct_close_bytes,round_trip");
        for (int length : new int[] {4096, 65536, 262144}) {
            byte[] input = new byte[length];
            for (int i = 0; i < length; i++) input[i] = unit[i % unit.length];
            System.out.printf("%d,%d,%d,true%n", length,
                compress(input, true).length, compress(input, false).length);
        }
    }
}
