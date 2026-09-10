import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.HexFormat;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;

/** Independent Apache Kafka Java magic-2 oracle; no Rust code is invoked. */
public final class Capture {
    static byte[] utf8(String text) { return text.getBytes(StandardCharsets.UTF_8); }
    static SimpleRecord[] records(int which) {
        return switch (which) {
            case 0 -> new SimpleRecord[] {
                new SimpleRecord(1700000000000L, null, utf8("hello")),
                new SimpleRecord(1700000000064L, new byte[0], null),
                new SimpleRecord(1699999999999L, utf8("κλειδί"), new byte[] {0, -1, 127}, new Header[] {
                    new RecordHeader("trace", utf8("α")), new RecordHeader("empty", new byte[0]), new RecordHeader("nil", null), new RecordHeader("trace", utf8("β"))
                })
            };
            case 1 -> new SimpleRecord[] { new SimpleRecord(0, null, (byte[])null), new SimpleRecord(Long.MAX_VALUE, new byte[0], new byte[0]) };
            case 2 -> new SimpleRecord[] {new SimpleRecord(23, utf8("a"), new byte[64]), new SimpleRecord(0, utf8("a"), new byte[128])};
            default -> throw new IllegalArgumentException();
        };
    }
    public static void main(String[] args) throws Exception {
        for (int c=0;c<3;c++) {
            MemoryRecords records=MemoryRecords.withIdempotentRecords(Compression.NONE,42L,(short)3,11,records(c));
            ByteBuffer buffer=records.buffer();byte[] bytes=new byte[buffer.remaining()];buffer.get(bytes);
            records.batches().forEach(batch -> batch.ensureValid());
            String captured=HexFormat.of().formatHex(bytes)+"\n";
            Path path=Path.of(args[1],"java-none-"+c+".hex");
            if(args[0].equals("--write")) Files.writeString(path,captured);
            else if(!Files.readString(path).equals(captured)) throw new AssertionError("fixture drift: "+path);
        }
    }
}
