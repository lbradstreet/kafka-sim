package io.krkafka.producer;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.internals.RecordHeaders;

/** Asynchronous interceptor context; every operation is confined to callGate. */
final class HeaderContextPool {
    record Context(RecordHeaders headers, long bytes) { }

    private final long capacity;
    private long used;

    HeaderContextPool(long capacity) { this.capacity = capacity; }

    Context acquire(byte[][] keys, byte[][] values, long bytes) {
        if (bytes > capacity - used) return null;
        RecordHeaders headers = new RecordHeaders();
        for (int i = 0; i < keys.length; i++) {
            headers.add(new String(keys[i], StandardCharsets.UTF_8), values[i] == null ? null : values[i].clone());
        }
        headers.setReadOnly();
        used += bytes;
        return new Context(headers, bytes);
    }

    void release(Context context) {
        if (context == null) return;
        if (context.bytes() > used) throw new IllegalStateException("Interceptor header accounting underflow");
        used -= context.bytes();
    }

    long used() { return used; }
}
