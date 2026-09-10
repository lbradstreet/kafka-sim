package io.krkafka.producer;

import static org.junit.jupiter.api.Assertions.*;
import io.krkafka.ffi.kr_header;
import io.krkafka.ffi.kr_record;
import io.krkafka.ffi.kr_span;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class RecordConversionTest {
    @Test void copiedRecordPreservesNullEmptyDuplicateHeadersAndHints() {
        byte[] key = {};
        byte[] value = {1, 2, 3};
        byte[][] headerKeys = {"k".getBytes(StandardCharsets.UTF_8), "k".getBytes(StandardCharsets.UTF_8), {0}};
        byte[][] headerValues = {null, {}, {9}};
        long bytes = NativeAccess.recordBytes(key, value, headerKeys, headerValues);
        try (ScratchPool pool = new ScratchPool(bytes, 1)) {
            var slab = pool.acquire(bytes);
            MemorySegment record = NativeAccess.pack(slab.memory, 41, 3, 99, 0x100000007L, key, value, headerKeys, headerValues);
            assertEquals(kr_record.sizeof(), kr_record.struct_size(record));
            assertEquals(41, kr_record.topic(record));
            assertEquals(3, kr_record.partition_hint(record));
            assertEquals(-1, kr_record.lane_hint(record));
            assertEquals(0x100000007L, kr_record.user_token(record));
            assertEquals(99, kr_record.timestamp_ms(record));
            assertEquals(0, kr_record.delivery_timeout_ns(record));
            assertEquals(0, kr_record.key_is_null(record));
            assertEquals(0, kr_span.len(kr_record.key(record)));
            assertEquals(3, kr_record.header_count(record));
            value[0] = 88;
            assertArrayEquals(new byte[]{1, 2, 3}, payload(kr_record.value(record)));
            var headers = kr_record.headers(record).reinterpret(kr_header.sizeof() * 3);
            for (int i = 0; i < 3; i++) {
                var header = kr_header.asSlice(headers, i);
                assertEquals(kr_header.sizeof(), kr_header.struct_size(header));
                assertArrayEquals(headerKeys[i], payload(kr_header.key(header)));
                assertEquals(i == 0 ? 1 : 0, kr_header.value_is_null(header));
            }
            assertArrayEquals(new byte[0], payload(kr_header.value(kr_header.asSlice(headers, 1))));
            NativeAccess.pack(slab.memory, 41, -1, 99, 9, null, null, new byte[0][], new byte[0][]);
            assertEquals(1, kr_record.key_is_null(record));
            assertEquals(1, kr_record.value_is_null(record));
            assertEquals(-1, kr_record.partition_hint(record));
            pool.release(slab);
        }
    }

    private static byte[] payload(MemorySegment span) {
        int size = kr_span.len(span);
        return size == 0 ? new byte[0] : kr_span.ptr(span).reinterpret(size).toArray(ValueLayout.JAVA_BYTE);
    }
}
