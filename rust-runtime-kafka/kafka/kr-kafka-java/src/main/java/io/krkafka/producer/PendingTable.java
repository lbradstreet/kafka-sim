package io.krkafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.Uuid;

/** Confined to callGate. Only bounded interceptor headers may retain payload bytes. */
final class PendingTable {
    enum State { FREE, RESERVED, ACCEPTED, TERMINAL_DISPATCH, RETIRED }

    static final class Entry {
        final int index;
        int generation = 1;
        State state = State.FREE;
        DeliveryFuture future;
        Callback callback;
        HeaderContextPool.Context acknowledgementHeaders;
        String topic;
        int topicHandle;
        Uuid topicId;
        int keySize;
        int valueSize;
        long timestamp;

        Entry(int index) { this.index = index; }
        long token() { return ((long) generation << 32) | Integer.toUnsignedLong(index); }
    }

    private final Entry[] entries;
    private int next;
    private int used;

    PendingTable(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("pending capacity must be positive");
        entries = new Entry[capacity];
        for (int i = 0; i < capacity; i++) entries[i] = new Entry(i);
    }

    Entry reserve(DeliveryFuture future, Callback callback, String topic, int topicHandle,
                  int keySize, int valueSize, long timestamp) {
        return reserve(future, callback, topic, topicHandle, Uuid.ZERO_UUID, keySize, valueSize, timestamp);
    }

    Entry reserve(DeliveryFuture future, Callback callback, String topic, int topicHandle, Uuid topicId,
                  int keySize, int valueSize, long timestamp) {
        for (int scanned = 0; scanned < entries.length; scanned++) {
            Entry entry = entries[next];
            next = (next + 1) % entries.length;
            if (entry.state != State.FREE) continue;
            entry.future = future;
            entry.callback = callback;
            entry.topic = topic;
            entry.topicHandle = topicHandle;
            entry.topicId = topicId;
            entry.keySize = keySize;
            entry.valueSize = valueSize;
            entry.timestamp = timestamp;
            entry.state = State.RESERVED;
            used++;
            return entry;
        }
        return null;
    }

    void accept(Entry entry) {
        require(entry.state == State.RESERVED, "accepting an unreserved slot");
        entry.state = State.ACCEPTED;
    }

    Entry terminal(long token) {
        Entry entry = accepted(token);
        entry.state = State.TERMINAL_DISPATCH;
        return entry;
    }

    Entry accepted(long token) {
        long index = token & 0xffff_ffffL;
        require(index < entries.length, "delivery slot is outside the pending table");
        Entry entry = entries[(int) index];
        require(entry.token() == token && entry.state == State.ACCEPTED,
                "duplicate, stale, or unaccepted delivery token");
        return entry;
    }

    void release(Entry entry) {
        require(entry.state == State.RESERVED || entry.state == State.TERMINAL_DISPATCH,
                "releasing a slot with a native obligation");
        entry.future = null;
        entry.callback = null;
        entry.acknowledgementHeaders = null;
        entry.topic = null;
        entry.topicId = null;
        entry.state = entry.generation == -1 ? State.RETIRED : State.FREE;
        if (entry.state == State.FREE) entry.generation++;
        used--;
    }

    Entry[] entries() { return entries; }
    int used() { return used; }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
