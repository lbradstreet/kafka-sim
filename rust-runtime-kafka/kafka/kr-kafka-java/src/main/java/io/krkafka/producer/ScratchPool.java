package io.krkafka.producer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Iterator;

/** Shared arenas migrate between callers; checkout and accounting use callGate. */
final class ScratchPool implements AutoCloseable {
    static final class Slab implements AutoCloseable {
        final Arena arena = Arena.ofShared();
        final MemorySegment memory;
        boolean inUse;
        Slab(long bytes) {
            try { memory = arena.allocate(bytes, 8); }
            catch (Throwable failure) { arena.close(); throw failure; }
        }
        @Override public void close() { arena.close(); }
    }

    private final long limit;
    private final int checkouts;
    private final ArrayList<Slab> slabs = new ArrayList<>();
    private long allocated;
    private int active;

    ScratchPool(long limit, int checkouts) {
        if (limit <= 0 || checkouts <= 0) throw new IllegalArgumentException("invalid scratch limits");
        this.limit = limit;
        this.checkouts = checkouts;
    }

    Slab acquire(long size) {
        if (size <= 0 || size > limit) throw new IllegalArgumentException("record exceeds scratch byte limit");
        if (active == checkouts) return null;
        Slab best = null;
        for (Slab slab : slabs)
            if (!slab.inUse && slab.memory.byteSize() >= size &&
                    (best == null || slab.memory.byteSize() < best.memory.byteSize())) best = slab;
        if (best == null) {
            Iterator<Slab> iterator = slabs.iterator();
            while (allocated + size > limit && iterator.hasNext()) {
                Slab slab = iterator.next();
                if (slab.inUse) continue;
                allocated -= slab.memory.byteSize();
                iterator.remove();
                slab.close();
            }
            if (allocated + size > limit) return null;
            best = new Slab(size);
            slabs.add(best);
            allocated += size;
        }
        best.inUse = true;
        active++;
        return best;
    }

    void release(Slab slab) {
        if (!slab.inUse) throw new IllegalStateException("scratch double release");
        slab.inUse = false;
        active--;
    }

    long allocatedBytes() { return allocated; }
    int active() { return active; }
    @Override public void close() {
        if (active != 0) throw new IllegalStateException("closing scratch with active writers");
        for (Slab slab : slabs) slab.close();
        slabs.clear();
        allocated = 0;
    }
}
