package io.krkafka.producer;

import static org.junit.jupiter.api.Assertions.*;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.FutureTask;
import org.junit.jupiter.api.Test;

class ScratchPoolTest {
    @Test void boundsBytesCheckoutsAndEvictsOnlyUnusedSlabs() {
        try (ScratchPool pool = new ScratchPool(128, 2)) {
            var first = pool.acquire(48);
            var second = pool.acquire(48);
            assertNull(pool.acquire(1));
            pool.release(first);
            assertNull(pool.acquire(90));
            assertEquals(48, pool.allocatedBytes());
            assertThrows(IllegalStateException.class, pool::close);
            pool.release(second);
            var big = pool.acquire(128);
            assertEquals(128, pool.allocatedBytes());
            assertEquals(1, pool.active());
            pool.release(big);
            assertThrows(IllegalStateException.class, () -> pool.release(big));
            assertThrows(IllegalArgumentException.class, () -> pool.acquire(129));
        }
    }

    @Test void sharedScratchCanMoveBetweenPlatformAndVirtualCallers() throws Exception {
        try (ScratchPool pool = new ScratchPool(64, 1)) {
            var slab = pool.acquire(64);
            slab.memory.set(ValueLayout.JAVA_LONG, 0, 42);
            pool.release(slab);
            FutureTask<Long> task = new FutureTask<>(() -> {
                var next = pool.acquire(64);
                try { return next.memory.get(ValueLayout.JAVA_LONG, 0); }
                finally { pool.release(next); }
            });
            Thread.ofVirtual().start(task).join();
            assertEquals(42L, task.get());
        }
    }
}
