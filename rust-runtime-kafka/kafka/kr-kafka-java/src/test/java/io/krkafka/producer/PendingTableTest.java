package io.krkafka.producer;

import static org.junit.jupiter.api.Assertions.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import org.junit.jupiter.api.Test;

class PendingTableTest {
    @Test void generatedHistoriesPreserveObligationsAndRejectStaleTokens() {
        for (long seed = 0; seed < 100; seed++) {
            Random random = new Random(seed);
            PendingTable table = new PendingTable(7);
            HashMap<Long, PendingTable.Entry> admitted = new HashMap<>();
            ArrayList<Long> retired = new ArrayList<>();
            for (int step = 0; step < 1000; step++) {
                switch (random.nextInt(4)) {
                    case 0, 1 -> {
                        PendingTable.Entry entry = table.reserve(new DeliveryFuture(null), null, "topic", 9, -1, 0, step);
                        if (entry == null) assertEquals(7, admitted.size());
                        else if (random.nextBoolean()) {
                            table.accept(entry);
                            assertNull(admitted.put(entry.token(), entry));
                        } else {
                            retired.add(entry.token());
                            table.release(entry);
                        }
                    }
                    case 2 -> {
                        if (!admitted.isEmpty()) {
                            long token = new ArrayList<>(admitted.keySet()).get(random.nextInt(admitted.size()));
                            PendingTable.Entry entry = table.terminal(token);
                            assertSame(admitted.remove(token), entry);
                            assertThrows(IllegalStateException.class, () -> table.terminal(token));
                            table.release(entry);
                            assertNull(entry.future);
                            assertNull(entry.topic);
                            retired.add(token);
                        }
                    }
                    case 3 -> {
                        if (!retired.isEmpty()) {
                            long token = retired.get(random.nextInt(retired.size()));
                            assertThrows(IllegalStateException.class, () -> table.terminal(token));
                        }
                    }
                }
                assertEquals(admitted.size(), table.used(), "seed=" + seed + " step=" + step);
                for (var entry : admitted.entrySet()) {
                    assertEquals(entry.getKey().longValue(), entry.getValue().token());
                    assertEquals(PendingTable.State.ACCEPTED, entry.getValue().state);
                }
            }
        }
    }

    @Test void generationExhaustionRetiresBeforeWrap() {
        PendingTable table = new PendingTable(1);
        PendingTable.Entry entry = table.entries()[0];
        entry.generation = -1;
        assertSame(entry, table.reserve(new DeliveryFuture(null), null, "t", 1, 0, 0, 0));
        long token = entry.token();
        table.accept(entry);
        table.terminal(token);
        table.release(entry);
        assertEquals(PendingTable.State.RETIRED, entry.state);
        assertNull(table.reserve(new DeliveryFuture(null), null, "t", 1, 0, 0, 0));
        assertThrows(IllegalStateException.class, () -> table.terminal(token));
        assertThrows(IllegalStateException.class, () -> table.terminal(0xffff_ffffL));
    }
}
