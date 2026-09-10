package io.krkafka.producer;

import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class DeliveryFutureTest {
    @Test void timeoutInterruptionAndCancellationDoNotCreateCompletion() throws Exception {
        DeliveryFuture future = new DeliveryFuture(null);
        assertFalse(future.cancel(true));
        assertFalse(future.isCancelled());
        assertThrows(TimeoutException.class, () -> future.get(0, TimeUnit.NANOSECONDS));
        Thread.currentThread().interrupt();
        assertThrows(InterruptedException.class, future::get);
        assertFalse(Thread.interrupted());
        assertFalse(future.isDone());
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("t", 0), 17, 0, 91, -1, 0);
        future.complete(metadata, null);
        assertSame(metadata, future.get());
        assertFalse(future.cancel(false));
    }

    @Test void pendingWaitOnPollerIsRejectedAndFailureIsPreserved() {
        DeliveryFuture future = new DeliveryFuture(Thread.currentThread());
        assertThrows(IllegalStateException.class, future::get);
        var error = new DeliveryUnknownException("uncertain", 12, 3);
        future.complete(null, error);
        assertSame(error, assertThrows(ExecutionException.class, future::get).getCause());
    }
}
