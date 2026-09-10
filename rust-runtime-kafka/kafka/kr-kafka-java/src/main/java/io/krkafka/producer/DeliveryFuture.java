package io.krkafka.producer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.RecordMetadata;

/** Package-private writable side; callers receive only the Future interface. */
final class DeliveryFuture implements Future<RecordMetadata> {
    private final CountDownLatch done = new CountDownLatch(1);
    private final Thread poller;
    private RecordMetadata metadata;
    private Exception failure;

    DeliveryFuture(Thread poller) { this.poller = poller; }

    void complete(RecordMetadata metadata, Exception failure) {
        this.metadata = metadata;
        this.failure = failure;
        done.countDown();
    }

    @Override public boolean cancel(boolean interrupt) { return false; }
    @Override public boolean isCancelled() { return false; }
    @Override public boolean isDone() { return done.getCount() == 0; }

    private void checkWait() {
        if (!isDone() && Thread.currentThread() == poller)
            throw new IllegalStateException("A producer callback cannot wait for pending delivery");
    }

    private RecordMetadata result() throws ExecutionException {
        if (failure != null) throw new ExecutionException(failure);
        return metadata;
    }

    @Override public RecordMetadata get() throws InterruptedException, ExecutionException {
        checkWait();
        done.await();
        return result();
    }

    @Override public RecordMetadata get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        checkWait();
        if (!done.await(timeout, unit)) throw new TimeoutException("Delivery is still pending");
        return result();
    }
}
