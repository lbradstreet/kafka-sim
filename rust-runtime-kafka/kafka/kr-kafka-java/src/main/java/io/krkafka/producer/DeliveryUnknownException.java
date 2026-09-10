package io.krkafka.producer;

import org.apache.kafka.common.KafkaException;

/** The record may have been written. Application replay may create a duplicate. */
public final class DeliveryUnknownException extends KafkaException {
    private final int nativeReason;
    private final int attempts;

    public DeliveryUnknownException(String message, int nativeReason, int attempts) {
        super(message);
        this.nativeReason = nativeReason;
        this.attempts = attempts;
    }

    public int nativeReason() { return nativeReason; }
    public int attempts() { return attempts; }
}
