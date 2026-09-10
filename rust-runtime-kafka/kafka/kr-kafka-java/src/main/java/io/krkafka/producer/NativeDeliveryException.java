package io.krkafka.producer;

import org.apache.kafka.common.KafkaException;

/** Structured native delivery diagnostics, also attached to mapped Kafka exceptions. */
public final class NativeDeliveryException extends KafkaException {
    private final int outcome;
    private final int reason;
    private final int attempts;

    public NativeDeliveryException(int outcome, int reason, int attempts) {
        super("Native delivery outcome=" + outcome + ", reason=" + reason + ", attempts=" + attempts);
        this.outcome = outcome;
        this.reason = reason;
        this.attempts = attempts;
    }

    public int outcome() { return outcome; }
    public int reason() { return reason; }
    public int attempts() { return attempts; }
}
