package io.krkafka.producer;

import static org.junit.jupiter.api.Assertions.*;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.*;
import org.junit.jupiter.api.Test;

class OutcomeTest {
    private static NativeAccess.Event event(int outcome, int reason) {
        return new NativeAccess.Event(1, 1, 1, 1, 0, outcome, reason, 101, true, 77, true, 3, 0);
    }

    @Test void allNativeUnknownReasonsPreserveCertaintyAndDiagnostics() {
        for (int reason = 0; reason <= 16; reason++) {
            var error = assertInstanceOf(DeliveryUnknownException.class, KrKafkaProducer.outcome(event(2, reason)));
            assertEquals(reason, error.nativeReason());
            assertEquals(3, error.attempts());
            var diagnostic = assertInstanceOf(NativeDeliveryException.class, error.getSuppressed()[0]);
            assertEquals(2, diagnostic.outcome());
            assertEquals(reason, diagnostic.reason());
        }
    }

    @Test void onlyProvenKafkaMappingsAreUsed() {
        assertNull(KrKafkaProducer.outcome(event(0, 0)));
        assertInstanceOf(TimeoutException.class, KrKafkaProducer.outcome(event(1, 1)));
        assertInstanceOf(RecordTooLargeException.class, KrKafkaProducer.outcome(event(1, 6)));
        assertInstanceOf(InvalidRecordException.class, KrKafkaProducer.outcome(event(1, 7)));
        assertInstanceOf(ProducerFencedException.class, KrKafkaProducer.outcome(event(1, 9)));
        assertInstanceOf(NetworkException.class, KrKafkaProducer.outcome(event(1, 11)));
        assertInstanceOf(BufferExhaustedException.class, KrKafkaProducer.outcome(event(1, 15)));
        assertInstanceOf(AuthenticationException.class, KrKafkaProducer.outcome(event(1, 16)));
        for (int reason : new int[]{0, 2, 3, 4, 5, 8, 10, 12, 13, 14})
            assertInstanceOf(NativeDeliveryException.class, KrKafkaProducer.outcome(event(1, reason)));
        assertThrows(IllegalStateException.class, () -> KrKafkaProducer.outcome(event(3, 0)));
        assertThrows(IllegalStateException.class, () -> KrKafkaProducer.outcome(event(0, 7)));
        assertThrows(IllegalStateException.class, () -> KrKafkaProducer.outcome(event(1, 17)));
    }
}
