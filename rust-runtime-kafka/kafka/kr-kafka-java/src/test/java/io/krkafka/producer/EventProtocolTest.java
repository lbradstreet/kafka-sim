package io.krkafka.producer;

import static org.junit.jupiter.api.Assertions.*;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

class EventProtocolTest {
    private static final Uuid ID = new Uuid(1, 2);

    private static NativeAccess.Event event(int kind, int outcome, int reason, int attempts) {
        return new NativeAccess.Event(kind, 1, 1, 1, 0, outcome, reason, 1, kind == 1,
                1, kind == 1, attempts, 1, ID);
    }

    @Test void everyUndefinedKindOutcomeReasonAndAttemptValueIsAProtocolFailure() {
        for (int kind : new int[]{Integer.MIN_VALUE, -1, 0, 2, 8, Integer.MAX_VALUE})
            assertThrows(NativeAccess.ProtocolFailure.class, () -> KrKafkaProducer.validateEvent(event(kind, 0, 0, 0)));
        for (int kind : new int[]{1, 3, 4, 5, 6, 7}) {
            for (int outcome : new int[]{Integer.MIN_VALUE, -1, 3, Integer.MAX_VALUE})
                assertThrows(NativeAccess.ProtocolFailure.class, () -> KrKafkaProducer.validateEvent(event(kind, outcome, 1, 1)));
            for (int reason : new int[]{Integer.MIN_VALUE, -1, 17, Integer.MAX_VALUE})
                assertThrows(NativeAccess.ProtocolFailure.class, () -> KrKafkaProducer.validateEvent(event(kind, 0, reason, 1)));
            for (int attempts : new int[]{Integer.MIN_VALUE, -1, 256, Integer.MAX_VALUE})
                assertThrows(NativeAccess.ProtocolFailure.class, () -> KrKafkaProducer.validateEvent(event(kind, 0, 1, attempts)));
        }
        for (int reason = 0; reason <= 16; reason++) {
            int knownReason = reason;
            assertDoesNotThrow(() -> KrKafkaProducer.validateEvent(event(1, 2, knownReason, 255)));
        }
        for (int kind : new int[]{3, 4, 6}) assertDoesNotThrow(() -> KrKafkaProducer.validateEvent(event(kind, 0, 0, 0)));
        for (int kind : new int[]{5, 7}) assertDoesNotThrow(() -> KrKafkaProducer.validateEvent(event(kind, 0, 4, 0)));
    }

    @Test void topicStatusEnumsRejectUnknownValuesBeforePublication() {
        for (int status = 0; status <= 6; status++) {
            for (int reason = 0; reason <= 16; reason++) NativeAccess.validateTopicStatus(status, reason);
            int knownStatus = status;
            assertThrows(NativeAccess.ProtocolFailure.class, () -> NativeAccess.validateTopicStatus(knownStatus, -1));
            assertThrows(NativeAccess.ProtocolFailure.class, () -> NativeAccess.validateTopicStatus(knownStatus, 17));
        }
        for (int status : new int[]{Integer.MIN_VALUE, -1, 7, Integer.MAX_VALUE})
            assertThrows(NativeAccess.ProtocolFailure.class, () -> NativeAccess.validateTopicStatus(status, 0));
    }

    @Test void omittedCorrelationAndNonDeliveryOutcomeFieldsCannotPassAsValidEvents() {
        assertThrows(NativeAccess.ProtocolFailure.class, () -> KrKafkaProducer.validateEvent(
                new NativeAccess.Event(1, 0, 1, 1, 0, 0, 0, 1, true, 1, true, 1, 0, ID)));
        assertThrows(NativeAccess.ProtocolFailure.class, () -> KrKafkaProducer.validateEvent(
                new NativeAccess.Event(1, 1, 0, 1, 0, 0, 0, 1, true, 1, true, 1, 0, ID)));
        assertThrows(NativeAccess.ProtocolFailure.class, () -> KrKafkaProducer.validateEvent(
                new NativeAccess.Event(1, 1, 1, 1, 0, 0, 0, 1, true, 1, true, 1, 0, Uuid.ZERO_UUID)));
        assertThrows(NativeAccess.ProtocolFailure.class, () -> KrKafkaProducer.validateEvent(event(3, 1, 0, 0)));
        assertThrows(NativeAccess.ProtocolFailure.class, () -> KrKafkaProducer.validateEvent(event(5, 0, 0, 0)));
        assertThrows(NativeAccess.ProtocolFailure.class, () -> KrKafkaProducer.validateEvent(event(7, 0, 0, 0)));
    }
}
