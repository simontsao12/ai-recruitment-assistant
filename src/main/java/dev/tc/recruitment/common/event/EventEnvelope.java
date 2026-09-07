package dev.tc.recruitment.common.event;

import java.time.Instant;
import java.util.UUID;

public record EventEnvelope<T>(
        UUID eventId,
        String eventType,
        int eventVersion,
        UUID aggregateId,
        UUID correlationId,
        Instant occurredAt,
        String producer,
        T data) {

    public static <T> EventEnvelope<T> create(String eventType, UUID aggregateId,
                                               UUID correlationId, String producer, T data) {
        return new EventEnvelope<>(UUID.randomUUID(), eventType, 1, aggregateId,
                correlationId, Instant.now(), producer, data);
    }
}
