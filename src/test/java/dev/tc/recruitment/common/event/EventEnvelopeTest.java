package dev.tc.recruitment.common.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EventEnvelopeTest {
    @Test
    void createsTraceableVersionedEvent() {
        UUID aggregateId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();

        EventEnvelope<Map<String, String>> event = EventEnvelope.create(
                "mail.received", aggregateId, correlationId, "mail-ingestion", Map.of("messageId", "m-1"));

        assertThat(event.eventId()).isNotNull();
        assertThat(event.eventVersion()).isEqualTo(1);
        assertThat(event.aggregateId()).isEqualTo(aggregateId);
        assertThat(event.correlationId()).isEqualTo(correlationId);
        assertThat(event.occurredAt()).isNotNull();
    }
}
