package dev.tc.recruitment.kafka;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import dev.tc.recruitment.common.PermanentFailure;
import java.time.Instant;
import java.util.UUID;

public record PipelineEvent(UUID eventId, UUID applicationId, UUID reviewId) {
    public static PipelineEvent parse(ObjectMapper mapper, String raw, String topic) {
        try {
            JsonNode envelope = mapper.readTree(raw);
            if (envelope.path("eventVersion").asInt() != 1 || !topic.equals(envelope.path("eventType").asText()))
                throw new IllegalArgumentException();
            UUID event = UUID.fromString(envelope.path("eventId").asText());
            UUID application = UUID.fromString(envelope.path("data").path("applicationId").asText());
            if (!application.equals(UUID.fromString(envelope.path("correlationId").asText())))
                throw new IllegalArgumentException();
            UUID.fromString(envelope.path("aggregateId").asText());
            Instant.parse(envelope.path("occurredAt").asText());
            if (envelope.path("producer").asText().isBlank()) throw new IllegalArgumentException();
            UUID review = envelope.path("data").hasNonNull("reviewId")
                    ? UUID.fromString(envelope.path("data").path("reviewId").asText()) : null;
            if ("candidate.reviewed".equals(topic) && review == null) throw new IllegalArgumentException();
            return new PipelineEvent(event, application, review);
        } catch (Exception e) { throw new PermanentFailure("INVALID_EVENT_SCHEMA"); }
    }
}
