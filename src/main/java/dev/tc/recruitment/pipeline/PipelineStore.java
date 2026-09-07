package dev.tc.recruitment.pipeline;

import dev.tc.recruitment.common.Json;
import dev.tc.recruitment.common.PermanentFailure;
import dev.tc.recruitment.common.event.EventEnvelope;
import java.util.*;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/** Deterministic persistence boundary used by workflow services, never by the LLM. */
@Service
public class PipelineStore {
    private final DatabaseClient db;
    private final Json json;
    public PipelineStore(DatabaseClient db, Json json) { this.db = db; this.json = json; }
    public DatabaseClient.GenericExecuteSpec sql(String query, Object... parameters) {
        var spec = db.sql(query);
        for (int i = 0; i < parameters.length; i += 2) {
            Object value = parameters[i + 1];
            spec = value == null ? spec.bindNull((String) parameters[i], String.class)
                    : spec.bind((String) parameters[i], value);
        }
        return spec;
    }
    public Mono<Void> execute(String query, Object... parameters) {
        return sql(query, parameters).fetch().rowsUpdated().then();
    }
    public Mono<Map<String,Object>> application(UUID id) {
        return sql("""
                SELECT a.*,j.title AS job_title,j.description AS job_description,j.required_skills,
                j.preferred_skills,j.minimum_experience_years,c.profile_json
                FROM application a JOIN job j ON j.id=a.job_id LEFT JOIN candidate c ON c.id=a.candidate_id
                WHERE a.id=:id
                """, "id", id).fetch().one().switchIfEmpty(Mono.error(new PermanentFailure("MISSING_APPLICATION")));
    }
    public Mono<Void> event(String type, UUID candidateId, UUID applicationId, Map<String,Object> data) {
        var envelope = EventEnvelope.create(type, candidateId == null ? applicationId : candidateId,
                applicationId, "recruitment-assistant", data);
        return execute("""
                INSERT INTO outbox_event(id,event_id,aggregate_type,aggregate_id,event_type,payload,status,retry_count,created_time)
                VALUES(:id,:event,'Application',:aggregate,:type,:payload,'PENDING',0,now())
                """, "id", UUID.randomUUID(), "event", envelope.eventId(), "aggregate", envelope.aggregateId(),
                "type", type, "payload", json.write(envelope));
    }
    public Mono<Boolean> processed(String consumer, UUID eventId) {
        return sql("SELECT 1 FROM processed_event WHERE consumer_name=:c AND event_id=:e", "c", consumer, "e", eventId)
                .fetch().one().hasElement();
    }
    /** Must be called inside the same transaction as the business writes. */
    public Mono<Boolean> claim(String consumer, UUID eventId) {
        return sql("""
                INSERT INTO processed_event(consumer_name,event_id,processed_time) VALUES(:c,:e,now())
                ON CONFLICT DO NOTHING
                """, "c", consumer, "e", eventId).fetch().rowsUpdated().map(count -> count == 1);
    }
    public static UUID id(Map<String,Object> row, String field) { return (UUID) row.get(field); }
    public static String text(Map<String,Object> row, String field) {
        Object value = row.get(field); return value == null ? "" : value.toString();
    }
}
