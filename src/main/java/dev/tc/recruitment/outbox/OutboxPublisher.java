package dev.tc.recruitment.outbox;

import dev.tc.recruitment.pipeline.PipelineStore;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private final PipelineStore store;
    private final KafkaTemplate<String,String> kafka;
    private final AtomicBoolean running = new AtomicBoolean();
    public OutboxPublisher(PipelineStore store, KafkaTemplate<String,String> kafka) { this.store = store; this.kafka = kafka; }
    @Scheduled(fixedDelayString="${app.outbox.publish-delay:2s}")
    public void publishPending() {
        if (!running.compareAndSet(false, true)) return;
        UUID lease = UUID.randomUUID();
        store.sql("""
                WITH batch AS (
                    SELECT id FROM outbox_event WHERE status='PENDING' AND next_attempt_time <= now()
                    AND (lease_until IS NULL OR lease_until < now()) ORDER BY created_time
                    FOR UPDATE SKIP LOCKED LIMIT 10
                )
                UPDATE outbox_event e SET lease_until=now()+interval '120 seconds',lease_token=:lease
                FROM batch WHERE e.id=batch.id RETURNING e.*
                """, "lease", lease).fetch().all().flatMap(event -> Mono.defer(() ->
                        Mono.fromFuture(kafka.send(PipelineStore.text(event,"event_type"),
                                PipelineStore.text(event,"aggregate_id"), PipelineStore.text(event,"payload"))))
                    .timeout(Duration.ofSeconds(20))
                    .then(store.execute("""
                            UPDATE outbox_event SET status='PUBLISHED',published_time=now(),lease_until=NULL,lease_token=NULL
                            WHERE id=:id AND lease_token=:lease
                            """, "id", event.get("id"), "lease", lease))
                    .onErrorResume(error -> {
                        log.warn("Outbox publish failed eventId={} errorType={}", event.get("event_id"), error.getClass().getSimpleName());
                        return store.execute("""
                                UPDATE outbox_event SET retry_count=retry_count+1,
                                status=CASE WHEN retry_count+1 >= 8 THEN 'FAILED' ELSE 'PENDING' END,
                                next_attempt_time=now()+make_interval(secs => LEAST(300,POWER(2,retry_count+1)::int)),
                                lease_until=NULL,lease_token=NULL WHERE id=:id AND lease_token=:lease
                                """, "id", event.get("id"), "lease", lease);
                    }), 2).doFinally(signal -> running.set(false))
                .subscribe(ignored -> {}, error -> log.warn("Outbox scan failed errorType={}", error.getClass().getSimpleName()));
    }
}
