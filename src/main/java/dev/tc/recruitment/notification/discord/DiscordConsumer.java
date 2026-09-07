package dev.tc.recruitment.notification.discord;

import tools.jackson.databind.ObjectMapper;
import dev.tc.recruitment.agent.model.*;
import dev.tc.recruitment.common.*;
import dev.tc.recruitment.kafka.PipelineEvent;
import dev.tc.recruitment.pipeline.PipelineStore;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

@Component
@ConditionalOnProperty(name="app.discord.enabled", havingValue="true")
public class DiscordConsumer {
    private final PipelineStore store;
    private final Json json;
    private final ObjectMapper mapper;
    private final DiscordNotifier discord;
    private final TransactionalOperator tx;
    public DiscordConsumer(PipelineStore store, Json json, ObjectMapper mapper, DiscordNotifier discord, TransactionalOperator tx) {
        this.store=store; this.json=json; this.mapper=mapper; this.discord=discord; this.tx=tx;
    }
    @KafkaListener(topics="candidate.reviewed", groupId="discord-notification")
    public void consume(ConsumerRecord<String,String> record) {
        var event = PipelineEvent.parse(mapper,record.value(),record.topic());
        deliver(event).block(Duration.ofMinutes(2));
    }
    private Mono<Void> deliver(PipelineEvent event) {
        return store.processed("discord",event.eventId()).flatMap(done -> done ? Mono.empty() :
            store.sql("""
                    SELECT r.*,a.candidate_profile_json,j.title AS job_title FROM review r
                    JOIN application a ON a.id=r.application_id JOIN job j ON j.id=r.job_id
                    WHERE r.id=:id AND r.application_id=:app
                    ""","id",event.reviewId(),"app",event.applicationId()).fetch().one()
                .switchIfEmpty(Mono.error(new PermanentFailure("MISSING_REVIEW")))
                .flatMap(row -> {
                    String recommendation=PipelineStore.text(row,"recommendation");
                    return store.execute("""
                            INSERT INTO notification(id,review_id,status,discord_channel_id)
                            VALUES(:id,:review,'PENDING',:channel) ON CONFLICT(review_id) DO NOTHING
                            ""","id",UUID.randomUUID(),"review",event.reviewId(),"channel",discord.channelId())
                        .then(store.sql("SELECT * FROM notification WHERE review_id=:id","id",event.reviewId()).fetch().one())
                        .flatMap(notification -> {
                            String status=PipelineStore.text(notification,"status");
                            if ("SENT".equals(status)) return store.claim("discord",event.eventId()).then();
                            if (!"PENDING".equals(status))
                                return Mono.error(new PermanentFailure("DISCORD_DELIVERY_REQUIRES_RECONCILIATION"));
                            if (!discord.channelId().equals(notification.get("discord_channel_id")))
                                return Mono.error(new PermanentFailure("DISCORD_CHANNEL_CHANGED"));
                            UUID id=PipelineStore.id(notification,"id");
                            return store.sql("UPDATE notification SET status='SENDING',updated_time=now() WHERE id=:id AND status='PENDING'",
                                    "id",id).fetch().rowsUpdated().flatMap(count -> {
                                if (count != 1) return Mono.error(new PermanentFailure("DISCORD_DELIVERY_REQUIRES_RECONCILIATION"));
                                CandidateProfile candidate=json.read(PipelineStore.text(row,"candidate_profile_json"),CandidateProfile.class);
                                ReviewResult result=new ReviewResult(((Number)row.get("score")).intValue(),
                                        ReviewResult.Recommendation.valueOf(recommendation),PipelineStore.text(row,"reason"),
                                        strings(row,"matched_skills"),strings(row,"missing_skills"),strings(row,"risk_flags"));
                                return discord.send(candidate,result,PipelineStore.text(row,"job_title"),id.toString().replace("-","").substring(0,24))
                                    .onErrorResume(error -> {
                                        // 429 is a definite refusal. Other failures may have happened after delivery.
                                        boolean rateLimited=error instanceof WebClientResponseException response && response.getStatusCode().value()==429;
                                        return store.execute("UPDATE notification SET status=:status,updated_time=now() WHERE id=:id",
                                                "status",rateLimited ? "PENDING" : "UNKNOWN","id",id)
                                                .then(Mono.error(rateLimited ? error : new PermanentFailure("DISCORD_DELIVERY_UNKNOWN")));
                                    })
                                    .flatMap(message -> store.execute("""
                                            UPDATE notification SET status='SENT',discord_message_id=:message,updated_time=now() WHERE id=:id
                                            ""","id",id,"message",message)
                                        .then(store.execute("""
                                                INSERT INTO discord_message_mapping(id,discord_message_id,discord_channel_id,candidate_id,application_id,job_id,review_id)
                                                VALUES(:id,:message,:channel,:candidate,:app,:job,:review) ON CONFLICT(review_id) DO NOTHING
                                                ""","id",UUID.randomUUID(),"message",message,"channel",discord.channelId(),
                                                "candidate",row.get("candidate_id"),"app",event.applicationId(),"job",row.get("job_id"),"review",event.reviewId()))
                                        .then(store.claim("discord",event.eventId())).then().as(tx::transactional));
                            });
                        });
                }));
    }
    private List<String> strings(Map<String,Object> row,String key) {
        return Arrays.asList(json.read(PipelineStore.text(row,key),String[].class));
    }
}
