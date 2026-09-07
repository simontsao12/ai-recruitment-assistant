package dev.tc.recruitment.kafka;

import tools.jackson.databind.ObjectMapper;
import dev.tc.recruitment.common.PermanentFailure;
import dev.tc.recruitment.pipeline.PipelineStore;
import java.time.Duration;
import java.util.stream.Stream;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaConfig {
    @Bean
    KafkaAdmin.NewTopics pipelineTopics() {
        return new KafkaAdmin.NewTopics(Stream.of("mail.received","candidate.extracted","candidate.review.requested",
                "candidate.reviewed","candidate.feedback.received","candidate.preference.updated",
                "mail.received.dlq","candidate.extracted.dlq","candidate.review.requested.dlq",
                "candidate.reviewed.dlq","candidate.feedback.received.dlq")
                .map(name -> TopicBuilder.name(name).partitions(1).replicas(1).build())
                .toArray(org.apache.kafka.clients.admin.NewTopic[]::new));
    }
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String,String> template, PipelineStore store, ObjectMapper mapper) {
        var deadLetter = new DeadLetterPublishingRecoverer(template,
                (record, error) -> new TopicPartition(record.topic()+".dlq", record.partition()));
        deadLetter.setFailIfSendResultIsError(true);
        var backoff = new ExponentialBackOffWithMaxRetries(4);
        backoff.setInitialInterval(2000); backoff.setMultiplier(2); backoff.setMaxInterval(30000);
        var handler = new DefaultErrorHandler((record, error) -> {
            // Publish first: do not acknowledge the record if DLQ delivery failed.
            deadLetter.accept(record, error);
            PipelineEvent event;
            try { event = PipelineEvent.parse(mapper, (String)record.value(), record.topic()); }
            catch (PermanentFailure ignored) { return; }
            if (!"candidate.reviewed".equals(record.topic())) {
                store.execute("""
                        UPDATE application SET status='PROCESSING_FAILED',failure_code=:code,updated_time=now() WHERE id=:id
                        """, "id", event.applicationId(), "code", "DLQ_" + record.topic()).block(Duration.ofSeconds(15));
            }
        }, backoff);
        handler.addNotRetryableExceptions(PermanentFailure.class);
        return handler;
    }
}
