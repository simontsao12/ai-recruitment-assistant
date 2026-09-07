package dev.tc.recruitment.kafka;

import tools.jackson.databind.ObjectMapper;
import dev.tc.recruitment.pipeline.CandidateWorkflow;
import dev.tc.recruitment.common.config.ProcessingTimeouts;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CandidateConsumers {
    private final CandidateWorkflow workflow;
    private final ObjectMapper mapper;
    private final ProcessingTimeouts timeouts;
    public CandidateConsumers(CandidateWorkflow workflow, ObjectMapper mapper, ProcessingTimeouts timeouts) {
        this.workflow = workflow; this.mapper = mapper; this.timeouts = timeouts;
    }
    @KafkaListener(topics="mail.received", groupId="resume-extraction")
    public void extract(ConsumerRecord<String,String> record) {
        var event = PipelineEvent.parse(mapper, record.value(), record.topic());
        // Intentional blocking on the dedicated Kafka listener thread, never on Netty.
        workflow.extract(event.eventId(), event.applicationId()).block(timeouts.workflow());
    }
    @KafkaListener(topics={"candidate.extracted","candidate.review.requested"}, groupId="candidate-review")
    public void review(ConsumerRecord<String,String> record) {
        var event = PipelineEvent.parse(mapper, record.value(), record.topic());
        workflow.review(event.eventId(), event.applicationId()).block(timeouts.workflow());
    }
}
