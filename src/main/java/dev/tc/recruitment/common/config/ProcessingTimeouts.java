package dev.tc.recruitment.common.config;

import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Shared budget for model execution, Kafka workflow and consumer polling. */
@Component
public class ProcessingTimeouts {
    private final Duration agent;
    private final Duration workflow;

    public ProcessingTimeouts(
            @Value("${app.processing.agent-timeout:10m}") Duration agent,
            @Value("${app.processing.workflow-timeout:12m}") Duration workflow,
            @Value("${spring.kafka.consumer.properties.max.poll.interval.ms:900000}") long maxPollIntervalMs) {
        if (agent.isNegative() || agent.isZero() || workflow.compareTo(agent) <= 0) {
            throw new IllegalArgumentException("Workflow timeout must exceed a positive agent timeout");
        }
        if (Duration.ofMillis(maxPollIntervalMs).compareTo(workflow.plusMinutes(1)) <= 0) {
            throw new IllegalArgumentException("Kafka max.poll.interval.ms must exceed workflow timeout by more than one minute");
        }
        this.agent = agent;
        this.workflow = workflow;
    }

    public Duration agent() { return agent; }
    public Duration workflow() { return workflow; }
}
