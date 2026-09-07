package dev.tc.recruitment.job;

import java.util.UUID;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface JobRepository extends ReactiveCrudRepository<Job, UUID> {
    reactor.core.publisher.Flux<Job> findByCreatedBy(UUID createdBy);
}
