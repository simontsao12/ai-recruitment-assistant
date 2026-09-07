package dev.tc.recruitment.job;

import tools.jackson.databind.ObjectMapper;
import dev.tc.recruitment.auth.EncryptedAuthorizedClientService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.math.BigDecimal;
import java.security.Principal;
import java.time.Instant;
import java.util.*;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/jobs")
public class JobController {
    private final JobRepository repository;
    private final R2dbcEntityTemplate template;
    private final ObjectMapper mapper;
    private final EncryptedAuthorizedClientService clients;
    public JobController(JobRepository repository, R2dbcEntityTemplate template, ObjectMapper mapper,
                         EncryptedAuthorizedClientService clients) {
        this.repository=repository; this.template=template; this.mapper=mapper; this.clients=clients;
    }
    @GetMapping
    Flux<Job> list(Principal principal) {
        return clients.userId(principal.getName()).flatMapMany(repository::findByCreatedBy);
    }
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    Mono<Job> create(Principal principal, @Valid @RequestBody CreateJob request) {
        return clients.userId(principal.getName())
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED,"Google login required")))
                .flatMap(owner -> {
                    Instant now=Instant.now();
                    try {
                        // insert explicitly: assigned UUIDs must not be mistaken for existing rows.
                        return template.insert(new Job(UUID.randomUUID(),request.title(),request.description(),request.department(),
                                mapper.writeValueAsString(request.requiredSkills()),mapper.writeValueAsString(request.preferredSkills()),
                                request.minimumExperienceYears(),"OPEN",owner,now,now));
                    } catch (Exception error) { return Mono.error(new IllegalArgumentException("INVALID_JOB")); }
                });
    }
    public record CreateJob(@NotBlank @Size(max=200) String title, @NotBlank @Size(max=30000) String description,
                            @Size(max=200) String department, List<@NotBlank String> requiredSkills,
                            List<@NotBlank String> preferredSkills,
                            @DecimalMin("0.0") @DecimalMax("80.0") BigDecimal minimumExperienceYears) {
        public CreateJob {
            requiredSkills=requiredSkills==null ? List.of() : List.copyOf(requiredSkills);
            preferredSkills=preferredSkills==null ? List.of() : List.copyOf(preferredSkills);
        }
    }
}
