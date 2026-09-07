package dev.tc.recruitment.gmail;

import dev.tc.recruitment.auth.EncryptedAuthorizedClientService;
import dev.tc.recruitment.pipeline.PipelineStore;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.security.Principal;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

/** MVP: one Gmail search and one destination job per signed-in HR. */
@RestController
@RequestMapping("/api/gmail/subscription")
public class GmailSubscriptionController {
    private final PipelineStore store;
    private final EncryptedAuthorizedClientService clients;
    public GmailSubscriptionController(PipelineStore store, EncryptedAuthorizedClientService clients) {
        this.store=store; this.clients=clients;
    }
    @GetMapping
    Mono<Map<String,Object>> get(Principal principal) {
        return store.sql("SELECT job_id,query,enabled FROM gmail_subscription WHERE principal_name=:p",
                "p",principal.getName()).fetch().one()
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND)));
    }
    @PutMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    Mono<Void> save(Principal principal,@Valid @RequestBody Subscription request) {
        return clients.userId(principal.getName())
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED)))
                .flatMap(owner -> store.sql("""
                        INSERT INTO gmail_subscription(principal_name,user_id,job_id,query,enabled)
                        SELECT :p,:owner,id,:query,:enabled FROM job WHERE id=:job AND created_by=:owner AND status='OPEN'
                        ON CONFLICT(principal_name) DO UPDATE SET job_id=excluded.job_id,query=excluded.query,enabled=excluded.enabled
                        ""","p",principal.getName(),"owner",owner,"job",request.jobId(),"query",request.query(),"enabled",request.enabled())
                        .fetch().rowsUpdated().flatMap(count -> count==1 ? Mono.empty()
                                : Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND,"Owned open job required"))));
    }
    public record Subscription(@NotNull UUID jobId,@NotBlank @Size(max=1000) String query,boolean enabled) {}
}
