package dev.tc.recruitment.application;

import dev.tc.recruitment.auth.EncryptedAuthorizedClientService;
import dev.tc.recruitment.pipeline.PipelineStore;
import java.security.Principal;
import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/applications")
public class ApplicationController {
    private final PipelineStore store;
    private final EncryptedAuthorizedClientService clients;
    public ApplicationController(PipelineStore store,EncryptedAuthorizedClientService clients) { this.store=store; this.clients=clients; }
    @GetMapping
    Flux<Map<String,Object>> list(Principal principal) {
        return clients.userId(principal.getName()).flatMapMany(owner ->
                store.sql("""
                        SELECT id,candidate_id,job_id,status,failure_code,received_time,updated_time FROM application
                        WHERE owner_id=:owner ORDER BY created_time DESC LIMIT 100
                        ""","owner",owner).fetch().all());
    }
    @GetMapping("/{id}")
    Mono<Map<String,Object>> detail(Principal principal,@PathVariable UUID id) {
        return owned(principal,id);
    }
    @GetMapping("/{id}/reviews")
    Flux<Map<String,Object>> reviews(Principal principal,@PathVariable UUID id) {
        return owned(principal,id).flatMapMany(app -> store.sql("""
                SELECT r.*,n.status AS notification_status,n.discord_message_id
                FROM review r LEFT JOIN notification n ON n.review_id=r.id
                WHERE r.application_id=:id ORDER BY r.review_version DESC
                ""","id",id).fetch().all());
    }
    private Mono<Map<String,Object>> owned(Principal principal,UUID id) {
        return clients.userId(principal.getName()).flatMap(owner -> store.sql("""
                SELECT id,candidate_id,job_id,status,failure_code,candidate_profile_json,received_time,updated_time
                FROM application WHERE id=:id AND owner_id=:owner
                ""","id",id,"owner",owner).fetch().one())
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND)));
    }
}
