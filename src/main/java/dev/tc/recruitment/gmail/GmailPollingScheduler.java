package dev.tc.recruitment.gmail;

import dev.tc.recruitment.pipeline.PipelineStore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.security.oauth2.client.*;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@ConditionalOnProperty(name="app.gmail.enabled", havingValue="true")
public class GmailPollingScheduler {
    private static final Logger log = LoggerFactory.getLogger(GmailPollingScheduler.class);
    private final PipelineStore store;
    private final ReactiveOAuth2AuthorizedClientManager manager;
    private final GoogleGmailClient gmail;
    private final MailIngestionService ingestion;
    private final AtomicBoolean running = new AtomicBoolean();
    public GmailPollingScheduler(PipelineStore store, ReactiveOAuth2AuthorizedClientManager manager,
                                 GoogleGmailClient gmail, MailIngestionService ingestion) {
        this.store = store; this.manager = manager; this.gmail = gmail; this.ingestion = ingestion;
    }
    @Scheduled(cron="${app.gmail.poll-cron}")
    public void poll() {
        if (!running.compareAndSet(false, true)) return;
        store.sql("""
                SELECT s.* FROM gmail_subscription s JOIN job j ON j.id=s.job_id
                WHERE s.enabled=true AND j.status='OPEN' AND j.created_by=s.user_id
                """).fetch().all().concatMap(subscription ->
                manager.authorize(OAuth2AuthorizeRequest.withClientRegistrationId("google")
                        .principal(PipelineStore.text(subscription,"principal_name")).build())
                    .switchIfEmpty(Mono.error(new IllegalStateException("GMAIL_REAUTH_REQUIRED")))
                    .flatMapMany(auth -> gmail.search(auth, PipelineStore.text(subscription,"query"))
                            .concatMap(mail -> ingestion.ingest(auth, PipelineStore.id(subscription,"user_id"),
                                    PipelineStore.id(subscription,"job_id"), mail.id())
                                .onErrorResume(error -> {
                                    log.warn("Gmail ingestion failed messageId={} errorType={}", mail.id(), error.getClass().getSimpleName());
                                    return Mono.empty();
                                })))
                    .onErrorResume(error -> {
                        log.warn("Gmail polling failed userId={} errorType={}",
                                subscription.get("user_id"), error.getClass().getSimpleName());
                        return Mono.empty();
                    }))
                .doFinally(signal -> running.set(false))
                .subscribe(ignored -> {}, error -> log.warn("Gmail scheduler failed errorType={}", error.getClass().getSimpleName()));
    }
}
