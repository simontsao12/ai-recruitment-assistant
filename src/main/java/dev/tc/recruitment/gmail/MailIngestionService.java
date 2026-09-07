package dev.tc.recruitment.gmail;

import dev.tc.recruitment.common.PermanentFailure;
import dev.tc.recruitment.pipeline.PipelineStore;
import dev.tc.recruitment.resume.ResumeStorage;
import java.util.*;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class MailIngestionService {
    private final PipelineStore store;
    private final GoogleGmailClient gmail;
    private final ResumeStorage storage;
    private final TransactionalOperator tx;
    public MailIngestionService(PipelineStore store, GoogleGmailClient gmail, ResumeStorage storage, TransactionalOperator tx) {
        this.store = store; this.gmail = gmail; this.storage = storage; this.tx = tx;
    }
    public Mono<Void> ingest(OAuth2AuthorizedClient auth, UUID owner, UUID job, String messageId) {
        return store.sql("SELECT id FROM application WHERE gmail_message_id=:id", "id", messageId).fetch().one()
                .hasElement().flatMap(exists -> exists ? Mono.empty() :
                    gmail.read(auth, messageId).flatMap(mail -> {
                        var supported = mail.attachments().stream().filter(attachment ->
                                attachment.fileName().toLowerCase(Locale.ROOT).matches(".*\\.(pdf|docx|doc|txt)$")).toList();
                        if (supported.isEmpty()) return Mono.error(new PermanentFailure("MISSING_SUPPORTED_RESUME"));
                        if (mail.attachments().stream().mapToLong(GoogleGmailClient.Attachment::size).sum() > 20 * 1024 * 1024)
                            return Mono.error(new PermanentFailure("MAIL_ATTACHMENTS_TOO_LARGE"));
                        return Flux.fromIterable(supported).concatMap(attachment -> {
                            UUID id = UUID.randomUUID();
                            return gmail.download(auth, messageId, attachment).flatMap(bytes -> storage.store(id, bytes))
                                    .map(reference -> new SavedResume(id, attachment.fileName(), attachment.contentType(), reference));
                        }).collectList().flatMap(resumes -> save(owner, job, mail, resumes));
                    }).onErrorResume(PermanentFailure.class, error -> failed(owner, job, messageId, error.getMessage())));
    }
    private Mono<Void> save(UUID owner, UUID job, GoogleGmailClient.Mail mail, List<SavedResume> resumes) {
        UUID application = UUID.randomUUID();
        return store.sql("""
                INSERT INTO application(id,job_id,owner_id,source,gmail_message_id,mail_subject,mail_body,status,received_time,created_time,updated_time)
                SELECT :id,id,:owner,'GMAIL',:mail,:subject,:body,'RECEIVED',now(),now(),now()
                FROM job WHERE id=:job AND created_by=:owner AND status='OPEN'
                ON CONFLICT(gmail_message_id) DO NOTHING
                """, "id", application, "job", job, "owner", owner, "mail", mail.id(),
                "subject", mail.subject(), "body", mail.body()).fetch().rowsUpdated().flatMap(count -> {
                    if (count == 0) return Mono.empty();
                    return Flux.fromIterable(resumes).concatMap(resume -> store.execute("""
                            INSERT INTO resume(id,application_id,file_name,content_type,storage_reference,parse_status,created_time)
                            VALUES(:id,:app,:name,:type,:reference,'PENDING',now())
                            """, "id", resume.id(), "app", application, "name", resume.fileName(),
                            "type", resume.contentType(), "reference", resume.reference()))
                            .then(store.event("mail.received", null, application, Map.of("applicationId", application)));
                }).as(tx::transactional);
    }
    private Mono<Void> failed(UUID owner, UUID job, String message, String code) {
        return store.execute("""
                INSERT INTO application(id,job_id,owner_id,source,gmail_message_id,status,failure_code,received_time,created_time,updated_time)
                VALUES(:id,:job,:owner,'GMAIL',:mail,'PROCESSING_FAILED',:code,now(),now(),now())
                ON CONFLICT(gmail_message_id) DO NOTHING
                """, "id", UUID.randomUUID(), "job", job, "owner", owner, "mail", message, "code", code);
    }
    private record SavedResume(UUID id, String fileName, String contentType, String reference) {}
}
