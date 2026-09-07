package dev.tc.recruitment.pipeline;

import dev.tc.recruitment.agent.extraction.ResumeExtractionAgent;
import dev.tc.recruitment.agent.review.CandidateReviewAgent;
import dev.tc.recruitment.agent.model.*;
import dev.tc.recruitment.common.*;
import dev.tc.recruitment.resume.ResumeStorage;
import java.math.BigDecimal;
import java.util.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class CandidateWorkflow {
    private final PipelineStore store;
    private final Json json;
    private final ResumeStorage storage;
    private final ResumeExtractionAgent extraction;
    private final CandidateReviewAgent review;
    private final TransactionalOperator tx;
    private final String model;
    public CandidateWorkflow(PipelineStore store, Json json, ResumeStorage storage, ResumeExtractionAgent extraction,
                             CandidateReviewAgent review, TransactionalOperator tx,
                             @Value("${spring.ai.ollama.chat.options.model}") String model) {
        this.store = store; this.json = json; this.storage = storage; this.extraction = extraction;
        this.review = review; this.tx = tx; this.model = model;
    }
    public Mono<Void> extract(UUID event, UUID application) {
        return store.processed("extraction", event).flatMap(done -> done ? Mono.empty() :
                store.application(application).flatMap(app ->
                    store.sql("SELECT * FROM resume WHERE application_id=:id ORDER BY id", "id", application).fetch().all()
                        .concatMap(row -> storage.parse(PipelineStore.text(row,"storage_reference"), PipelineStore.text(row,"file_name"))
                                .map(text -> new Parsed(PipelineStore.id(row,"id"), text))
                                .onErrorResume(PermanentFailure.class, error ->
                                        store.execute("UPDATE resume SET parse_status='FAILED' WHERE id=:id", "id", row.get("id"))
                                                .then(Mono.error(error))))
                        .collectList().flatMap(parsed -> {
                            if (parsed.isEmpty()) return Mono.error(new PermanentFailure("MISSING_RESUME"));
                            String text = String.join("\n\n", parsed.stream().map(Parsed::text).toList());
                            if (text.length() > 150_000) return Mono.error(new PermanentFailure("RESUME_TEXT_TOO_LARGE"));
                            return extraction.extract(application, new ResumeExtractionAgent.ExtractionInput(
                                    PipelineStore.text(app,"mail_subject"), PipelineStore.text(app,"mail_body"), text))
                                    .flatMap(candidate -> saveExtraction(event, application, candidate, parsed));
                        })));
    }
    private Mono<Void> saveExtraction(UUID event, UUID application, CandidateProfile profile, List<Parsed> parsed) {
        return store.claim("extraction", event).flatMap(claimed -> {
            if (!claimed) return Mono.empty();
            String email = profile.email() == null || profile.email().isBlank() ? null :
                    profile.email().strip().toLowerCase(Locale.ROOT);
            return store.sql("""
                    INSERT INTO candidate(id,name,email,phone,current_company,current_title,experience_years,skills,education,
                    profile_json,created_time,updated_time)
                    VALUES(:id,:name,:email,:phone,:company,:title,:years,:skills,:education,:profile,now(),now())
                    ON CONFLICT (lower(email)) WHERE email IS NOT NULL AND email <> ''
                    DO UPDATE SET updated_time=now()
                    RETURNING id
                    """, "id", UUID.randomUUID(), "name", profile.name(), "email", email, "phone", profile.phone(),
                    "company", profile.currentCompany(), "title", profile.currentTitle(),
                    "years", BigDecimal.valueOf(profile.experienceYears()), "skills", json.write(profile.skills()),
                    "education", json.write(profile.education()), "profile", json.write(profile))
                    .map((row, meta) -> row.get("id", UUID.class)).one()
                    .flatMap(candidate -> store.execute("""
                            UPDATE application SET candidate_id=:candidate,status='EXTRACTED',failure_code=NULL,
                            candidate_profile_json=:profile,updated_time=now() WHERE id=:id
                            """, "candidate", candidate, "profile", json.write(profile), "id", application)
                            .thenMany(Flux.fromIterable(parsed).concatMap(part -> store.execute("""
                                    UPDATE resume SET candidate_id=:candidate,parsed_content=:text,parse_status='PARSED' WHERE id=:id
                                    """, "candidate", candidate, "text", part.text(), "id", part.id())))
                            .then(store.event("candidate.extracted", candidate, application, Map.of("applicationId", application))));
        }).as(tx::transactional);
    }
    public Mono<Void> review(UUID event, UUID application) {
        return store.processed("review", event).flatMap(done -> done ? Mono.empty() :
                store.application(application).flatMap(app -> {
                    if (app.get("candidate_id") == null) return Mono.error(new PermanentFailure("MISSING_CANDIDATE"));
                    CandidateProfile profile = json.read(PipelineStore.text(app,"candidate_profile_json"), CandidateProfile.class);
                    String description = PipelineStore.text(app,"job_description") + "\nRequired skills: " +
                            PipelineStore.text(app,"required_skills") + "\nPreferred skills: " + PipelineStore.text(app,"preferred_skills") +
                            "\nMinimum experience years: " + PipelineStore.text(app,"minimum_experience_years");
                    return review.review(application, description, profile).flatMap(result -> saveReview(event, application, app, result));
                }));
    }
    private Mono<Void> saveReview(UUID event, UUID application, Map<String,Object> app, ReviewResult result) {
        UUID reviewId = UUID.randomUUID();
        UUID candidate = PipelineStore.id(app,"candidate_id");
        return store.claim("review", event).flatMap(claimed -> {
            if (!claimed) return Mono.empty();
            // Row lock serializes review version assignment across review requests.
            return store.sql("SELECT id FROM application WHERE id=:id FOR UPDATE", "id", application).fetch().one()
                .then(store.execute("""
                        INSERT INTO review(id,candidate_id,application_id,job_id,score,recommendation,reason,matched_skills,
                        missing_skills,risk_flags,model,prompt_version,review_version,created_time)
                        SELECT :id,:candidate,:app,:job,:score,:recommendation,:reason,:matched,:missing,:risks,:model,:prompt,
                        COALESCE(MAX(review_version),0)+1,now() FROM review WHERE application_id=:app
                        """, "id", reviewId, "candidate", candidate, "app", application, "job", PipelineStore.id(app,"job_id"),
                        "score", result.score(), "recommendation", result.recommendation().name(), "reason", result.reason(),
                        "matched", json.write(result.matchedSkills()), "missing", json.write(result.missingSkills()),
                        "risks", json.write(result.riskFlags()), "model", model, "prompt", CandidateReviewAgent.PROMPT_VERSION))
                .then(store.execute("UPDATE application SET status='REVIEWED',failure_code=NULL,updated_time=now() WHERE id=:id", "id", application))
                .then(store.event("candidate.reviewed", candidate, application,
                        Map.of("applicationId", application, "reviewId", reviewId)));
        }).as(tx::transactional);
    }
    private record Parsed(UUID id, String text) {}
}
