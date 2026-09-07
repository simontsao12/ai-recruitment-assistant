package dev.tc.recruitment.agent.review;

import dev.tc.recruitment.agent.StructuredAgentService;
import dev.tc.recruitment.agent.model.*;
import dev.tc.recruitment.agent.security.SensitiveDataSanitizer;
import dev.tc.recruitment.common.*;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class CandidateReviewAgent {
    public static final String PROMPT_VERSION = "candidate-review-v1";
    private final StructuredAgentService agents;
    private final Json json;
    private final SensitiveDataSanitizer sanitizer;
    public CandidateReviewAgent(StructuredAgentService agents, Json json, SensitiveDataSanitizer sanitizer) {
        this.agents = agents; this.json = json; this.sanitizer = sanitizer;
    }
    public Mono<ReviewResult> review(String description, CandidateProfile candidate) {
        return review(UUID.randomUUID(), description, candidate);
    }
    public Mono<ReviewResult> review(UUID application, String description, CandidateProfile candidate) {
        var anonymous = new CandidateProfile(null, null, null, candidate.currentCompany(), candidate.currentTitle(),
                candidate.experienceYears(), candidate.skills(), candidate.education(), candidate.workExperience(), candidate.projects());
        String context = json.write(Map.of("job", description, "candidate", anonymous));
        return agents.run("CANDIDATE_REVIEW", application, PROMPT_VERSION, sanitizer.sanitize(context), ReviewResult.class, result -> {
            if (result == null || result.score() < 0 || result.score() > 100 || result.recommendation() == null ||
                    result.reason() == null || result.reason().isBlank() || result.matchedSkills() == null ||
                    result.missingSkills() == null || result.riskFlags() == null)
                throw new PermanentFailure("INVALID_REVIEW_OUTPUT");
        });
    }
}
