package dev.tc.recruitment.agent.extraction;

import dev.tc.recruitment.agent.StructuredAgentService;
import dev.tc.recruitment.agent.model.CandidateProfile;
import dev.tc.recruitment.common.Json;
import dev.tc.recruitment.common.PermanentFailure;
import java.util.UUID;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class ResumeExtractionAgent {
    public static final String PROMPT_VERSION = "resume-extraction-v1";
    private final StructuredAgentService agents;
    private final Json json;
    public ResumeExtractionAgent(StructuredAgentService agents, Json json) { this.agents = agents; this.json = json; }
    public Mono<CandidateProfile> extract(ExtractionInput input) { return extract(UUID.randomUUID(), input); }
    public Mono<CandidateProfile> extract(UUID application, ExtractionInput input) {
        return agents.run("RESUME_EXTRACTION", application, PROMPT_VERSION, json.write(input), CandidateProfile.class, candidate -> {
            if (candidate == null || !Double.isFinite(candidate.experienceYears()) || candidate.experienceYears() < 0 ||
                    candidate.experienceYears() > 80 || candidate.skills() == null || candidate.education() == null ||
                    candidate.workExperience() == null || candidate.projects() == null)
                throw new PermanentFailure("INVALID_CANDIDATE_PROFILE");
            if (candidate.name() != null && candidate.name().length() > 200 ||
                    candidate.email() != null && candidate.email().length() > 320 ||
                    candidate.phone() != null && candidate.phone().length() > 64 ||
                    candidate.currentCompany() != null && candidate.currentCompany().length() > 200 ||
                    candidate.currentTitle() != null && candidate.currentTitle().length() > 200)
                throw new PermanentFailure("INVALID_CANDIDATE_FIELD_LENGTH");
        });
    }
    public record ExtractionInput(String mailSubject, String mailBody, String resumeContent) {}
}
