package dev.tc.recruitment.agent.web;

import dev.tc.recruitment.agent.extraction.ResumeExtractionAgent;
import dev.tc.recruitment.agent.model.CandidateProfile;
import dev.tc.recruitment.agent.model.ReviewResult;
import dev.tc.recruitment.agent.review.CandidateReviewAgent;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/demo")
public class AgentDemoController {
    private final ResumeExtractionAgent extractionAgent;
    private final CandidateReviewAgent reviewAgent;

    public AgentDemoController(ResumeExtractionAgent extractionAgent, CandidateReviewAgent reviewAgent) {
        this.extractionAgent = extractionAgent;
        this.reviewAgent = reviewAgent;
    }

    @PostMapping("/review")
    Mono<DemoResult> review(@Valid @RequestBody DemoRequest request) {
        return extractionAgent.extract(new ResumeExtractionAgent.ExtractionInput(
                        request.mailSubject(), request.mailBody(), request.resumeContent()))
                .flatMap(candidate -> reviewAgent.review(request.jobDescription(), candidate)
                        .map(review -> new DemoResult(candidate, review)));
    }

    public record DemoRequest(@NotBlank @Size(max=30000) String jobDescription, @NotBlank @Size(max=1000) String mailSubject,
                              @NotBlank @Size(max=30000) String mailBody, @NotBlank @Size(max=150000) String resumeContent) {
    }

    public record DemoResult(CandidateProfile candidate, ReviewResult review) {
    }
}
