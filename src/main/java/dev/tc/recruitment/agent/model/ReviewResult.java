package dev.tc.recruitment.agent.model;

import java.util.List;

public record ReviewResult(
        int score,
        Recommendation recommendation,
        String reason,
        List<String> matchedSkills,
        List<String> missingSkills,
        List<String> riskFlags) {
    public enum Recommendation {
        HIGHLY_RECOMMENDED, RECOMMENDED, REVIEW_REQUIRED, NOT_RECOMMENDED
    }
}
