package dev.tc.recruitment.agent.model;

import java.util.List;

public record CandidateProfile(
        String name,
        String email,
        String phone,
        String currentCompany,
        String currentTitle,
        double experienceYears,
        List<String> skills,
        List<String> education,
        List<String> workExperience,
        List<String> projects) {
}
