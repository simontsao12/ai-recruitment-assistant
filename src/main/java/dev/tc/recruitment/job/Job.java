package dev.tc.recruitment.job;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("job")
public record Job(
        @Id UUID id,
        String title,
        String description,
        String department,
        String requiredSkills,
        String preferredSkills,
        BigDecimal minimumExperienceYears,
        String status,
        UUID createdBy,
        Instant createdTime,
        Instant updatedTime) {
}
