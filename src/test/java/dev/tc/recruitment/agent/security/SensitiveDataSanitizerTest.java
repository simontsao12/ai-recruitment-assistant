package dev.tc.recruitment.agent.security;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SensitiveDataSanitizerTest {
    private final SensitiveDataSanitizer sanitizer = new SensitiveDataSanitizer();

    @Test
    void redactsEmailTaiwanMobileAndIdentityNumber() {
        String result = sanitizer.sanitize(
                "Contact candidate@example.com or 0912-345-678, id A123456789");

        assertThat(result)
                .doesNotContain("candidate@example.com", "0912-345-678", "A123456789")
                .contains("[EMAIL_REDACTED]", "[PHONE_REDACTED]", "[ID_REDACTED]");
    }

    @Test
    void nullInputBecomesEmptyText() {
        assertThat(sanitizer.sanitize(null)).isEmpty();
    }
}
