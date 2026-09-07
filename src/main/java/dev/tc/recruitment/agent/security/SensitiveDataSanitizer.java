package dev.tc.recruitment.agent.security;

import org.springframework.stereotype.Component;

@Component
public class SensitiveDataSanitizer {
    private static final String EMAIL = "(?i)(?<![\\w.+-])[\\w.+-]+@[\\w.-]+\\.[a-z]{2,}(?![\\w.-])";
    private static final String PHONE = "(?<!\\d)(?:\\+?886[- ]?)?0?9\\d{2}[- ]?\\d{3}[- ]?\\d{3}(?!\\d)";
    private static final String TAIWAN_ID = "(?i)(?<![A-Z0-9])[A-Z][12]\\d{8}(?![A-Z0-9])";

    public String sanitize(String input) {
        if (input == null) return "";
        return input
                .replaceAll(EMAIL, "[EMAIL_REDACTED]")
                .replaceAll(PHONE, "[PHONE_REDACTED]")
                .replaceAll(TAIWAN_ID, "[ID_REDACTED]");
    }
}
