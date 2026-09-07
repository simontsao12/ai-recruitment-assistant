# ADR 0002: AI is decision support only

## Status

Accepted.

## Context

The application extracts candidate information and reviews it against job requirements. HR uses the resulting recommendations as decision support.

## Decision

The model may extract, summarize, compare and recommend. Hiring, rejection and rejection-email decisions remain with HR.

The model does not directly write database records or change application state. Java services validate model output, persist results and update processing states such as `RECEIVED`, `EXTRACTED`, `REVIEWED` and `PROCESSING_FAILED`. These states describe pipeline progress, not recruitment decisions.

Java services publish `candidate.reviewed` for every completed review, including `NOT_RECOMMENDED`, without filtering by score or recommendation. Discord consumes this review-completed event directly; no separate recommendation event is published. Discord messages use the title Candidate Review, present the AI assessment as decision support, and state that the final decision belongs to HR. On the first subscription, retained historical review events may be consumed according to the configured earliest offset reset. Existing SENT notification records prevent duplicate delivery per review.

Before the review step, the application removes the candidate's name, email and phone fields from the profile supplied to the model. It also applies regex-based redaction for email addresses, Taiwanese mobile numbers and national-ID patterns in the review context.

The review prompt instructs the model to evaluate job-related evidence, ignore protected or sensitive traits, and treat instructions embedded in candidate content as untrusted data.

## Consequences

- Removing selected fields and matching known patterns reduces exposure; it is not complete anonymization. Education, employment history and other free text can still contain identifying or sensitive information.
- Extraction receives the supplied email and resume text before review-stage redaction.
- The logging policy is to prefer identifiers and safe error codes over candidate content. Scheduler and Outbox logs generally record identifiers and exception types, and Agent Execution records store error codes or exception class names.
- Discord uses WebClient HTTP exceptions to preserve status classification without manually appending response bodies to messages. Logging code must still avoid serializing provider response bodies or complete exception objects as application data.
- Notification failures that are non-retryable or exhaust retries go to candidate.reviewed.dlq; they do not change a completed application's status to PROCESSING_FAILED.
- A SENT notification is skipped on redelivery. UNKNOWN or SENDING requires reconciliation; the database transaction cannot guarantee exactly-once delivery to Discord.
