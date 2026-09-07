package dev.tc.recruitment.gmail;

import tools.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import dev.tc.recruitment.common.PermanentFailure;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
public class GoogleGmailClient {
    private final WebClient client;
    public GoogleGmailClient(WebClient.Builder builder) {
        client = builder.baseUrl("https://gmail.googleapis.com/gmail/v1")
                .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(32 * 1024 * 1024)).build();
    }
    private Mono<JsonNode> get(OAuth2AuthorizedClient auth, String path, String query, String page) {
        return client.get().uri(uri -> {
            uri.path(path);
            if (query != null) uri.queryParam("q", query).queryParam("maxResults", 100);
            if (page != null && !page.isBlank()) uri.queryParam("pageToken", page);
            return uri.build();
        }).headers(headers -> headers.setBearerAuth(auth.getAccessToken().getTokenValue()))
                .retrieve().bodyToMono(JsonNode.class).timeout(Duration.ofSeconds(30))
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)).filter(error ->
                        error instanceof java.util.concurrent.TimeoutException ||
                        error instanceof org.springframework.web.reactive.function.client.WebClientRequestException ||
                        error instanceof WebClientResponseException response &&
                                (response.getStatusCode().value() == 429 || response.getStatusCode().is5xxServerError()))
                        .onRetryExhaustedThrow((spec, signal) -> signal.failure()));
    }
    public Flux<GmailMessageSummary> search(OAuth2AuthorizedClient auth, String query) {
        return get(auth, "/users/me/messages", query, null)
                .expand(page -> page.hasNonNull("nextPageToken")
                        ? get(auth, "/users/me/messages", query, page.path("nextPageToken").asText()) : Mono.empty())
                .concatMapIterable(page -> page.path("messages"))
                .map(message -> new GmailMessageSummary(message.path("id").asText(), message.path("threadId").asText()));
    }
    public Mono<Mail> read(OAuth2AuthorizedClient auth, String messageId) {
        return get(auth, "/users/me/messages/" + messageId, null, null).map(message -> {
            JsonNode payload = message.path("payload");
            String subject = "";
            for (JsonNode header : payload.path("headers"))
                if ("Subject".equalsIgnoreCase(header.path("name").asText())) subject = header.path("value").asText();
            List<Attachment> attachments = new ArrayList<>();
            StringBuilder body = new StringBuilder();
            collect(payload, attachments, body);
            if (attachments.size() > 10) throw new PermanentFailure("TOO_MANY_ATTACHMENTS");
            return new Mail(messageId, subject, body.toString(), attachments);
        });
    }
    private void collect(JsonNode part, List<Attachment> attachments, StringBuilder body) {
        String name = part.path("filename").asText();
        JsonNode content = part.path("body");
        if (!name.isBlank()) {
            if (name.length() > 500 || part.path("mimeType").asText().length() > 100)
                throw new PermanentFailure("INVALID_ATTACHMENT_METADATA");
            attachments.add(new Attachment(name, part.path("mimeType").asText(),
                    content.path("attachmentId").asText(), content.path("data").asText(), content.path("size").asLong()));
        } else if ("text/plain".equals(part.path("mimeType").asText()) && content.hasNonNull("data")) {
            body.append(new String(decode(content.path("data").asText()), java.nio.charset.StandardCharsets.UTF_8)).append('\n');
        }
        for (JsonNode child : part.path("parts")) collect(child, attachments, body);
    }
    private static byte[] decode(String value) {
        try { return Base64.getUrlDecoder().decode(value); }
        catch (IllegalArgumentException e) { throw new PermanentFailure("INVALID_ATTACHMENT_ENCODING"); }
    }
    public Mono<byte[]> download(OAuth2AuthorizedClient auth, String messageId, Attachment attachment) {
        if (attachment.size() > 10 * 1024 * 1024) return Mono.error(new PermanentFailure("ATTACHMENT_TOO_LARGE"));
        return attachment.attachmentId().isBlank()
                ? Mono.fromSupplier(() -> decode(attachment.inlineData()))
                : get(auth, "/users/me/messages/" + messageId + "/attachments/" + attachment.attachmentId(), null, null)
                    .map(node -> decode(node.path("data").asText()));
    }
    public record Mail(String id, String subject, String body, List<Attachment> attachments) {}
    public record Attachment(String fileName, String contentType, String attachmentId, String inlineData, long size) {}
}
