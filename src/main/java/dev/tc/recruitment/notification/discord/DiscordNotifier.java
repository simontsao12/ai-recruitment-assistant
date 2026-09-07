package dev.tc.recruitment.notification.discord;

import tools.jackson.databind.JsonNode;
import dev.tc.recruitment.agent.model.*;
import dev.tc.recruitment.common.PermanentFailure;
import java.time.Duration;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
@ConditionalOnProperty(name="app.discord.enabled", havingValue="true")
public class DiscordNotifier {
    private final WebClient client;
    private final String channel;
    public DiscordNotifier(WebClient.Builder builder, @Value("${app.discord.bot-token}") String token,
                           @Value("${app.discord.channel-id}") String channel) {
        if (token.isBlank() || !channel.matches("[0-9]+")) throw new IllegalStateException("DISCORD_CONFIGURATION_REQUIRED");
        this.channel = channel;
        client = builder.baseUrl("https://discord.com/api/v10")
                .defaultHeader(HttpHeaders.AUTHORIZATION,"Bot "+token).build();
    }
    public String channelId() { return channel; }
    public Mono<String> send(CandidateProfile candidate, ReviewResult review, String title, String nonce) {
        String content = """
                🤖 Candidate Review
                Job: %s
                Candidate: %s
                Score: %d / 100
                Recommendation: %s
                Matched: %s
                Missing: %s
                AI Review: %s
                AI assessment only. HR makes the final decision.
                """.formatted(clip(title,150),clip(candidate.name(),150),review.score(),review.recommendation(),
                clip(String.join(", ",review.matchedSkills()),300),clip(String.join(", ",review.missingSkills()),300),
                clip(review.reason(),650));
        return client.post().uri("/channels/{channel}/messages",channel)
                .bodyValue(Map.of("content",content,"allowed_mentions",Map.of("parse",java.util.List.of()),
                        "nonce",nonce,"enforce_nonce",true))
                .retrieve()
                .bodyToMono(JsonNode.class).timeout(Duration.ofSeconds(30))
                .map(body -> {
                    String id = body.path("id").asText();
                    if (id.isBlank()) throw new PermanentFailure("DISCORD_RESPONSE_MISSING_ID");
                    return id;
                });
    }
    private static String clip(String value, int limit) {
        if (value == null) return "(unknown)";
        return value.length() <= limit ? value : value.substring(0,limit)+"…";
    }
}
