package dev.tc.recruitment.gmail;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.security.oauth2.client.annotation.RegisteredOAuth2AuthorizedClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/gmail")
public class GmailController {
    private final GoogleGmailClient gmailClient;
    private final String defaultQuery;

    public GmailController(GoogleGmailClient gmailClient,
                           @Value("${app.gmail.query}") String defaultQuery) {
        this.gmailClient = gmailClient;
        this.defaultQuery = defaultQuery;
    }

    @GetMapping("/messages")
    Flux<GmailMessageSummary> messages(
            @RegisteredOAuth2AuthorizedClient("google") OAuth2AuthorizedClient client) {
        return gmailClient.search(client, defaultQuery);
    }
}
