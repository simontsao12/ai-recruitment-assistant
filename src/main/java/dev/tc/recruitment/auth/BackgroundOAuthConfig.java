package dev.tc.recruitment.auth;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.client.*;
import org.springframework.security.oauth2.client.registration.ReactiveClientRegistrationRepository;

@Configuration
public class BackgroundOAuthConfig {
    @Bean
    ReactiveOAuth2AuthorizedClientManager backgroundAuthorizedClientManager(
            ReactiveClientRegistrationRepository registrations, EncryptedAuthorizedClientService clients) {
        var manager = new AuthorizedClientServiceReactiveOAuth2AuthorizedClientManager(registrations, clients);
        manager.setAuthorizedClientProvider(ReactiveOAuth2AuthorizedClientProviderBuilder.builder().refreshToken().build());
        return manager;
    }
}
