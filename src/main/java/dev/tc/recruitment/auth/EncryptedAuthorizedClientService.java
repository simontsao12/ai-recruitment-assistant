package dev.tc.recruitment.auth;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.client.*;
import org.springframework.security.oauth2.client.registration.ReactiveClientRegistrationRepository;
import org.springframework.security.oauth2.core.*;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

@Service
public class EncryptedAuthorizedClientService implements ReactiveOAuth2AuthorizedClientService {
    private final DatabaseClient db;
    private final ReactiveClientRegistrationRepository registrations;
    private final TokenCipher cipher;
    private final TransactionalOperator tx;
    public EncryptedAuthorizedClientService(DatabaseClient db, ReactiveClientRegistrationRepository registrations,
                                            TokenCipher cipher, TransactionalOperator tx) {
        this.db = db; this.registrations = registrations; this.cipher = cipher; this.tx = tx;
    }
    @Override
    @SuppressWarnings("unchecked")
    public <T extends OAuth2AuthorizedClient> Mono<T> loadAuthorizedClient(String registrationId, String principalName) {
        return registrations.findByRegistrationId(registrationId).flatMap(registration ->
            db.sql("SELECT * FROM oauth2_authorized_client WHERE registration_id=:r AND principal_name=:p")
                .bind("r", registrationId).bind("p", principalName)
                .map((row, meta) -> {
                    String context = principalName + ":" + registrationId;
                    OAuth2AccessToken access = new OAuth2AccessToken(OAuth2AccessToken.TokenType.BEARER,
                            cipher.decrypt(row.get("access_token_ciphertext", String.class), context + ":access"),
                            row.get("access_token_issued_at", OffsetDateTime.class).toInstant(),
                            row.get("access_token_expires_at", OffsetDateTime.class).toInstant(),
                            new HashSet<>(Arrays.asList(row.get("scopes", String.class).split(" "))));
                    String encryptedRefresh = row.get("refresh_token_ciphertext", String.class);
                    OffsetDateTime issued = row.get("refresh_token_issued_at", OffsetDateTime.class);
                    OAuth2RefreshToken refresh = encryptedRefresh == null ? null : new OAuth2RefreshToken(
                            cipher.decrypt(encryptedRefresh, context + ":refresh"), issued == null ? null : issued.toInstant());
                    return (T) new OAuth2AuthorizedClient(registration, principalName, access, refresh);
                }).one());
    }
    public Mono<UUID> userId(String principal) {
        return db.sql("SELECT user_id FROM oauth2_authorized_client WHERE principal_name=:p AND registration_id='google'")
                .bind("p", principal).map((row, meta) -> row.get("user_id", UUID.class)).one();
    }
    @Override
    public Mono<Void> saveAuthorizedClient(OAuth2AuthorizedClient client, Authentication principal) {
        return Mono.defer(() -> {
            Mono<UUID> owner = userId(principal.getName());
            if (principal.getPrincipal() instanceof OAuth2User user) {
                String email = user.getAttribute("email");
                String name = user.getAttribute("name");
                if (email == null) return Mono.error(new IllegalStateException("OAUTH_EMAIL_REQUIRED"));
                owner = db.sql("""
                        INSERT INTO app_user(id,email,display_name,gmail_account,status,created_time,updated_time)
                        VALUES(:id,:email,:name,:email,'ACTIVE',now(),now())
                        ON CONFLICT(email) DO UPDATE SET display_name=excluded.display_name,updated_time=now()
                        RETURNING id
                        """).bind("id", UUID.randomUUID()).bind("email", email.toLowerCase(java.util.Locale.ROOT))
                        .bind("name", name == null ? email : name).map((row, meta) -> row.get("id", UUID.class)).one();
            }
            String context = principal.getName() + ":" + client.getClientRegistration().getRegistrationId();
            OAuth2AccessToken access = client.getAccessToken();
            OAuth2RefreshToken refresh = client.getRefreshToken();
            String refreshCipher = refresh == null ? null : cipher.encrypt(refresh.getTokenValue(), context + ":refresh");
            return owner.switchIfEmpty(Mono.error(new IllegalStateException("OAUTH_OWNER_REQUIRED"))).flatMap(id -> {
                var sql = db.sql("""
                        INSERT INTO oauth2_authorized_client(user_id,registration_id,principal_name,access_token_ciphertext,
                        access_token_issued_at,access_token_expires_at,refresh_token_ciphertext,refresh_token_issued_at,scopes,updated_time)
                        VALUES(:u,:r,:p,:a,:issued,:expires,:refresh,:refreshIssued,:scopes,now())
                        ON CONFLICT(principal_name,registration_id) DO UPDATE SET
                        access_token_ciphertext=excluded.access_token_ciphertext,
                        access_token_issued_at=excluded.access_token_issued_at,
                        access_token_expires_at=excluded.access_token_expires_at,
                        refresh_token_ciphertext=COALESCE(excluded.refresh_token_ciphertext,oauth2_authorized_client.refresh_token_ciphertext),
                        refresh_token_issued_at=COALESCE(excluded.refresh_token_issued_at,oauth2_authorized_client.refresh_token_issued_at),
                        scopes=excluded.scopes,updated_time=now()
                        """).bind("u", id).bind("r", client.getClientRegistration().getRegistrationId())
                        .bind("p", principal.getName()).bind("a", cipher.encrypt(access.getTokenValue(), context + ":access"))
                        .bind("issued", access.getIssuedAt() == null ? Instant.now() : access.getIssuedAt())
                        .bind("expires", access.getExpiresAt()).bind("scopes", String.join(" ", access.getScopes()));
                sql = refreshCipher == null ? sql.bindNull("refresh", String.class) : sql.bind("refresh", refreshCipher);
                sql = refresh == null || refresh.getIssuedAt() == null ? sql.bindNull("refreshIssued", Instant.class)
                        : sql.bind("refreshIssued", refresh.getIssuedAt());
                return sql.fetch().rowsUpdated().then();
            }).as(tx::transactional);
        });
    }
    @Override
    public Mono<Void> removeAuthorizedClient(String registrationId, String principalName) {
        return db.sql("DELETE FROM oauth2_authorized_client WHERE registration_id=:r AND principal_name=:p")
                .bind("r", registrationId).bind("p", principalName).fetch().rowsUpdated().then();
    }
}
