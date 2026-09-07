package dev.tc.recruitment.common.web;

import org.springframework.security.web.server.csrf.CsrfToken;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@RestController
public class CsrfController {
    @GetMapping("/api/csrf")
    public Mono<CsrfToken> csrf(ServerWebExchange exchange) {
        return exchange.getAttributeOrDefault(CsrfToken.class.getName(),Mono.empty());
    }
}
