package dev.tc.recruitment.common.web;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HomeController {
    @GetMapping("/")
    Map<String, String> home() {
        return Map.of(
                "name", "AI Recruitment Assistant",
                "login", "/oauth2/authorization/google",
                "health", "/actuator/health");
    }
}
