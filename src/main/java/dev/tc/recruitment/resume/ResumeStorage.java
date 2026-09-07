package dev.tc.recruitment.resume;

import dev.tc.recruitment.common.PermanentFailure;
import java.io.ByteArrayInputStream;
import java.nio.file.*;
import java.util.Locale;
import java.util.UUID;
import org.apache.tika.Tika;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
public class ResumeStorage {
    private final Path root;
    public ResumeStorage(@Value("${app.storage.resume-directory}") String directory) {
        root = Path.of(directory).toAbsolutePath().normalize();
    }
    public Mono<String> store(UUID id, byte[] bytes) {
        return Mono.fromCallable(() -> {
            if (bytes.length > 10 * 1024 * 1024) throw new PermanentFailure("ATTACHMENT_TOO_LARGE");
            Files.createDirectories(root);
            Files.write(root.resolve(id.toString()), bytes, StandardOpenOption.CREATE_NEW);
            return id.toString();
        }).subscribeOn(Schedulers.boundedElastic());
    }
    public Mono<String> parse(String reference, String fileName) {
        return Mono.fromCallable(() -> {
            Path file = root.resolve(UUID.fromString(reference).toString()).normalize();
            if (!file.startsWith(root)) throw new PermanentFailure("INVALID_STORAGE_REFERENCE");
            String lower = fileName.toLowerCase(Locale.ROOT);
            if (!(lower.endsWith(".pdf") || lower.endsWith(".docx") || lower.endsWith(".doc") || lower.endsWith(".txt")))
                throw new PermanentFailure("UNSUPPORTED_FILE");
            byte[] bytes = Files.readAllBytes(file);
            if (bytes.length > 10 * 1024 * 1024) throw new PermanentFailure("ATTACHMENT_TOO_LARGE");
            String text;
            if (lower.endsWith(".txt")) text = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            else {
                Tika tika = new Tika(); tika.setMaxStringLength(100_000);
                try (var input = new ByteArrayInputStream(bytes)) { text = tika.parseToString(input); }
                catch (org.apache.tika.exception.TikaException e) { throw new PermanentFailure("INVALID_RESUME"); }
            }
            if (text.isBlank()) throw new PermanentFailure("EMPTY_RESUME_OCR_REQUIRED");
            if (text.length() > 100_000) throw new PermanentFailure("RESUME_TEXT_TOO_LARGE");
            return text;
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
