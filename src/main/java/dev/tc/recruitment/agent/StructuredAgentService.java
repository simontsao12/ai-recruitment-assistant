package dev.tc.recruitment.agent;

import dev.tc.recruitment.common.PermanentFailure;
import dev.tc.recruitment.pipeline.PipelineStore;
import java.nio.charset.StandardCharsets;
import dev.tc.recruitment.common.config.ProcessingTimeouts;
import java.util.UUID;
import java.util.function.Consumer;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.converter.BeanOutputConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
public class StructuredAgentService {
    private final ChatClient client;
    private final PipelineStore store;
    private final String configuredModel;
    private final ProcessingTimeouts timeouts;
    public StructuredAgentService(ChatClient.Builder builder, PipelineStore store,
                                  @Value("${spring.ai.ollama.chat.options.model}") String model, ProcessingTimeouts timeouts) {
        client = builder.build(); this.store = store; configuredModel = model; this.timeouts = timeouts;
    }
    public <T> Mono<T> run(String type, UUID aggregate, String version, String context, Class<T> resultType, Consumer<T> validate) {
        return Mono.defer(() -> {
            UUID execution = UUID.randomUUID();
            return store.execute("""
                    INSERT INTO agent_execution(id,agent_type,aggregate_id,model,prompt_version,status,started_time)
                    VALUES(:id,:type,:aggregate,:model,:version,'RUNNING',now())
                    """, "id", execution, "type", type, "aggregate", aggregate, "model", configuredModel, "version", version)
                .then(Mono.fromCallable(() -> {
                    String policy = new ClassPathResource("prompts/" + version + ".txt").getContentAsString(StandardCharsets.UTF_8);
                    var converter = new BeanOutputConverter<>(resultType);
                    var response = client.prompt().system(policy + "\n" + converter.getFormat()).user(context).call().chatResponse();
                    if (response == null || response.getResult() == null)
                        throw new PermanentFailure("EMPTY_AGENT_OUTPUT");
                    T output;
                    try { output = converter.convert(response.getResult().getOutput().getText()); }
                    catch (Exception e) { throw new PermanentFailure("INVALID_AGENT_OUTPUT"); }
                    validate.accept(output);
                    var usage = response.getMetadata().getUsage();
                    return new Result<>(output, response.getMetadata().getModel(),
                            usage == null ? null : usage.getPromptTokens(), usage == null ? null : usage.getCompletionTokens());
                }).subscribeOn(Schedulers.boundedElastic()).timeout(timeouts.agent()))
                .flatMap(result -> {
                    // Bind numeric nulls with their proper SQL type.
                    var sql = store.sql("""
                            UPDATE agent_execution SET status='SUCCEEDED',model=:model,prompt_tokens=:prompt,
                            completion_tokens=:completion,completed_time=now() WHERE id=:id
                            """, "id", execution, "model", result.model() == null ? configuredModel : result.model());
                    sql = result.prompt() == null ? sql.bindNull("prompt", Integer.class) : sql.bind("prompt", result.prompt());
                    sql = result.completion() == null ? sql.bindNull("completion", Integer.class) : sql.bind("completion", result.completion());
                    return sql.fetch().rowsUpdated().thenReturn(result.value());
                })
                .onErrorResume(error -> store.execute("""
                        UPDATE agent_execution SET status='FAILED',error_message=:code,completed_time=now() WHERE id=:id
                        """, "id", execution, "code", error instanceof PermanentFailure ? error.getMessage() : error.getClass().getSimpleName())
                        .then(Mono.error(error)));
        });
    }
    private record Result<T>(T value, String model, Integer prompt, Integer completion) {}
}
