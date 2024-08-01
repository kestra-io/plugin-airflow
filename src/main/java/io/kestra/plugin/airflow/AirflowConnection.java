package io.kestra.plugin.airflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.airflow.model.DagRunResponse;
import io.kestra.plugin.core.http.HttpInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Slf4j
public abstract class AirflowConnection extends Task {

    protected final static ObjectMapper objectMapper = JacksonMapper.ofJson();

    public static final String DAG_RUNS_ENDPOINT_FORMAT = "%s/api/v1/dags/%s/dagRuns";

    public static final String JSON_CONTENT_TYPE = "application/json";

    @Schema(
        title = "The base URL of the Airflow instance"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String baseUrl;

    @Schema(
        title = "Adds custom headers"
    )
    @PluginProperty
    private Map<String, String> headers;

    @Schema(
        title = "Request options"
    )
    @PluginProperty
    protected HttpInterface.RequestOptions options;

    protected DagRunResponse triggerDag(RunContext runContext, String dagId, String requestBody) throws Exception {
        String baseUrl = runContext.render(this.baseUrl);
        URI triggerUri = URI.create(DAG_RUNS_ENDPOINT_FORMAT.formatted(baseUrl, dagId));

        try (HttpClient client = getClientBuilder().build()) {
            HttpRequest request = getRequestBuilder(runContext, triggerUri)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new IllegalStateException("Failed to trigger DAG: " + response.body());
            }

            return objectMapper.readValue(response.body(), DagRunResponse.class);
        }
    }

    protected DagRunResponse getDagStatus(RunContext runContext, String dagId, String dagRunId) throws Exception {
        URI statusUri = URI.create(DAG_RUNS_ENDPOINT_FORMAT.formatted(getBaseUrl(), dagId) + "/" + dagRunId);

        try (HttpClient client = getClientBuilder().build()) {
            HttpRequest statusRequest = getRequestBuilder(runContext, statusUri)
                .GET()
                .build();

            HttpResponse<String> response = client.send(statusRequest, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new IllegalStateException("Failed to get DAG run status: " + response.body());
            }

            return objectMapper.readValue(response.body(), DagRunResponse.class);
        }
    }

    private HttpClient.Builder getClientBuilder() {
        HttpClient.Builder clientBuilder = HttpClient.newBuilder();

        if (this.options != null && this.options.getConnectTimeout() != null) {
            clientBuilder.connectTimeout(options.getConnectTimeout());
        }

        return clientBuilder;
    }

    private HttpRequest.Builder getRequestBuilder(RunContext runContext, URI uri) throws IllegalVariableEvaluationException {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(uri)
            .header("Content-Type", JSON_CONTENT_TYPE);

        setupCustomHeaders(runContext, requestBuilder);

        return requestBuilder;
    }

    private void setupCustomHeaders(RunContext runContext, HttpRequest.Builder requestBuilder) throws IllegalVariableEvaluationException {
        if (this.options != null && this.options.getBasicAuthUser() != null && this.options.getBasicAuthPassword() != null) {
            String authorizationString = "%s:%s"
                .formatted(
                    runContext.render(this.options.getBasicAuthUser()),
                    runContext.render(this.options.getBasicAuthPassword())
                );

            String auth = Base64
                .getEncoder()
                .encodeToString(authorizationString.getBytes());

            requestBuilder.header("Authorization", "Basic " + auth);
        }

        if (this.headers != null && !headers.isEmpty()) {
            this.headers.forEach(requestBuilder::header);
        }
    }

}
