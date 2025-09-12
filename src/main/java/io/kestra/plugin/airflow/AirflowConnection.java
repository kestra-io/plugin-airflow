package io.kestra.plugin.airflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.airflow.model.DagRunResponse;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
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
    private Property<String> baseUrl;

    @Schema(
        title = "Adds custom headers"
    )
    @PluginProperty
    private Property<Map<String, String>> headers;

    @Schema(
        title = "Request options"
    )
    @PluginProperty
    protected HttpConfiguration options;

    protected DagRunResponse triggerDag(RunContext runContext, String dagId, String requestBody) throws Exception {
        String baseUrl = runContext.render(this.baseUrl).as(String.class).orElseThrow();
        URI triggerUri = URI.create(DAG_RUNS_ENDPOINT_FORMAT.formatted(baseUrl, dagId));

        try (HttpClient client = getClientBuilder(runContext).build()) {
            HttpRequest request = getRequestBuilder(runContext, triggerUri)
                .method("POST")
                .body(HttpRequest.StringRequestBody.builder().content(requestBody).build())
                .build();

            HttpResponse<DagRunResponse> response = client.request(request, DagRunResponse.class);

            if (response.getStatus().getCode() != 200) {
                throw new IllegalStateException("Failed to trigger DAG: " + response.getBody());
            }

            return response.getBody();
        }
    }

    protected DagRunResponse getDagStatus(RunContext runContext, String dagId, String dagRunId) throws Exception {
        String baseUrl = runContext.render(this.baseUrl).as(String.class).orElseThrow();
        URI statusUri = URI.create(DAG_RUNS_ENDPOINT_FORMAT.formatted(baseUrl, dagId) + "/" + dagRunId);

        try (HttpClient client = getClientBuilder(runContext).build()) {
            HttpRequest statusRequest = getRequestBuilder(runContext, statusUri)
                .build();

            HttpResponse<DagRunResponse> response = client.request(statusRequest, DagRunResponse.class);

            if (response.getStatus().getCode() != 200) {
                throw new IllegalStateException("Failed to get DAG run status: " + response.getBody());
            }

            return response.getBody();
        }
    }

    private io.kestra.core.http.client.HttpClient.HttpClientBuilder getClientBuilder(RunContext runContext) {
        return io.kestra.core.http.client.HttpClient.builder()
            .configuration(this.options)
            .runContext(runContext);
    }

    private HttpRequest.HttpRequestBuilder getRequestBuilder(RunContext runContext, URI uri) {
        return HttpRequest.builder()
            .uri(uri)
            .addHeader("Content-Type", JSON_CONTENT_TYPE);
    }
}
