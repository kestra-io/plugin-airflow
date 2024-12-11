package io.kestra.plugin.airflow.dags;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.Await;
import io.kestra.plugin.airflow.AirflowConnection;
import io.kestra.plugin.airflow.model.DagRunResponse;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwSupplier;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Slf4j
@Schema(
    title = "Trigger an Airflow DAG with custom inputs and wait for its completion.",
    description = "Launch a DAG run, optionally wait for its completion and return the final state of the DAG run."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger a DAG run with custom inputs, and authenticate with basic authentication",
            full = true,
            code = """
id: airflow
namespace: company.team

tasks:
  - id: run_dag
    type: io.kestra.plugin.airflow.dags.TriggerDagRun
    baseUrl: http://host.docker.internal:8080
    dagId: example_astronauts
    wait: true
    pollFrequency: PT1S
    options:
      basicAuthUser: "{{ secret('AIRFLOW_USERNAME') }}"
      basicAuthPassword: "{{ secret('AIRFLOW_PASSWORD') }}"
    body:
      conf:
        source: kestra
        namespace: "{{ flow.namespace }}"
        flow: "{{ flow.id }}"
        task: "{{ task.id }}"
        execution: "{{ execution.id }}"
"""
        ),
        @Example(
            title = "Trigger a DAG run with custom inputs, and authenticate with a Bearer token",
            full = true,
            code = """
id: airflow_header_authorization
namespace: company.team

tasks:
  - id: run_dag
    type: io.kestra.plugin.airflow.dags.TriggerDagRun
    baseUrl: http://host.docker.internal:8080
    dagId: example_astronauts
    wait: true
    headers:
      authorization: "Bearer {{ secret('AIRFLOW_TOKEN') }}"
"""
        )
    }
)
public class TriggerDagRun extends AirflowConnection implements RunnableTask<TriggerDagRun.Output> {

    @Schema(
        title = "The ID of the DAG to trigger"
    )
    @NotNull
    private Property<String> dagId;

    @Schema(
        title = "The maximum total wait duration."
    )
    @Builder.Default
    Property<Duration> maxDuration = Property.of(Duration.ofMinutes(60));

    @Schema(
        title = "Specify how often the task should poll for the DAG run status."
    )
    @Builder.Default
    Property<Duration> pollFrequency = Property.of(Duration.ofSeconds(1));

    @Schema(
        title = "Whether task should wait for the DAG to run to completion",
        description = "Default value is false"
    )
    @Builder.Default
    private Property<Boolean> wait = Property.of(Boolean.FALSE);

    @Schema(
        title = "Overrides the default configuration payload"
    )
    private Property<Map<String, Object>> body;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String dagId = runContext.render(this.dagId).as(String.class).orElseThrow();

        DagRunResponse triggerResult = triggerDag(runContext, dagId, buildBody(runContext));
        String dagRunId = triggerResult.getDagRunId();

        Output.OutputBuilder outputBuilder = Output.builder()
            .dagId(dagId)
            .dagRunId(dagRunId)
            .state(triggerResult.getState());

        if (runContext.render(this.wait).as(Boolean.class).orElseThrow().equals(Boolean.FALSE)) {
            return outputBuilder.build();
        }

        DagRunResponse statusResult = Await.until(
            throwSupplier(() -> {
                DagRunResponse result = getDagStatus(runContext, dagId, dagRunId);
                String state = result.getState();

                if ("success".equalsIgnoreCase(state) || "failed".equalsIgnoreCase(state)) {
                    return result;
                }

                return null;
            }),
            runContext.render(this.pollFrequency).as(Duration.class).orElseThrow(),
            runContext.render(this.maxDuration).as(Duration.class).orElseThrow()
        );

        if (statusResult == null) {
            throw new IllegalStateException("DAG run did not complete within the specified timeout");
        }

        return outputBuilder
            .state(statusResult.getState())
            .started(statusResult.getStartDate().toLocalDateTime())
            .ended(statusResult.getEndDate().toLocalDateTime())
            .build();
    }

    private String buildBody(RunContext runContext) throws JsonProcessingException, IllegalVariableEvaluationException {
        RunContext.FlowInfo flowInfo = runContext.flowInfo();

        var renderedBody = runContext.render(this.body).asMap(String.class, Object.class);
        if (!renderedBody.isEmpty()) {
            return objectMapper.writeValueAsString(renderedBody);
        }

        Map<String, Object> conf = Map.of(
            "source", "kestra",
            "flow", flowInfo.id(),
            "namespace", flowInfo.namespace(),
            "task", this.id,
            "execution", runContext.getTriggerExecutionId()
        );

        return objectMapper.writeValueAsString(conf);
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "DAG ID"
        )
        private String dagId;

        @Schema(
            title = "DAG run ID"
        )
        private String dagRunId;

        @Schema(
            title = "State"
        )
        private String state;

        @Schema(
            title = "DAG run started date"
        )
        private LocalDateTime started;

        @Schema(
            title = "DAG run completed date"
        )
        private LocalDateTime ended;
    }

}
