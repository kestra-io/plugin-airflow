package io.kestra.plugin.airflow.dags;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
    @PluginProperty(dynamic = true)
    private String dagId;

    @Schema(
        title = "The job ID to check status for."
    )
    @PluginProperty(dynamic = true)
    private String jobId;

    @Schema(
        title = "The maximum total wait duration."
    )
    @PluginProperty
    @Builder.Default
    Duration maxDuration = Duration.ofMinutes(60);

    @Schema(
        title = "Specify how often the task should poll for the DAG run status."
    )
    @PluginProperty
    @Builder.Default
    Duration pollFrequency = Duration.ofSeconds(1);

    @Schema(
        title = "Whether task should wait for the DAG to run to completion",
        description = "Default value is false"
    )
    @PluginProperty
    @Builder.Default
    private Boolean wait = Boolean.FALSE;

    @Schema(
        title = "Overrides the default configuration payload"
    )
    @PluginProperty
    private Map<String, Object> body;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String dagId = runContext.render(this.dagId);

        DagRunResponse triggerResult = triggerDag(runContext, dagId, buildBody(runContext));
        String dagRunId = triggerResult.getDagRunId();

        Output.OutputBuilder outputBuilder = Output.builder()
            .dagId(dagId)
            .dagRunId(dagRunId)
            .state(triggerResult.getState());

        if (this.wait.equals(Boolean.FALSE)) {
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
            this.pollFrequency,
            this.maxDuration
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

    private String buildBody(RunContext runContext) throws JsonProcessingException {
        RunContext.FlowInfo flowInfo = runContext.flowInfo();

        Map<String, Object> conf = this.body;

        if (this.body == null) {
            conf = Map.of(
                "source", "kestra",
                "flow", flowInfo.id(),
                "namespace", flowInfo.namespace(),
                "task", this.id,
                "execution", runContext.getTriggerExecutionId()
            );
        }

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
