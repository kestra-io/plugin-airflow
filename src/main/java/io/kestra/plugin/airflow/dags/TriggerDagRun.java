package io.kestra.plugin.airflow.dags;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.airflow.AirflowConnection;
import io.kestra.plugin.airflow.model.DagRunResponse;
import io.kestra.plugin.core.flow.WaitFor;
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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Slf4j
@Schema(
    title = "Trigger Airflow DAG",
    description = "Trigger an Airflow DAG run and wait for its completion."
)
@Plugin(
    examples = {
        @Example(
            title = "Basic authorization",
            code = {
                "  - id: trigger_dag",
                "    type: io.kestra.plugin.airflow.TriggerDagRun",
                "    baseUrl: http://airflow.example.com",
                "    dagId: example_dag",
                "    checkFrequency: PT30S",
                "      interval: PT30S",
                "      maxIterations: 100",
                "      maxDuration: PT1H",
                "    options:",
                "      basicAuthUser: myusername",
                "      basicAuthPassword: mypassword"
            }
        ),
        @Example(
            title = "Bearer authorization",
            code = {
                "  - id: trigger_dag",
                "    type: io.kestra.plugin.airflow.TriggerDagRun",
                "    baseUrl: http://airflow.example.com",
                "    dagId: example_dag",
                "    checkFrequency: PT30S",
                "      interval: PT30S",
                "    headers:",
                "      authorization: 'Bearer {{ TOKEN }}'"
            }
        ),
        @Example(
            title = "Basic authorization. Custom body",
            code = {
                "  - id: trigger_dag",
                "    type: io.kestra.plugin.airflow.TriggerDagRun",
                "    baseUrl: http://airflow.example.com",
                "    dagId: example_dag",
                "    checkFrequency: PT30S",
                "      interval: PT30S",
                "    options:",
                "      basicAuthUser: myusername",
                "      basicAuthPassword: mypassword",
                "    body: |",
                "       {",
                "         \"conf\": {",
                "           \"source\": \"kestra\",",
                "           \"flow\": \"{{ flow.id }}\",",
                "           \"namespace\": \"{{ flow.namespace }}\",",
                "           \"task\": \"{{ task.id }}\",",
                "           \"execution\": \"{{ execution.id }}\"",
                "           }",
                "       }"
            }
        ),
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
        title = "The frequency at which to check the DAG run status"
    )
    @PluginProperty
    @Builder.Default
    private WaitFor.CheckFrequency checkFrequency = WaitFor.CheckFrequency.builder().build();

    @Schema(
        title = "Whether task should wait DAG run to complete",
        description = "Default value is false"
    )
    @PluginProperty
    @Builder.Default
    private Boolean wait = Boolean.FALSE;

    @Schema(
        title = "Overrides the default conf payload"
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

        ZonedDateTime started = ZonedDateTime.now();
        AtomicInteger iterations = new AtomicInteger();

        while (!this.timedOut(iterations, started)) {
            DagRunResponse statusResult = getDagStatus(runContext, dagId, dagRunId);
            String state = statusResult.getState();

            if ("success".equalsIgnoreCase(state) || "failed".equalsIgnoreCase(state)) {
                return outputBuilder
                    .state(state)
                    .started(statusResult.getStartDate().toLocalDateTime())
                    .ended(statusResult.getEndDate().toLocalDateTime())
                    .build();
            }

            runContext.logger().info("DAG run is {}", state);
            iterations.getAndIncrement();

            //noinspection BusyWait
            Thread.sleep(this.checkFrequency.getInterval().toMillis());
        }

        throw new IllegalStateException("DAG run did not complete within the specified timeout");
    }

    private boolean timedOut(AtomicInteger iteration, ZonedDateTime start) {
        if (this.checkFrequency != null && iteration.get() >= this.checkFrequency.getMaxIterations()) {
            return true;
        }

        return this.checkFrequency != null &&
            ZonedDateTime.now().toEpochSecond() > start.plus(this.checkFrequency.getMaxDuration()).toEpochSecond();
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
