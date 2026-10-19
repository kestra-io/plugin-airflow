package io.kestra.plugin.airflow.dags;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.http.client.configurations.BasicAuthConfiguration;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class TriggerDagRunTest {
    private static final String BASE_URL = "http://localhost:8082";
    private static final String USER = "airflow";
    private static final String PASSWORD = "airflow";

    @Inject
    private TestRunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();

        TriggerDagRun task = TriggerDagRun.builder()
            .baseUrl(Property.ofValue(getBaseUrl()))
            .dagId(Property.ofValue("tutorial_dag"))
            .options(
                HttpConfiguration.builder()
                    .auth(
                        BasicAuthConfiguration.builder()
                            .username(getUser())
                            .password(getPassword())
                            .build()
                    )
                    .build()
            )
            .body(
                Property.ofValue(
                    Map.of(
                        "conf", Map.of(
                            "source", "kestra",
                            "flow", "airflow",
                            "namespace", "unittest",
                            "task", "trigger",
                            "execution", "123"
                        )
                    )
                )
            )
            .build();

        TriggerDagRun.Output runOutput = task.run(runContext);

        assertThat(runOutput.getDagRunId(), is(notNullValue()));
        assertThat(runOutput.getState(), is(notNullValue()));
        assertThat(runOutput.getState(), is(equalToIgnoringCase("queued")));
    }

    @Test
    void runWithoutBody() throws Exception {
        TriggerDagRun task = TriggerDagRun.builder()
            .id("trigger")
            .type(TriggerDagRun.class.getName())
            .baseUrl(Property.ofValue(getBaseUrl()))
            .dagId(Property.ofValue("tutorial_dag"))
            .options(
                HttpConfiguration.builder()
                    .auth(
                        BasicAuthConfiguration.builder()
                            .username(getUser())
                            .password(getPassword())
                            .build()
                    )
                    .build()
            )
            .build();

        Flow flow = Flow.builder()
            .id("airflow")
            .namespace("unittest")
            .tenantId(MAIN_TENANT)
            .revision(1)
            .build();
        Execution execution = TestsUtils.mockExecution(flow, Map.of());
        RunContext runContext = runContextFactory.of(flow, task, execution, TestsUtils.mockTaskRun(execution, task));

        TriggerDagRun.Output runOutput = task.run(runContext);

        assertThat(runOutput.getDagRunId(), is(notNullValue()));
        assertThat(runOutput.getState(), is(equalToIgnoringCase("queued")));
    }

    @Test
    void waitForComplete() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        TriggerDagRun task = TriggerDagRun.builder()
            .baseUrl(Property.ofValue(getBaseUrl()))
            .dagId(Property.ofValue("tutorial_dag"))
            .wait(Property.ofValue(true))
            .options(
                HttpConfiguration.builder()
                    .auth(
                        BasicAuthConfiguration.builder()
                            .username(getUser())
                            .password(getPassword())
                            .build()
                    )
                    .build()
            )
            .body(
                Property.ofValue(
                    Map.of(
                        "conf", Map.of(
                            "source", "kestra",
                            "flow", "airflow",
                            "namespace", "unittest",
                            "task", "trigger",
                            "execution", "123"
                        )
                    )
                )
            )
            .build();

        TriggerDagRun.Output runOutput = task.run(runContext);

        assertThat(runOutput.getDagRunId(), is(notNullValue()));
        assertThat(runOutput.getState(), is(notNullValue()));
        assertThat(runOutput.getState(), is(equalToIgnoringCase("success")));
    }

    private static Property<String> getPassword() {
        return Property.ofValue(PASSWORD);
    }

    private static Property<String> getUser() {
        return Property.ofValue(USER);
    }

    private static String getBaseUrl() {
        return BASE_URL;
    }
}
