package io.kestra.plugin.airflow.dags;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.core.http.HttpInterface;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Disabled(
    "For CI/CD"
)
class TriggerDagRunTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();

        TriggerDagRun task = TriggerDagRun.builder()
            .baseUrl(getBaseUrl())
            .dagId("tutorial_dag")
            .options(
                HttpInterface.RequestOptions.builder()
                    .basicAuthUser(getUser())
                    .basicAuthPassword(getPassword())
                    .build()
            )
            .body(
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
            .build();

        TriggerDagRun.Output runOutput = task.run(runContext);

        assertThat(runOutput.getDagRunId(), is(notNullValue()));
        assertThat(runOutput.getState(), is(notNullValue()));

        assertThat(runOutput.getState(), is(equalToIgnoringCase("queued")));
    }

    @Test
    void waitForComplete() throws Exception {
        RunContext runContext = runContextFactory.of();

        TriggerDagRun task = TriggerDagRun.builder()
            .baseUrl(getBaseUrl())
            .dagId("tutorial_dag")
            .wait(true)
            .options(
                HttpInterface.RequestOptions.builder()
                    .basicAuthUser(getUser())
                    .basicAuthPassword(getPassword())
                    .build()
            )
            .body(
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
            .build();

        TriggerDagRun.Output runOutput = task.run(runContext);

        assertThat(runOutput.getDagRunId(), is(notNullValue()));
        assertThat(runOutput.getState(), is(notNullValue()));

        assertThat(runOutput.getState(), is(equalToIgnoringCase("success")));
    }

    private static String getPassword() {
        return "airflow";
    }

    private static String getUser() {
        return "airflow";
    }

    private static String getBaseUrl() {
        return "http://localhost:8080";
    }
}
