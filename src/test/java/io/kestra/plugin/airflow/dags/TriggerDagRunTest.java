package io.kestra.plugin.airflow.dags;

import io.kestra.core.http.client.configurations.BasicAuthConfiguration;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
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
            .baseUrl(Property.ofValue(getBaseUrl()))
            .dagId(Property.ofValue("tutorial_dag"))
            .options(
                HttpConfiguration.builder()
                    .auth(BasicAuthConfiguration.builder()
                        .username(getUser())
                        .password(getPassword())
                        .build()
                    )
                    .build()
            )
            .body(Property.ofValue(
                Map.of(
                    "conf", Map.of(
                        "source", "kestra",
                        "flow", "airflow",
                        "namespace", "unittest",
                        "task", "trigger",
                        "execution", "123"
                    )
                )
            ))
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
            .baseUrl(Property.ofValue(getBaseUrl()))
            .dagId(Property.ofValue("tutorial_dag"))
            .wait(Property.ofValue(true))
            .options(
                HttpConfiguration.builder()
                    .auth(BasicAuthConfiguration.builder()
                        .username(getUser())
                        .password(getPassword())
                        .build()
                    )
                    .build()
            )
            .body(Property.ofValue(
                Map.of(
                    "conf", Map.of(
                        "source", "kestra",
                        "flow", "airflow",
                        "namespace", "unittest",
                        "task", "trigger",
                        "execution", "123"
                    )
                )
            ))
            .build();

        TriggerDagRun.Output runOutput = task.run(runContext);

        assertThat(runOutput.getDagRunId(), is(notNullValue()));
        assertThat(runOutput.getState(), is(notNullValue()));

        assertThat(runOutput.getState(), is(equalToIgnoringCase("success")));
    }

    private static Property<String> getPassword() {
        return Property.ofValue("airflow");
    }

    private static Property<String> getUser() {
        return Property.ofValue("airflow");
    }

    private static String getBaseUrl() {
        return "http://localhost:8080";
    }
}
