package io.kestra.plugin.airflow.dags;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class TriggerDagRunDefaultBodyTest {
    private static final String DAG_RUNS = "/api/v1/dags/example_astronauts/dagRuns";

    @RegisterExtension
    static final WireMockExtension wireMock = WireMockExtension.newInstance()
        .options(wireMockConfig().dynamicPort())
        .build();

    @Inject
    private TestRunContextFactory runContextFactory;

    @Test
    void shouldNestDefaultMetadataUnderConfWhenBodyIsOmitted() throws Exception {
        wireMock.stubFor(
            post(urlEqualTo(DAG_RUNS))
                .willReturn(okJson("""
                    {"dag_id":"example_astronauts","dag_run_id":"manual__1","state":"queued"}
                    """))
        );

        var task = TriggerDagRun.builder()
            .id("run_dag")
            .type(TriggerDagRun.class.getName())
            .baseUrl(Property.ofValue(wireMock.baseUrl()))
            .dagId(Property.ofValue("example_astronauts"))
            .build();

        var flow = Flow.builder()
            .id("airflow")
            .namespace("company.team")
            .tenantId(MAIN_TENANT)
            .revision(1)
            .build();
        var execution = TestsUtils.mockExecution(flow, Map.of());
        var runContext = runContextFactory.of(flow, task, execution, TestsUtils.mockTaskRun(execution, task));

        var output = task.run(runContext);

        assertThat(output.getDagRunId(), is("manual__1"));
        wireMock.verify(
            postRequestedFor(urlEqualTo(DAG_RUNS))
                .withRequestBody(equalToJson("""
                    {
                      "conf": {
                        "source": "kestra",
                        "flow": "airflow",
                        "namespace": "company.team",
                        "task": "run_dag",
                        "execution": "%s"
                      }
                    }
                    """.formatted(execution.getId())))
        );
    }

    @Test
    void shouldSendTheGivenBodyWhenBodyIsSet() throws Exception {
        wireMock.stubFor(
            post(urlEqualTo(DAG_RUNS))
                .willReturn(okJson("""
                    {"dag_id":"example_astronauts","dag_run_id":"manual__2","state":"queued"}
                    """))
        );

        var task = TriggerDagRun.builder()
            .id("run_dag")
            .baseUrl(Property.ofValue(wireMock.baseUrl()))
            .dagId(Property.ofValue("example_astronauts"))
            .body(Property.ofValue(Map.of("conf", Map.of("dataset", "astronauts"))))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getDagRunId(), is("manual__2"));
        wireMock.verify(
            postRequestedFor(urlEqualTo(DAG_RUNS))
                .withRequestBody(equalToJson("""
                    {
                      "conf": {
                        "dataset": "astronauts"
                      }
                    }
                    """))
        );
    }
}
