package io.kestra.plugin.airflow.dags;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;

import io.kestra.core.context.TestRunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class TriggerDagRunHeadersTest {
    private static final String DAG_RUNS = "/api/v1/dags/example_astronauts/dagRuns";

    @RegisterExtension
    static final WireMockExtension wireMock = WireMockExtension.newInstance()
        .options(wireMockConfig().dynamicPort())
        .build();

    @Inject
    private TestRunContextFactory runContextFactory;

    @Test
    void shouldSendCustomHeadersOnTriggerAndStatusRequests() throws Exception {
        stubDagRun("manual__headers");

        var task = TriggerDagRun.builder()
            .id(IdUtils.create())
            .type(TriggerDagRun.class.getName())
            .baseUrl(Property.ofValue(wireMock.baseUrl()))
            .dagId(Property.ofValue("example_astronauts"))
            .body(Property.ofValue(Map.of("conf", Map.of())))
            .headers(Property.ofValue(Map.of(
                "Authorization", "Bearer my-token",
                "X-Custom", "kestra"
            )))
            .wait(Property.ofValue(true))
            .pollFrequency(Property.ofValue(Duration.ofMillis(100)))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getState(), is("success"));
        wireMock.verify(
            postRequestedFor(urlEqualTo(DAG_RUNS))
                .withHeader("Authorization", equalTo("Bearer my-token"))
                .withHeader("X-Custom", equalTo("kestra"))
                .withHeader("Content-Type", equalTo("application/json"))
        );
        wireMock.verify(
            getRequestedFor(urlEqualTo("%s/%s".formatted(DAG_RUNS, "manual__headers")))
                .withHeader("Authorization", equalTo("Bearer my-token"))
                .withHeader("X-Custom", equalTo("kestra"))
        );
    }

    @Test
    void shouldWorkWithoutCustomHeaders() throws Exception {
        stubDagRun("manual__no_headers");

        var task = TriggerDagRun.builder()
            .id(IdUtils.create())
            .type(TriggerDagRun.class.getName())
            .baseUrl(Property.ofValue(wireMock.baseUrl()))
            .dagId(Property.ofValue("example_astronauts"))
            .body(Property.ofValue(Map.of("conf", Map.of())))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getDagRunId(), is("manual__no_headers"));
        wireMock.verify(
            postRequestedFor(urlEqualTo(DAG_RUNS))
                .withHeader("Authorization", absent())
        );
    }

    @Test
    void shouldLetCustomContentTypeReplaceTheDefault() throws Exception {
        stubDagRun("manual__content_type");

        var task = TriggerDagRun.builder()
            .id(IdUtils.create())
            .type(TriggerDagRun.class.getName())
            .baseUrl(Property.ofValue(wireMock.baseUrl()))
            .dagId(Property.ofValue("example_astronauts"))
            .body(Property.ofValue(Map.of("conf", Map.of())))
            .headers(Property.ofValue(Map.of(
                "content-type", "application/json; charset=UTF-8"
            )))
            .build();

        var output = task.run(runContextFactory.of());

        assertThat(output.getDagRunId(), is("manual__content_type"));
        wireMock.verify(
            postRequestedFor(urlEqualTo(DAG_RUNS))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
        );
    }

    private static void stubDagRun(String dagRunId) {
        wireMock.stubFor(
            post(urlEqualTo(DAG_RUNS))
                .willReturn(okJson("""
                    {"dag_id":"example_astronauts","dag_run_id":"%s","state":"queued"}
                    """.formatted(dagRunId)))
        );
        wireMock.stubFor(
            get(urlEqualTo("%s/%s".formatted(DAG_RUNS, dagRunId)))
                .willReturn(okJson("""
                    {"dag_id":"example_astronauts","dag_run_id":"%s","state":"success",
                     "start_date":"2026-10-19T08:00:00+00:00","end_date":"2026-10-19T08:00:05+00:00"}
                    """.formatted(dagRunId)))
        );
    }
}
