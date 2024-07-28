package io.kestra.plugin.airflow.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.time.OffsetDateTime;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class DagRunResponse {

    @JsonProperty("dag_id")
    private String dagId;

    @JsonProperty("dag_run_id")
    private String dagRunId;

    @JsonProperty("end_date")
    private OffsetDateTime endDate;

    @JsonProperty("execution_date")
    private OffsetDateTime executionDate;

    @JsonProperty("start_date")
    private OffsetDateTime startDate;

    @JsonProperty("run_type")
    private String runType;

    private String state;

}