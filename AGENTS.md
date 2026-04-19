# Kestra Airflow Plugin

## What

- Provides plugin components under `io.kestra.plugin.airflow`.
- Includes classes such as `AirflowConnection`, `DagRunResponse`, `TriggerDagRun`.

## Why

- What user problem does this solve? Teams need to let Kestra workflows interact with Apache Airflow from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Airflow steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Airflow.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `airflow`

Infrastructure dependencies (Docker Compose services):

- `airflow-init`
- `airflow-scheduler`
- `airflow-webserver`
- `airflow_db`

### Key Plugin Classes

- `io.kestra.plugin.airflow.dags.TriggerDagRun`

### Project Structure

```
plugin-airflow/
├── src/main/java/io/kestra/plugin/airflow/model/
├── src/test/java/io/kestra/plugin/airflow/model/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
