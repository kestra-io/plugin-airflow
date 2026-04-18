# Kestra Airflow Plugin

## What

- Provides plugin components under `io.kestra.plugin.airflow`.
- Includes classes such as `AirflowConnection`, `DagRunResponse`, `TriggerDagRun`.

## Why

- This plugin integrates Kestra with Airflow Dags.
- It provides tasks that trigger and monitor Apache Airflow DAG runs.

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
