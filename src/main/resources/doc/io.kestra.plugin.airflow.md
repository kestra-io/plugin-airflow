# How to use the Airflow plugin

Trigger Airflow DAG runs from Kestra flows and optionally wait for completion.

## Authentication

Set `baseUrl` to your Airflow instance URL (e.g. `http://localhost:8080`). Pass credentials via `options` (e.g. basic auth username and password via `options.auth`) or via custom `headers`. Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and apply connection properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

`dags.TriggerDagRun` triggers a DAG run — set `dagId` (required). Pass run configuration via `body` as a map. By default the task returns immediately after triggering (`wait: false`); set `wait: true` to poll until the run reaches a terminal state. Control polling with `pollFrequency` (default 1 second) and cap wait time with `maxDuration` (default 60 minutes).
