#!/usr/bin/env bash
set -euo pipefail

docker compose -f docker-compose-ci.yml up -d

echo "waiting for Airflow health endpoint"
timeout 180 bash -c '
  until curl --silent --fail http://localhost:8082/health >/dev/null; do
    sleep 3
  done
'
echo "Airflow is healthy"

echo "waiting for tutorial_dag to be available"
timeout 120 bash -c '
  until curl -s -u airflow:airflow --fail \
    http://localhost:8082/api/v1/dags/tutorial_dag >/dev/null; do
    sleep 2
  done
'
echo "tutorial_dag is available"


curl -s -u airflow:airflow -X PATCH \
  -H 'Content-Type: application/json' \
  -d '{"is_paused": false}' \
  http://localhost:8082/api/v1/dags/tutorial_dag >/dev/null

# we wait until API confirms unpaused
timeout 60 bash -c '
  until [ "$(curl -s -u airflow:airflow http://localhost:8082/api/v1/dags/tutorial_dag | jq -r .is_paused)" = "false" ]; do
    sleep 2
  done
'
echo "tutorial_dag is unpaused"
