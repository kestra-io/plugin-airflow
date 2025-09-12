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
echo "tutorial_dag is now available"
