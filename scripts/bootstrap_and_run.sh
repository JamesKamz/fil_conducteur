#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DAG_ID="main_etl_pipeline"
if [[ "${1:-}" == "--dag-id" && -n "${2:-}" ]]; then
  DAG_ID="$2"
fi

echo "[1/5] Starting core containers (postgres, minio, spark)..."
docker compose up -d postgres minio spark

echo "[2/5] Running one-time Airflow initialization..."
docker compose run --rm --user 0:0 airflow-init

echo "[3/5] Starting Airflow service..."
docker compose up -d airflow

echo "[4/5] Waiting for Airflow service to accept CLI commands..."
for i in {1..30}; do
  if docker compose exec -T airflow airflow version >/dev/null 2>&1; then
    break
  fi
  if [[ "$i" -eq 30 ]]; then
    echo "Airflow is not ready after waiting. Check logs with: docker compose logs airflow"
    exit 1
  fi
  sleep 3
done

echo "[5/5] Unpausing and triggering DAG ${DAG_ID}..."
docker compose exec -T airflow airflow dags unpause "$DAG_ID"
docker compose exec -T airflow airflow dags trigger "$DAG_ID"

echo "Service and DAG summary"
docker compose ps
docker compose exec -T airflow airflow dags list-runs -d "$DAG_ID" --no-backfill | head -n 20 || true

echo "Done. Airflow: http://localhost:8080  MinIO: http://localhost:9001"
