#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILES=(-f "${PROJECT_DIR}/docker-compose.yml" -f "${PROJECT_DIR}/docker-compose.benchmarksql.yml")

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v podman >/dev/null 2>&1 && podman compose version >/dev/null 2>&1; then
  COMPOSE=(podman compose)
elif command -v podman-compose >/dev/null 2>&1; then
  COMPOSE=(podman-compose)
else
  echo "Docker Compose or Podman Compose is required." >&2
  exit 1
fi

compose() {
  "${COMPOSE[@]}" "${COMPOSE_FILES[@]}" "$@"
}

start_databases() {
  compose up -d mariadb-1 mariadb-2 mariadb-3 mariadb-4
  wait_for_databases
}

wait_for_databases() {
  local node
  local attempt
  for node in 1 2 3 4; do
    echo "==> Waiting for mariadb-${node}"
    for attempt in $(seq 1 60); do
      if compose exec -T "mariadb-${node}" mariadb-admin \
        ping --host=127.0.0.1 \
        --user=root \
        --password="${ACP_MARIADB_ROOT_PASSWORD:-rootpass}" \
        --silent >/dev/null 2>&1; then
        break
      fi
      if [[ "${attempt}" -eq 60 ]]; then
        echo "mariadb-${node} did not become ready within 120 seconds." >&2
        return 1
      fi
      sleep 2
    done
  done
}

run_for_node() {
  local node="$1"
  local operation="$2"
  echo "==> BenchmarkSQL ${operation}: mariadb-${node}"
  compose --profile benchmarksql run --rm --no-deps \
    -e "BMSQL_DB_HOST=mariadb-${node}" \
    benchmarksql "${operation}"
}

for_each_node() {
  local operation="$1"
  local node
  for node in 1 2 3 4; do
    run_for_node "${node}" "${operation}"
  done
}

verify() {
  local node
  for node in 1 2 3 4; do
    echo "==> BenchmarkSQL tables: mariadb-${node}"
    compose exec -T "mariadb-${node}" mariadb \
      --user="${ACP_MARIADB_USER:-acp}" \
      --password="${ACP_MARIADB_PASSWORD:-acp_secret}" \
      "${ACP_MARIADB_DATABASE:-acp}" \
      --batch --skip-column-names \
      --execute="SELECT COUNT(*) AS warehouses FROM bmsql_warehouse; SELECT COUNT(*) AS items FROM bmsql_item;"
  done
}

case "${1:-help}" in
  build-image)
    compose --profile benchmarksql build benchmarksql
    ;;
  start)
    start_databases
    ;;
  load-all)
    start_databases
    for_each_node build
    verify
    ;;
  load)
    node="${2:?Usage: scripts/benchmarksql.sh load NODE}"
    start_databases
    run_for_node "${node}" build
    ;;
  destroy-all)
    for_each_node destroy
    ;;
  verify)
    verify
    ;;
  run)
    node="${2:-1}"
    start_databases
    run_for_node "${node}" run
    ;;
  *)
    cat <<'EOF'
Usage: scripts/benchmarksql.sh COMMAND

Commands:
  build-image       Build the pinned BenchmarkSQL container image
  start             Start the four MariaDB services
  load-all          Load the TPC-C-like schema and smoke-test data on all nodes
  load NODE         Load one node (useful when resuming an interrupted load)
  verify            Count warehouses and items on all nodes
  run [NODE]        Run a one-minute smoke benchmark against node 1-4
  destroy-all       Drop BenchmarkSQL tables from all four nodes

Environment:
  BMSQL_WAREHOUSES   Warehouses loaded per database (default: 1)
  BMSQL_LOAD_WORKERS Parallel loader connections (default: 2)
  ACP_MARIADB_*      Existing Compose database credentials/settings
EOF
    ;;
esac
