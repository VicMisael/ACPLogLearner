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

require_one_warehouse_per_node() {
  if [[ "${BMSQL_WAREHOUSES:-1}" != "1" ]]; then
    echo "BMSQL_WAREHOUSES must be 1: each MariaDB node represents exactly one warehouse." >&2
    exit 2
  fi
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
    -e "BMSQL_WAREHOUSE_ID=${node}" \
    -e "BMSQL_TOTAL_WAREHOUSES=4" \
    benchmarksql "${operation}"
}

shard_node() {
  local node="$1"
  echo "==> Assigning global warehouse ${node} to mariadb-${node}"
  compose exec -T "mariadb-${node}" mariadb \
    --user="${ACP_MARIADB_USER:-acp}" \
    --password="${ACP_MARIADB_PASSWORD:-acp_secret}" \
    "${ACP_MARIADB_DATABASE:-acp}" \
    --execute="
      SET FOREIGN_KEY_CHECKS = 0;
      START TRANSACTION;
      UPDATE bmsql_district SET d_w_id = ${node} WHERE d_w_id = 1;
      UPDATE bmsql_customer SET c_w_id = ${node} WHERE c_w_id = 1;
      UPDATE bmsql_history
        SET h_c_w_id = IF(h_c_w_id = 1, ${node}, h_c_w_id),
            h_w_id = IF(h_w_id = 1, ${node}, h_w_id);
      UPDATE bmsql_new_order SET no_w_id = ${node} WHERE no_w_id = 1;
      UPDATE bmsql_oorder SET o_w_id = ${node} WHERE o_w_id = 1;
      UPDATE bmsql_order_line
        SET ol_w_id = IF(ol_w_id = 1, ${node}, ol_w_id),
            ol_supply_w_id = IF(ol_supply_w_id = 1, ${node}, ol_supply_w_id);
      UPDATE bmsql_stock SET s_w_id = ${node} WHERE s_w_id = 1;
      UPDATE bmsql_warehouse SET w_id = ${node} WHERE w_id = 1;
      COMMIT;
      SET FOREIGN_KEY_CHECKS = 1;
    "
}

for_each_node() {
  local operation="$1"
  local node
  for node in 1 2 3 4; do
    run_for_node "${node}" "${operation}"
    if [[ "${operation}" == "build" ]]; then
      shard_node "${node}"
    fi
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
      --execute="
        SELECT CONCAT(
          'warehouse=', MIN(w_id),
          ', warehouses=', COUNT(*),
          ', items=', (SELECT COUNT(*) FROM bmsql_item)
        ) FROM bmsql_warehouse;
        SELECT CASE WHEN
          (SELECT COUNT(*) FROM bmsql_warehouse) = 1 AND
          (SELECT COUNT(*) FROM bmsql_item) = 100000 AND
          (SELECT COUNT(*) FROM bmsql_warehouse WHERE w_id <> ${node}) = 0 AND
          (SELECT COUNT(*) FROM bmsql_district WHERE d_w_id <> ${node}) = 0 AND
          (SELECT COUNT(*) FROM bmsql_customer WHERE c_w_id <> ${node}) = 0 AND
          (SELECT COUNT(*) FROM bmsql_history WHERE h_c_w_id <> ${node} OR h_w_id <> ${node}) = 0 AND
          (SELECT COUNT(*) FROM bmsql_new_order WHERE no_w_id <> ${node}) = 0 AND
          (SELECT COUNT(*) FROM bmsql_oorder WHERE o_w_id <> ${node}) = 0 AND
          (SELECT COUNT(*) FROM bmsql_order_line
             WHERE ol_w_id <> ${node} OR ol_supply_w_id <> ${node}) = 0 AND
          (SELECT COUNT(*) FROM bmsql_stock WHERE s_w_id <> ${node}) = 0
        THEN CONCAT('shard-ok: warehouse ', ${node})
        ELSE CONCAT('shard-invalid: warehouse ', ${node}) END;
      " | tee /dev/stderr | grep -q "shard-ok: warehouse ${node}"
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
    require_one_warehouse_per_node
    start_databases
    for_each_node build
    verify
    ;;
  load)
    node="${2:?Usage: scripts/benchmarksql.sh load NODE}"
    if [[ ! "${node}" =~ ^[1-4]$ ]]; then
      echo "NODE must be between 1 and 4." >&2
      exit 2
    fi
    require_one_warehouse_per_node
    start_databases
    run_for_node "${node}" build
    shard_node "${node}"
    ;;
  shard-all)
    start_databases
    for node in 1 2 3 4; do
      shard_node "${node}"
    done
    verify
    ;;
  shard)
    node="${2:?Usage: scripts/benchmarksql.sh shard NODE}"
    if [[ ! "${node}" =~ ^[1-4]$ ]]; then
      echo "NODE must be between 1 and 4." >&2
      exit 2
    fi
    start_databases
    shard_node "${node}"
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
  load-all          Load one globally identified warehouse per MariaDB node
  load NODE         Load warehouse NODE on MariaDB node NODE (1-4)
  shard-all         Remap existing one-warehouse datasets and verify all shards
  shard NODE        Remap an existing dataset to warehouse/node NODE (1-4)
  verify            Verify warehouse ownership and references on every shard
  run [NODE]        Run a one-minute smoke benchmark against node 1-4
  destroy-all       Drop BenchmarkSQL tables from all four nodes

Environment:
  BMSQL_LOAD_WORKERS Parallel loader connections (default: 2)
  ACP_MARIADB_*      Existing Compose database credentials/settings
EOF
    ;;
esac
