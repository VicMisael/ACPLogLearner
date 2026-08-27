#!/usr/bin/env bash
set -euo pipefail

: "${BMSQL_DB_HOST:?BMSQL_DB_HOST is required}"
: "${BMSQL_DATABASE:?BMSQL_DATABASE is required}"
: "${BMSQL_USER:?BMSQL_USER is required}"
: "${BMSQL_PASSWORD:?BMSQL_PASSWORD is required}"

BMSQL_WAREHOUSES="${BMSQL_WAREHOUSES:-1}"
BMSQL_LOAD_WORKERS="${BMSQL_LOAD_WORKERS:-2}"
BMSQL_PROPERTIES="/tmp/benchmarksql.properties"

cat > "${BMSQL_PROPERTIES}" <<EOF
db=mariadb
driver=org.mariadb.jdbc.Driver
application=Generic
conn=jdbc:mariadb://${BMSQL_DB_HOST}:3306/${BMSQL_DATABASE}
user=${BMSQL_USER}
password=${BMSQL_PASSWORD}

warehouses=${BMSQL_WAREHOUSES}
useWarehouseFrom=-1
useWarehouseTo=-1
loadWorkers=${BMSQL_LOAD_WORKERS}
monkeys=1
sutThreads=2
maxDeliveryBGThreads=1
maxDeliveryBGPerWarehouse=1
rampupMins=0
rampupSUTMins=0
rampupTerminalMins=0
runMins=1
reportIntervalSecs=10
restartSUTThreadProbability=0.0
keyingTimeMultiplier=1.0
thinkTimeMultiplier=1.0
terminalMultiplier=1
traceTerminalIO=false

paymentWeight=43.2
orderStatusWeight=4.2
deliveryWeight=4.2
stockLevelWeight=4.2
rollbackPercent=1.01

resultDirectory=/benchmarksql/results/${BMSQL_DB_HOST}_result_%tY-%tm-%td_%tH%tM%tS
EOF

cd /benchmarksql/run

case "${1:-build}" in
  build)
    exec ./runDatabaseBuild.sh "${BMSQL_PROPERTIES}"
    ;;
  destroy)
    exec ./runDatabaseDestroy.sh "${BMSQL_PROPERTIES}"
    ;;
  run)
    exec ./runBenchmark.sh "${BMSQL_PROPERTIES}"
    ;;
  shell)
    exec /bin/bash
    ;;
  *)
    echo "Usage: entrypoint.sh {build|destroy|run|shell}" >&2
    exit 2
    ;;
esac
