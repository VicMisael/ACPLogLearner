# Saved Agent Context

Date saved: 2026-08-05

## Project Summary

A thesis project where the goal is to use `VFDT` to select the best atomic commit protocol at commit time.

## Current Direction

- Keep `MariaDbTransactionJournalWorkload` only as an XA smoke test.
- Move toward a realistic `TPC-C-like` workload using `BenchmarkSQL`.
- Use `BenchmarkSQL` for schema/data generation and OLTP-style transaction patterns.
- Do not rely on `BenchmarkSQL` for distributed coordination.
- Keep distributed `XA/2PC` under the Java coordinator in this repo.
- Target 4 MariaDB instances.
- Preferred data layout: warehouse sharding across the 4 nodes.
- Next realistic implementation step: implement one TPC-C transaction type first, preferably `NewOrder`, inside a `MariaDbXaWorkload`.

## What Was Verified

- Podman was installed and configured.
- `podman-compose` was installed as the compose provider.
- The 4-instance MariaDB stack defined in `docker-compose.yml` was brought up successfully.
- The current MariaDB XA smoke test ran successfully against `mariadb-1`.
- The journal table persisted one committed row per participant for the same transaction id.

## Important Technical Clarification

The current `MariaDbTransactionJournalWorkload` is not a realistic business workload. It only:

- writes `PREPARED` during XA prepare
- writes `COMMITTED` after commit
- writes `ABORTED` after rollback

That proves the XA path works, but it is only a persistence probe, not a thesis-grade OLTP workload.

## TPC-C Plan

`BenchmarkSQL` should be used as the practical `TPC-C-like` tool.

How it fits:

- `BenchmarkSQL` loads realistic schema and data into MariaDB.
- `BenchmarkSQL` can run realistic OLTP-style transaction mixes.
- `BenchmarkSQL` targets one JDBC database per run.
- `BenchmarkSQL` does not coordinate one distributed XA transaction across the 4 MariaDB nodes.

Therefore:

- use `BenchmarkSQL` for schema/data and transaction shape
- use the Java coordinator for distributed XA execution

## Intended Distributed Setup

Preferred layout: shard warehouses across the four MariaDB nodes.

Example:

- `mariadb-1`: warehouses `1-25`
- `mariadb-2`: warehouses `26-50`
- `mariadb-3`: warehouses `51-75`
- `mariadb-4`: warehouses `76-100`

Then the Java coordinator routes each logical transaction to the correct participant databases and runs:

- `XA START`
- business SQL
- `XA END`
- `XA PREPARE`
- `XA COMMIT` or `XA ROLLBACK`

## Known Code Limitation

The current `MariaDbMain` still uses a single `ACP_MARIADB_URL`, so all logical participants still point to one MariaDB instance. The infrastructure now supports 4 DB instances, but the Java side still needs per-participant JDBC configuration.

## Next Steps

1. Load `TPC-C-like` schema/data into the 4 MariaDB nodes using `BenchmarkSQL`.
2. Shard warehouses by node.
3. Implement one realistic workload class first, preferably `MariaDbTpccNewOrderWorkload`.
4. Wire participants to distinct JDBC URLs instead of one shared `ACP_MARIADB_URL`.
5. Keep the journal workload available as a smoke-test fallback.
