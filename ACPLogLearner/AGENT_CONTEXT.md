# Saved Agent Context

Date saved: 2026-08-28

## Project Summary

A master's thesis project investigating how online machine-learning models can select the best atomic commit protocol at commit time. `VFDT` is the current candidate model, but the thesis is not restricted to VFDT and may evaluate other models.

The current thesis execution plan is documented in `docs/alps-thesis-plan.md`.

## Core Thesis Decision: Execution Data and MariaDB Logs

The central thesis idea is to use both transaction-execution data and MariaDB
log-derived data to determine the best atomic commit protocol for the current
context. The target is not merely to predict whether a transaction will commit
or abort. The evaluation must show whether ALPS selects the protocol with the
lowest expected cost among classic 2PC, presumed abort, and presumed commit.

The intended input combines:

- features known from the current request before protocol selection, such as
  transaction type, participants, participant count, local/distributed shape,
  warehouses, and order-line count;
- historical execution metrics from completed transactions, such as commit and
  abort rates, phase latency, blocking time, messages, timeouts, and errors;
- historical MariaDB log and storage-engine metrics, especially InnoDB redo/WAL
  activity, LSN movement, redo writes and flushes, checkpoint pressure, XA state
  and duration from Performance Schema, and later possibly row-level binary-log
  information.

`AlpsFeatures` is the planned immutable model-input record. It is not an
alternative to using MariaDB logs: it is the destination for request features
and historical features extracted from those logs.

### Leakage rule

The MariaDB logs and final execution metrics produced by transaction `T` cannot
be used to select the protocol for `T`. They become historical input for later
transactions. The required online order is:

1. build features for `T` from its request and history through `T-1`;
2. predict context and select the protocol;
3. execute `T`;
4. collect execution and MariaDB log deltas for `T`;
5. derive feedback, update rolling history, and train the model.

### MariaDB log terminology and collection direction

- InnoDB redo log is the MariaDB mechanism corresponding to physical WAL and is
  the primary log-derived signal of interest.
- MariaDB binary log is a separate logical change stream. It may later provide
  table/row-change features, but it is not the same as InnoDB redo.
- Do not begin by parsing the internal bytes of `ib_logfile0`. First collect
  supported observable metrics such as `Innodb_lsn_current`,
  `Innodb_lsn_flushed`, `Innodb_log_writes`, checkpoint age, and relevant
  Performance Schema transaction/XA data.
- Correlate observations with the Java `TransactionId`/XA XID where supported.
- Under concurrent execution, global InnoDB counter deltas contain activity from
  other transactions; experiments must account for this with serialized trials,
  controlled isolation, or explicit attribution limits.

### Best-protocol evaluation

The three context labels (`LIKELY_COMMIT`, `LIKELY_ABORT`, and `UNCERTAIN`) may
remain as an explainable intermediate layer, but classification accuracy alone
does not establish that the best protocol was selected. Controlled experiments
must replay equivalent transaction specifications under all three protocols and
derive an oracle protocol from correctness and raw cost metrics. Cost evaluation
should preserve latency, MariaDB redo/WAL activity, logical log writes, message
count, blocking time, timeouts, and failures separately before applying any
weighted scalar objective.

The next log-focused implementation direction is to introduce records and
collectors equivalent to `MariaDbLogSnapshot`, `MariaDbLogDelta`,
`MariaDbWalMetricsCollector`, `RollingMetricsRegistry`, and the expanded
`AlpsFeatures`.

## Current Event-Pipeline Decision

Keep learning events in memory, consistent with the existing prototype. Do not
introduce JSONL, Kafka, or Redpanda into the core implementation at this stage.
Machine-readable aggregate experiment results may still be exported after a run.
JSONL replay and Kafka/Redpanda streaming remain documented future extensions
behind the same event-publishing abstraction.

## Immediate Goal

Set up and validate the four-instance MariaDB environment using Docker Compose. This database environment will support XA smoke testing now and the later distributed `TPC-C-like` workload.

The repository now includes a pinned, containerized BenchmarkSQL setup for loading and validating a small independent dataset on each MariaDB node. The one-warehouse smoke dataset has been loaded successfully on all four nodes.

The next step is to wire the Java application to the four MariaDB nodes and their BenchmarkSQL datasets. Each protocol participant must receive its own JDBC/XA configuration instead of sharing the single `ACP_MARIADB_URL` currently used by `MariaDbMain`.

## Current Direction

- Wire the Java application to the validated four-node MariaDB setup before extending the workload or learning pipeline.
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
- BenchmarkSQL was built from pinned upstream commit `98f5cb4b446f4296ca5e85ad27303482a6bbd624`.
- The BenchmarkSQL schema, indexes, foreign keys, and one-warehouse smoke dataset were loaded successfully on all four MariaDB nodes.
- Each node was verified with `1` row in `bmsql_warehouse` and `100000` rows in `bmsql_item`.
- The host has Podman but still needs a permanently installed Compose provider (`sudo apt-get install podman-compose`); validation used a temporary extracted copy because sudo authentication was unavailable to the agent.

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

The upstream BenchmarkSQL loader always starts warehouse identifiers at `1`; `useWarehouseFrom` and `useWarehouseTo` only constrain benchmark execution. Loading each node independently therefore does not yet produce the intended global warehouse ranges. A loader extension or deterministic partitioning step is required before calling the four datasets globally sharded.

## Next Steps

1. Wire the Java participants to distinct JDBC/XA URLs for the four MariaDB nodes instead of one shared `ACP_MARIADB_URL`.
2. Verify the Java application can address and execute XA work against each node's BenchmarkSQL dataset.
3. Implement global warehouse sharding by node.
4. Implement one realistic workload class first, preferably `MariaDbTpccNewOrderWorkload`.
5. Keep the journal workload available as a smoke-test fallback.
