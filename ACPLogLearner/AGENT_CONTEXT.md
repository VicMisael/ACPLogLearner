# Saved Agent Context

Date saved: 2026-09-12

## Project Summary

A master's thesis project investigating how online machine-learning models can select the best atomic commit protocol at commit time. `VFDT` is the current prototype and first candidate to evaluate, not the final model decision. The thesis is not restricted to VFDT and must keep the classifier replaceable so that other online models and the threshold-only baseline can be evaluated without changing protocol execution, feature collection, or policy selection.

The current thesis execution plan is documented in `docs/alps-thesis-plan.md`.

The test topology now assigns one global warehouse to each MariaDB node:
warehouse `1` to `mariadb-1`, through warehouse `4` to `mariadb-4`. The load
script remaps and verifies all warehouse-bearing BenchmarkSQL rows after load.

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
- Direct `ib_logfile` parsing remains out of scope unless supported counters are
  shown to be insufficient. Its internal format is version-dependent, global to
  the server, and does not solve per-transaction attribution by itself.
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

The first redo/WAL collection slice is implemented. `RedoMetricsCollector`
exposes snapshots independently of MariaDB, `MariaDbWalMetricsCollector` reads
supported global InnoDB counters over separate JDBC connections, and
`RedoDelta` calculates per-node changes while preserving unavailable values and
detecting counter resets/server restarts. `MariaDbTpccClusterMain` surrounds
each sharded New-Order with snapshots and reports a delta after every XA branch
has resolved.

The deltas are now carried in immutable `TransactionTrialRecord` values produced
by `MariaDbTpccTransactionRunner`. A trial contains transaction/specification
IDs, protocol, outcome, total and phase latency, message and protocol-log counts,
and per-participant redo metrics with reset/unavailable flags. Both the cluster
smoke runner and `MariaDbAlpsMain` use this shared execution path.

`RollingMetricsRegistry` maintains bounded global and per-participant histories
and builds expanded `AlpsFeatures` before execution. Completed trials are added
only afterward, enforcing the `T-1` leakage boundary. The MariaDB ALPS runner
feeds these request and historical metrics to VFDT.

The learning boundary is workload-independent. `TransactionContext` contains
generic identity, transaction type, participants, and named numeric workload
attributes. `TransactionContextExtractor<S>` adapts a workload specification;
`TpccNewOrderFeatureExtractor` is the TPC-C-specific adapter. Neither
`AlpsFeatures`, `RollingMetricsRegistry`, nor the VFDT predictor imports or
hard-codes TPC-C concepts.

The default thesis feature profile is `GENERIC`. It trains on domain-neutral
transaction shape (read/write/insert/update/delete counts, estimated rows,
participant fan-out, and remote-operation ratio), global rolling history, and
mean/worst-case summaries across the selected participants. It does not expose
raw participant IDs, transaction names, or TPC-C fields. The node-aware,
workload-aware, and complete profiles are retained explicitly for feature
ablation experiments, selected through `ACP_ALPS_FEATURE_PROFILE`.

## Current Event-Pipeline Decision

Keep learning events in memory, consistent with the existing prototype. Do not
introduce JSONL, Kafka, or Redpanda into the core implementation at this stage.
Machine-readable aggregate experiment results may still be exported after a run.
JSONL replay and Kafka/Redpanda streaming remain documented future extensions
behind the same event-publishing abstraction.

## Online Learning and Outcome Vocabulary

ALPS is an online-learning system evaluated prequentially. For each observation,
it must predict first, execute unseen work, derive the observed label, score the
earlier prediction, and only then train the model. There is no conventional
random train/test split in the live path, and all reported accuracy, macro-F1,
regret, and adaptation results must preserve temporal order.

Keep three different concepts separate:

- execution status describes what happened to an individual transaction, with
  `COMMITTED`, `ABORTED`, and `TIMED_OUT` currently represented by
  `TrialOutcome`; later recovery experiments may justify `FAILED`, `IN_DOUBT`,
  or heuristic-resolution statuses;
- observed context is the current three-class learning target:
  `LIKELY_COMMIT`, `UNCERTAIN`, or `LIKELY_ABORT`;
- oracle protocol is the lowest-cost correct protocol found by replaying the
  same specification under all candidates, with ties and invalid trials stored
  explicitly rather than disguised as transaction outcomes.

More execution statuses can be added without necessarily increasing the number
of classifier classes. Expanding the online-learning target is a thesis design
decision and must be supported by enough temporally ordered examples per class.
A strong later comparison is the current context-plus-policy formulation versus
direct online prediction of the oracle protocol; do not conflate either target
with low-level execution failure states.

The intended extension is multiple online transaction-outcome targets, not
direct prediction of the best protocol and not merely more values in
`TrialOutcome`. Model these as separate learner heads over the same leakage-safe
`AlpsFeatures`, rather than a Cartesian product of labels:

- commit/abort likelihood or calibrated commit probability;
- expected total and phase latency;
- expected redo bytes, LSN advancement, redo writes, and redo waits;
- expected message and protocol-log counts;
- optional timeout, blocking, or recovery-risk probabilities when enough events
  exist to support those targets.

The policy consumes this predicted outcome/cost bundle and selects classic 2PC,
presumed abort, or presumed commit. The paired-trial oracle remains an evaluation
baseline for policy quality and regret; it is not the primary learner target.

Each head must predict before execution and train only when its own feedback is
available. Missing or invalid metrics must skip that head's update rather than
be encoded as zero. Protocol-specific outcome estimates require controlled
exploration or same-specification replay because a live transaction reveals
feedback only for the selected protocol.

## Immediate Goal

Set up and validate the four-instance MariaDB environment using Docker Compose. This database environment will support XA smoke testing now and the later distributed `TPC-C-like` workload.

The repository now includes a pinned, containerized BenchmarkSQL setup for
loading and validating one global warehouse shard on each MariaDB node. The
four-shard smoke dataset has been loaded and verified successfully.

The Java cluster and ALPS runners are wired to four JDBC/XA configurations and
use the participant number as the global warehouse ID.

## Current Direction

- Wire the Java application to the validated four-node MariaDB setup before extending the workload or learning pipeline.
- Keep `MariaDbTransactionJournalWorkload` only as an XA smoke test.
- Move toward a realistic `TPC-C-like` workload using `BenchmarkSQL`.
- Use `BenchmarkSQL` for schema/data generation and OLTP-style transaction patterns.
- Do not rely on `BenchmarkSQL` for distributed coordination.
- Keep distributed `XA/2PC` under the Java coordinator in this repo.
- Target 4 MariaDB instances.
- Data layout: one warehouse shard per MariaDB node for tests.
- Extend the existing `NewOrder` implementation from independent local branches
  into one logical TPC-C New-Order whose home and remote-stock work is routed to
  the warehouse-owning shards.

## What Was Verified

- Podman was installed and configured.
- `podman-compose` was installed as the compose provider.
- The 4-instance MariaDB stack defined in `docker-compose.yml` was brought up successfully.
- The current MariaDB XA smoke test ran successfully against `mariadb-1`.
- The journal table persisted one committed row per participant for the same transaction id.
- BenchmarkSQL was built from pinned upstream commit `98f5cb4b446f4296ca5e85ad27303482a6bbd624`.
- The BenchmarkSQL schema, indexes, foreign keys, and sharded smoke dataset were loaded successfully on all four MariaDB nodes.
- Each node was verified with warehouse ID `N` only on `mariadb-N` and `100000` replicated rows in `bmsql_item`.
- The host has Podman and `podman-compose` installed; the four-node Compose
  workflow and BenchmarkSQL tool container have been executed successfully.

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

Test layout:

- `mariadb-1`: warehouse `1`
- `mariadb-2`: warehouse `2`
- `mariadb-3`: warehouse `3`
- `mariadb-4`: warehouse `4`

Then the Java coordinator routes each logical transaction to the correct participant databases and runs:

- `XA START`
- business SQL
- `XA END`
- `XA PREPARE`
- `XA COMMIT` or `XA ROLLBACK`

## Known Scope Limitation

The distributed smoke workload now models one logical New-Order with its order
rows on a home shard and remote stock updates on supplier shards. Its
deterministic smoke specification intentionally exercises every selected shard;
the standard TPC-C remote-line distribution, complete transaction output/tax
calculation, snapshot-based replay, and the other TPC-C transaction types remain
to be implemented.

## Current TPC-C Fidelity Status

The current setup is materially closer to TPC-C than the earlier four-copy
smoke environment:

- the schema, initial data, indexes, and constraints come from the pinned
  BenchmarkSQL distribution;
- warehouse identifiers are globally partitioned as `P1/W1`, `P2/W2`,
  `P3/W3`, and `P4/W4`;
- each node contains its own warehouse-dependent rows and a complete replicated
  `bmsql_item` catalog;
- the Java coordinator executes one logical New-Order whose home order rows and
  supplier stock updates are split into real MariaDB XA branches;
- committed remote lines set `o_all_local=0`, retain their global
  `ol_supply_w_id`, and increment `s_remote_cnt` on the owning supplier shard;
- invalid-final-item trials roll back home and supplier changes under all three
  atomic commit protocols and leave no prepared XA branches.

It is not yet a complete distributed TPC-C transaction mix. Payment must support
the TPC-C remote-customer case, and Order-Status, Delivery, and Stock-Level must
be integrated. The controlled generator must also reproduce the standard
transaction and rollback proportions before thesis measurements begin.

Use this description for the current system:

> TPC-C-like workload based on BenchmarkSQL, with a warehouse-sharded dataset
> and MariaDB XA execution of New-Order branches across four databases.

Do not claim official TPC-C compliance. The setup has not implemented the full
distributed transaction semantics, measurement rules, or audited TPC-C
requirements.

## Next Steps

1. Add automated database integration tests and invariant checks for the
   globally sharded New-Order and redo/WAL capture boundary.
2. Build reproducible same-specification trials under all three protocols for
   the oracle dataset.
3. Export machine-readable aggregate experiment results after each run.
4. Add the remote-customer Payment case and then the remaining TPC-C transaction
   types while preserving the standard mix and rollback proportions.
5. Keep the journal workload available as a smoke-test fallback.
