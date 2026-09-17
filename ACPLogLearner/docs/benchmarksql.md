# BenchmarkSQL setup

This project runs BenchmarkSQL as a Compose tool container alongside the four
MariaDB services. The image is built from upstream BenchmarkSQL commit
`98f5cb4b446f4296ca5e85ad27303482a6bbd624` so that experiment setup is
reproducible.

Docker Compose or Podman Compose is required. On Ubuntu with Podman, install the
Compose provider once:

```bash
sudo apt-get install podman-compose
```

## Sharded smoke load

Load one globally identified warehouse into each MariaDB node:

```bash
./scripts/benchmarksql.sh load-all
```

The command starts the databases, builds the BenchmarkSQL image when necessary,
loads its schema and data, remaps every TPC-C warehouse reference, and verifies
the resulting ownership:

| MariaDB node | Global warehouse |
| --- | ---: |
| `mariadb-1` | `1` |
| `mariadb-2` | `2` |
| `mariadb-3` | `3` |
| `mariadb-4` | `4` |

Every node contains the replicated `bmsql_item` catalog, but warehouse-owned
rows (`district`, `customer`, `stock`, orders, and history) exist only on the
owning node. `BMSQL_WAREHOUSES` must remain `1` for this topology.

BenchmarkSQL database builds are not idempotent. If its tables already exist,
explicitly destroy them before loading again:

```bash
./scripts/benchmarksql.sh destroy-all
./scripts/benchmarksql.sh load-all
```

`destroy-all` removes the `bmsql_*` tables but does not remove the project's
`tx_journal` table or the MariaDB volumes.

To migrate four datasets loaded by an older checkout (all using warehouse `1`)
without reloading their data, run:

```bash
./scripts/benchmarksql.sh shard-all
```

## Verification and smoke benchmark

```bash
./scripts/benchmarksql.sh verify
./scripts/benchmarksql.sh run 1
```

The included run configuration lasts one minute and is intended only to validate
connectivity and transaction execution, not to produce thesis measurements.

## Distributed XA New-Order smoke test

After loading the four databases, the Java coordinator can execute one atomic
transaction across the four warehouse shards. XA branch `P1` uses warehouse `1`,
`P2` uses warehouse `2`, and so on:

```bash
./gradlew runMariaDbTpccCluster
```

The default JDBC endpoints correspond to the Compose host ports `33061` through
`33064`. The common credentials and run parameters can be overridden:

```bash
ACP_MARIADB_USER=acp \
ACP_MARIADB_PASSWORD=acp_secret \
ACP_MARIADB_1_URL=jdbc:mariadb://127.0.0.1:33061/acp \
ACP_MARIADB_2_URL=jdbc:mariadb://127.0.0.1:33062/acp \
ACP_MARIADB_3_URL=jdbc:mariadb://127.0.0.1:33063/acp \
ACP_MARIADB_4_URL=jdbc:mariadb://127.0.0.1:33064/acp \
ACP_PROTOCOL=TWO_PC_PRESUMED_COMMIT \
ACP_TPCC_TRANSACTIONS=10 \
./gradlew runMariaDbTpccCluster
```

To exercise atomic rollback with the same cross-shard New-Order shape, replace
the final item with the TPC-C invalid item identifier:

```bash
ACP_PROTOCOL=TWO_PC \
ACP_TPCC_FORCE_ROLLBACK=true \
./gradlew runMariaDbTpccCluster
```

`ACP_PROTOCOL` accepts `TWO_PC`, `TWO_PC_PRESUMED_ABORT`, or
`TWO_PC_PRESUMED_COMMIT`.

This entry point is an integration and protocol smoke test, not a compliant
TPC-C benchmark. It creates one logical New-Order on warehouse `1`. Order and
order-line rows remain on that home shard, while stock updates for remotely
supplied lines execute on warehouses `2`, `3`, and `4` as branches of the same
XA transaction. The controlled smoke specification intentionally touches all
four shards; it does not represent the standard TPC-C remote-line frequency.

### Redo/WAL metrics

The cluster runner takes one snapshot immediately before each logical
New-Order and another after all XA branches resolve. It prints a delta per
participant with:

- current and flushed LSN advancement;
- bytes written to the InnoDB redo log;
- redo write requests, writes, and waits;
- checkpoint age before and after the transaction;
- counter reset or server restart detection.

Snapshots use dedicated JDBC connections and `SHOW GLOBAL STATUS`. A metric
that is absent in the running MariaDB version or invalidated by a counter reset
is printed as `unavailable`, not zero. These are server-global deltas, so a
concurrent workload can contribute to them. Keep thesis attribution trials
serialized or explicitly report that limitation.

## ALPS prequential experiment on MariaDB

Run the current ML model and policy against real XA New-Order transactions:

```bash
ACP_ALPS_EXAMPLES=10 \
ACP_ALPS_HISTORY_WINDOW_SIZE=30 \
ACP_ALPS_LABEL_WINDOW_SIZE=30 \
./gradlew runMariaDbAlps
```

The live learner is a categorical multi-head VFDT trained exclusively with
classic 2PC executions. It predicts three protocol-independent outcomes:
completion (`COMMIT`, `ABORT`, or `TIMEOUT`), total latency (`LOW`, `MEDIUM`, or
`HIGH`), and InnoDB redo bytes (`LOW`, `MEDIUM`, or `HIGH`). Protocol identity is
not a model feature. After prediction, the policy maps the completion
distribution to a recommendation. The baseline runner records that
recommendation but always executes classic 2PC.

`ACP_ALPS_HISTORY_WINDOW_SIZE` controls the number of completed transactions
retained in each global and per-participant rolling history (default `30`).
`ACP_ALPS_LABEL_WINDOW_SIZE` controls the valid latency and redo samples used by
the categorical thresholds (default `30`, minimum `3`). Features and thresholds
are frozen before execution; the current trial enters both histories only after
prediction, execution, evaluation, and training. Transaction `T` therefore sees
history only through `T-1`.

Until a metric has three valid samples, fixed defaults define its classes.
Afterward, independent online 33rd and 66th percentiles define the boundaries
for latency and redo. Configure the defaults with `ACP_ALPS_LATENCY_LOW_MS`,
`ACP_ALPS_LATENCY_MEDIUM_MS`, `ACP_ALPS_REDO_LOW_BYTES`, and
`ACP_ALPS_REDO_MEDIUM_BYTES`. A timeout skips the censored latency update;
unavailable redo skips the redo update. Completion always receives a label.

`ACP_ALPS_FEATURE_PROFILE` controls the VFDT feature ablation. Its default is
`GENERIC`, which excludes transaction names, warehouse fields, raw participant
IDs, and per-node identity. Available profiles are:

- `GENERIC`: transaction shape, global history, and topology-neutral summaries
  of the selected participants;
- `GENERIC_WITH_NODE_IDENTITY`: generic features plus participant-ID indicators
  and per-node history;
- `GENERIC_WITH_WORKLOAD`: generic features plus transaction type and named
  workload attributes;
- `COMPLETE`: generic, node-identity, and workload-specific features.

The runner prints `vfdtFeatures` at startup so every experiment records the
exact model input schema.

To validate that the end-to-end pipeline can learn a participant pattern, use a
controlled deterministic profile:

```bash
ACP_ALPS_EXAMPLES=30 \
ACP_ALPS_HISTORY_WINDOW_SIZE=30 \
ACP_ALPS_LABEL_WINDOW_SIZE=30 \
ACP_ALPS_FEATURE_PROFILE=GENERIC_WITH_NODE_IDENTITY \
ACP_ALPS_P1_ABORT_RATE=0.0 \
ACP_ALPS_P2_ABORT_RATE=0.1 \
ACP_ALPS_P3_ABORT_RATE=0.4 \
ACP_ALPS_P4_ABORT_RATE=0.8 \
./gradlew runMariaDbAlps
```

The abort-rate variables are fault-injection controls, not measured properties
of the containers. Results from this mode demonstrate learnability under a known
signal and must not be presented as a naturally discovered database behavior.

Every live example therefore comes from the same execution baseline. Presumed
abort and presumed commit require separate same-specification oracle experiments
before their relative latency, redo, and recovery effects can be claimed. No
protocol exploration is used in baseline training.

Set `ACP_ALPS_SCENARIO=PHASED` to exercise abrupt drift. The deterministic
schedule cycles through `BASELINE`, `LATENCY_SPIKE`, `ABORT_SPIKE`,
`COMBINED_SPIKE`, and `RECOVERY`. `ACP_ALPS_PHASE_LENGTH` controls transactions
per phase, `ACP_ALPS_LATENCY_SPIKE_MS` injects explicit fixed message latency in
both directions, and `ACP_ALPS_ABORT_SPIKE_RATE` sets the minimum participant
abort probability during abort phases. Phase identity and injected values are
reported for analysis but are deliberately excluded from the model features, so
the experiment measures adaptation from prior observations rather than leaking
the active condition.

```bash
ACP_ALPS_EXAMPLES=100 \
ACP_ALPS_SCENARIO=PHASED \
ACP_ALPS_PHASE_LENGTH=20 \
ACP_ALPS_LATENCY_SPIKE_MS=40 \
ACP_ALPS_ABORT_SPIKE_RATE=0.70 \
./gradlew runMariaDbAlps
```

The final report includes both global and per-phase prequential accuracy, label
distribution, and selected-protocol counts. The schedule also keeps participant
fan-out and remote-line ratio varying through the controlled specification set,
which exercises redo and distributed-transaction pressure without fabricating
redo counters.

For every transaction, the runner performs:

```text
features and thresholds from T-1
-> predict protocol-independent completion, latency, and redo
-> derive a counterfactual protocol recommendation
-> execute one real classic 2PC XA transaction
-> label and score the baseline prediction
-> train heads with valid classic-2PC feedback
-> update threshold and rolling-history windows
```

The current recommendation is driven by the completion distribution: commit
favors presumed commit, abort favors presumed abort, and ties favor classic 2PC.
Latency and redo characterize baseline cost but cannot distinguish protocols
until analytical deltas or oracle observations are available. The runner prints
prequential accuracy per head, label and recommendation counts, exact feature
names, and the learned rules for all three trees.

The runner waits for every participant's database commit or rollback to return
before shutting down actors. This is especially important for presumed commit,
where the coordinator can forget a committed transaction before participants
finish applying `XA COMMIT`.

## How the shard load works

The upstream loader generates warehouse identifiers starting at `1`. After each
one-warehouse load, `scripts/benchmarksql.sh` rewrites all warehouse primary and
foreign-key fields to the node's global warehouse ID with foreign-key checks
temporarily disabled in the same database session. `verify` checks every
warehouse-bearing table and fails if any reference points outside that node's
warehouse. For BenchmarkSQL's single-node smoke run, the generated properties
restrict execution to the warehouse owned by that node.

The sharding step removes BenchmarkSQL's `ol_stock_fkey` from
`bmsql_order_line`. That foreign key assumes the supplying stock row is in the
same database as the order line, which is false for a remote supplier in the
four-database topology. The order-to-order-line foreign key remains local. The
Java transaction planner routes each `ol_supply_w_id` to its owning shard, and
cross-shard stock references must be checked by the experiment correctness
verifier.
