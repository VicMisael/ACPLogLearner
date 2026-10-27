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

## ALPS paired experiment on MariaDB

The current experiment uses nine VFDT trees: completion, latency and redo for
each of the three candidate protocols. It predicts before executing the
transaction, compares the candidate scores, and evaluates the adaptive choice
against fixed protocols, a heuristic, random selection and a paired oracle.

After loading and verifying the cluster, run a small smoke experiment:

```bash
ACP_DEFENSIBLE_SEEDS=101 \
ACP_DEFENSIBLE_WARMUP=1 \
ACP_DEFENSIBLE_PHASE_LENGTH=1 \
ACP_DEFENSIBLE_ADAPTATION_STREAK=1 \
ACP_DEFENSIBLE_LATENCY_SPIKE_MS=1 \
ACP_DEFENSIBLE_OUTPUT=build/experiments/paired-smoke \
bash ./gradlew runMariaDbDefensibleExperiment
```

Choose a new output directory for each run. These smoke parameters validate
the pipeline; they are not thesis measurements. The default configuration runs
a substantially larger multi-seed matrix. Consult
[the experimental methodology](defensible-adaptive-experiment.md) before
starting that matrix.

Each specification is executed under all three protocols with shared inputs
and participant votes. The database state is restored and checked between
candidate executions. Each predictor learns from its own protocol's result;
the adaptive history receives only the selected candidate's result.

The experiment uses the generic 37-feature profile, excluding node identity,
transaction names and workload-specific fields. Historical features use only
completed observations, and latency and redo class thresholds remain fixed
by the experiment configuration. Predictions precede execution and training.

Connection settings use `ACP_MARIADB_USER`, `ACP_MARIADB_PASSWORD`,
`ACP_MARIADB_DATABASE`, and `ACP_MARIADB_1_URL` through `ACP_MARIADB_4_URL`.
Experiment settings use the `ACP_DEFENSIBLE_*` variables documented in the
methodology. Results are exported as per-transaction, per-seed, aggregate,
classifier and policy-comparison CSVs.

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
