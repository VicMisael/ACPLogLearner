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
ACP_ALPS_WINDOW_SIZE=2 \
./gradlew runMariaDbAlps
```

With no configured abort injection, every participant has an abort rate of zero.
This is the natural cluster smoke case and is expected to produce only the
`LIKELY_COMMIT` class when the databases are healthy.

To validate that the end-to-end pipeline can learn a participant pattern, use a
controlled deterministic profile:

```bash
ACP_ALPS_EXAMPLES=30 \
ACP_ALPS_WINDOW_SIZE=3 \
ACP_ALPS_P1_ABORT_RATE=0.0 \
ACP_ALPS_P2_ABORT_RATE=0.1 \
ACP_ALPS_P3_ABORT_RATE=0.4 \
ACP_ALPS_P4_ABORT_RATE=0.8 \
./gradlew runMariaDbAlps
```

The abort-rate variables are fault-injection controls, not measured properties
of the containers. Results from this mode demonstrate learnability under a known
signal and must not be presented as a naturally discovered database behavior.

For every observation window, the runner performs:

```text
features -> context prediction -> protocol policy -> real XA transactions
         -> observed context label -> online model update
```

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
