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

## Initial smoke load

Load one warehouse into each MariaDB node:

```bash
./scripts/benchmarksql.sh load-all
```

The command starts the databases, builds the BenchmarkSQL image when necessary,
loads its schema and data into each `acp` database, and verifies warehouse and
item counts. Override the small default only after the smoke load succeeds:

```bash
BMSQL_WAREHOUSES=25 BMSQL_LOAD_WORKERS=4 ./scripts/benchmarksql.sh load-all
```

BenchmarkSQL database builds are not idempotent. If its tables already exist,
explicitly destroy them before loading again:

```bash
./scripts/benchmarksql.sh destroy-all
./scripts/benchmarksql.sh load-all
```

`destroy-all` removes the `bmsql_*` tables but does not remove the project's
`tx_journal` table or the MariaDB volumes.

## Verification and smoke benchmark

```bash
./scripts/benchmarksql.sh verify
./scripts/benchmarksql.sh run 1
```

The included run configuration lasts one minute and is intended only to validate
connectivity and transaction execution, not to produce thesis measurements.

## Distributed XA New-Order smoke test

After loading the four databases, the Java coordinator can execute one atomic
transaction whose four XA branches each perform a New-Order transaction against
that node's local BenchmarkSQL dataset:

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

`ACP_PROTOCOL` accepts `TWO_PC`, `TWO_PC_PRESUMED_ABORT`, or
`TWO_PC_PRESUMED_COMMIT`.

This entry point is an integration and protocol smoke test, not a compliant
TPC-C benchmark. Each XA branch currently creates a local New-Order on warehouse
`1` in an independent database. Global warehouse sharding and the VFDT-driven
multi-run evaluation should be layered on only after this path commits reliably.

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

## Sharding limitation

The upstream loader always generates warehouse identifiers starting at `1`.
Consequently, loading 25 warehouses on every node creates local ranges `1-25` on
all four databases. The runtime properties `useWarehouseFrom` and
`useWarehouseTo` do not change loader identifiers.

Global ranges such as `1-25`, `26-50`, `51-75`, and `76-100` therefore require a
small loader extension or a separate deterministic data-partitioning step. Until
that is implemented and verified, this setup should be described as four
independent BenchmarkSQL datasets rather than a globally sharded TPC-C dataset.
