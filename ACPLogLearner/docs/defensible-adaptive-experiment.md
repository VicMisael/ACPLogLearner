# Minimum defensible adaptive-protocol experiment

`MariaDbDefensibleExperimentMain` evaluates protocol selection using real
MariaDB XA executions rather than treating classifier accuracy as the final
result.

## Experimental unit

For every immutable New-Order specification, the harness:

1. builds features using history through `T-1`;
2. uses one independent VFDT outcome bundle per candidate protocol and selects
   the candidate with the lowest predicted categorical-policy score;
3. snapshots all district and stock rows that the transaction can change;
4. executes classic 2PC, presumed abort, and presumed commit in a seeded random
   order;
5. restores the original rows after every candidate and verifies exact equality;
6. rejects the block if `XA RECOVER` reports an unresolved prepared branch;
7. computes the valid lowest-cost candidate as the paired oracle;
8. evaluates adaptive, heuristic, random, fixed-protocol, and oracle policies;
9. trains the VFDT only after evaluating the current decision.

All three protocol-specific VFDTs receive the same protocol-neutral feature
vector. Each one is trained after the decision using only its matching paired
trial. This is full feedback supplied by the controlled paired experiment; a
future deployment that executes only the selected protocol still requires
exploration or another partial-feedback method.

Cleanup happens outside the measured runner boundary, so cleanup redo is not
included in candidate cost. Candidate order is randomized to reduce systematic
cache/order bias. The database is logically restored after every candidate;
physical cache and background-server effects cannot be perfectly reset and
remain a threat to validity to report with the results.

## State-controller connections

The snapshot, restoration, and verification controller owns one MariaDB JDBC
pool per warehouse, with a minimum of one and a maximum of two connections.
Returning a connection resets its session and rolls back unfinished work.
The experiment closes these pools on both normal and exceptional exit.
XA execution and redo collection retain their separate connection lifecycles.

This avoids opening a new physical connection for every stock-row check. On
macOS, that connection churn previously exhausted the ephemeral TCP port range
with sockets in `TIME_WAIT`, causing `Can't assign requested address`.

With the four-node sharded dataset available, run all tests including the opt-in
pool integration checks with:

```bash
ACP_MARIADB_INTEGRATION_TEST=true ./gradlew test --rerun-tasks
```

The integration checks use warehouse 1 and the usual `ACP_MARIADB_1_URL`,
`ACP_MARIADB_USER`, and `ACP_MARIADB_PASSWORD` overrides. They verify physical
connection reuse, session reset, rollback of an uncommitted district update,
and closure on exceptional exit. Run them without a concurrent workload.

## Default defensible matrix

The default execution uses:

- 10 independent seeds;
- 100 warm-up logical transactions per seed;
- five phases with 1,000 measured logical transactions each;
- three real XA candidate executions per logical transaction;
- fixed, predeclared latency and redo thresholds;
- a 30-transaction leakage-safe history window;
- a 20-transaction consecutive-oracle-match adaptation criterion.

This is 51,000 logical blocks and 153,000 protocol executions. Run a small smoke
experiment before starting the full matrix.

```bash
ACP_DEFENSIBLE_SEEDS=101 \
ACP_DEFENSIBLE_WARMUP=1 \
ACP_DEFENSIBLE_PHASE_LENGTH=1 \
ACP_DEFENSIBLE_ADAPTATION_STREAK=1 \
ACP_DEFENSIBLE_LATENCY_SPIKE_MS=1 \
ACP_DEFENSIBLE_OUTPUT=/tmp/acp-defensible-smoke \
./gradlew runMariaDbDefensibleExperiment
```

Run the default matrix with:

```bash
./gradlew runMariaDbDefensibleExperiment
```

Database connections use `ACP_MARIADB_USER`, `ACP_MARIADB_PASSWORD`,
`ACP_MARIADB_DATABASE`, and `ACP_MARIADB_1_URL` through `ACP_MARIADB_4_URL`,
shared with the cluster smoke runner. Important experiment overrides are:

| Variable | Default |
| --- | ---: |
| `ACP_DEFENSIBLE_SEEDS` | `101,...,110` |
| `ACP_DEFENSIBLE_WARMUP` | `100` |
| `ACP_DEFENSIBLE_PHASE_LENGTH` | `1000` |
| `ACP_DEFENSIBLE_LATENCY_SPIKE_MS` | `50` |
| `ACP_DEFENSIBLE_ABORT_SPIKE_RATE` | `0.70` |
| `ACP_DEFENSIBLE_HISTORY_WINDOW_SIZE` | `30` |
| `ACP_DEFENSIBLE_ADAPTATION_STREAK` | `20` |
| `ACP_DEFENSIBLE_OUTPUT` | timestamped directory under `build/experiments` |

The scalar objective is a configured weighted sum of normalized latency, redo
bytes, messages, and logical protocol writes. Invalid outcomes, timeouts,
unavailable required redo, and redo-counter resets receive explicit penalties.
All weights, scales, thresholds, seeds, environment metadata, and source revision
are written to `config.properties`; cost weights should be varied in separate
sensitivity runs rather than selected after seeing the result.

## Artifacts

- `transactions.csv`: one row per logical transaction, with all three raw paired
  outcomes/costs, every policy decision, oracle, regret, and model overhead;
- `seed-summary.csv`: global and per-phase policy metrics for every seed;
- `aggregate-summary.csv`: seed-level means and 95% Student-t confidence
  intervals;
- `classifier-summary.csv`: per-head accuracy, macro-F1, balanced accuracy, and
  inference/training overhead globally and by phase;
- `policy-comparisons.csv`: paired seed-level adaptive-minus-baseline cost
  differences and confidence intervals;
- `config.properties`: reproducibility metadata and complete experiment knobs.

A negative confidence interval in `policy-comparisons.csv` supports the claim
that adaptive selection costs less than that baseline. The adaptive claim is
only supported if correctness has no invalid trials, adaptive cost beats the
best fixed protocol with an interval excluding zero, protocol choices change
across phases, and adaptation delay is finite in the relevant phases.

The adaptive policy uses the current categorical VFDT outcome architecture and
an explicit cost policy over protocol-specific completion, latency, and redo
predictions. This experiment can falsify its benefit; it does not assume that
the adaptive policy will win.
