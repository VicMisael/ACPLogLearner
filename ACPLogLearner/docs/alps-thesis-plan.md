# ALPS Thesis Plan

## Working title

**ALPS: An Adaptive Learning-Based Protocol Selector for Atomic Commit in Distributed Databases**

## 1. Thesis objective

Design and evaluate ALPS, an online-learning mechanism that infers transaction
context from execution events and historical metrics. A separate, explicit
policy uses that context to select an atomic commit protocol independently for
each distributed transaction. The initial classifier has exactly three labels:

1. `LIKELY_COMMIT`
2. `LIKELY_ABORT`
3. `UNCERTAIN`

The main claim to test is that a selector trained from transaction context and
recent system metrics can reduce transaction cost compared with always using one
fixed protocol, without changing transaction correctness.

The model does not directly make a correctness-sensitive protocol decision. The
policy layer maps the predicted context to one of `TWO_PC`,
`TWO_PC_PRESUMED_ABORT`, or `TWO_PC_PRESUMED_COMMIT`. This separation is part of
the ALPS design and must remain visible in the implementation and evaluation.

## 2. Research questions

- **RQ1:** Can an online machine-learning model infer useful transaction context
  from distributed execution events and recent system metrics?
- **RQ2:** Does ALPS improve latency, log-write cost, message cost, or blocking
  time compared with fixed 2PC, presumed-abort, and presumed-commit policies?
- **RQ3:** How quickly does the selector adapt when commit rate, contention,
  participant count, network conditions, or database load changes?
- **RQ4:** Which features contribute most to protocol selection?
- **RQ5:** What is the overhead of feature collection, inference, and online
  training?

## 3. Hypotheses

- **H1:** ALPS has lower average transaction cost than fixed classic 2PC.
- **H2:** ALPS approaches the performance of an oracle that knows the best
  protocol for each transaction context.
- **H3:** Presumed-commit is favored in stable, commit-heavy contexts, while
  presumed-abort is favored in abort-heavy contexts.
- **H4:** Classic 2PC remains useful for uncertain contexts in which neither
  presumed variant consistently dominates.
- **H5:** The online ML model adapts after a workload change without requiring
  offline retraining or stopping transaction processing.

## 4. Scope

### Included

- Java 21 coordinator and participant implementations in this repository.
- MariaDB 11.4 with XA branches on four database instances.
- BenchmarkSQL schema and TPC-C-like data.
- New-Order as the first transaction type.
- Three context labels only.
- A replaceable online ML classifier; VFDT is the current implementation.
- Controlled experiments, fixed-policy baselines, and an oracle baseline.

### Not initially included

- Consensus-based commit protocols.
- More than three context labels.
- Production failure recovery across machine restarts.
- JSONL or Kafka/Redpanda event transport; events remain in memory initially.
- A full audited TPC-C result or an official TPC-C compliance claim.
- Automatic model selection among multiple learning algorithms.

These can be extensions after the three-label experiment is reproducible.

### Current ML model

VFDT (Very Fast Decision Tree, implemented as a Hoeffding Tree) is the current
online classifier and the first model to evaluate. ALPS must depend on a generic
context-classifier interface so the thesis architecture is not defined by VFDT.
Other models can be compared later without changing event collection, feature
extraction, policy selection, or protocol execution.

## 5. ALPS execution model

```text
Transaction request
        |
        v
Build pre-decision feature vector
        |
        v
Online ML model predicts one of three context labels
(current model: VFDT)
        |
        v
Policy maps context to a commit protocol
        |
        v
Run distributed XA transaction on MariaDB participants
        |
        v
Collect outcome and performance metrics
        |
        v
Assign feedback label and train the ML model online
```

The selector must run before the coordinator sends the first prepare message.
Only values available at that point may be predictor inputs. Metrics produced by
the current transaction are feedback for later transactions and must not be used
to predict the current transaction.

## 6. Real database topology

Use the existing four-node MariaDB Compose environment:

| Participant | Default host port | Initial dataset |
| --- | ---: | --- |
| `P1` | `33061` | BenchmarkSQL warehouse `1` |
| `P2` | `33062` | BenchmarkSQL warehouse `1` |
| `P3` | `33063` | BenchmarkSQL warehouse `1` |
| `P4` | `33064` | BenchmarkSQL warehouse `1` |

The initial four-node setup contains independent local warehouse namespaces. It
is sufficient for XA integration testing, but it must not be described as a
globally sharded TPC-C database.

Before the main thesis experiment, introduce deterministic global warehouse
ownership. One acceptable target is:

| Node | Global warehouse range |
| --- | --- |
| `P1` | `1-25` |
| `P2` | `26-50` |
| `P3` | `51-75` |
| `P4` | `76-100` |

The loader or a post-load partitioning tool must create these identifiers and
verify all primary-key and foreign-key references. The warehouse router must be
tested independently before performance measurements begin.

## 7. Workload progression

### Stage A: XA integration smoke test

Use `MariaDbTpccClusterMain` to run one global transaction whose branches execute
New-Order operations on all four local datasets.

Acceptance criteria:

- Each participant uses a different JDBC URL.
- All four branches reach `XA PREPARE`.
- A committed transaction persists on all four nodes.
- An injected abort rolls back every branch.
- `XA RECOVER` is empty after normal completion.
- All three protocols pass commit and abort cases.

### Stage B: Globally sharded New-Order

Route warehouses to their owning database. Generate both:

- single-participant transactions;
- distributed transactions involving two, three, or four participants.

Keep the item set, warehouse selection, customer selection, item quantities, and
random seeds recorded for replay.

Acceptance criteria:

- No request reads or writes a warehouse through the wrong participant.
- New-Order preserves district order identifiers, stock quantities, orders,
  new-order rows, and order-line rows.
- Replaying a transaction specification produces the same logical work under
  each candidate protocol.

### Stage C: Controlled context and policy dataset

For every generated transaction context, execute equivalent trials under all
three protocols. Use isolated database snapshots or restore the same snapshot
before each protocol trial. Randomize protocol execution order to reduce bias
from warm-up and time-dependent load.

Each context therefore produces three comparable protocol metric records. The
observed transaction behavior produces the context label used to train the ML model.
The best protocol is retained as an oracle value for evaluating the policy; it
is not the ML classifier's class label.

### Stage D: Online ALPS run

Process a time-ordered transaction stream using prequential evaluation:

1. Build features from the request and metrics observed before the transaction.
2. Predict a context label with the configured ML model.
3. Map the context to a protocol through the policy.
4. Execute only the selected protocol.
5. Record transaction events, metrics, and correctness.
6. Derive the observed context label after completion.
7. Score the context prediction and policy decision separately.
8. Train the configured ML model with the observed context label.

No test transaction may be used to train the model before its prediction is
scored.

## 8. Pre-decision feature vector

Start with a small, explainable feature set:

### Transaction context

- transaction type;
- participant count;
- participant membership or stable participant encoding;
- local or distributed transaction;
- number of local and remote order lines;
- number of warehouses and districts touched;
- requested row count or order-line count.

### Rolling historical metrics

- recent commit rate;
- recent abort rate;
- recent timeout rate;
- exponentially weighted latency per participant;
- recent prepare duration;
- recent decision/acknowledgment duration;
- recent blocking duration;
- recent message count;
- recent coordinator and participant log-write counts.

### Load and contention

- active transaction count;
- request arrival rate;
- connection-pool utilization, if a pool is introduced;
- database lock-wait time;
- deadlock count;
- participant health or reachability;
- configured or measured network delay.

Use fixed-size rolling windows or exponentially weighted moving averages. Record
the window size and smoothing factor as experiment configuration.

Do not use the current transaction's final outcome, final latency, log writes,
or blocking time as input features for that same transaction.

## 9. Defining context labels and policy quality

The ML classifier has three context labels:

| Context label | Initial definition |
| --- | --- |
| `LIKELY_COMMIT` | rolling abort rate is below the lower threshold |
| `LIKELY_ABORT` | rolling abort rate is above the upper threshold |
| `UNCERTAIN` | rolling abort rate lies between the two thresholds |

Thresholds and rolling-window size are experiment configuration and must be
recorded with every result. Labels are derived after a transaction or observation
window completes; they cannot leak into the prediction for that same example.

The initial policy is explicit and testable:

| Predicted context | Selected protocol |
| --- | --- |
| `LIKELY_COMMIT` | `TWO_PC_PRESUMED_COMMIT` |
| `LIKELY_ABORT` | `TWO_PC_PRESUMED_ABORT` |
| `UNCERTAIN` | `TWO_PC` |

Policy quality is evaluated against controlled trials of all three protocols.
Preserve raw metrics even when an oracle uses a scalar cost objective.

A starting objective is:

```text
cost(protocol, transaction) =
    normalized_latency
  + alpha * normalized_log_writes
  + beta  * normalized_messages
  + gamma * normalized_blocking_time
  + timeout_penalty
  + failure_penalty
```

The protocol with minimum cost among the three controlled trials is the oracle
protocol. It evaluates the policy decision but is not used as the ML model's class label.

Before selecting weights:

1. Report every metric independently.
2. Justify `alpha`, `beta`, and `gamma` from the research objective.
3. Run a sensitivity analysis with multiple reasonable weight configurations.
4. Use a deterministic oracle tie-breaking rule.
5. Report Pareto dominance when protocols trade one metric for another.

Suggested deterministic tie-break order:

1. correct completion;
2. no timeout;
3. lower latency;
5. fewer messages;
6. lower blocking time.

The thesis must distinguish logical log records currently counted by the Java
simulation from physical or forced log activity measured at MariaDB.

## 10. Metrics to collect

### Correctness

- global commit or abort decision;
- participant decision consistency;
- unresolved prepared XA branches;
- invariant violations in TPC-C tables;
- timeout and recovery count.

### Protocol performance

- end-to-end transaction latency;
- prepare-phase latency;
- decision-phase latency;
- acknowledgment-phase latency;
- participant blocking time;
- protocol message count;
- coordinator log-write count;
- participant log-write count;
- MariaDB lock-wait time;
- commit and abort throughput.

### Classifier and policy performance

- prequential context-classification accuracy;
- per-context precision, recall, and F1;
- context confusion matrix;
- policy agreement with the oracle protocol;
- cumulative regret against the oracle;
- cost relative to each fixed protocol;
- model size, tree depth, and node count;
- inference and training overhead;
- examples required to recover after concept drift.

Use macro-F1 in addition to accuracy because context labels may be imbalanced.

## 11. Baselines

Compare ALPS with:

1. Always classic 2PC.
2. Always presumed abort.
3. Always presumed commit.
4. The current rule-based outcome mapping.
5. Majority-label selection.
6. Random selection with a fixed seed.
7. Offline oracle choosing the lowest-cost protocol for every context.

Optional later baselines include a batch decision tree or another online model,
but they are not required before the current VFDT experiment is correct and reproducible.

## 12. Experimental factors

Vary one or more of the following according to a recorded experiment matrix:

- commit/abort probability;
- transaction arrival rate;
- participant count;
- remote order-line fraction;
- order-line count;
- database contention;
- injected participant delay;
- injected network latency and jitter;
- temporary participant unavailability;
- workload phase changes that create concept drift.

Define at least these workload regimes:

- commit-heavy;
- abort-heavy;
- balanced/uncertain;
- low contention;
- high contention;
- stable conditions;
- abrupt concept drift;
- gradual concept drift.

## 13. Experimental protocol

- Use Java 21 and pinned dependency/container versions.
- Record hardware, operating system, JVM flags, MariaDB settings, and container
  resource limits.
- Seed every controllable random generator.
- Separate warm-up transactions from measured transactions.
- Restore or verify database state between paired protocol trials.
- Randomize candidate-protocol order within each trial block.
- Repeat each configuration across multiple independent seeds.
- Store one machine-readable row per transaction.
- Include configuration and source revision with every result file.
- Report medians and tail percentiles in addition to means.
- Report confidence intervals and paired effect sizes where trials are paired.
- Do not infer an improvement from prediction accuracy alone; the primary result
  is transaction cost and regret.

## 14. Required Java components

Keep entry points separate so smoke tests and thesis experiments cannot be
confused:

| Component | Responsibility |
| --- | --- |
| `Main` | Original standalone classic-2PC simulation demo |
| Gradle `runExperiment` | Deterministic adaptive simulation and ML-model development |
| `MariaDbTpccClusterMain` | Four-node XA/TPC-C integration smoke test |
| `MariaDbAlpsDatasetMain` | Controlled context labeling and protocol trials |
| `MariaDbAlpsMain` | Online ALPS selection and evaluation |

Add the following reusable components:

- `AlpsEvent`: structured learning event independent of correctness logs;
- `EventPublisher`: non-correctness-critical event recording interface;
- `InMemoryEventPublisher`: thread-safe event store keyed by transaction ID;
- `TransactionTrace`: events grouped by transaction identifier;
- `AlpsFeatures`: immutable, pre-decision feature vector;
- `ContextClassifier`: replaceable ML interface predicting
  `TransactionOutcomeClass`;
- `VfdtContextClassifier`: current VFDT implementation of `ContextClassifier`;
- `ProtocolSelectionPolicy`: maps context to `SelectedProtocol`;
- `RollingMetricsRegistry`: leakage-safe history by participant/context;
- `ProtocolCostFunction`: documented oracle/policy evaluation objective;
- `ProtocolTrialRunner`: executes one replayable transaction specification;
- `TpccTransactionSpec`: immutable seeded New-Order request;
- `WarehouseRouter`: maps global warehouses to participants;
- `ExperimentRecordWriter`: writes CSV or JSON Lines results;
- `CorrectnessVerifier`: checks XA and TPC-C invariants;
- `ExperimentConfig`: records all parameters and seeds.

## 15. Data-record format

Write one immutable record per transaction. At minimum include:

```text
experiment_id
run_id
transaction_index
transaction_spec_id
random_seed
timestamp
workload_phase
feature_vector
predicted_context
observed_context
predicted_protocol
executed_protocol
oracle_protocol
committed
timed_out
latency_ms
prepare_ms
decision_ms
blocking_ms
message_count
coordinator_log_writes
participant_log_writes
selector_inference_ns
selector_training_ns
model_type
model_size
model_description
```

Never rely only on console output for thesis measurements.

## 16. Implementation phases and deliverables

### Phase 1: Event foundation

- Add `AlpsEvent`, event roles, and event types.
- Add `EventPublisher`, `NoOpEventPublisher`, and `InMemoryEventPublisher`.
- Keep events in memory for both simulation and MariaDB experiments.
- Add deterministic ordering, deduplication, and publisher tests.
- Ensure event publication failures cannot change protocol decisions.

**Deliverable:** tested in-memory learning-event API.

**Completion gate:** no coordinator, participant, or XA behavior changes when a
no-op or failing publisher is used.

### Phase 2: Protocol and XA instrumentation

- Inject `EventPublisher` without replacing correctness logs.
- Emit coordinator events at selection, prepare, vote, decision, acknowledgment,
  timeout, and completion boundaries.
- Emit participant events at prepare, vote, decision, acknowledgment, and timeout
  boundaries.
- Emit XA start, end, prepare, commit, rollback, duration, and error events.
- Preserve a monotonic sequence per transaction/node.

**Deliverable:** complete in-memory transaction traces from simulation and MariaDB.

**Completion gate:** existing deterministic protocol tests pass unchanged, and a
trace contains a terminal event for every normally completed transaction.

### Phase 3: Trace aggregation and feature extraction

- Group learning events by `txId` in `TransactionTrace`.
- Deduplicate by `eventId` and order by sequence.
- Derive completed-transaction metrics from traces.
- Add rolling global, per-node, and per-participant-set statistics.
- Build only pre-decision features for the next transaction.
- Derive the three observed context labels after completion.

**Deliverable:** versioned context dataset and leakage-safe feature vectors.

**Completion gate:** tests prove that current-transaction terminal metrics never
appear in its own prediction features.

### Phase 4: Real database correctness, sharding, and replay

- Complete the four-node New-Order smoke test.
- Add commit, abort, `XA RECOVER`, and cleanup integration tests.
- Verify all three protocols.
- Implement global warehouse identifiers and routing.
- Introduce immutable, seeded transaction specifications.
- Add database snapshot/reset support for paired trials.
- Verify equivalent logical work across protocols.

**Deliverable:** repeatable, replayable, distributed New-Order workload.

**Completion gate:** every protocol preserves atomicity and TPC-C invariants in
commit, abort, and injected-failure tests.

### Phase 5: Context classifier and policy integration

- Feed historical features to the configured `ContextClassifier`.
- Keep the three context classes as the ML target.
- Use VFDT as the current and first evaluated implementation.
- Apply `ProtocolSelectionPolicy` after context prediction.
- Enforce predict, select, execute, observe, then train order.
- Execute controlled trials under all three protocols for oracle comparison.
- Measure classifier and policy overhead independently.

**Deliverable:** `MariaDbAlpsMain` with prequential context and policy evaluation.

**Completion gate:** results contain predicted/observed context, selected/oracle
protocol, raw costs, and model state for every transaction.

### Phase 6: Main experiment

- Run fixed-policy, rule-based, configured-ML, random, and oracle baselines.
- Report VFDT results explicitly as the current configured ML model.
- Run stable and concept-drift workloads.
- Repeat across seeds and collect confidence intervals.
- Perform feature ablation and cost-weight sensitivity analysis.

**Deliverable:** final result tables, plots, raw records, and analysis scripts.

### Phase 7: Thesis writing

- Describe protocol semantics and cost differences.
- Document ALPS architecture and learning procedure.
- State the label objective and leakage controls explicitly.
- Present correctness before performance.
- Report limitations and threats to validity.

**Deliverable:** reproducible thesis artifact and final manuscript.

## 17. Immediate next steps

1. Implement and test `AlpsEvent` and `EventPublisher`.
2. Implement `NoOpEventPublisher` and a thread-safe
   `InMemoryEventPublisher` keyed by `txId`.
3. Instrument one complete classic-2PC path and validate its event trace.
4. Extend instrumentation to presumed abort, presumed commit, and MariaDB XA.
5. Add `TransactionTrace` aggregation and rolling statistics.
6. Install or configure a Compose provider and start the four MariaDB nodes.
7. Validate commit, forced abort, and `XA RECOVER` under all protocols.
8. Implement `TpccTransactionSpec` and warehouse routing.
9. Generate controlled protocol trials and context labels.
10. Implement `MariaDbAlpsMain` with context prediction followed by policy
    selection.

## 18. Thesis success criteria

The thesis objective is satisfied when:

- the configured ML model predicts one of the three context labels before each
  transaction, with VFDT used as the current implementation;
- an explicit policy maps that context to one of the three protocols;
- predictions use only transaction context and previously observed metrics;
- the selected protocol executes a real distributed MariaDB XA transaction;
- correctness is maintained across every evaluated policy;
- ALPS is compared against all fixed protocols and an oracle;
- results show cost, regret, adaptation, and overhead rather than accuracy alone;
- experiments are reproducible from recorded configuration, seeds, source
  revision, database state, and machine-readable transaction records.

## 19. Future event transports

The core ALPS experiment keeps learning events in memory. After the in-memory
pipeline is correct and evaluated, the same `EventPublisher` abstraction can
support external transports without changing protocol correctness logic.

### JSONL event export

Add `JsonlEventPublisher` as the first optional transport:

- write one structured `AlpsEvent` per line;
- preserve `eventId`, `txId`, node-local sequence, and timestamp;
- flush asynchronously or at transaction completion;
- support replay into `TransactionTrace` aggregation;
- record malformed and duplicate events without affecting transactions;
- compare replayed traces with their original in-memory traces.

JSONL is appropriate for reproducibility, offline inspection, debugging, and
sharing experiment traces without operating a message broker.

### Kafka or Redpanda event streaming

Add `KafkaEventPublisher` only after JSONL replay and the in-memory pipeline are
stable:

- use `txId` as the message key so one transaction remains in one partition;
- retain `eventId` for consumer-side deduplication;
- preserve a monotonic per-node or per-transaction sequence;
- publish learning events asynchronously;
- tolerate duplicate delivery and consumer restarts;
- measure publication and consumption lag;
- verify that broker failure never changes commit, abort, or recovery behavior.

Potential topics are:

```text
alps-events
alps-feature-vectors
alps-predictions
```

Begin with only `alps-events` unless separating topics is justified by measured
load or operational requirements.

### Required architectural boundary

Neither JSONL nor Kafka/Redpanda may replace:

- the durable coordinator correctness log;
- participant recovery records;
- MariaDB prepared XA state inspected through `XA RECOVER`.

External event transport remains an asynchronous learning and observability
facility. ALPS must produce the same transaction decision when the transport is
healthy, slow, unavailable, or disabled.
