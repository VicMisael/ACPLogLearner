# ALPS Codex Agent Context

## Project Name

**ALPS — Adaptive Log-based Protocol Switching**

## High-Level Goal

ALPS is a research prototype for adaptive selection of atomic commit strategies in distributed transaction systems.

The core idea is:

```text
distributed execution logs -> features -> context prediction -> policy -> commit strategy
```

The system should not initially predict the protocol directly. Instead, it should infer the expected transaction context or outcome class from logs and then allow a policy layer to map that context to a commit strategy.

## Current Research Direction

The project started as a simulator for atomic commit protocols and is now moving toward a real environment with:

- a real DBMS;
- distributed transaction execution;
- real participant/coordinator logs;
- a learning component;
- eventually Kafka or another distributed log/event-stream system for learning events.

The current priority is to move from a purely simulated environment to a real DBMS-backed distributed transaction environment.

## Important Design Principle

Keep these two logs conceptually separate:

```text
1. Correctness log
   - Used for protocol recovery.
   - Must be durable.
   - Must be written before critical protocol transitions.
   - Should not depend only on Kafka.

2. Learning/event log
   - Used by the feature extractor and learning module.
   - Can be Kafka, Redpanda, JSONL, or another event stream.
   - Can be asynchronous.
   - Should not be required for transaction correctness.
```

Kafka is appropriate for the learning/event log, not as the only correctness log.

## Current Prototype Status

The existing prototype is written in Java.

Implemented or prototyped:

- actor-style simulated environment;
- local message bus / simulation message bus;
- timers and timeout handlers;
- coordinator and participant abstractions;
- 2PC classic;
- 2PC Presumed Abort;
- 2PC Presumed Commit;
- in-memory coordinator logs;
- in-memory participant logs;
- VFDT / Hoeffding Tree learning prototype;
- simple feature extraction based on node participation.

Current prototype features are intentionally simple:

```text
has_n1
has_n2
has_n3
has_n4
has_n5
...
```

Each feature indicates whether a node participates in the transaction.

Current output classes:

```java
LIKELY_COMMIT
LIKELY_ABORT
UNCERTAIN
```

Example learned VFDT rules from the prototype:

```text
if has_n4 <= 0.09:
  if has_n5 <= 0.09:
    class = LIKELY_COMMIT
  if has_n5 > 0.09:
    class = UNCERTAIN

if has_n4 > 0.09:
  class = LIKELY_ABORT
```

Since participation features are binary, thresholds such as `0.09` effectively separate absence (`0`) from presence (`1`).

## Research Framing

The prototype currently validates the pipeline:

```text
transaction execution -> logs -> features -> online model -> learned patterns
```

It does **not yet** fully validate final adaptive protocol selection in a real DBMS environment.

The current evidence should be framed as preliminary:

- combinations of participants can produce distinct transaction behavior;
- the model can learn patterns from logs;
- even simple structural features provide useful signal;
- richer features are needed next.

Avoid overclaiming that the system already chooses the optimal protocol.

## Target Architecture

Recommended real architecture:

```text
Client / Workload Generator
        |
        v
ALPS Transaction Coordinator
        |
        |-- MariaDB Node 1 / XA Resource
        |-- MariaDB Node 2 / XA Resource
        |-- MariaDB Node 3 / XA Resource
        |
        v
Correctness Log

Coordinator / Participants emit events
        |
        v
Kafka / Redpanda / JSONL Event Stream
        |
        v
Feature Extractor
        |
        v
Online Learner
        |
        v
Policy Engine
```

## Recommended Real DBMS Path

Use **MariaDB + XA** first.

Reason:

- ALPS can act as an external transaction coordinator.
- MariaDB provides XA primitives.
- The coordinator can control prepare/commit/rollback.
- The project can preserve the atomic commit protocol focus.
- It avoids modifying DBMS internals.

Avoid modifying MariaDB internals initially.

## XA Mapping

Map ALPS resource operations to MariaDB XA commands:

| ALPS operation | MariaDB XA |
|---|---|
| begin local branch | `XA START xid` |
| finish local work | `XA END xid` |
| prepare | `XA PREPARE xid` |
| commit | `XA COMMIT xid` |
| abort | `XA ROLLBACK xid` |
| recovery inspection | `XA RECOVER` |

Suggested XID structure:

```text
gtrid = global ALPS transaction id
bqual = participant / branch id
formatID = 1
```

Example SQL:

```sql
XA START 'alps-tx-123', 'node-1', 1;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
XA END 'alps-tx-123', 'node-1', 1;
XA PREPARE 'alps-tx-123', 'node-1', 1;
XA COMMIT 'alps-tx-123', 'node-1', 1;
```

Use `XA RECOVER` to inspect prepared in-doubt transactions.

## Real Resource Abstraction

The existing resource interface should be implemented with a real DB resource.

Example shape:

```java
public interface TransactionalResource {
    boolean prepare(TransactionId txId);
    void commit(TransactionId txId);
    void abort(TransactionId txId);
}
```

Create:

```java
public final class MariaDbXaResource implements TransactionalResource {
    // wraps JDBC connection and XA SQL commands
}
```

This class should:

- start an XA branch;
- execute local SQL work;
- end the XA branch;
- prepare the branch;
- commit or rollback the branch;
- measure timings;
- emit structured events.

## Kafka / Distributed Learning Log

Use Kafka or Redpanda as the event stream for the learning module.

Recommended topics for the first version:

```text
alps-events
alps-feature-vectors
alps-predictions
```

Start simpler if needed:

```text
alps-events
```

Partitioning strategy:

```text
Kafka key = txId
```

This keeps events for a transaction in the same partition and simplifies aggregation.

## Event Schema

Use a generic structured event.

```java
public record AlpsEvent(
    String eventId,
    String txId,
    String nodeId,
    String role,          // COORDINATOR or PARTICIPANT
    String protocol,
    String eventType,
    long timestampMs,
    long sequence,
    Map<String, String> attributes
) {}
```

Important event fields:

- `eventId`: unique id for deduplication.
- `txId`: global transaction id.
- `nodeId`: emitting node.
- `role`: coordinator or participant.
- `protocol`: selected commit strategy.
- `eventType`: event name.
- `timestampMs`: event time.
- `sequence`: monotonic sequence per node or per transaction.
- `attributes`: event-specific values.

## Suggested Event Types

Coordinator-level:

```text
TX_STARTED
PROTOCOL_SELECTED
BEGIN_COMMIT_WRITTEN
PREPARE_BROADCAST
VOTE_RECEIVED
GLOBAL_COMMIT_WRITTEN
GLOBAL_ABORT_WRITTEN
GLOBAL_COMMIT_SENT
GLOBAL_ABORT_SENT
ACK_RECEIVED
TX_FINISHED
TIMEOUT
RECOVERY_STARTED
RECOVERY_FINISHED
```

Participant-level:

```text
PREPARE_RECEIVED
RESOURCE_PREPARE_STARTED
RESOURCE_PREPARED
RESOURCE_PREPARE_FAILED
VOTE_COMMIT_SENT
VOTE_ABORT_SENT
GLOBAL_COMMIT_RECEIVED
GLOBAL_ABORT_RECEIVED
RESOURCE_COMMIT_STARTED
RESOURCE_COMMITTED
RESOURCE_ROLLBACK_STARTED
RESOURCE_ROLLED_BACK
ACK_SENT
TIMEOUT
XA_START
XA_END
XA_PREPARE
XA_COMMIT
XA_ROLLBACK
XA_ERROR
```

## Event Publisher Abstraction

Add:

```java
public interface EventPublisher {
    void publish(AlpsEvent event);
}
```

Implementations:

```java
NoOpEventPublisher
InMemoryEventPublisher
JsonlEventPublisher
KafkaEventPublisher
```

Recommended build order:

1. `NoOpEventPublisher`
2. `JsonlEventPublisher`
3. `KafkaEventPublisher`

Do not make the transaction correctness path depend on Kafka availability.

## Feature Extraction

The feature extractor consumes events and groups them by `txId`.

Flow:

```text
consume event
append to TransactionTrace[txId]
if terminal event observed:
    extract features
    train/update learner
    optionally publish feature vector
    remove or archive trace
```

Terminal events:

```text
TX_FINISHED
TX_ABORTED
TX_RECOVERED
```

Possible feature record:

```java
public record TransactionFeatures(
    String txId,
    int participantCount,
    long totalLatencyMs,
    long timeoutCount,
    long voteAbortCount,
    long voteCommitCount,
    double abortRateWindow,
    double avgPrepareLatencyWindow,
    double maxPrepareLatencyWindow,
    double heterogeneityScore,
    String observedClass
) {}
```

## Rolling Statistics

The learner should use historical context, not only a single transaction.

Maintain rolling windows:

- global;
- per node;
- per participant set;
- per protocol.

Useful rolling features:

```text
abort_rate_last_100
commit_rate_last_100
avg_prepare_latency_last_100
p95_prepare_latency_last_100
timeout_rate_last_100
node_slowdown_score
participant_set_abort_rate
```

## Learning Component

Current learning prototype uses VFDT / Hoeffding Tree.

Keep the learning pipeline asynchronous:

```text
completed transaction -> events -> features -> model update -> future predictions
```

Do not block the current transaction on model training.

## Context Classes

Two possible levels of classification exist.

Simple current classes:

```java
public enum TransactionOutcomeClass {
    LIKELY_COMMIT,
    LIKELY_ABORT,
    UNCERTAIN
}
```

Richer future context classes:

```java
public enum TransactionContextClass {
    STABLE_COMMIT_FAVORABLE,
    LOW_ABORT_PROB,
    HIGH_ABORT_PROB,
    HIGH_NETWORK_LATENCY,
    HIGH_HETEROGENEITY,
    LARGE_PARTICIPANT_SET,
    MIXED_PROFILE
}
```

The richer classes should be introduced only after the event/log features support them.

## Policy Engine

Do not let ML directly make correctness-critical choices.

Instead:

```text
ML predicts context
Policy maps context -> commit strategy
```

Example interface:

```java
public interface ProtocolSelectionPolicy {
    MessageType chooseProtocol(TransactionFeatures features, TransactionContextClass context);
}
```

This separation improves:

- interpretability;
- safety;
- testability;
- thesis defensibility.

## Correctness Recovery Design

Correctness source of truth:

```text
ALPS durable coordinator log
MariaDB XA prepared state via XA RECOVER
```

Recovery sketch:

```text
on coordinator restart:
    read ALPS coordinator log
    run XA RECOVER on each DB participant
    match recovered XIDs to ALPS txIds
    if GLOBAL_COMMIT exists:
        XA COMMIT prepared branches
    else if GLOBAL_ABORT exists:
        XA ROLLBACK prepared branches
    else:
        follow the selected protocol's recovery rule
```

Kafka should not be the only source of recovery decisions.

## Implementation Milestones

### Milestone 1 — event abstraction

- Add `AlpsEvent`.
- Add `EventPublisher`.
- Emit events from current simulator.
- Write JSONL logs.

### Milestone 2 — feature extractor

- Read JSONL events.
- Group by `txId`.
- Generate feature vectors.
- Feed VFDT.

### Milestone 3 — Kafka/Redpanda

- Replace JSONL event transport with Kafka/Redpanda.
- Use `txId` as message key.
- Add deduplication by `eventId`.

### Milestone 4 — MariaDB XA resource

- Start two or three MariaDB containers.
- Execute manual XA transaction from Java.
- Implement `MariaDbXaResource`.
- Log each XA step.

### Milestone 5 — real recovery

- Stop after `XA PREPARE`.
- Verify `XA RECOVER`.
- Restart coordinator.
- Complete commit/rollback using ALPS durable log.

### Milestone 6 — adaptive experiment

Compare:

```text
static 2PC Presumed Abort
static 2PC Presumed Commit
ALPS adaptive policy
```

Workloads:

```text
commit-heavy
abort-heavy
mixed
changing workload
heterogeneous participants
high latency
```

Metrics:

```text
latency
throughput
abort rate
timeout rate
log writes
recovery behavior
learning overhead
```

## Avoid For Now

Do not start with:

- Paxos Commit implementation;
- 3PC implementation;
- modifying MariaDB internals;
- parsing InnoDB redo/undo logs;
- making Kafka the only correctness log;
- overly complex reinforcement learning.

Focus on:

```text
real DBMS + XA + distributed event logs + feature extraction + online learning + policy-based selection
```

## TiKV Note

TiKV is useful as future work or an alternative backend, but it changes the direction.

TiKV already has its own Percolator-style distributed transaction protocol and Raft-backed storage. It is excellent for studying real distributed transaction behavior, but it does not cleanly expose a way to swap commit protocols.

Prefer MariaDB/XA for the primary ALPS implementation.

TiKV can be mentioned later as:

```text
future validation platform for adaptive transaction strategy selection
```

## Current Thesis Positioning

A precise research question:

```text
Can distributed execution logs from real XA-backed transactions support adaptive per-transaction selection between atomic commit strategy variants?
```

A safe novelty claim:

```text
ALPS investigates log-driven context inference from coordinator and participant execution logs to support per-transaction adaptive selection of commit strategies, separating learning from the final protocol-selection policy.
```

## Coding Style Notes

- Keep protocol logic separate from DB resource logic.
- Keep event publishing separate from correctness logging.
- Prefer interfaces for replaceable components:
  - `TransactionalResource`
  - `EventPublisher`
  - `FeatureExtractor`
  - `ContextClassifier`
  - `ProtocolSelectionPolicy`
  - `CoordinatorLogManager`
  - `ParticipantLogManager`
- Keep tests deterministic where possible.
- In simulations, inject latency/failure via explicit components, not random sleeps hidden in protocol logic.
- In real DB tests, log every XA command with duration and success/failure.

## Suggested Package Layout

```text
ufc.victor.alps
  protocol/
    coordinator/
    participant/
    common/
  resource/
    TransactionalResource.java
    MariaDbXaResource.java
  log/
    correctness/
    event/
  learning/
    AlpsEvent.java
    TransactionTrace.java
    TransactionFeatures.java
    FeatureExtractor.java
    VFDTContextClassifier.java
    ProtocolSelectionPolicy.java
  runtime/
    TransactionCoordinator.java
    WorkloadRunner.java
  localenv/
    actors/
    kafka/
    docker/
```

## Next Best Task for Codex

Implement the event logging layer without changing protocol correctness:

1. Create `AlpsEvent`.
2. Create `EventPublisher`.
3. Create `JsonlEventPublisher`.
4. Inject `EventPublisher` into coordinator and participants.
5. Emit events at protocol boundaries.
6. Add a simple `TransactionTrace` aggregator that reads JSONL and groups events by `txId`.

This is the safest next step because it prepares the project for Kafka and real DBMS integration without disrupting the existing 2PC protocol implementation.
