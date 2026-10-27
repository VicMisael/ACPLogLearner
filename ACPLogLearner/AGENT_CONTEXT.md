# Saved Agent Context

## Current experiment cleanup (2026-10-26)

Writing scope confirmed by the user: the article describes the current paired
MariaDB experiment, not the earlier simulation or the 2PC-only baseline. Keep
architecture, prediction, selection, labeling and evaluation descriptions aligned
with MariaDbDefensibleExperimentMain.

The user requested removal of the earlier experiment paths. The sole ALPS
experiment entry point is now `runMariaDbDefensibleExperiment`
(`MariaDbDefensibleExperimentMain`), with nine trees predicting completion,
latency and redo separately for the three candidates. The single-label
simulation experiment, its `TransactionOutcomeClass`/labeler/policy/predictor,
and the 2PC-only `MariaDbAlpsMain` experiment have been removed. Protocol demos
and simulation infrastructure used by protocol tests remain. Older descriptions
below and historical experiment reports may refer to removed entry points; use
the current runner and `MAPA_DO_CODIGO.md` for implementation details.

Date saved: 2026-10-14

## Latest handoff: research and writing (2026-10-14)

Read `docs/MAC_HANDOFF.md` first when resuming on macOS. It contains the
current writing decisions, reference inventory, migration checks, and smoke-run
commands. This section and that handoff supersede older plans below when they
conflict; older sections contain historical stages, not all current limitations.

- The user is writing a roughly 20-page dissertation proposal in Portuguese,
  designed to become the foundation of the dissertation. The discussed 80–120
  dissertation pages were a planning estimate, not a university requirement.
- The user rejected the broad rewrite in `docs/artigo/alps-revisado.tex` and is
  rewriting in their own voice. Preserve that file but do not treat its prose
  as approved or overwrite it without a request. Prefer focused paragraph-level
  feedback and LaTeX snippets. The newer introduction and related-work drafts
  were developed in chat and are NOT automatically reflected in that file.
- The user's original title expands ALPS as **Adaptive Log-based Protocol
  Switching**. Do not silently replace it with another expansion. Other project
  documents use a different working title; naming is not harmonized.
- FLAC is the central related work. Other important comparisons: dynamic 2PC
  (Yu, Wang, Pu, 2004), 1-2PC (Al-Houmaily, Chrysanthis, 2004), Presumed-Either,
  Lampson–Lomet, EasyCommit, FC (ICDE 2024), ForeSight (arXiv:2508.17375), and
  Transaction Triaging. Treat FLAC and FC as closely related works; do not claim
  their contributions are independent without investigating their relationship.
- Avoid claiming novelty merely from selection per transaction. Explain the
  proposed combination of pre-decision features, historical/redo metrics,
  incremental outcome/cost prediction, and a separate selection policy.
- The local report for the single-seed paired run gives mean cost 2.515 for
  VFDT and 2.463 for the heuristic: VFDT was about 2.1% more costly, although
  better than all fixed protocols in that run. This is not multi-seed evidence.
  The heuristic has fixed rules over a changing history, not a fixed protocol.
- Possible explanations (NOT established causes): commit/abort phases favor
  the heuristic's direct signal, VFDT has no explicit forgetting, and categorical
  prediction scores approximate rather than equal the measured oracle cost.
  Next analyses: per-phase regret, transition delay, feature ablation, multiple
  seeds, and cost-weight sensitivity. Do not tune scenarios just to make ML win.
- There are 19 reference PDFs under `docs/referencias/artigos`, with sources in
  its README. ForeSight was read on the web but not downloaded into that folder.
- The Mac has not been tested. Earlier Windows environment diagnostics must
  not be treated as Mac status. No new experiments were executed in this handoff.

Research scope clarified: 2026-09-25

Last updated: 2026-10-02

## Latest experiment and thesis writing (2026-10-02)

The user requested analysis of the latest experiment and the start of thesis
writing. The current result source is
`build/experiments/mac-2h-20261002-091218`, which completed in 2h01m40s.
This supersedes the four-seed run as the primary presentation result source;
older sections below retain historical results, not current estimates.

Seven seeds (101-107), 100 warm-up blocks per seed and 430 observations per
phase produced 15,050 measured logical transactions, 700 warm-up blocks, and
47,250 XA executions in total. All 45,150 measured candidate executions were
valid, with no duplicate seed/index records or expected-outcome mismatches.
Each policy had 9,756 commits and 5,294 aborts (35.1761% abort rate), with no
timeouts. Controlled participant votes explain identical outcomes across
protocols; the 0.70 abort parameter applies per participant, not per transaction.

Adaptive mean cost was 2.883293 versus 2.709450 for the heuristic. Adaptive
reduced cost by 21.56% versus fixed 2PC, 6.87% versus fixed presumed abort,
4.89% versus fixed presumed commit, and 11.55% versus random. However, adaptive
cost was 6.42% higher than the heuristic, with a paired 95% difference interval
[0.1350, 0.2126] excluding zero. Unlike the earlier four-seed result, this run
supports lower heuristic cost under the tested setup. The direction of every
comparison was consistent across all seven seeds. Do not merge this run with
the previous one as eleven independent seeds, since identifiers overlap.

The agreed additional metric has now been calculated: pooled cost per commit
is 4.4479 for adaptive and 4.1797 for heuristic, including aborted-attempt costs
and excluding warm-up. It is not a measurement of actual retries to success.
Adaptive oracle agreement was 75.06%; heuristic agreement was 88.87%.
Six of seven seeds missed the 20-consecutive-oracle-match adaptation criterion
in the combined phase; one missed it during recovery. Completion accuracy
averaged 91.29%; redo macro-F1 ranged from 0.476 to 0.510 across protocol models.

An initial Portuguese methodology/results/discussion draft is saved at
`docs/mestrado/resultados-preliminares.md`. It includes cost and paired intervals,
abort rate and cost per commit, raw operational metrics, phase behavior, the
nine prediction heads, and validity limitations with links to original artifacts.
No final thesis template or bibliography has been applied yet. The results
remain ready for advisor discussion; improvements are subsequent research.

## Presentation decision: aborts and useful work (2026-10-02)

The user agreed to present operational cost together with abort rate and cost
per committed transaction. Preserve the current experimental cost function:
expected aborts incur their measured latency, redo, messages, and protocol-log
costs, but no extra abort penalty. Unexpected outcomes and timeouts are handled
separately by the existing invalid-trial checks and penalties.

For each policy, report abort rate as aborted measured attempts divided by all
measured attempts, and cost per commit as the total operational cost of all its
measured attempts (including aborts) divided by its number of commits. Use the
same measurement population and exclude warm-up. Count one selected candidate
per logical transaction for each policy, not all three paired executions as
three independent business attempts. If there are no commits, cost per commit
is undefined. These summaries have now been calculated in the latest analysis
above; they have not been added to the runner's exported summary schema.

Explain that controlled abort decisions are shared across protocol candidates.
A correct required abort preserves consistency. Adding the same abort penalty
to every candidate in a paired block would leave its ranking and regret
unchanged, although absolute costs and relative percentages would change.
Retried transactions are not currently measured by this experiment: discuss
retry cost as future work, not an observed result. The user has approved this
presentation framing, not a change to the cost function or a new experiment.

## Advisor presentation readiness (2026-09-29)

The user considers the current work ready to present to their master's advisor
as preliminary experimental results. Further model improvements and the full
ten-seed evaluation are follow-up research, not prerequisites for this meeting.

The initial presentation used `build/experiments/mac-20260925-123031`.
Use the newer seven-seed result above for the current presentation.
The repeatedly attached `mac-20260925-114906/run.log` is the earlier failed run
and must not be used as the completed experiment's evidence.

The completed Mac experiment ran for 1h08m57s with seeds 101-104, 100 warm-up
blocks per seed, 430 measured transactions per phase, five phases, and 50 ms
injected latency during spike phases. It produced 8,600 measured logical
transactions and 27,000 XA executions including warm-up. Analysis found no
duplicate measured records, invalid candidate trials, or mismatches between
expected and observed completion. The socket exhaustion error did not recur.

Mean configured costs were: oracle 2.6318, static heuristic 2.7016, adaptive
VFDT 2.8299, fixed presumed commit 3.0214, fixed presumed abort 3.0886, random
3.2410, and fixed classic 2PC 3.6611. Adaptive reduced mean cost by 22.70%,
8.38%, and 6.34% relative to fixed 2PC, presumed abort, and presumed commit,
respectively. Each paired seed-level 95% interval excluded zero in favor of
adaptive selection under this scenario.

The heuristic had lower mean cost in all four seeds. Adaptive cost was 4.75%
higher relative to the heuristic, but the paired adaptive-minus-heuristic 95%
interval was [-0.0044, 0.2610], including zero. Do not claim that adaptive
selection beats the heuristic or that this interval establishes a difference.
The heuristic uses a rolling 30-transaction commit history with fixed thresholds:
at least two-thirds commit selects presumed commit, at most one-third selects
presumed abort, otherwise classic 2PC; fewer than 10 observations selects 2PC.
"Static" describes its fixed rule, not an unchanging protocol choice.

Completion prediction accuracy averaged 91.86%. Latency accuracy ranged from
73.40% to 80.57% across protocol models. Redo accuracy ranged from 76.31% to
79.95%, but macro-F1 was only 0.483-0.513. Adaptive oracle agreement was 77.93%
overall, falling to 63.60% in the combined spike and 69.53% in recovery. Two
seeds did not reach the 20-consecutive-oracle-match criterion during recovery.

Presentation framing: predictions about future transaction properties can
support lower-cost protocol selection than fixed protocols in the evaluated
scenarios. The current learner has not demonstrated an advantage over the
simple historical heuristic. This paired experiment supplies feedback from all
candidates and does not yet establish equivalent gains under partial feedback
in production. Keep both the predictor targets and candidate protocol set
extensible; the current three protocols are not the limit of the thesis.

A Keynote outline was proposed, but no deck has been created. Plan 10 main
slides for 12-15 minutes: title, research question, ALPS architecture, future
transaction prediction targets, four-node XA environment, paired evaluation
and baselines, five-phase experiment, cost results versus fixed protocols,
heuristic comparison and adaptation limits, conclusions and next steps.
Optional backup slides cover cost weights, prediction metrics, and seed-level
results. Use editable charts, a horizontal cost comparison, and a phase
timeline. Identify the workload as TPC-C-like, not officially compliant TPC-C.
Follow-up research includes ten seeds, cost-weight sensitivity, better redo
prediction, drift-aware learners, and alignment of policy scores with measured
cost. Do not imply these follow-ups must be completed before presenting.

## macOS connection-pool validation (2026-09-25)

The first four-seed Mac run failed after 27 seconds with
`Can't assign requested address`. A debugger reproduction observed 16,353 TCP
sockets in `TIME_WAIT` against the Mac's 16,384-port ephemeral range. MariaDB
had only 2-3 simultaneous connections and no max-connection errors.

`MariaDbNewOrderStateController` now owns a separate MariaDB JDBC pool per
warehouse (minimum 1, maximum 2 connections), resets returned sessions, and
closes its pools through the experiment's try-with-resources scope. XA and redo
collection retain their own connection lifecycles.

All 47 tests passed, including two opt-in real MariaDB tests for reuse,
session reset, rollback, and exceptional cleanup. A real five-phase validation
completed in 3m07s with seed 101, 100 warm-up blocks, 250 measured blocks per
phase, and 1 ms injected latency: 1,350 logical blocks / 4,050 XA executions.
The observed socket peak was 10,794 in `TIME_WAIT`; all four nodes had no
prepared XA branches or experiment connections remaining after completion.
Artifacts: `build/experiments/mac-pool-validation-20260925`.
This is a connection-management validation, not a replacement for the planned
one-hour experiment with the original 50 ms latency setting and multiple seeds.

## Project Summary

A master's thesis project investigating how online machine-learning models can select the best atomic commit protocol at commit time. The current implementation uses categorical multi-head `VFDT`, but VFDT is only the first candidate. The thesis must keep the learner replaceable so continuous online predictors and non-learning baselines can reuse protocol execution, feature collection, labels, and policy selection.

The current thesis execution plan is documented in `docs/alps-thesis-plan.md`.

The research objective is to predict data about future transactions: their
expected outcomes, behavior, and execution costs, using information available
before those outcomes occur. The current completion, latency, and redo heads
are the first implemented prediction targets, not a closed set. A separate
policy uses these predictions to choose an atomic commit protocol.

The candidate protocol set is extensible. Classic 2PC, presumed abort, and
presumed commit are the three currently implemented and evaluated candidates,
not a limit on the thesis scope. Additional protocols may be incorporated into
execution, prediction, policy selection, and comparative evaluation.

The test topology now assigns one global warehouse to each MariaDB node:
warehouse `1` to `mariadb-1`, through warehouse `4` to `mariadb-4`. The load
script remaps and verifies all warehouse-bearing BenchmarkSQL rows after load.

## Core Thesis Decision: Execution Data and MariaDB Logs

The central thesis idea is to use both transaction-execution data and MariaDB
log-derived data to predict a bundle of outcomes for every candidate atomic
commit protocol for future transactions. A separate policy selects the lowest
expected cost among the available candidates, currently classic 2PC, presumed
abort, and presumed commit. Evaluation must compare that
choice with paired-trial oracle data rather than rely on accuracy alone.

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
feeds these request and historical metrics to three protocol-independent VFDT
heads trained exclusively from classic 2PC executions.

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
- predicted transaction outcomes are the current targets: completion
  (`COMMIT`, `ABORT`, `TIMEOUT`), latency class, and redo class (both
  `LOW`, `MEDIUM`, `HIGH`);
- oracle protocol is the lowest-cost correct protocol found by replaying the
  same specification under all candidates, with ties and invalid trials stored
  explicitly rather than disguised as transaction outcomes.

More execution statuses can be added without necessarily increasing the number
of classifier classes. Expanding the online-learning target is a thesis design
decision and must be supported by enough temporally ordered examples per class.
A strong later comparison is categorical multi-head outcome prediction versus
continuous multi-head prediction. Direct online prediction of the oracle
protocol may remain an ablation, but is not the primary formulation.

The first multiple-target implementation is complete. It uses separate heads
over the same leakage-safe `AlpsFeatures`, rather than a Cartesian product of
labels:

- completion category and its class-probability distribution;
- total-latency category and its class-probability distribution;
- redo-bytes category and its class-probability distribution.

Latency and redo use independently adaptive 33rd/66th-percentile thresholds
computed from bounded valid history through `T-1`; each transaction freezes and
stores the thresholds used for its labels. Phase latency, LSN, redo writes and
waits, messages, protocol-log writes, blocking, and recovery risk remain future
heads once sufficient training data and suitable labels exist.

The policy consumes this predicted outcome/cost bundle and selects classic 2PC,
presumed abort, or presumed commit. The paired-trial oracle remains an evaluation
baseline for policy quality and regret; it is not the primary learner target.

Each head must predict before execution and train only when its own feedback is
available. Missing or invalid metrics must skip that head's update rather than
be encoded as zero.

The chosen baseline architecture trains only from classic 2PC. `MariaDbAlpsMain`
builds features and freezes thresholds, predicts protocol-independent outcomes,
derives a protocol recommendation from the completion distribution, executes
classic 2PC, evaluates the prior prediction, trains valid heads, and only then
updates histories. No `candidate_protocol_*` feature or live protocol exploration
belongs in this baseline learner. Presumed abort and presumed commit effects are
introduced later through analytical protocol-cost rules and same-specification
oracle trials; recommendations in the baseline run are counterfactual and are
not executed.

The runner also supports a deterministic `PHASED` scenario for online drift
experiments: baseline, network-latency spike, abort spike, combined spike, and
recovery. Network delay is injected explicitly through the transaction runner's
message bus rather than hidden sleeps in protocol code. The active phase is
reported but excluded from model input; per-phase summaries expose adaptation
and recovery behavior.

## Concept Drift Decision

Concept drift means that the relationship between model input and target changes
over time: `P_t(Y|X) != P_(t+delta)(Y|X)`. In ALPS, similar transaction shape,
participants, and history may begin producing a different completion, latency,
or redo outcome. Distinguish abrupt, gradual, incremental,
and recurring drift from isolated outliers and from pure covariate drift, where
only the frequency of input contexts changes.

The current VFDT is incremental but has no explicit forgetting or drift
detection. The 150-transaction phased experiment showed this limitation: after
abort-heavy phases, recovery completion accuracy was `0.0333`, with presumed
abort still selected for 26 of 30 healthy transactions. The latency and redo
heads remained single-leaf majority classifiers.

The next model comparison should preserve the exact features, labels, classic
2PC transaction sequence, and prequential evaluation:

1. current VFDT as the non-adaptive incremental baseline;
2. Hoeffding Adaptive Tree with ADWIN as the primary drift-aware, interpretable
   alternative;
3. Adaptive Random Forest as a stronger but less interpretable adaptive
   ensemble;
4. fixed-window VFDT as a simple forgetting baseline.

Hoeffding Adaptive Tree is the recommended next implementation. ADWIN monitors
error distributions with an adaptive window; HAT trains alternate branches and
replaces old branches when the alternatives become more accurate. Experiments
must report macro-F1 and balanced accuracy per phase, drift-detection delay,
false alarms, transactions to recovery, cumulative cost/regret, model size, and
training/inference overhead—not only global accuracy.

A previous, protocol-conditioned 2,000-transaction phased VFDT experiment used
400 examples per
phase and completed in `3m48s`. Global prequential accuracy was `0.9375` for
completion, `0.7135` for latency, and `0.6020` for redo. Recovery completion
accuracy reached `0.9350`. The final trees had respectively 31, 25, and 29 nodes;
redo split first on generic `remote_operation_ratio`. These results show that
the earlier 150-example run was too short to judge representational capacity,
but do not prove drift adaptation because each phase supplied 400 examples and
rolling historical features exposed regime changes.

The long run also exposed a labeling concern: moving quantiles redefine cost
classes relative to the current regime. Once a latency-spike window fills,
roughly `230 ms` can become `LOW`. Thesis experiments must compare fixed absolute
thresholds, calibration-period thresholds frozen before evaluation, and
continuous prediction. Always preserve and report raw latency and redo values.

After adopting classic 2PC as the exclusive learning baseline, a new phased run
executed 2,000/2,000 transactions with `TWO_PC`; protocol identity was absent
from the 37 generic features. Accuracy was `0.9215` for completion, `0.8205` for
latency, and `0.6095` for redo. The policy recommendations were 1 classic 2PC,
815 presumed abort, and 1,184 presumed commit, but these are counterfactual and
do not establish protocol cost savings. In recovery it still recommended
presumed abort 47 times before returning mostly to presumed commit. All three
trees reached 29 nodes; redo split first on `remote_operation_ratio`.

The full result is documented in
`docs/resultado-vfdt-baseline-2pc-2000.md`; the raw log is
`/tmp/alps-2pc-baseline-2000.log`.

## Immediate Goal

Develop the Portuguese thesis draft and present the completed seven-seed Mac
experiment to the advisor as preliminary results. Start from
`docs/mestrado/resultados-preliminares.md` and the Keynote outline above. The
latest results favor adaptive selection over all three fixed protocols but
favor the heuristic over adaptive selection, with paired intervals excluding
zero. Completing ten seeds and cost-weight sensitivity analysis remains
follow-up work and does not block writing or the advisor presentation.

The categorical multi-head VFDT trained exclusively on classic 2PC is the
validated predictive baseline requested for this stage. Its counterfactual PA
and PC recommendations are evidence that the policy reacts to changing
conditions, but are not yet evidence of execution-cost savings.

## Adaptive Protocol Selection Proof Plan

For each logical transaction specification, replay all three protocol candidates
from equivalent database state with the same participants, inputs, injected
latency, abort decisions, phase, and random seed. Preserve completion status,
latency, redo, messages, protocol-log writes, and correctness. Compute the oracle
as the lowest-cost correct protocol, retaining ties and invalid trials explicitly.

Evaluate the live adaptive selector against:

1. always classic 2PC;
2. always presumed abort;
3. always presumed commit;
4. random selection;
5. a static threshold heuristic;
6. the paired oracle upper bound.

Use prequential evaluation, multiple seeds, identical transaction streams, and
confidence intervals. The primary evidence is cumulative regret relative to the
paired oracle, supported by mean and tail latency, redo, messages, correctness,
protocol distribution per phase, and transactions required to adapt after each
phase boundary. A successful claim requires the adaptive policy to beat the best
fixed policy, not merely predict labels accurately.

Live selection creates partial feedback because only the chosen protocol's
outcome is observed. Preserve selection probabilities and use controlled
exploration, periodic paired oracle trials, or another explicit contextual-bandit
method so early choices do not permanently bias the learner.

The minimum defensible paired experiment is now implemented in
`MariaDbDefensibleExperimentMain` and exposed as
`./gradlew runMariaDbDefensibleExperiment`. It snapshots every affected
New-Order row, executes the three candidates in seeded random order, restores
and verifies exact logical state after each candidate, and fails closed when
`XA RECOVER` exposes a prepared branch. It maintains one independent VFDT
outcome bundle per protocol; all receive the same protocol-neutral `T-1`
features and each trains only on its matching paired outcome after selection.
It compares adaptive, static heuristic,
random, all three fixed protocols, and oracle policies over five phases and ten
seeds by default. Outputs include transaction-level CSV, per-seed/per-phase
summaries, seed-level 95% confidence intervals, paired adaptive-versus-baseline
intervals, regret, p95 latency, invalid-trial counts, and adaptation delay.

The implementation passed a one-seed, one-transaction-per-phase real MariaDB
smoke run. That smoke validates the harness and state restoration only; it is
not a thesis result. The full default matrix still needs to be executed and
analyzed. See `docs/defensible-adaptive-experiment.md`.

A larger calibration run completed on 2026-09-17 in `15m14s`. It used seed
`101`, 100 warm-up blocks, 430 measured transactions in each of five phases,
2,150 measured logical transactions, and 6,750 real XA executions. All paired
trials were valid; there were no unresolved branches or restoration failures.
The adaptive policy obtained mean cost `2.515`, mean regret `0.111`, oracle
agreement `0.8433`, and p95 latency `221 ms`. Relative to adaptive cost, always
2PC was 25.66% worse, always presumed abort 11.36% worse, always presumed commit
9.07% worse, and random selection 15.59% worse. The static heuristic remained
2.11% better than VFDT (`2.463` versus `2.515`) and achieved `0.9098` oracle
agreement, so it is the strongest current non-oracle baseline.

The adaptive choices changed coherently by regime: presumed commit was selected
426/430 times in baseline and 429/430 during the latency spike; presumed abort
was selected 366/430 times in the abort spike and 367/430 in the combined spike;
recovery returned to presumed commit for 333/430 transactions. This is promising
evidence of behavioral adaptation, not final statistical proof. With one seed,
the reported confidence interval collapses to the point estimate and must not be
used for inferential claims. The full result and interpretation are documented
in `docs/resultado-experimento-adaptativo-15min.md`.

The four-instance MariaDB environment and warehouse-sharded BenchmarkSQL dataset
are already set up and validated. They support the XA trials and distributed
`TPC-C-like` workload required by this next stage.

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
