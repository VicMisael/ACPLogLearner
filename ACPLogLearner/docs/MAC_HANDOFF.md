# ALPS / ACPLogLearner — Mac handoff

Updated: 2026-10-14 (America/Sao_Paulo).

## Resume here

This is a master's thesis project at UFC investigating online selection of
atomic commit protocols for distributed transactions. The immediate user focus
is writing a Portuguese dissertation proposal of about 20 pages, reusing it
later as dissertation chapters. Implementation is advanced; scientific claims
still need the planned validation. Do not describe the dissertation as finished.

Read next:

1. `AGENT_CONTEXT.md` (latest handoff first; some older sections are historical).
2. `docs/defensible-adaptive-experiment.md`.
3. `docs/resultado-experimento-adaptativo-15min.md`.
4. `docs/resultado-vfdt-baseline-2pc-2000.md`.
5. `docs/referencias/artigos/README.md`.

## Writing state and user preferences

- Write and discuss the academic text in Portuguese. The user wants to write in
  their own voice; offer small corrections, not unsolicited wholesale rewrites.
- `docs/artigo/alps-revisado.tex` is an assistant-produced draft the user did
  not like. Preserve it, but it is not the approved current manuscript. Its
  compilation was not confirmed in the earlier conversation.
- Original manuscript: `C:/Users/Misael/Downloads/sbc-template.tex`; a sibling
  `sbc-template2.tex` also existed. These paths will not work on macOS. Transfer
  the actual original manuscript, its `.bib`, SBC `.sty`/`.bst`, and figures if
  they are available. Only the first `.tex` was inspected; companion bibliography
  and style files were not supplied. Newer user-authored paragraphs live in chat.
- The original ALPS expansion is **Adaptive Log-based Protocol Switching**.
  The standalone revised title and some planning documents differ. Naming needs
  user agreement, not an automatic global rename.
- Introduction flow agreed: ACID/trans transactions → distributed completion →
  failures and blocking → 2PC/PA/PC costs → limitations of a fixed choice → ALPS.
- Explain that atomic commit addresses the global outcome; it does not alone
  guarantee all of ACID. Selecting PA/PC does not remove 2PC blocking.
- Describe counters and log-derived metrics accurately: the current MariaDB
  collector does not directly parse redo files. Avoid claiming measured node
  heterogeneity or isolated network latency when inputs are only aggregate
  historical execution metrics.

### Related-work draft

The user has a nearly complete draft in chat. Its intended organization is:

1. Protocol optimizations: Lampson–Lomet, Presumed-Either, Skeen, EasyCommit.
2. Dynamic selection/adaptation: 1-2PC, dynamic 2PC, **FLAC as the central work**,
   plus FC.
3. Prediction/context: ForeSight and Transaction Triaging (one discussion only;
   an older duplicated Triaging subsection was commented out).
4. Positioning: incremental outcome/cost prediction, separate policy, and
   paired evaluation against fixed protocols and a simple heuristic.

Important distinctions:

- FLAC uses a robustness-level state machine and reinforcement-learning-tuned
  parameters to adapt its protocol family to failure conditions.
- The local FC PDF is *FC: Adaptive Atomic Commit via Failure Detection*,
  ICDE 2024, DOI `10.1109/ICDE60146.2024.00162`. Investigate its relationship
  to the FLAC preprint before presenting them as separate contributions.
- ForeSight predicts transaction conflicts with an Association Sum-Product
  Network to guide deterministic scheduling. It does not select PA/PC.
  Source: https://arxiv.org/abs/2508.17375 (2025; v2 inspected).
- Transaction Triaging reorganizes request/response streams in programmable
  networks; it does not perform learned commit-protocol selection.
- Dynamic selection already exists in prior work. Do not claim novelty just
  from making a different choice per transaction.

User-facing BibTeX keys from the discussion (the standalone draft uses other
keys and inline `thebibliography`; do not mix without reconciling citations):

```text
gupta2018easycommit
pan2023flacrobustfailureawareatomic
ozsu_valduriez_2019
lampson1993presumedcommit
skeen1981nonblocking
attaluri2002presumedeither
jepsen2021transactiontriaging
alhoumaily2004onetwophase
yu2004dynamic2pc
pan2024fc
huang2025foresight
```

## Technical architecture

- Java 21, Gradle wrapper 8.8, MOA `2023.04.0`, MariaDB JDBC `3.5.3`, JUnit 5.
- Candidates: `TWO_PC`, `TWO_PC_PRESUMED_ABORT`, `TWO_PC_PRESUMED_COMMIT`.
- Coordinator dispatcher binds the protocol at transaction start; participant
  dispatcher recognizes the initial prepare message type.
- `EmptyPayload` is a stateless singleton assigned by `Message`; it is unrelated
  to the VFDT input. Messages carry type, transaction ID, sender, recipient, time.
- Generic input: 37 numeric features (10 transaction-shape, 10 global-history,
  17 selected-participant-history aggregates). Optional profiles add transaction
  type/workload details and node identity. See `VfdtCategoricalModel.java`.
- `VfdtCategoricalOutcomePredictor` has three independent Hoeffding trees:
  completion (`COMMIT/ABORT/TIMEOUT`), latency and redo (`LOW/MEDIUM/HIGH`). Each
  returns a class, normalized votes, and confidence. Confidence is not known to
  be calibrated. The model predicts outcomes; an explicit policy selects protocol.
- Leakage rule: current request + history through T−1 → predict/select → execute
  → score → train valid heads → update history. Do not use the current final
  metrics, injected abort probability, or phase identity as prediction inputs.
- Current VFDT is incremental, without explicit forgetting/drift detection.

### Two experiment modes — do not conflate

`runMariaDbAlps`: one three-head predictor trained only on classic 2PC. All
transactions execute 2PC; recommendations are counterfactual. Generic features
do not contain candidate protocol identity. When all candidates share one
prediction, latency/redo terms cannot differentiate the choice.

`runMariaDbDefensibleExperiment`: one three-head predictor per candidate (nine
trees). All receive the same protocol-neutral feature vector. Each immutable
New-Order specification is run under all three protocols, in seeded randomized
order, with logical-state snapshot/restore/verification after each trial. Each
predictor trains on its matching candidate feedback after evaluating the choice.
This provides full feedback; selected-only deployment would require a strategy
for partial feedback. Logical restoration does not reset caches/background I/O.

Four MariaDB 11.4 instances have global warehouse ownership P1/W1 through P4/W4,
with replicated item catalogs and host ports 33061–33064. Workload is TPC-C-like
New-Order, not a full or audited TPC-C result. Java coordinates XA branches;
BenchmarkSQL supplies schema/data. Full restart recovery is not validated.

## Evidence and outstanding experiments

Use the local result reports as reported evidence, not freshly verified results.
The 2026-09-17 paired run used seed 101, 100 warm-ups, 430 measured transactions
per phase, five phases, 2,150 measured logical transactions and 6,750 XA attempts
including warm-up. Mean cost: oracle 2.404; heuristic 2.463; adaptive 2.515;
fixed PC 2.766; fixed PA 2.837; random 2.979; fixed 2PC 3.383.

Adaptive was about 2.1% more costly than the heuristic, although less costly
than all fixed protocols in that run. One seed does not establish statistical
superiority. The heuristic has fixed thresholds but reacts to changing history.
Possible explanations, not proven causes: scenario commit/abort shifts match
the heuristic's signal, VFDT retains stale patterns, and policy category scores
approximate the measured oracle objective.

Next scientific tasks: multi-seed paired confidence intervals, per-phase regret
and transition analysis, predeclared cost-weight sensitivity, feature ablation,
protocol correctness/recovery checks, and drift-aware learner comparisons.
Do not tune workloads or hide baselines just to make learning win.

Full default matrix: 10 seeds, 100 warm-ups + 5×1,000 measured specifications
per seed = 51,000 blocks and 153,000 candidate executions. Previous host timing
does not predict Mac runtime; measure a smoke/intermediate run first.

## What to transfer

- Copy the entire project source, `AGENT_CONTEXT.md`, `docs/`, `scripts/`,
  `docker/`, both Compose files, Gradle build/settings, `gradlew`, and
  `gradle/wrapper/` including the wrapper JAR.
- Include all 19 PDFs already collected in `docs/referencias/artigos` and its
  README. ForeSight has not been downloaded there yet. Two Gama articles remain
  pending (stream evaluation and concept-drift survey).
- Transfer the approved manuscript/BibTeX separately if edited outside this
  project. Pasted chat changes are not automatically saved into a `.tex` file.
- `build/` is ignored: copy `build/experiments/` and any raw outputs separately
  if present. Historical `/tmp/alps-*.log` references may point to another host;
  do not assume those files exist here.
- Docker named volumes are outside the source tree. Copying the repo does not
  migrate loaded databases. Rebuild a fresh dataset for new experiments or
  migrate database backups deliberately; preserve old experiment artifacts.
- Do not rely on Windows `.gradle/`, `build/classes/`, IDE state, or absolute
  Windows paths. Regenerate local build caches on Mac. Transfer credentials
  privately if needed; keep secrets out of context documents and Git.
- This session could not read Git status: `git status` failed with a parent-path
  error, and there is no `.git` entry at this project root. Check the parent
  repository and transfer method before assuming a clone includes these files.
  No commit or push was performed for this handoff.

## Mac setup and validation

These are commands derived from the checked-in build/scripts, not a tested Mac
installation. Prerequisites: JDK 21, a running Docker Compose or compatible
Podman Compose environment, Bash, and network access for dependencies/images.
Use architecture-appropriate Java/container tooling; validate image support on
Apple Silicon rather than forcing an untested platform globally.

From the project directory, validate Java and the build first:

```bash
java -version
export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
export GRADLE_USER_HOME="$HOME/.gradle"
bash ./gradlew --version
bash ./gradlew test
docker compose version
docker compose config --quiet
```

Use `bash` to avoid depending on executable bits lost during a Windows copy.
If scripts report `\r`/bad-interpreter errors, check CRLF line endings and
normalize only the affected shell scripts to LF. The container entrypoint also
needs valid Unix line endings. Do not mechanically rewrite all source files.

For a **fresh** local database environment:

```bash
bash ./scripts/benchmarksql.sh load-all
bash ./scripts/benchmarksql.sh verify
bash ./gradlew runMariaDbTpccCluster
```

`load-all` starts the nodes, builds the pinned BenchmarkSQL image, loads data,
and remaps warehouses. It is not idempotent: for an existing dataset verify it
first and consult `docs/benchmarksql.md`; do not automatically destroy tables or
volumes. Keep `BMSQL_WAREHOUSES=1` for this topology.

After integration validation, run the small paired smoke experiment:

```bash
ACP_DEFENSIBLE_SEEDS=101 \
ACP_DEFENSIBLE_WARMUP=1 \
ACP_DEFENSIBLE_PHASE_LENGTH=1 \
ACP_DEFENSIBLE_ADAPTATION_STREAK=1 \
ACP_DEFENSIBLE_LATENCY_SPIKE_MS=1 \
ACP_DEFENSIBLE_OUTPUT=build/experiments/mac-smoke \
bash ./gradlew runMariaDbDefensibleExperiment
```

Use a new output directory for each run. Inspect correctness, restoration and
XA status before a larger run. The smoke parameters are not thesis measurements.
The full matrix uses `bash ./gradlew runMariaDbDefensibleExperiment`; consult
`docs/defensible-adaptive-experiment.md` before starting that longer workflow.

Main outputs: `transactions.csv`, `seed-summary.csv`, `aggregate-summary.csv`,
`classifier-summary.csv`, `policy-comparisons.csv`, `config.properties`.
Treat the Mac as a new experimental environment: record configuration and keep
its results distinguishable from earlier hosts.

## Suggested first message on the Mac

> Read AGENT_CONTEXT.md and docs/MAC_HANDOFF.md. Resume the ALPS master's thesis
> project. I am writing the proposal in Portuguese and want focused feedback
> preserving my text. FLAC is the central related work. Start by checking which
> manuscripts, references and experiment artifacts arrived. Do not treat the
> assistant's alps-revisado.tex draft as my approved manuscript. Validate the
> local Java/build/container setup before running experiments.
