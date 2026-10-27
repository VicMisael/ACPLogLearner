package ufc.victor;

import ufc.victor.db.mariadb.MariaDbNewOrderStateController;
import ufc.victor.db.mariadb.MariaDbTpccNewOrderPlanner;
import ufc.victor.db.mariadb.MariaDbTpccTransactionRunner;
import ufc.victor.db.mariadb.MariaDbWalMetricsCollector;
import ufc.victor.db.mariadb.MariaDbXaConfig;
import ufc.victor.db.mariadb.TpccNewOrderFeatureExtractor;
import ufc.victor.db.mariadb.TpccNewOrderSpec;
import ufc.victor.db.mariadb.TpccNewOrderSpecFactory;
import ufc.victor.db.mariadb.WarehouseRouter;
import ufc.victor.experiment.AlpsFeatureProfile;
import ufc.victor.experiment.AlpsFeatures;
import ufc.victor.experiment.CategoricalOutcomeLabeler;
import ufc.victor.experiment.CategoricalOutcomeObservation;
import ufc.victor.experiment.CategoricalOutcomePrediction;
import ufc.victor.experiment.CategoricalPolicyWeights;
import ufc.victor.experiment.CategoricalProtocolSelectionPolicy;
import ufc.victor.experiment.CompletionCategory;
import ufc.victor.experiment.CostCategory;
import ufc.victor.experiment.OnlineExperimentSchedule;
import ufc.victor.experiment.OutcomeThresholds;
import ufc.victor.experiment.PairedProtocolTrial;
import ufc.victor.experiment.PairedProtocolTrialRunner;
import ufc.victor.experiment.ProtocolCostFunction;
import ufc.victor.experiment.ProtocolCostWeights;
import ufc.victor.experiment.RollingMetricsRegistry;
import ufc.victor.experiment.TransactionTrialRecord;
import ufc.victor.experiment.TrialOutcome;
import ufc.victor.protocol.SelectedProtocol;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.SplittableRandom;

/**
 * Paired, multi-seed MariaDB experiment for policy cost, oracle agreement, and regret.
 * Every protocol trial is followed by exact logical state restoration and verification.
 */
public final class MariaDbDefensibleExperimentMain {
    private static final List<List<Integer>> PARTICIPANT_SETS = List.of(
            List.of(1), List.of(2), List.of(3), List.of(4),
            List.of(1, 2), List.of(1, 3), List.of(1, 4),
            List.of(2, 3), List.of(2, 4), List.of(3, 4),
            List.of(1, 2, 3), List.of(1, 2, 4), List.of(1, 3, 4),
            List.of(2, 3, 4), List.of(1, 2, 3, 4)
    );

    private MariaDbDefensibleExperimentMain() {
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnvironment();
        Files.createDirectories(config.outputDirectory());
        writeConfig(config);

        Map<Integer, MariaDbXaConfig> databaseConfigs = MariaDbTpccClusterMain.configs();
        MariaDbTpccNewOrderPlanner planner = new MariaDbTpccNewOrderPlanner(
                new WarehouseRouter(databaseConfigs)
        );
        MariaDbTpccTransactionRunner databaseRunner = new MariaDbTpccTransactionRunner(
                planner,
                new MariaDbWalMetricsCollector(MariaDbTpccClusterMain.redoConfigs(databaseConfigs)),
                config.timeoutMs()
        );
        ProtocolCostFunction costFunction = new ProtocolCostFunction(config.costWeights());

        List<SeedResult> seedResults = new ArrayList<>();
        Path transactionPath = config.outputDirectory().resolve("transactions.csv");
        try (MariaDbNewOrderStateController stateController =
                     new MariaDbNewOrderStateController(databaseConfigs);
             TransactionWriter writer = new TransactionWriter(transactionPath)) {
            for (long seed : config.seeds()) {
                SeedResult result = runSeed(
                        config, seed, databaseRunner, stateController, costFunction, writer
                );
                seedResults.add(result);
                System.out.println("completed seed=" + seed + ", adaptive="
                        + result.metrics().get(Policy.ADAPTIVE).summary());
            }
        }
        writeSeedSummaries(config.outputDirectory().resolve("seed-summary.csv"), seedResults);
        writeAggregateSummaries(
                config.outputDirectory().resolve("aggregate-summary.csv"), seedResults
        );
        writeClassifierSummaries(
                config.outputDirectory().resolve("classifier-summary.csv"), seedResults
        );
        writePolicyComparisons(
                config.outputDirectory().resolve("policy-comparisons.csv"), seedResults
        );
        System.out.println("Experiment complete: " + config.outputDirectory().toAbsolutePath());
    }

    private static SeedResult runSeed(
            Config config,
            long seed,
            MariaDbTpccTransactionRunner databaseRunner,
            MariaDbNewOrderStateController stateController,
            ProtocolCostFunction costFunction,
            TransactionWriter writer
    ) throws Exception {
        RollingMetricsRegistry adaptiveHistory = new RollingMetricsRegistry(config.historyWindowSize());
        TpccNewOrderFeatureExtractor featureExtractor = new TpccNewOrderFeatureExtractor();
        Map<SelectedProtocol, VfdtCategoricalOutcomePredictor> predictors =
                new EnumMap<>(SelectedProtocol.class);
        for (SelectedProtocol protocol : SelectedProtocol.values()) {
            predictors.put(protocol, new VfdtCategoricalOutcomePredictor(
                    List.of("P1", "P2", "P3", "P4"),
                    List.of(TpccNewOrderFeatureExtractor.TRANSACTION_TYPE),
                    TpccNewOrderFeatureExtractor.WORKLOAD_FEATURE_NAMES,
                    AlpsFeatureProfile.GENERIC
            ));
        }
        CategoricalProtocolSelectionPolicy adaptivePolicy = new CategoricalProtocolSelectionPolicy(
                config.selectionWeights()
        );
        CategoricalOutcomeLabeler fixedLabeler = new CategoricalOutcomeLabeler(
                3,
                config.thresholds().latencyLowUpperMs(),
                config.thresholds().latencyMediumUpperMs(),
                config.thresholds().redoLowUpperBytes(),
                config.thresholds().redoMediumUpperBytes()
        );
        FixedThresholdPolicy heuristic = new FixedThresholdPolicy(config.heuristicWindowSize());
        OnlineExperimentSchedule schedule = new OnlineExperimentSchedule(
                OnlineExperimentSchedule.Mode.PHASED,
                config.phaseLength(),
                config.latencySpikeMs(),
                config.abortSpikeRate()
        );
        SplittableRandom randomPolicy = new SplittableRandom(mix(seed, 0x5deece66dL));
        Map<Policy, PolicyMetrics> metrics = new EnumMap<>(Policy.class);
        Arrays.stream(Policy.values()).forEach(policy -> metrics.put(policy, new PolicyMetrics()));
        AdaptationTracker adaptation = new AdaptationTracker(config.adaptationStreak());
        Map<SelectedProtocol, PredictionMetrics> predictionMetrics =
                new EnumMap<>(SelectedProtocol.class);
        Arrays.stream(SelectedProtocol.values()).forEach(protocol ->
                predictionMetrics.put(protocol, new PredictionMetrics()));
        int measuredTransactions = config.phaseLength() * 5;
        int allTransactions = config.warmupTransactions() + measuredTransactions;

        for (int streamIndex = 0; streamIndex < allTransactions; streamIndex++) {
            boolean measured = streamIndex >= config.warmupTransactions();
            int measuredIndex = streamIndex - config.warmupTransactions();
            OnlineExperimentSchedule.Condition condition = measured
                    ? schedule.conditionFor(measuredIndex)
                    : new OnlineExperimentSchedule.Condition(
                            "WARMUP", 0, config.warmupAbortRate()
                    );
            List<Integer> participants = PARTICIPANT_SETS.get(
                    Math.floorMod((int) (seed + streamIndex), PARTICIPANT_SETS.size())
            );
            int specificationIndex = specificationIndex(seed, streamIndex);
            TpccNewOrderSpec specification = TpccNewOrderSpecFactory.controlled(
                    participants, specificationIndex
            );
            Map<Integer, Boolean> votes = votes(seed, streamIndex, participants, condition);
            TrialOutcome expectedOutcome = votes.values().stream().allMatch(Boolean::booleanValue)
                    ? TrialOutcome.COMMITTED : TrialOutcome.ABORTED;

            AlpsFeatures features = adaptiveHistory.featuresFor(featureExtractor.extract(specification));
            Map<SelectedProtocol, CategoricalOutcomePrediction> predictions =
                    new EnumMap<>(SelectedProtocol.class);
            Map<SelectedProtocol, Long> inferenceByProtocol = new EnumMap<>(SelectedProtocol.class);
            for (SelectedProtocol protocol : SelectedProtocol.values()) {
                long inferenceStarted = System.nanoTime();
                predictions.put(protocol, predictors.get(protocol).predict(features));
                inferenceByProtocol.put(protocol, System.nanoTime() - inferenceStarted);
            }
            Map<SelectedProtocol, Double> selectionScores = new EnumMap<>(SelectedProtocol.class);
            predictions.forEach((protocol, candidatePrediction) -> selectionScores.put(
                    protocol, adaptivePolicy.score(protocol, candidatePrediction)
            ));
            SelectedProtocol adaptive = selectAdaptive(selectionScores);
            CategoricalOutcomePrediction prediction = predictions.get(adaptive);
            long inferenceNanos = inferenceByProtocol.values().stream().mapToLong(Long::longValue).sum();
            SelectedProtocol staticHeuristic = heuristic.select();
            SelectedProtocol random = SelectedProtocol.values()[
                    randomPolicy.nextInt(SelectedProtocol.values().length)
            ];

            PairedProtocolTrialRunner<TpccNewOrderSpec, MariaDbNewOrderStateController.Snapshot>
                    pairedRunner = new PairedProtocolTrialRunner<>(
                    stateController,
                    (spec, protocol) -> databaseRunner.run(
                            spec, protocol, votes, condition.injectedNetworkLatencyMs()
                    ),
                    costFunction
            );
            PairedProtocolTrial paired = pairedRunner.run(
                    specification,
                    expectedOutcome,
                    mix(seed, streamIndex)
            );

            Map<Policy, SelectedProtocol> selections = selections(
                    adaptive, staticHeuristic, random, paired.oracleProtocol()
            );
            if (measured) {
                for (Map.Entry<Policy, SelectedProtocol> selection : selections.entrySet()) {
                    metrics.get(selection.getKey()).record(
                            condition.phase(), selection.getValue(), paired
                    );
                }
                adaptation.record(condition.phase(), adaptive == paired.oracleProtocol());
            }

            Map<SelectedProtocol, CategoricalOutcomeObservation> observations =
                    new EnumMap<>(SelectedProtocol.class);
            Map<SelectedProtocol, Long> trainingByProtocol = new EnumMap<>(SelectedProtocol.class);
            for (SelectedProtocol protocol : SelectedProtocol.values()) {
                CategoricalOutcomeObservation candidateObservation = fixedLabeler.label(
                        paired.trials().get(protocol), config.thresholds()
                );
                observations.put(protocol, candidateObservation);
                long trainingStarted = System.nanoTime();
                predictors.get(protocol).train(features, candidateObservation);
                trainingByProtocol.put(protocol, System.nanoTime() - trainingStarted);
            }
            TransactionTrialRecord adaptiveTrial = paired.trials().get(adaptive);
            long trainingNanos = trainingByProtocol.values().stream().mapToLong(Long::longValue).sum();
            adaptiveHistory.record(adaptiveTrial);
            heuristic.record(paired.trials().get(staticHeuristic));

            if (measured) {
                for (SelectedProtocol protocol : SelectedProtocol.values()) {
                    predictionMetrics.get(protocol).record(
                            condition.phase(), predictions.get(protocol), observations.get(protocol),
                            inferenceByProtocol.get(protocol), trainingByProtocol.get(protocol)
                    );
                }
                writer.write(new TransactionRow(
                        config.experimentId(), seed, measuredIndex, specification, features,
                        condition.phase(), expectedOutcome, prediction, predictions, selectionScores,
                        selections,
                        paired, inferenceNanos, trainingNanos,
                        modelSizes(predictors, true), modelSizes(predictors, false)
                ));
            }
        }
        return new SeedResult(seed, metrics, adaptation.delays(), predictionMetrics);
    }

    private static SelectedProtocol selectAdaptive(Map<SelectedProtocol, Double> scores) {
        return Arrays.stream(SelectedProtocol.values())
                .min(Comparator
                        .comparingDouble((SelectedProtocol protocol) ->
                                scores.get(protocol))
                        .thenComparingInt(Enum::ordinal))
                .orElseThrow();
    }

    private static Map<String, Integer> modelSizes(
            Map<SelectedProtocol, VfdtCategoricalOutcomePredictor> predictors,
            boolean nodes
    ) {
        Map<String, Integer> result = new LinkedHashMap<>();
        predictors.forEach((protocol, predictor) -> {
            Map<String, Integer> sizes = nodes ? predictor.nodeCounts() : predictor.treeDepths();
            sizes.forEach((head, size) -> result.put(protocol + "." + head, size));
        });
        return Map.copyOf(result);
    }

    private static Map<Policy, SelectedProtocol> selections(
            SelectedProtocol adaptive,
            SelectedProtocol heuristic,
            SelectedProtocol random,
            SelectedProtocol oracle
    ) {
        Map<Policy, SelectedProtocol> result = new EnumMap<>(Policy.class);
        result.put(Policy.ADAPTIVE, adaptive);
        result.put(Policy.STATIC_HEURISTIC, heuristic);
        result.put(Policy.RANDOM, random);
        result.put(Policy.ALWAYS_TWO_PC, SelectedProtocol.TWO_PC);
        result.put(Policy.ALWAYS_PRESUMED_ABORT, SelectedProtocol.TWO_PC_PRESUMED_ABORT);
        result.put(Policy.ALWAYS_PRESUMED_COMMIT, SelectedProtocol.TWO_PC_PRESUMED_COMMIT);
        result.put(Policy.ORACLE, oracle);
        return result;
    }

    private static Map<Integer, Boolean> votes(
            long seed,
            int streamIndex,
            List<Integer> participants,
            OnlineExperimentSchedule.Condition condition
    ) {
        Map<Integer, Boolean> votes = new LinkedHashMap<>();
        for (int participant : participants) {
            SplittableRandom random = new SplittableRandom(mix(seed, 31L * streamIndex + participant));
            votes.put(participant, random.nextDouble() >= condition.abortRateFloor());
        }
        return Map.copyOf(votes);
    }

    private static int specificationIndex(long seed, int streamIndex) {
        // Keep arithmetic inside TpccNewOrderSpecFactory below Integer.MAX_VALUE.
        return Math.floorMod((int) mix(seed, streamIndex), 10_000_000);
    }

    private static long mix(long first, long second) {
        long value = first + 0x9e3779b97f4a7c15L + (second << 6) + (second >>> 2);
        value ^= value >>> 30;
        value *= 0xbf58476d1ce4e5b9L;
        value ^= value >>> 27;
        value *= 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }

    private static void writeConfig(Config config) throws IOException {
        Path path = config.outputDirectory().resolve("config.properties");
        try (PrintWriter writer = writer(path)) {
            writer.println("experiment_id=" + config.experimentId());
            writer.println("created_at=" + Instant.now());
            writer.println("git_revision=" + gitRevision());
            writer.println("git_dirty=" + gitDirty());
            writer.println("java_version=" + System.getProperty("java.version"));
            writer.println("os=" + System.getProperty("os.name") + " " + System.getProperty("os.version"));
            writer.println("seeds=" + config.seeds());
            writer.println("warmup_transactions=" + config.warmupTransactions());
            writer.println("phase_length=" + config.phaseLength());
            writer.println("latency_spike_ms=" + config.latencySpikeMs());
            writer.println("abort_spike_rate=" + config.abortSpikeRate());
            writer.println("history_window_size=" + config.historyWindowSize());
            writer.println("heuristic_window_size=" + config.heuristicWindowSize());
            writer.println("adaptation_streak=" + config.adaptationStreak());
            writer.println("thresholds=" + config.thresholds());
            writer.println("selection_weights=" + config.selectionWeights());
            writer.println("cost_weights=" + config.costWeights());
        }
    }

    private static String gitRevision() {
        try {
            Process process = new ProcessBuilder("git", "rev-parse", "HEAD")
                    .redirectErrorStream(true).start();
            String result = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            return process.waitFor() == 0 ? result : "unknown";
        } catch (Exception ignored) {
            return "unknown";
        }
    }

    private static String gitDirty() {
        try {
            Process process = new ProcessBuilder("git", "status", "--porcelain")
                    .redirectErrorStream(true).start();
            String result = new String(
                    process.getInputStream().readAllBytes(), StandardCharsets.UTF_8
            );
            return process.waitFor() == 0 ? Boolean.toString(!result.isBlank()) : "unknown";
        } catch (Exception ignored) {
            return "unknown";
        }
    }

    private static void writeSeedSummaries(Path path, List<SeedResult> results) throws IOException {
        try (PrintWriter writer = writer(path)) {
            writer.println("seed,policy,phase,examples,total_cost,mean_cost,p95_latency_ms,"
                    + "cumulative_regret,mean_regret,oracle_agreement,invalid_trials,selections,adaptation_delay");
            for (SeedResult result : results) {
                for (Policy policy : Policy.values()) {
                    PolicyMetrics value = result.metrics().get(policy);
                    writeSummary(writer, result.seed(), policy, "ALL", value,
                            policy == Policy.ADAPTIVE ? result.adaptationDelays().toString() : "");
                    value.phases().forEach((phase, phaseMetrics) ->
                            writeSummary(writer, result.seed(), policy, phase, phaseMetrics, "")
                    );
                }
            }
        }
    }

    private static void writeSummary(
            PrintWriter writer,
            long seed,
            Policy policy,
            String phase,
            MetricsValue value,
            String adaptationDelay
    ) {
        writer.println(csv(
                seed, policy, phase, value.examples(), value.totalCost(), value.meanCost(),
                value.p95LatencyMs(), value.cumulativeRegret(), value.meanRegret(),
                value.oracleAgreement(), value.invalidTrials(), value.selections(), adaptationDelay
        ));
    }

    private static void writeAggregateSummaries(Path path, List<SeedResult> results)
            throws IOException {
        try (PrintWriter writer = writer(path)) {
            writer.println("policy,seeds,mean_cost,mean_cost_ci95_low,mean_cost_ci95_high,"
                    + "mean_regret,mean_regret_ci95_low,mean_regret_ci95_high,mean_oracle_agreement");
            for (Policy policy : Policy.values()) {
                List<Double> costs = results.stream()
                        .map(result -> result.metrics().get(policy).meanCost()).toList();
                List<Double> regrets = results.stream()
                        .map(result -> result.metrics().get(policy).meanRegret()).toList();
                List<Double> agreements = results.stream()
                        .map(result -> result.metrics().get(policy).oracleAgreement()).toList();
                Interval cost = Interval.mean95(costs);
                Interval regret = Interval.mean95(regrets);
                writer.println(csv(
                        policy, results.size(), cost.mean(), cost.low(), cost.high(),
                        regret.mean(), regret.low(), regret.high(), mean(agreements)
                ));
            }
        }
    }

    private static void writeClassifierSummaries(Path path, List<SeedResult> results)
            throws IOException {
        try (PrintWriter writer = writer(path)) {
            writer.println("seed,phase,head,examples,accuracy,macro_f1,balanced_accuracy,"
                    + "mean_inference_ns,mean_training_ns");
            for (SeedResult result : results) {
                result.predictions().forEach((protocol, protocolMetrics) -> {
                    writePredictionSummary(
                            writer, result.seed(), "ALL", protocol, protocolMetrics
                    );
                    protocolMetrics.phases().forEach((phase, metrics) ->
                            writePredictionSummary(
                                    writer, result.seed(), phase, protocol, metrics
                            )
                    );
                });
            }
        }
    }

    private static void writePredictionSummary(
            PrintWriter writer,
            long seed,
            String phase,
            SelectedProtocol protocol,
            PredictionMetrics metrics
    ) {
        writer.println(csv(seed, phase, protocol + ".completion", metrics.completion.examples(),
                metrics.completion.accuracy(), metrics.completion.macroF1(),
                metrics.completion.balancedAccuracy(), metrics.meanInferenceNanos(),
                metrics.meanTrainingNanos()));
        writer.println(csv(seed, phase, protocol + ".latency", metrics.latency.examples(),
                metrics.latency.accuracy(), metrics.latency.macroF1(),
                metrics.latency.balancedAccuracy(), metrics.meanInferenceNanos(),
                metrics.meanTrainingNanos()));
        writer.println(csv(seed, phase, protocol + ".redo", metrics.redo.examples(),
                metrics.redo.accuracy(), metrics.redo.macroF1(),
                metrics.redo.balancedAccuracy(), metrics.meanInferenceNanos(),
                metrics.meanTrainingNanos()));
    }

    private static void writePolicyComparisons(Path path, List<SeedResult> results)
            throws IOException {
        try (PrintWriter writer = writer(path)) {
            writer.println("baseline,seeds,adaptive_minus_baseline_mean_cost,ci95_low,ci95_high,"
                    + "adaptive_relative_cost_reduction");
            for (Policy baseline : List.of(
                    Policy.ALWAYS_TWO_PC,
                    Policy.ALWAYS_PRESUMED_ABORT,
                    Policy.ALWAYS_PRESUMED_COMMIT,
                    Policy.STATIC_HEURISTIC,
                    Policy.RANDOM
            )) {
                List<Double> pairedDifferences = results.stream().map(result ->
                        result.metrics().get(Policy.ADAPTIVE).meanCost()
                                - result.metrics().get(baseline).meanCost()
                ).toList();
                Interval interval = Interval.mean95(pairedDifferences);
                double adaptiveMean = results.stream().mapToDouble(result ->
                        result.metrics().get(Policy.ADAPTIVE).meanCost()).average().orElse(Double.NaN);
                double baselineMean = results.stream().mapToDouble(result ->
                        result.metrics().get(baseline).meanCost()).average().orElse(Double.NaN);
                double reduction = baselineMean == 0.0
                        ? Double.NaN : (baselineMean - adaptiveMean) / baselineMean;
                writer.println(csv(
                        baseline, results.size(), interval.mean(), interval.low(), interval.high(),
                        reduction
                ));
            }
        }
    }

    private static PrintWriter writer(Path path) throws IOException {
        BufferedWriter buffered = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
        return new PrintWriter(buffered, false);
    }

    private static String csv(Object... values) {
        return Arrays.stream(values).map(MariaDbDefensibleExperimentMain::csvValue)
                .reduce((left, right) -> left + "," + right).orElse("");
    }

    private static String csvValue(Object value) {
        String text = value == null ? "" : value.toString();
        if (text.contains(",") || text.contains("\"") || text.contains("\n")) {
            return "\"" + text.replace("\"", "\"\"") + "\"";
        }
        return text;
    }

    private static double mean(List<Double> values) {
        return values.stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);
    }

    private enum Policy {
        ADAPTIVE,
        STATIC_HEURISTIC,
        RANDOM,
        ALWAYS_TWO_PC,
        ALWAYS_PRESUMED_ABORT,
        ALWAYS_PRESUMED_COMMIT,
        ORACLE
    }

    private static final class FixedThresholdPolicy {
        private final int windowSize;
        private final Deque<Boolean> committed = new ArrayDeque<>();

        private FixedThresholdPolicy(int windowSize) {
            this.windowSize = windowSize;
        }

        private SelectedProtocol select() {
            if (committed.size() < Math.min(10, windowSize)) return SelectedProtocol.TWO_PC;
            double commitRate = committed.stream().filter(Boolean::booleanValue).count()
                    / (double) committed.size();
            if (commitRate >= 2.0 / 3.0) return SelectedProtocol.TWO_PC_PRESUMED_COMMIT;
            if (commitRate <= 1.0 / 3.0) return SelectedProtocol.TWO_PC_PRESUMED_ABORT;
            return SelectedProtocol.TWO_PC;
        }

        private void record(TransactionTrialRecord trial) {
            committed.addLast(trial.committed());
            while (committed.size() > windowSize) committed.removeFirst();
        }
    }

    private static final class PolicyMetrics implements MetricsValue {
        private int examples;
        private double totalCost;
        private double cumulativeRegret;
        private int oracleMatches;
        private int invalidTrials;
        private final List<Long> latenciesMs = new ArrayList<>();
        private final Map<SelectedProtocol, Integer> selections = new EnumMap<>(SelectedProtocol.class);
        private final Map<String, PolicyMetrics> phases = new LinkedHashMap<>();

        private void record(String phase, SelectedProtocol selected, PairedProtocolTrial paired) {
            recordOnly(selected, paired);
            phases.computeIfAbsent(phase, ignored -> new PolicyMetrics()).recordOnly(selected, paired);
        }

        private void recordOnly(SelectedProtocol selected, PairedProtocolTrial paired) {
            examples++;
            totalCost += paired.costs().get(selected).total();
            cumulativeRegret += paired.regret(selected);
            oracleMatches += selected == paired.oracleProtocol() ? 1 : 0;
            invalidTrials += paired.costs().get(selected).valid() ? 0 : 1;
            latenciesMs.add(paired.trials().get(selected).totalLatency().toMillis());
            selections.merge(selected, 1, Integer::sum);
        }

        @Override public int examples() { return examples; }
        @Override public double totalCost() { return totalCost; }
        @Override public double meanCost() { return ratio(totalCost, examples); }
        @Override public long p95LatencyMs() { return percentile95(latenciesMs); }
        @Override public double cumulativeRegret() { return cumulativeRegret; }
        @Override public double meanRegret() { return ratio(cumulativeRegret, examples); }
        @Override public double oracleAgreement() { return ratio(oracleMatches, examples); }
        @Override public int invalidTrials() { return invalidTrials; }
        @Override public Map<SelectedProtocol, Integer> selections() { return Map.copyOf(selections); }
        private Map<String, PolicyMetrics> phases() { return Map.copyOf(phases); }
        private String summary() {
            return "meanCost=" + meanCost() + ", meanRegret=" + meanRegret()
                    + ", oracleAgreement=" + oracleAgreement() + ", p95LatencyMs=" + p95LatencyMs();
        }
    }

    private interface MetricsValue {
        int examples();
        double totalCost();
        double meanCost();
        long p95LatencyMs();
        double cumulativeRegret();
        double meanRegret();
        double oracleAgreement();
        int invalidTrials();
        Map<SelectedProtocol, Integer> selections();
    }

    private static long percentile95(List<Long> values) {
        if (values.isEmpty()) return 0L;
        List<Long> sorted = values.stream().sorted().toList();
        int index = Math.max(0, (int) Math.ceil(0.95 * sorted.size()) - 1);
        return sorted.get(index);
    }

    private static double ratio(double numerator, int denominator) {
        return denominator == 0 ? Double.NaN : numerator / denominator;
    }

    private static final class AdaptationTracker {
        private final int requiredStreak;
        private final Map<String, PhaseAdaptation> phases = new LinkedHashMap<>();

        private AdaptationTracker(int requiredStreak) {
            this.requiredStreak = requiredStreak;
        }

        private void record(String phase, boolean oracleMatch) {
            phases.computeIfAbsent(phase, ignored -> new PhaseAdaptation(requiredStreak))
                    .record(oracleMatch);
        }

        private Map<String, Integer> delays() {
            Map<String, Integer> delays = new LinkedHashMap<>();
            phases.forEach((phase, value) -> delays.put(phase, value.delay()));
            return Map.copyOf(delays);
        }
    }

    private static final class PredictionMetrics {
        private final ClassMetrics<CompletionCategory> completion =
                new ClassMetrics<>(CompletionCategory.class);
        private final ClassMetrics<CostCategory> latency = new ClassMetrics<>(CostCategory.class);
        private final ClassMetrics<CostCategory> redo = new ClassMetrics<>(CostCategory.class);
        private final Map<String, PredictionMetrics> phases = new LinkedHashMap<>();
        private long inferenceNanos;
        private long trainingNanos;
        private int overheadExamples;

        private void record(
                String phase,
                CategoricalOutcomePrediction prediction,
                CategoricalOutcomeObservation observation,
                long inferenceNanos,
                long trainingNanos
        ) {
            recordOnly(prediction, observation, inferenceNanos, trainingNanos);
            phases.computeIfAbsent(phase, ignored -> new PredictionMetrics())
                    .recordOnly(prediction, observation, inferenceNanos, trainingNanos);
        }

        private void recordOnly(
                CategoricalOutcomePrediction prediction,
                CategoricalOutcomeObservation observation,
                long inferenceNanos,
                long trainingNanos
        ) {
            completion.record(observation.completion(), prediction.completion().predictedClass());
            observation.latency().ifPresent(actual ->
                    latency.record(actual, prediction.latency().predictedClass()));
            observation.redo().ifPresent(actual ->
                    redo.record(actual, prediction.redo().predictedClass()));
            this.inferenceNanos += inferenceNanos;
            this.trainingNanos += trainingNanos;
            overheadExamples++;
        }

        private Map<String, PredictionMetrics> phases() {
            return Map.copyOf(phases);
        }

        private double meanInferenceNanos() {
            return ratio(inferenceNanos, overheadExamples);
        }

        private double meanTrainingNanos() {
            return ratio(trainingNanos, overheadExamples);
        }
    }

    private static final class ClassMetrics<L extends Enum<L>> {
        private final L[] labels;
        private final Map<L, Map<L, Integer>> confusion;
        private int examples;
        private int correct;

        private ClassMetrics(Class<L> labelType) {
            labels = labelType.getEnumConstants();
            confusion = new EnumMap<>(labelType);
            for (L actual : labels) confusion.put(actual, new EnumMap<>(labelType));
        }

        private void record(L actual, L predicted) {
            examples++;
            correct += actual == predicted ? 1 : 0;
            confusion.get(actual).merge(predicted, 1, Integer::sum);
        }

        private int examples() {
            return examples;
        }

        private double accuracy() {
            return ratio(correct, examples);
        }

        private double macroF1() {
            double total = 0.0;
            int included = 0;
            for (L label : labels) {
                int truePositive = count(label, label);
                int falsePositive = 0;
                int falseNegative = 0;
                for (L other : labels) {
                    if (other == label) continue;
                    falsePositive += count(other, label);
                    falseNegative += count(label, other);
                }
                if (truePositive + falsePositive + falseNegative == 0) continue;
                int predictedPositive = truePositive + falsePositive;
                int actualPositive = truePositive + falseNegative;
                double precision = predictedPositive == 0
                        ? 0.0 : truePositive / (double) predictedPositive;
                double recall = actualPositive == 0
                        ? 0.0 : truePositive / (double) actualPositive;
                total += precision + recall == 0.0
                        ? 0.0 : 2.0 * precision * recall / (precision + recall);
                included++;
            }
            return included == 0 ? Double.NaN : total / included;
        }

        private double balancedAccuracy() {
            double totalRecall = 0.0;
            int included = 0;
            for (L label : labels) {
                int support = Arrays.stream(labels)
                        .mapToInt(predicted -> count(label, predicted)).sum();
                if (support == 0) continue;
                totalRecall += count(label, label) / (double) support;
                included++;
            }
            return included == 0 ? Double.NaN : totalRecall / included;
        }

        private int count(L actual, L predicted) {
            return confusion.get(actual).getOrDefault(predicted, 0);
        }
    }

    private static final class PhaseAdaptation {
        private final int requiredStreak;
        private int examples;
        private int streak;
        private int delay = -1;

        private PhaseAdaptation(int requiredStreak) {
            this.requiredStreak = requiredStreak;
        }

        private void record(boolean match) {
            examples++;
            if (delay >= 0) return;
            streak = match ? streak + 1 : 0;
            if (streak == requiredStreak) delay = examples - requiredStreak;
        }

        private int delay() {
            return delay;
        }
    }

    private record SeedResult(
            long seed,
            Map<Policy, PolicyMetrics> metrics,
            Map<String, Integer> adaptationDelays,
            Map<SelectedProtocol, PredictionMetrics> predictions
    ) {
        private SeedResult {
            metrics = Map.copyOf(metrics);
            adaptationDelays = Map.copyOf(adaptationDelays);
            predictions = Map.copyOf(predictions);
        }
    }

    private record Interval(double mean, double low, double high) {
        private static Interval mean95(List<Double> values) {
            double mean = MariaDbDefensibleExperimentMain.mean(values);
            if (values.size() < 2) return new Interval(mean, mean, mean);
            double sumSquares = values.stream()
                    .mapToDouble(value -> Math.pow(value - mean, 2.0)).sum();
            double standardError = Math.sqrt(sumSquares / (values.size() - 1))
                    / Math.sqrt(values.size());
            double margin = tCritical95(values.size() - 1) * standardError;
            return new Interval(mean, mean - margin, mean + margin);
        }

        private static double tCritical95(int degreesOfFreedom) {
            double[] values = {
                    0.0, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306,
                    2.262, 2.228, 2.201, 2.179, 2.160, 2.145, 2.131, 2.120,
                    2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064,
                    2.060, 2.056, 2.052, 2.048, 2.045, 2.042
            };
            return degreesOfFreedom < values.length ? values[degreesOfFreedom] : 1.96;
        }
    }

    private record TransactionRow(
            String experimentId,
            long seed,
            int transactionIndex,
            TpccNewOrderSpec specification,
            AlpsFeatures features,
            String phase,
            TrialOutcome expectedOutcome,
            CategoricalOutcomePrediction prediction,
            Map<SelectedProtocol, CategoricalOutcomePrediction> predictions,
            Map<SelectedProtocol, Double> selectionScores,
            Map<Policy, SelectedProtocol> selections,
            PairedProtocolTrial paired,
            long inferenceNanos,
            long trainingNanos,
            Map<String, Integer> nodeCounts,
            Map<String, Integer> depths
    ) {
    }

    private static final class TransactionWriter implements AutoCloseable {
        private final PrintWriter writer;

        private TransactionWriter(Path path) throws IOException {
            writer = MariaDbDefensibleExperimentMain.writer(path);
            writer.println("experiment_id,seed,transaction_index,specification_id,phase,participants,feature_vector,"
                    + "expected_outcome,predicted_completion,predicted_latency,predicted_redo,"
                    + "two_pc_predicted_bundle,presumed_abort_predicted_bundle,"
                    + "presumed_commit_predicted_bundle,two_pc_selection_score,"
                    + "presumed_abort_selection_score,presumed_commit_selection_score,"
                    + "adaptive_protocol,heuristic_protocol,random_protocol,oracle_protocol,"
                    + "two_pc_cost,presumed_abort_cost,presumed_commit_cost,adaptive_regret,"
                    + "heuristic_regret,random_regret,two_pc_regret,presumed_abort_regret,"
                    + "presumed_commit_regret,two_pc_outcome,presumed_abort_outcome,"
                    + "presumed_commit_outcome,two_pc_latency_ms,presumed_abort_latency_ms,"
                    + "presumed_commit_latency_ms,two_pc_redo_bytes,presumed_abort_redo_bytes,"
                    + "presumed_commit_redo_bytes,two_pc_messages,presumed_abort_messages,"
                    + "presumed_commit_messages,two_pc_protocol_logs,presumed_abort_protocol_logs,"
                    + "presumed_commit_protocol_logs,two_pc_valid,presumed_abort_valid,"
                    + "presumed_commit_valid,inference_ns,training_ns,node_counts,tree_depths");
        }

        private void write(TransactionRow row) {
            SelectedProtocol twoPc = SelectedProtocol.TWO_PC;
            SelectedProtocol abort = SelectedProtocol.TWO_PC_PRESUMED_ABORT;
            SelectedProtocol commit = SelectedProtocol.TWO_PC_PRESUMED_COMMIT;
            writer.println(csv(
                    row.experimentId(), row.seed(), row.transactionIndex(),
                    row.specification().specId(), row.phase(),
                    row.specification().participantWarehouseIds(), row.features(), row.expectedOutcome(),
                    row.prediction().completion().predictedClass(),
                    row.prediction().latency().predictedClass(),
                    row.prediction().redo().predictedClass(),
                    row.predictions().get(twoPc), row.predictions().get(abort),
                    row.predictions().get(commit), row.selectionScores().get(twoPc),
                    row.selectionScores().get(abort), row.selectionScores().get(commit),
                    row.selections().get(Policy.ADAPTIVE),
                    row.selections().get(Policy.STATIC_HEURISTIC),
                    row.selections().get(Policy.RANDOM), row.paired().oracleProtocol(),
                    cost(row, twoPc), cost(row, abort), cost(row, commit),
                    regret(row, Policy.ADAPTIVE), regret(row, Policy.STATIC_HEURISTIC),
                    regret(row, Policy.RANDOM), row.paired().regret(twoPc),
                    row.paired().regret(abort), row.paired().regret(commit),
                    trial(row, twoPc).outcome(), trial(row, abort).outcome(),
                    trial(row, commit).outcome(), latency(row, twoPc), latency(row, abort),
                    latency(row, commit), redo(row, twoPc), redo(row, abort), redo(row, commit),
                    trial(row, twoPc).messageCount(), trial(row, abort).messageCount(),
                    trial(row, commit).messageCount(), trial(row, twoPc).totalProtocolLogWrites(),
                    trial(row, abort).totalProtocolLogWrites(), trial(row, commit).totalProtocolLogWrites(),
                    row.paired().costs().get(twoPc).valid(), row.paired().costs().get(abort).valid(),
                    row.paired().costs().get(commit).valid(), row.inferenceNanos(), row.trainingNanos(),
                    row.nodeCounts(), row.depths()
            ));
            writer.flush();
        }

        private static double cost(TransactionRow row, SelectedProtocol protocol) {
            return row.paired().costs().get(protocol).total();
        }

        private static double regret(TransactionRow row, Policy policy) {
            return row.paired().regret(row.selections().get(policy));
        }

        private static TransactionTrialRecord trial(TransactionRow row, SelectedProtocol protocol) {
            return row.paired().trials().get(protocol);
        }

        private static long latency(TransactionRow row, SelectedProtocol protocol) {
            return trial(row, protocol).totalLatency().toMillis();
        }

        private static String redo(TransactionRow row, SelectedProtocol protocol) {
            OptionalLong redo = trial(row, protocol).totalRedoBytes();
            return redo.isPresent() ? Long.toString(redo.getAsLong()) : "";
        }

        @Override
        public void close() {
            writer.close();
        }
    }

    private record Config(
            String experimentId,
            Path outputDirectory,
            List<Long> seeds,
            int warmupTransactions,
            double warmupAbortRate,
            int phaseLength,
            int latencySpikeMs,
            double abortSpikeRate,
            int historyWindowSize,
            int heuristicWindowSize,
            int adaptationStreak,
            long timeoutMs,
            OutcomeThresholds thresholds,
            CategoricalPolicyWeights selectionWeights,
            ProtocolCostWeights costWeights
    ) {
        private static Config fromEnvironment() {
            String experimentId = env("ACP_DEFENSIBLE_EXPERIMENT_ID",
                    "defensible-" + DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss", Locale.ROOT)
                            .withZone(java.time.ZoneOffset.UTC).format(Instant.now()));
            Path output = Path.of(env(
                    "ACP_DEFENSIBLE_OUTPUT", "build/experiments/" + experimentId
            ));
            return new Config(
                    experimentId,
                    output,
                    seeds(env("ACP_DEFENSIBLE_SEEDS", "101,102,103,104,105,106,107,108,109,110")),
                    positiveInt("ACP_DEFENSIBLE_WARMUP", 100),
                    probability("ACP_DEFENSIBLE_WARMUP_ABORT_RATE", 0.35),
                    positiveInt("ACP_DEFENSIBLE_PHASE_LENGTH", 1_000),
                    nonNegativeInt("ACP_DEFENSIBLE_LATENCY_SPIKE_MS", 50),
                    probability("ACP_DEFENSIBLE_ABORT_SPIKE_RATE", 0.70),
                    positiveInt("ACP_DEFENSIBLE_HISTORY_WINDOW_SIZE", 30),
                    positiveInt("ACP_DEFENSIBLE_HEURISTIC_WINDOW_SIZE", 30),
                    positiveInt("ACP_DEFENSIBLE_ADAPTATION_STREAK", 20),
                    positiveLong("ACP_DEFENSIBLE_TIMEOUT_MS", 60_000L),
                    new OutcomeThresholds(
                            nonNegativeLong("ACP_DEFENSIBLE_LATENCY_LOW_MS", 25L),
                            nonNegativeLong("ACP_DEFENSIBLE_LATENCY_MEDIUM_MS", 75L),
                            nonNegativeLong("ACP_DEFENSIBLE_REDO_LOW_BYTES", 2_000L),
                            nonNegativeLong("ACP_DEFENSIBLE_REDO_MEDIUM_BYTES", 10_000L),
                            0L, 0L
                    ),
                    new CategoricalPolicyWeights(
                            nonNegativeDouble("ACP_DEFENSIBLE_COMPLETION_WEIGHT", 1.0),
                            nonNegativeDouble("ACP_DEFENSIBLE_LATENCY_CLASS_WEIGHT", 1.0),
                            nonNegativeDouble("ACP_DEFENSIBLE_REDO_CLASS_WEIGHT", 1.0),
                            nonNegativeDouble("ACP_DEFENSIBLE_SELECTION_TIMEOUT_PENALTY", 10.0)
                    ),
                    new ProtocolCostWeights(
                            nonNegativeDouble("ACP_DEFENSIBLE_COST_LATENCY_WEIGHT", 1.0),
                            nonNegativeDouble("ACP_DEFENSIBLE_COST_REDO_WEIGHT", 1.0),
                            nonNegativeDouble("ACP_DEFENSIBLE_COST_MESSAGE_WEIGHT", 1.0),
                            nonNegativeDouble("ACP_DEFENSIBLE_COST_LOG_WEIGHT", 1.0),
                            positiveDouble("ACP_DEFENSIBLE_COST_LATENCY_SCALE_MS", 100.0),
                            positiveDouble("ACP_DEFENSIBLE_COST_REDO_SCALE_BYTES", 10_000.0),
                            positiveDouble("ACP_DEFENSIBLE_COST_MESSAGE_SCALE", 10.0),
                            positiveDouble("ACP_DEFENSIBLE_COST_LOG_SCALE", 10.0),
                            nonNegativeDouble("ACP_DEFENSIBLE_COST_TIMEOUT_PENALTY", 100.0),
                            nonNegativeDouble("ACP_DEFENSIBLE_COST_FAILURE_PENALTY", 1_000.0)
                    )
            );
        }

        private static List<Long> seeds(String value) {
            List<Long> result = Arrays.stream(value.split(","))
                    .map(String::trim).filter(item -> !item.isEmpty()).map(Long::parseLong).toList();
            if (result.isEmpty()) throw new IllegalArgumentException("at least one seed is required");
            if (result.stream().distinct().count() != result.size()) {
                throw new IllegalArgumentException("seeds must be unique");
            }
            return result;
        }

        private static String env(String name, String defaultValue) {
            return MariaDbTpccClusterMain.envOrDefault(name, defaultValue);
        }

        private static int positiveInt(String name, int defaultValue) {
            return Math.toIntExact(positiveLong(name, defaultValue));
        }

        private static long positiveLong(String name, long defaultValue) {
            long value = Long.parseLong(env(name, Long.toString(defaultValue)));
            if (value < 1L) throw new IllegalArgumentException(name + " must be at least 1");
            return value;
        }

        private static int nonNegativeInt(String name, int defaultValue) {
            int value = Integer.parseInt(env(name, Integer.toString(defaultValue)));
            if (value < 0) throw new IllegalArgumentException(name + " must not be negative");
            return value;
        }

        private static long nonNegativeLong(String name, long defaultValue) {
            long value = Long.parseLong(env(name, Long.toString(defaultValue)));
            if (value < 0L) throw new IllegalArgumentException(name + " must not be negative");
            return value;
        }

        private static double positiveDouble(String name, double defaultValue) {
            double value = Double.parseDouble(env(name, Double.toString(defaultValue)));
            if (!Double.isFinite(value) || value <= 0.0) {
                throw new IllegalArgumentException(name + " must be finite and positive");
            }
            return value;
        }

        private static double nonNegativeDouble(String name, double defaultValue) {
            double value = Double.parseDouble(env(name, Double.toString(defaultValue)));
            if (!Double.isFinite(value) || value < 0.0) {
                throw new IllegalArgumentException(name + " must be finite and non-negative");
            }
            return value;
        }

        private static double probability(String name, double defaultValue) {
            double value = Double.parseDouble(env(name, Double.toString(defaultValue)));
            if (!Double.isFinite(value) || value < 0.0 || value > 1.0) {
                throw new IllegalArgumentException(name + " must be between 0 and 1");
            }
            return value;
        }
    }
}
