package ufc.victor;

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
import ufc.victor.experiment.CategoricalOutcomePrediction;
import ufc.victor.experiment.CategoricalOutcomeLabeler;
import ufc.victor.experiment.CategoricalOutcomeObservation;
import ufc.victor.experiment.CategoricalPolicyWeights;
import ufc.victor.experiment.CategoricalProtocolSelectionPolicy;
import ufc.victor.experiment.CompletionCategory;
import ufc.victor.experiment.CostCategory;
import ufc.victor.experiment.OutcomeThresholds;
import ufc.victor.experiment.OnlineExperimentSchedule;
import ufc.victor.experiment.RollingMetricsRegistry;
import ufc.victor.experiment.TransactionTrialRecord;
import ufc.victor.protocol.SelectedProtocol;

import java.sql.SQLException;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Prequential multi-head VFDT experiment backed by real MariaDB XA transactions. */
public final class MariaDbAlpsMain {

    private static final List<List<Integer>> PARTICIPANT_SETS = List.of(
            List.of(1), List.of(2), List.of(3), List.of(4),
            List.of(1, 2), List.of(1, 3), List.of(1, 4),
            List.of(2, 3), List.of(2, 4), List.of(3, 4),
            List.of(1, 2, 3), List.of(1, 2, 4), List.of(1, 3, 4),
            List.of(2, 3, 4), List.of(1, 2, 3, 4)
    );

    private MariaDbAlpsMain() {
    }

    public static void main(String[] args) throws InterruptedException, SQLException {
        int examples = MariaDbTpccClusterMain.positiveIntEnv("ACP_ALPS_EXAMPLES", 90);
        int historyWindowSize = MariaDbTpccClusterMain.positiveIntEnv(
                "ACP_ALPS_HISTORY_WINDOW_SIZE", 30
        );
        int labelWindowSize = MariaDbTpccClusterMain.positiveIntEnv(
                "ACP_ALPS_LABEL_WINDOW_SIZE", 30
        );
        long timeoutMs = MariaDbTpccClusterMain.positiveLongEnv(
                "ACP_ALPS_TIMEOUT_MS", 60_000L
        );
        AlpsFeatureProfile featureProfile = AlpsFeatureProfile.valueOf(
                env("ACP_ALPS_FEATURE_PROFILE", "GENERIC").toUpperCase()
        );
        Map<Integer, MariaDbXaConfig> configs = MariaDbTpccClusterMain.configs();
        MariaDbTpccNewOrderPlanner planner = new MariaDbTpccNewOrderPlanner(
                new WarehouseRouter(configs)
        );
        MariaDbTpccTransactionRunner runner = new MariaDbTpccTransactionRunner(
                planner,
                new MariaDbWalMetricsCollector(MariaDbTpccClusterMain.redoConfigs(configs)),
                timeoutMs
        );
        RollingMetricsRegistry history = new RollingMetricsRegistry(historyWindowSize);
        TpccNewOrderFeatureExtractor featureExtractor = new TpccNewOrderFeatureExtractor();
        CategoricalOutcomeLabeler labeler = new CategoricalOutcomeLabeler(
                labelWindowSize,
                nonNegativeLongEnv("ACP_ALPS_LATENCY_LOW_MS", 25L),
                nonNegativeLongEnv("ACP_ALPS_LATENCY_MEDIUM_MS", 75L),
                nonNegativeLongEnv("ACP_ALPS_REDO_LOW_BYTES", 2_000L),
                nonNegativeLongEnv("ACP_ALPS_REDO_MEDIUM_BYTES", 10_000L)
        );
        VfdtCategoricalOutcomePredictor predictor = new VfdtCategoricalOutcomePredictor(
                List.of("P1", "P2", "P3", "P4"),
                List.of(TpccNewOrderFeatureExtractor.TRANSACTION_TYPE),
                TpccNewOrderFeatureExtractor.WORKLOAD_FEATURE_NAMES,
                featureProfile
        );
        CategoricalPolicyWeights weights = new CategoricalPolicyWeights(
                nonNegativeDoubleEnv("ACP_ALPS_COMPLETION_WEIGHT", 1.0),
                nonNegativeDoubleEnv("ACP_ALPS_LATENCY_WEIGHT", 1.0),
                nonNegativeDoubleEnv("ACP_ALPS_REDO_WEIGHT", 1.0),
                nonNegativeDoubleEnv("ACP_ALPS_TIMEOUT_PENALTY", 10.0)
        );
        CategoricalProtocolSelectionPolicy policy = new CategoricalProtocolSelectionPolicy(weights);
        OnlineExperimentSchedule schedule = new OnlineExperimentSchedule(
                OnlineExperimentSchedule.Mode.valueOf(
                        env("ACP_ALPS_SCENARIO", "STABLE").toUpperCase()
                ),
                MariaDbTpccClusterMain.positiveIntEnv("ACP_ALPS_PHASE_LENGTH", 30),
                nonNegativeIntEnv("ACP_ALPS_LATENCY_SPIKE_MS", 75),
                probabilityEnv("ACP_ALPS_ABORT_SPIKE_RATE", 0.70)
        );
        Map<Integer, Double> abortProbabilities = abortProbabilities();
        Evaluation evaluation = new Evaluation();
        Map<String, Evaluation> phaseEvaluations = new LinkedHashMap<>();

        System.out.println("=== ALPS / CATEGORICAL MULTI-HEAD VFDT ===");
        System.out.println("examples=" + examples
                + ", historyWindowSize=" + historyWindowSize
                + ", labelWindowSize=" + labelWindowSize
                + ", featureProfile=" + featureProfile
                + ", weights=" + weights
                + ", scenario=" + env("ACP_ALPS_SCENARIO", "STABLE").toUpperCase()
                + ", abortProbabilities=" + abortProbabilities);
        System.out.println("vfdtFeatures=" + predictor.featureNames());

        for (int example = 0; example < examples; example++) {
            List<Integer> participantNumbers = PARTICIPANT_SETS.get(example % PARTICIPANT_SETS.size());
            OnlineExperimentSchedule.Condition condition = schedule.conditionFor(example);
            TpccNewOrderSpec specification = TpccNewOrderSpecFactory.controlled(
                    participantNumbers, example
            );
            AlpsFeatures features = history.featuresFor(featureExtractor.extract(specification));
            OutcomeThresholds thresholds = labeler.thresholds();
            CategoricalOutcomePrediction prediction = predictor.predict(features);
            SelectedProtocol recommendedProtocol = policy.select(prediction);
            SelectedProtocol executedProtocol = SelectedProtocol.TWO_PC;

            TransactionTrialRecord trial = runner.run(
                    specification,
                    executedProtocol,
                    votes(participantNumbers, abortProbabilities, condition, example),
                    condition.injectedNetworkLatencyMs()
            );
            CategoricalOutcomeObservation observation = labeler.label(trial, thresholds);
            evaluation.record(
                    prediction, observation, recommendedProtocol, trial
            );
            phaseEvaluations.computeIfAbsent(condition.phase(), ignored -> new Evaluation())
                    .record(prediction, observation, recommendedProtocol, trial);
            predictor.train(features, observation);
            labeler.record(trial);
            history.record(trial);

            System.out.println("example=" + (example + 1)
                    + ", phase=" + condition.phase()
                    + ", injectedNetworkLatencyMs=" + condition.injectedNetworkLatencyMs()
                    + ", participants=" + features.participantNodeIds()
                    + ", recommended=" + recommendedProtocol
                    + ", executedBaseline=" + executedProtocol
                    + ", predictedCompletion=" + prediction.completion().predictedClass()
                    + ", observedCompletion=" + observation.completion()
                    + ", predictedLatency=" + prediction.latency().predictedClass()
                    + ", observedLatency=" + observation.latency().orElse(null)
                    + ", predictedRedo=" + prediction.redo().predictedClass()
                    + ", observedRedo=" + observation.redo().orElse(null)
                    + ", latencyMs=" + trial.totalLatency().toMillis()
                    + ", redoBytes=" + (trial.totalRedoBytes().isPresent()
                    ? trial.totalRedoBytes().getAsLong() : "unavailable")
                    + ", thresholds=" + thresholds);
        }

        System.out.println("\n=== ALPS MULTI-HEAD SUMMARY ===");
        System.out.println(evaluation.summary());
        phaseEvaluations.forEach((phase, phaseEvaluation) ->
                System.out.println("phase=" + phase + ", " + phaseEvaluation.summary())
        );
        System.out.println("nodes=" + predictor.nodeCounts() + ", depths=" + predictor.treeDepths());
        predictor.learnedRules().forEach((head, rules) ->
                System.out.println("\n[" + head + "]\n" + rules)
        );
    }

    private static Map<Integer, Boolean> votes(
            List<Integer> participantNumbers,
            Map<Integer, Double> abortProbabilities,
            OnlineExperimentSchedule.Condition condition,
            int example
    ) {
        Map<Integer, Boolean> votes = new LinkedHashMap<>();
        for (int nodeNumber : participantNumbers) {
            long mixed = 31L * (example + 1L) + 13L * nodeNumber;
            double sample = Math.floorMod(mixed * 1_103_515_245L + 12_345L, 10_000L) / 10_000.0;
            double abortProbability = Math.max(
                    abortProbabilities.get(nodeNumber), condition.abortRateFloor()
            );
            votes.put(nodeNumber, sample >= abortProbability);
        }
        return Map.copyOf(votes);
    }

    private static Map<Integer, Double> abortProbabilities() {
        Map<Integer, Double> probabilities = new LinkedHashMap<>();
        for (int node = 1; node <= 4; node++) {
            double probability = probabilityEnv("ACP_ALPS_P" + node + "_ABORT_RATE", 0.0);
            probabilities.put(node, probability);
        }
        return Map.copyOf(probabilities);
    }

    private static long nonNegativeLongEnv(String name, long defaultValue) {
        long value = Long.parseLong(env(name, Long.toString(defaultValue)));
        if (value < 0L) throw new IllegalArgumentException(name + " must not be negative");
        return value;
    }

    private static int nonNegativeIntEnv(String name, int defaultValue) {
        int value = Integer.parseInt(env(name, Integer.toString(defaultValue)));
        if (value < 0) throw new IllegalArgumentException(name + " must not be negative");
        return value;
    }

    private static double nonNegativeDoubleEnv(String name, double defaultValue) {
        double value = Double.parseDouble(env(name, Double.toString(defaultValue)));
        if (!Double.isFinite(value) || value < 0.0) {
            throw new IllegalArgumentException(name + " must be finite and non-negative");
        }
        return value;
    }

    private static double probabilityEnv(String name, double defaultValue) {
        double value = Double.parseDouble(env(name, Double.toString(defaultValue)));
        if (!Double.isFinite(value) || value < 0.0 || value > 1.0) {
            throw new IllegalArgumentException(name + " must be between 0 and 1");
        }
        return value;
    }

    private static String env(String name, String defaultValue) {
        return MariaDbTpccClusterMain.envOrDefault(name, defaultValue);
    }

    private static final class Evaluation {
        private int examples;
        private int completionCorrect;
        private int latencyExamples;
        private int latencyCorrect;
        private int redoExamples;
        private int redoCorrect;
        private long totalLatencyMs;
        private long totalRedoBytes;
        private int redoMetricExamples;
        private final Map<CompletionCategory, Integer> completionLabels =
                new EnumMap<>(CompletionCategory.class);
        private final Map<CostCategory, Integer> latencyLabels = new EnumMap<>(CostCategory.class);
        private final Map<CostCategory, Integer> redoLabels = new EnumMap<>(CostCategory.class);
        private final Map<SelectedProtocol, Integer> selections = new EnumMap<>(SelectedProtocol.class);

        private void record(
                CategoricalOutcomePrediction prediction,
                CategoricalOutcomeObservation observation,
                SelectedProtocol recommendedProtocol,
                TransactionTrialRecord trial
        ) {
            examples++;
            totalLatencyMs += trial.totalLatency().toMillis();
            trial.totalRedoBytes().ifPresent(value -> {
                totalRedoBytes += value;
                redoMetricExamples++;
            });
            completionCorrect += prediction.completion().predictedClass() == observation.completion() ? 1 : 0;
            completionLabels.merge(observation.completion(), 1, Integer::sum);
            selections.merge(recommendedProtocol, 1, Integer::sum);
            observation.latency().ifPresent(label -> {
                latencyExamples++;
                latencyCorrect += prediction.latency().predictedClass() == label ? 1 : 0;
                latencyLabels.merge(label, 1, Integer::sum);
            });
            observation.redo().ifPresent(label -> {
                redoExamples++;
                redoCorrect += prediction.redo().predictedClass() == label ? 1 : 0;
                redoLabels.merge(label, 1, Integer::sum);
            });
        }

        private String summary() {
            return "examples=" + examples
                    + ", completionAccuracy=" + ratio(completionCorrect, examples)
                    + ", latencyAccuracy=" + ratio(latencyCorrect, latencyExamples)
                    + ", redoAccuracy=" + ratio(redoCorrect, redoExamples)
                    + ", averageLatencyMs=" + ratio(totalLatencyMs, examples)
                    + ", averageRedoBytes=" + ratio(totalRedoBytes, redoMetricExamples)
                    + ", completionLabels=" + completionLabels
                    + ", latencyLabels=" + latencyLabels
                    + ", redoLabels=" + redoLabels
                    + ", recommendations=" + selections;
        }

        private static double ratio(long numerator, int denominator) {
            return denominator == 0 ? Double.NaN : numerator / (double) denominator;
        }
    }
}
