package ufc.victor;

import ufc.victor.db.mariadb.MariaDbTpccNewOrderPlanner;
import ufc.victor.db.mariadb.MariaDbTpccTransactionRunner;
import ufc.victor.db.mariadb.MariaDbWalMetricsCollector;
import ufc.victor.db.mariadb.MariaDbXaConfig;
import ufc.victor.db.mariadb.TpccNewOrderSpec;
import ufc.victor.db.mariadb.TpccNewOrderSpecFactory;
import ufc.victor.db.mariadb.TpccNewOrderFeatureExtractor;
import ufc.victor.db.mariadb.WarehouseRouter;
import ufc.victor.experiment.AlpsFeatures;
import ufc.victor.experiment.AlpsFeatureProfile;
import ufc.victor.experiment.RollingMetricsRegistry;
import ufc.victor.experiment.TransactionOutcomeClass;
import ufc.victor.experiment.TransactionOutcomeLabeler;
import ufc.victor.experiment.TransactionTrialRecord;
import ufc.victor.experiment.VfdtProtocolSwitchingPolicy;
import ufc.victor.protocol.SelectedProtocol;

import java.sql.SQLException;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Prequential ALPS experiment backed by real MariaDB XA New-Order transactions. */
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
        int examples = MariaDbTpccClusterMain.positiveIntEnv("ACP_ALPS_EXAMPLES", 30);
        int observationWindowSize = MariaDbTpccClusterMain.positiveIntEnv("ACP_ALPS_WINDOW_SIZE", 3);
        int historyWindowSize = MariaDbTpccClusterMain.positiveIntEnv("ACP_ALPS_HISTORY_WINDOW_SIZE", 30);
        long timeoutMs = MariaDbTpccClusterMain.positiveLongEnv("ACP_ALPS_TIMEOUT_MS", 60_000L);
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
        AlpsFeatureProfile featureProfile = AlpsFeatureProfile.valueOf(
                MariaDbTpccClusterMain.envOrDefault("ACP_ALPS_FEATURE_PROFILE", "GENERIC").toUpperCase()
        );
        Map<Integer, Double> abortProbabilities = abortProbabilities();

        VFDTTransactionOutcomePredictor model = new VFDTTransactionOutcomePredictor(
                List.of("P1", "P2", "P3", "P4"),
                List.of(TpccNewOrderFeatureExtractor.TRANSACTION_TYPE),
                TpccNewOrderFeatureExtractor.WORKLOAD_FEATURE_NAMES,
                featureProfile
        );
        TransactionOutcomeLabeler labeler = new TransactionOutcomeLabeler(0.25, 0.60);
        VfdtProtocolSwitchingPolicy policy = new VfdtProtocolSwitchingPolicy();
        Map<TransactionOutcomeClass, Integer> labels = new EnumMap<>(TransactionOutcomeClass.class);
        Map<SelectedProtocol, Integer> selections = new EnumMap<>(SelectedProtocol.class);

        int correct = 0;
        long totalTransactions = 0L;
        long totalCommitted = 0L;
        long totalLatencyMs = 0L;
        long totalMessages = 0L;
        long totalLogWrites = 0L;

        System.out.println("=== ALPS / REAL MARIADB XA PREQUENTIAL EXPERIMENT ===");
        System.out.println("examples=" + examples
                + ", observationWindowSize=" + observationWindowSize
                + ", historyWindowSize=" + historyWindowSize
                + ", featureProfile=" + featureProfile
                + ", abortProbabilities=" + abortProbabilities);
        System.out.println("vfdtFeatures=" + model.getFeatureNames());

        int transactionIndex = 0;
        for (int example = 0; example < examples; example++) {
            List<Integer> participantNumbers = PARTICIPANT_SETS.get(example % PARTICIPANT_SETS.size());
            TpccNewOrderSpec firstSpec = TpccNewOrderSpecFactory.controlled(
                    participantNumbers, transactionIndex
            );
            AlpsFeatures features = history.featuresFor(featureExtractor.extract(firstSpec));
            TransactionOutcomeClass prediction = model.predict(features);
            SelectedProtocol selectedProtocol = policy.select(prediction);

            int committedInWindow = 0;
            long windowLatencyMs = 0L;
            long windowMessages = 0L;
            long windowLogWrites = 0L;
            for (int offset = 0; offset < observationWindowSize; offset++) {
                TpccNewOrderSpec spec = offset == 0
                        ? firstSpec
                        : TpccNewOrderSpecFactory.controlled(participantNumbers, transactionIndex);
                Map<Integer, Boolean> votes = votes(
                        participantNumbers, abortProbabilities, example, offset
                );

                TransactionTrialRecord trial = runner.run(spec, selectedProtocol, votes);
                history.record(trial);
                transactionIndex++;

                committedInWindow += trial.committed() ? 1 : 0;
                windowLatencyMs += trial.totalLatency().toMillis();
                windowMessages += trial.messageCount();
                windowLogWrites += trial.totalProtocolLogWrites();
            }

            double commitRate = committedInWindow / (double) observationWindowSize;
            double abortRate = 1.0 - commitRate;
            TransactionOutcomeClass observed = labeler.label(abortRate);
            if (prediction == observed) correct++;
            model.train(features, observed);
            labels.merge(observed, 1, Integer::sum);
            selections.merge(selectedProtocol, 1, Integer::sum);

            totalTransactions += observationWindowSize;
            totalCommitted += committedInWindow;
            totalLatencyMs += windowLatencyMs;
            totalMessages += windowMessages;
            totalLogWrites += windowLogWrites;

            System.out.println("example=" + (example + 1)
                    + ", participants=" + features.participantNodeIds()
                    + ", historicalSamples=" + features.globalHistory().sampleCount()
                    + ", historicalRedoBytes=" + features.globalHistory().averageRedoBytes()
                    + ", predicted=" + prediction
                    + ", selected=" + selectedProtocol
                    + ", observed=" + observed
                    + ", commitRate=" + commitRate
                    + ", avgLatencyMs=" + (windowLatencyMs / (double) observationWindowSize)
                    + ", avgMessages=" + (windowMessages / (double) observationWindowSize)
                    + ", avgProtocolLogWrites=" + (windowLogWrites / (double) observationWindowSize));
        }

        System.out.println("\n=== ALPS REAL-DATABASE SUMMARY ===");
        System.out.println("contextAccuracy=" + (correct / (double) examples)
                + ", labels=" + labels + ", selections=" + selections);
        System.out.println("transactions=" + totalTransactions
                + ", commitRate=" + (totalCommitted / (double) totalTransactions)
                + ", avgLatencyMs=" + (totalLatencyMs / (double) totalTransactions)
                + ", avgMessages=" + (totalMessages / (double) totalTransactions)
                + ", avgProtocolLogWrites=" + (totalLogWrites / (double) totalTransactions));
        System.out.println("model=VFDT, nodes=" + model.getNodeCount() + ", depth=" + model.getTreeDepth());
        System.out.println(model.getLearnedRules());
    }

    private static Map<Integer, Boolean> votes(
            List<Integer> participantNumbers,
            Map<Integer, Double> abortProbabilities,
            int example,
            int offset
    ) {
        Map<Integer, Boolean> votes = new LinkedHashMap<>();
        for (int nodeNumber : participantNumbers) {
            long mixed = 31L * (example + 1L) + 17L * (offset + 1L) + 13L * nodeNumber;
            double sample = Math.floorMod(mixed * 1_103_515_245L + 12_345L, 10_000L) / 10_000.0;
            votes.put(nodeNumber, sample >= abortProbabilities.get(nodeNumber));
        }
        return Map.copyOf(votes);
    }

    private static Map<Integer, Double> abortProbabilities() {
        Map<Integer, Double> probabilities = new LinkedHashMap<>();
        for (int node = 1; node <= 4; node++) {
            double probability = Double.parseDouble(MariaDbTpccClusterMain.envOrDefault(
                    "ACP_ALPS_P" + node + "_ABORT_RATE", "0.0"
            ));
            if (probability < 0.0 || probability > 1.0) {
                throw new IllegalArgumentException("Abort probability must be between 0 and 1 for P" + node);
            }
            probabilities.put(node, probability);
        }
        return Map.copyOf(probabilities);
    }
}
