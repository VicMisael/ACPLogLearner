package ufc.victor.experiment;

import ufc.victor.LogFeatures;
import ufc.victor.VFDTTransactionOutcomePredictor;
import ufc.victor.protocol.SelectedProtocol;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ExperimentRunner {

    private static final String COORDINATOR_NODE_ID = "coord";
    private static final SelectedProtocol OBSERVATION_PROTOCOL = SelectedProtocol.TWO_PC;
    private static final long DEFAULT_TIMEOUT_MS = 5_000L;
    private static final int TIMING_SCALE_DIVISOR = 4;
    private static final int RUNS_PER_TEMPLATE = 12;
    private static final int TRANSACTIONS_PER_OBSERVATION = 5;
    private static final double LIKELY_COMMIT_ABORT_THRESHOLD = 0.25;
    private static final double LIKELY_ABORT_THRESHOLD = 0.60;
    private static final long STREAM_SHUFFLE_SEED = 2_026_041_4L;

    public static void main(String[] args) throws InterruptedException {
        NodeCatalog nodeCatalog = registeredNodeCatalog();
        List<Scenario> onlineStream = buildOnlineStream(registeredScenarios());

        DistributedSimulation simulation = new DistributedSimulation();
        ParticipantSetFeatureExtractor featureExtractor = new ParticipantSetFeatureExtractor();
        TransactionOutcomeLabeler outcomeLabeler = new TransactionOutcomeLabeler(
                LIKELY_COMMIT_ABORT_THRESHOLD,
                LIKELY_ABORT_THRESHOLD
        );
        VFDTTransactionOutcomePredictor predictor =
                new VFDTTransactionOutcomePredictor(knownParticipantNodeIds(nodeCatalog));
        OnlineSummary onlineSummary = runOnlineStream(
                onlineStream,
                nodeCatalog,
                simulation,
                featureExtractor,
                outcomeLabeler,
                predictor
        );

        System.out.println("\nOnline Evaluation:");
        System.out.println(
                "examples=" + onlineSummary.examples()
                        + ", outcomeAccuracy=" + onlineSummary.outcomeAccuracy()
                        + ", avgWindowCommitRate=" + onlineSummary.averageWindowCommitRate()
                        + ", avgWindowAbortRate=" + onlineSummary.averageWindowAbortRate()
                        + ", labelCounts=" + onlineSummary.labelCounts()
        );
        System.out.println("\nVFDT stats:");
        System.out.println("nodes=" + predictor.getNodeCount() + ", depth=" + predictor.getTreeDepth());
        System.out.println("\nLearned VFDT rules:");
        System.out.println(predictor.getLearnedRules());
    }

    private static List<String> knownParticipantNodeIds(NodeCatalog nodeCatalog) {
        return nodeCatalog.nodeIds().stream()
                .filter(nodeId -> !COORDINATOR_NODE_ID.equals(nodeId))
                .sorted()
                .toList();
    }

    private static NodeCatalog registeredNodeCatalog() {
        return new NodeCatalog(List.of(
                profile(COORDINATOR_NODE_ID, 5, 15, 5, 0.0),
                profile("n1", 4, 8, 6, 0.01),
                profile("n2", 6, 12, 8, 0.03),
                profile("n3", 12, 24, 18, 0.08),
                profile("n4", 10, 20, 16, 0.28),
                profile("n5", 22, 42, 34, 0.25),
                profile("n6", 55, 110, 72, 0.12),
                profile("n7", 3, 6, 5, 0.00),
                profile("n8", 38, 72, 96, 0.10),
                profile("n9", 16, 32, 18, 0.62),
                profile("n10", 8, 16, 12, 0.02),
                profile("n11", 70, 140, 88, 0.04),
                profile("n12", 7, 14, 9, 0.48),
                profile("n13", 2, 4, 3, 0.00),
                profile("n14", 85, 160, 120, 0.01),
                profile("n15", 18, 36, 22, 0.72),
                profile("n16", 28, 56, 44, 0.18)
        ));
    }

    private static NodeProfile profile(
            String nodeId,
            int minLatencyMs,
            int maxLatencyMs,
            int prepareDelayMs,
            double abortProbability
    ) {
        return new NodeProfile(
                nodeId,
                scaledDelay(minLatencyMs),
                scaledDelay(maxLatencyMs),
                scaledDelay(prepareDelayMs),
                abortProbability
        );
    }

    private static int scaledDelay(int delayMs) {
        return Math.max(1, delayMs / TIMING_SCALE_DIVISOR);
    }

    private static List<Scenario> registeredScenarios() {
        List<ScenarioTemplate> templates = List.of(
                template("alpha", List.of("n1", "n7")),
                template("beta", List.of("n4", "n9")),
                template("gamma", List.of("n1", "n6", "n11")),
                template("delta", List.of("n6", "n11")),
                template("epsilon", List.of("n2", "n4", "n5", "n9")),
                template("zeta", List.of("n1", "n7", "n10")),
                template("eta", List.of("n2", "n3", "n6", "n11")),
                template("theta", List.of("n4", "n5", "n8", "n9", "n12")),
                template("iota", List.of("n1", "n2", "n3", "n7", "n10")),
                template("kappa", List.of("n3", "n6", "n10", "n11")),
                template("lambda", List.of("n3", "n5", "n6", "n8", "n11")),
                template("mu", List.of("n1", "n4", "n6", "n7", "n11", "n12")),
                template("nu", List.of("n13", "n14")),
                template("xi", List.of("n13", "n15")),
                template("omicron", List.of("n7", "n10", "n13")),
                template("pi", List.of("n8", "n11", "n14")),
                template("rho", List.of("n12", "n15", "n16")),
                template("sigma", List.of("n2", "n13", "n14", "n16")),
                template("tau", List.of("n4", "n9", "n12", "n15")),
                template("upsilon", List.of("n1", "n7", "n10", "n13", "n16")),
                template("phi", List.of("n3", "n8", "n11", "n14", "n16")),
                template("chi", List.of("n4", "n5", "n9", "n12", "n15")),
                template("psi", List.of("n1", "n6", "n11", "n14", "n16", "n13")),
                template("omega", List.of("n2", "n4", "n9", "n12", "n15", "n16"))
        );

        List<Scenario> scenarios = new ArrayList<>(templates.size() * RUNS_PER_TEMPLATE);
        for (int templateIndex = 0; templateIndex < templates.size(); templateIndex++) {
            ScenarioTemplate template = templates.get(templateIndex);
            for (int run = 1; run <= RUNS_PER_TEMPLATE; run++) {
                scenarios.add(scenario(
                        template.familyId() + "_run" + run,
                        template.participantNodeIds(),
                        11_000L + ((templateIndex + 1L) * 1_000L) + run
                ));
            }
        }
        return List.copyOf(scenarios);
    }

    private static ScenarioTemplate template(String familyId, List<String> participantNodeIds) {
        return new ScenarioTemplate(familyId, participantNodeIds);
    }

    private static Scenario scenario(
            String id,
            List<String> participantNodeIds,
            long seed
    ) {
        return new Scenario(
                id,
                COORDINATOR_NODE_ID,
                participantNodeIds,
                seed,
                DEFAULT_TIMEOUT_MS
        );
    }

    private static OnlineSummary runOnlineStream(
            List<Scenario> scenarios,
            NodeCatalog nodeCatalog,
            DistributedSimulation simulation,
            ParticipantSetFeatureExtractor featureExtractor,
            TransactionOutcomeLabeler outcomeLabeler,
            VFDTTransactionOutcomePredictor predictor
    ) throws InterruptedException {
        int examples = 0;
        int outcomeCorrect = 0;
        long totalCommittedTransactions = 0L;
        long totalTransactions = 0L;
        Map<TransactionOutcomeClass, Integer> labelCounts = new EnumMap<>(TransactionOutcomeClass.class);

        for (Scenario scenario : scenarios) {
            LogFeatures features = featureExtractor.extract(scenario);
            TransactionOutcomeClass predictedOutcome = predictor.predict(features);
            ObservationWindowSummary windowSummary = runObservationWindow(scenario, nodeCatalog, simulation);
            TransactionOutcomeClass actualOutcome = outcomeLabeler.label(windowSummary.abortRate());

            if (predictedOutcome == actualOutcome) {
                outcomeCorrect++;
            }
            examples++;
            totalCommittedTransactions += windowSummary.committedTransactions();
            totalTransactions += windowSummary.totalTransactions();
            labelCounts.merge(actualOutcome, 1, Integer::sum);

            predictor.train(features, actualOutcome);

            System.out.println(
                    "Online example=" + examples
                            + " scenario=" + scenario.id()
                            + " participantSet=" + scenario.participantSetKey()
                            + " predictedOutcome=" + predictedOutcome
                            + " actualOutcome=" + actualOutcome
                            + " windowAbortRate=" + windowSummary.abortRate()
                            + " windowCommitRate=" + windowSummary.commitRate()
                            + " features={" + features.explanationSummary() + "}"
                            + " avgMetrics={latencyMs=" + windowSummary.averageMetrics().latencyMs()
                            + ", committed=" + windowSummary.averageMetrics().committed()
                            + ", messages=" + windowSummary.averageMetrics().messageCount()
                            + ", logWrites=" + windowSummary.averageMetrics().totalLogWrites()
                            + ", blockingMs=" + windowSummary.averageMetrics().blockingTimeMs()
                            + "}"
            );
        }

        if (examples == 0 || totalTransactions == 0L) {
            return new OnlineSummary(0, 0.0, 0.0, 0.0, Map.of());
        }

        double averageWindowCommitRate = totalCommittedTransactions / (double) totalTransactions;
        return new OnlineSummary(
                examples,
                outcomeCorrect / (double) examples,
                averageWindowCommitRate,
                1.0 - averageWindowCommitRate,
                Map.copyOf(labelCounts)
        );
    }

    private static ObservationWindowSummary runObservationWindow(
            Scenario scenario,
            NodeCatalog nodeCatalog,
            DistributedSimulation simulation
    ) throws InterruptedException {
        List<SimulationResult> results = new ArrayList<>(TRANSACTIONS_PER_OBSERVATION);
        long committedTransactions = 0L;

        for (int txIndex = 0; txIndex < TRANSACTIONS_PER_OBSERVATION; txIndex++) {
            TransactionPlan transactionPlan = buildTransactionPlan(scenario, nodeCatalog, txIndex);
            SimulationResult result = simulation.run(scenario, nodeCatalog, OBSERVATION_PROTOCOL, transactionPlan);
            if (result.metrics().committed()) {
                committedTransactions++;
            }
            results.add(result);
        }

        TxMetrics averageMetrics = averageMetrics(results);
        return new ObservationWindowSummary(
                results.size(),
                committedTransactions,
                averageMetrics
        );
    }

    private static TransactionPlan buildTransactionPlan(
            Scenario scenario,
            NodeCatalog nodeCatalog,
            int txIndex
    ) {
        List<NodeProfile> participants = nodeCatalog.resolve(scenario.participantNodeIds());
        Random random = new Random((scenario.randomSeed() * 31L) + txIndex);
        List<Boolean> participantShouldCommit = new ArrayList<>(participants.size());
        for (NodeProfile participant : participants) {
            participantShouldCommit.add(random.nextDouble() >= participant.abortProbability());
        }
        return new TransactionPlan(List.copyOf(participantShouldCommit));
    }

    private static TxMetrics averageMetrics(List<SimulationResult> results) {
        long totalLatency = 0L;
        long totalMessages = 0L;
        long totalLogWrites = 0L;
        long totalBlocking = 0L;
        int committedCount = 0;

        for (SimulationResult result : results) {
            totalLatency += result.metrics().latencyMs();
            totalMessages += result.metrics().messageCount();
            totalLogWrites += result.metrics().totalLogWrites();
            totalBlocking += result.metrics().blockingTimeMs();
            if (result.metrics().committed()) {
                committedCount++;
            }
        }

        int size = results.size();
        return new TxMetrics(
                totalLatency / size,
                committedCount * 2 >= size,
                totalMessages / size,
                totalLogWrites / size,
                totalBlocking / size
        );
    }

    private static List<Scenario> buildOnlineStream(List<Scenario> scenarios) {
        List<Scenario> stream = new ArrayList<>(scenarios);
        shuffleInPlace(stream, new Random(STREAM_SHUFFLE_SEED));
        return List.copyOf(stream);
    }

    private static <T> void shuffleInPlace(List<T> items, Random random) {
        for (int i = items.size() - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            T tmp = items.get(i);
            items.set(i, items.get(j));
            items.set(j, tmp);
        }
    }

    private record ScenarioTemplate(
            String familyId,
            List<String> participantNodeIds
    ) {
    }

    private record ObservationWindowSummary(
            long totalTransactions,
            long committedTransactions,
            TxMetrics averageMetrics
    ) {
        private double commitRate() {
            return committedTransactions / (double) totalTransactions;
        }

        private double abortRate() {
            return 1.0 - commitRate();
        }
    }

    private record OnlineSummary(
            int examples,
            double outcomeAccuracy,
            double averageWindowCommitRate,
            double averageWindowAbortRate,
            Map<TransactionOutcomeClass, Integer> labelCounts
    ) {
    }
}
