package ufc.victor.experiment;

import ufc.victor.LogFeatures;
import ufc.victor.VFDTProtocolPredictor;
import ufc.victor.protocol.SelectedProtocol;
import ufc.victor.protocol.commom.message.MessageType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ExperimentRunner {

    private static final LogDrivenExperimentBuilder EXPERIMENTS = new LogDrivenExperimentBuilder();
    private static final int TRAINING_EPOCHS = 6;
    private static final long DEFAULT_TIMEOUT_MS = 20_000L;

    public static void main(String[] args) throws InterruptedException {
        List<ProtocolRegistration> protocols = registeredProtocols();
        List<WorkloadRegistration> workloads = registeredWorkloads();
        int splitIndex = Math.max(1, (int) Math.floor(workloads.size() * 0.7));

        List<WorkloadRegistration> trainingWorkloads = List.copyOf(workloads.subList(0, splitIndex));
        List<WorkloadRegistration> testWorkloads = List.copyOf(workloads.subList(splitIndex, workloads.size()));

        DistributedSimulation simulation = new DistributedSimulation();
        List<TrainingExample> trainingDataset = collectDataset("train", trainingWorkloads, protocols, simulation);
        List<TrainingExample> testDataset = collectDataset("test", testWorkloads, protocols, simulation);

        VFDTProtocolPredictor predictor = new VFDTProtocolPredictor(0.0);
        trainPredictor(predictor, trainingDataset);

        List<ProtocolSelector> selectors = registeredSelectors(protocols, predictor);
        for (ProtocolSelector selector : selectors) {
            EvaluationSummary trainingSummary = evaluate(selector, "train", trainingDataset);
            EvaluationSummary testSummary = evaluate(selector, "test", testDataset);
            System.out.println(
                    "Selector=" + selector.name()
                            + " train accuracy=" + trainingSummary.accuracy()
                            + " train avg regret=" + trainingSummary.averageRegret()
                            + " test accuracy=" + testSummary.accuracy()
                            + " test avg regret=" + testSummary.averageRegret()
            );
        }

        System.out.println("\nVFDT stats:");
        System.out.println("nodes=" + predictor.getNodeCount() + ", depth=" + predictor.getTreeDepth());
        System.out.println("\nLearned VFDT rules:");
        System.out.println(predictor.getLearnedRules());
    }

    static double score(TxMetrics metrics) {
        return metrics.latencyMs()
                + (3.0 * metrics.messageCount())
                + (6.0 * metrics.totalLogWrites())
                + (4.0 * metrics.blockingTimeMs());
    }

    private static List<ProtocolRegistration> registeredProtocols() {
        return List.of(
                new ProtocolRegistration("2pc", SelectedProtocol.TWO_PC),
                new ProtocolRegistration("pa", SelectedProtocol.TWO_PC_PRESUMED_ABORT),
                new ProtocolRegistration("pc", SelectedProtocol.TWO_PC_PRESUMED_COMMIT)
        );
    }

    private static List<ProtocolSelector> registeredSelectors(
            List<ProtocolRegistration> protocols,
            VFDTProtocolPredictor predictor
    ) {
        List<ProtocolSelector> selectors = new ArrayList<>();
        selectors.add(new LearnedProtocolSelector("vfdt", predictor));
        for (ProtocolRegistration protocol : protocols) {
            selectors.add(new StaticProtocolSelector("static_" + protocol.id(), protocol.protocol()));
        }
        return selectors;
    }

    private static List<WorkloadRegistration> registeredWorkloads() {
        NodeProfile coordinator = new NodeProfile("coord", 5, 15, 5, 0.0);
        return List.of(
                curatedPresumedCommitWorkload(coordinator),
                curatedPresumedAbortWorkload(coordinator),
                latencySkewWorkload(coordinator),
                messageLossWorkload(coordinator),
                largeRecoveryCommitWorkload(coordinator),
                largeRecoveryAbortWorkload(coordinator)
        );
    }

    private static WorkloadRegistration curatedPresumedCommitWorkload(NodeProfile coordinator) {
        return new WorkloadRegistration(
                "pc_balanced_tail",
                List.of(
                        scenario(
                                "pc_tail_base",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-fast", 8, 18, 16, 0.05),
                                        new NodeProfile("p2-mid", 16, 32, 24, 0.05),
                                        new NodeProfile("p3-tail", 35, 70, 55, 0.05)
                                ),
                                FaultPlan.none(),
                                11_001L
                        ),
                        scenario(
                                "pc_tail_ack_recovery",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-fast", 8, 18, 16, 0.05),
                                        new NodeProfile("p2-mid", 16, 32, 24, 0.05),
                                        new NodeProfile("p3-tail", 35, 70, 55, 0.05)
                                ),
                                new FaultPlan(
                                        Map.of(),
                                        Map.of(MessageType.ACK, 450),
                                        List.of(new CrashSpec("COORD", CrashTrigger.COORDINATOR_AFTER_DECISION, 350))
                                ),
                                11_101L
                        ),
                        scenario(
                                "pc_tail_abort_spike",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-fast", 8, 18, 16, 0.05),
                                        new NodeProfile("p2-mid", 16, 32, 24, 0.05),
                                        new NodeProfile("p3-abort", 35, 70, 55, 0.85)
                                ),
                                FaultPlan.none(),
                                11_201L
                        )
                )
        );
    }

    private static WorkloadRegistration curatedPresumedAbortWorkload(NodeProfile coordinator) {
        return new WorkloadRegistration(
                "pa_balanced_predecision",
                List.of(
                        scenario(
                                "pa_predecision_base",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-fast", 10, 20, 18, 0.05),
                                        new NodeProfile("p2-mid", 18, 34, 26, 0.05),
                                        new NodeProfile("p3-tail", 40, 80, 58, 0.05)
                                ),
                                FaultPlan.none(),
                                12_001L
                        ),
                        scenario(
                                "pa_predecision_loss",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-fast", 10, 20, 18, 0.05),
                                        new NodeProfile("p2-mid", 18, 34, 26, 0.05),
                                        new NodeProfile("p3-tail", 40, 80, 58, 0.05)
                                ),
                                new FaultPlan(
                                        Map.of(MessageType.VOTE_COMMIT, 0.18),
                                        Map.of(MessageType.ACK, 200),
                                        List.of(new CrashSpec("COORD", CrashTrigger.COORDINATOR_BEFORE_DECISION, 300))
                                ),
                                12_101L
                        ),
                        scenario(
                                "pa_predecision_abort_spike",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-fast", 10, 20, 18, 0.05),
                                        new NodeProfile("p2-mid", 18, 34, 26, 0.05),
                                        new NodeProfile("p3-abort", 40, 80, 58, 0.85)
                                ),
                                new FaultPlan(
                                        Map.of(MessageType.VOTE_COMMIT, 0.10),
                                        Map.of(),
                                        List.of()
                                ),
                                12_201L
                        )
                )
        );
    }

    private static WorkloadRegistration latencySkewWorkload(NodeProfile coordinator) {
        return new WorkloadRegistration(
                "latency_skew_commit",
                List.of(
                        scenario(
                                "latency_skew_base",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-fast", 5, 10, 12, 0.08),
                                        new NodeProfile("p2-fast", 5, 12, 14, 0.08),
                                        new NodeProfile("p3-slow", 45, 95, 70, 0.08)
                                ),
                                FaultPlan.none(),
                                13_001L
                        ),
                        scenario(
                                "latency_skew_ack_delay",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-fast", 5, 10, 12, 0.08),
                                        new NodeProfile("p2-fast", 5, 12, 14, 0.08),
                                        new NodeProfile("p3-slow", 45, 95, 70, 0.08)
                                ),
                                new FaultPlan(
                                        Map.of(),
                                        Map.of(MessageType.ACK, 350),
                                        List.of()
                                ),
                                13_101L
                        ),
                        scenario(
                                "latency_skew_participant_crash",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-fast", 5, 10, 12, 0.08),
                                        new NodeProfile("p2-fast", 5, 12, 14, 0.08),
                                        new NodeProfile("p3-slow", 45, 95, 70, 0.08)
                                ),
                                new FaultPlan(
                                        Map.of(),
                                        Map.of(),
                                        List.of(new CrashSpec("P3", CrashTrigger.PARTICIPANT_AFTER_READY, 400))
                                ),
                                13_201L
                        )
                )
        );
    }

    private static WorkloadRegistration messageLossWorkload(NodeProfile coordinator) {
        return new WorkloadRegistration(
                "message_loss_abort",
                List.of(
                        scenario(
                                "message_loss_base",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-mid", 12, 28, 20, 0.15),
                                        new NodeProfile("p2-mid", 18, 34, 28, 0.15),
                                        new NodeProfile("p3-slow", 30, 65, 48, 0.20)
                                ),
                                FaultPlan.none(),
                                14_001L
                        ),
                        scenario(
                                "message_loss_votes",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-mid", 12, 28, 20, 0.15),
                                        new NodeProfile("p2-mid", 18, 34, 28, 0.15),
                                        new NodeProfile("p3-slow", 30, 65, 48, 0.20)
                                ),
                                new FaultPlan(
                                        Map.of(MessageType.VOTE_COMMIT, 0.15),
                                        Map.of(),
                                        List.of()
                                ),
                                14_101L
                        ),
                        scenario(
                                "message_loss_coord_crash",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-mid", 12, 28, 20, 0.15),
                                        new NodeProfile("p2-mid", 18, 34, 28, 0.15),
                                        new NodeProfile("p3-abort", 30, 65, 48, 0.55)
                                ),
                                new FaultPlan(
                                        Map.of(MessageType.VOTE_COMMIT, 0.12),
                                        Map.of(MessageType.ACK, 180),
                                        List.of(new CrashSpec("COORD", CrashTrigger.COORDINATOR_BEFORE_DECISION, 300))
                                ),
                                14_201L
                        )
                )
        );
    }

    private static WorkloadRegistration largeRecoveryCommitWorkload(NodeProfile coordinator) {
        return new WorkloadRegistration(
                "large_commit_recovery",
                List.of(
                        scenario(
                                "large_commit_base",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-fast", 10, 25, 20, 0.08),
                                        new NodeProfile("p2-mid", 20, 45, 28, 0.08),
                                        new NodeProfile("p3-mid", 25, 55, 35, 0.08),
                                        new NodeProfile("p4-slow", 45, 90, 60, 0.08),
                                        new NodeProfile("p5-tail", 65, 120, 85, 0.08)
                                ),
                                FaultPlan.none(),
                                15_001L
                        ),
                        scenario(
                                "large_commit_ack_recovery",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-fast", 10, 25, 20, 0.08),
                                        new NodeProfile("p2-mid", 20, 45, 28, 0.08),
                                        new NodeProfile("p3-mid", 25, 55, 35, 0.08),
                                        new NodeProfile("p4-slow", 45, 90, 60, 0.08),
                                        new NodeProfile("p5-tail", 65, 120, 85, 0.08)
                                ),
                                new FaultPlan(
                                        Map.of(),
                                        Map.of(MessageType.ACK, 250),
                                        List.of(new CrashSpec("COORD", CrashTrigger.COORDINATOR_AFTER_DECISION, 500))
                                ),
                                15_101L
                        ),
                        scenario(
                                "large_commit_abort_spike",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-fast", 10, 25, 20, 0.08),
                                        new NodeProfile("p2-mid", 20, 45, 28, 0.08),
                                        new NodeProfile("p3-mid", 25, 55, 35, 0.08),
                                        new NodeProfile("p4-slow", 45, 90, 60, 0.08),
                                        new NodeProfile("p5-abort", 65, 120, 85, 0.65)
                                ),
                                FaultPlan.none(),
                                15_201L
                        )
                )
        );
    }

    private static WorkloadRegistration largeRecoveryAbortWorkload(NodeProfile coordinator) {
        return new WorkloadRegistration(
                "large_abort_recovery",
                List.of(
                        scenario(
                                "large_abort_base",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-fast", 10, 25, 22, 0.18),
                                        new NodeProfile("p2-mid", 20, 45, 30, 0.20),
                                        new NodeProfile("p3-mid", 25, 55, 38, 0.24),
                                        new NodeProfile("p4-slow", 45, 90, 65, 0.28),
                                        new NodeProfile("p5-tail", 65, 120, 90, 0.30)
                                ),
                                FaultPlan.none(),
                                16_001L
                        ),
                        scenario(
                                "large_abort_votes",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-fast", 10, 25, 22, 0.18),
                                        new NodeProfile("p2-mid", 20, 45, 30, 0.20),
                                        new NodeProfile("p3-mid", 25, 55, 38, 0.24),
                                        new NodeProfile("p4-slow", 45, 90, 65, 0.28),
                                        new NodeProfile("p5-tail", 65, 120, 90, 0.30)
                                ),
                                new FaultPlan(
                                        Map.of(MessageType.VOTE_COMMIT, 0.15),
                                        Map.of(MessageType.ACK, 250),
                                        List.of(new CrashSpec("COORD", CrashTrigger.COORDINATOR_BEFORE_DECISION, 500))
                                ),
                                16_101L
                        ),
                        scenario(
                                "large_abort_participant_crash",
                                coordinator,
                                List.of(
                                        new NodeProfile("p1-fast", 10, 25, 22, 0.18),
                                        new NodeProfile("p2-mid", 20, 45, 30, 0.20),
                                        new NodeProfile("p3-mid", 25, 55, 38, 0.24),
                                        new NodeProfile("p4-slow", 45, 90, 65, 0.28),
                                        new NodeProfile("p5-tail", 65, 120, 90, 0.30)
                                ),
                                new FaultPlan(
                                        Map.of(),
                                        Map.of(),
                                        List.of(new CrashSpec("P4", CrashTrigger.PARTICIPANT_AFTER_READY, 450))
                                ),
                                16_201L
                        )
                )
        );
    }

    private static ScenarioRegistration scenario(
            String id,
            NodeProfile coordinator,
            List<NodeProfile> participants,
            FaultPlan faultPlan,
            long seed
    ) {
        return new ScenarioRegistration(
                id,
                new Scenario(id, coordinator, participants, faultPlan, seed, DEFAULT_TIMEOUT_MS)
        );
    }

    private static void trainPredictor(VFDTProtocolPredictor predictor, List<TrainingExample> dataset) {
        Random random = new Random(1234L);
        List<TrainingExample> workingSet = new ArrayList<>(dataset);

        for (int epoch = 0; epoch < TRAINING_EPOCHS; epoch++) {
            shuffle(workingSet, random);
            for (TrainingExample example : workingSet) {
                predictor.train(example.features(), example.bestResult().protocol());
            }
        }
    }

    private static void shuffle(List<TrainingExample> examples, Random random) {
        for (int i = examples.size() - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            TrainingExample tmp = examples.get(i);
            examples.set(i, examples.get(j));
            examples.set(j, tmp);
        }
    }

    private static List<TrainingExample> collectDataset(
            String split,
            List<WorkloadRegistration> workloads,
            List<ProtocolRegistration> protocols,
            DistributedSimulation simulation
    ) throws InterruptedException {
        List<TrainingExample> dataset = new ArrayList<>();

        for (WorkloadRegistration workload : workloads) {
            List<ProtocolWorkloadResult> protocolResults = new ArrayList<>();
            for (ProtocolRegistration protocol : protocols) {
                protocolResults.add(runWorkload(workload, protocol, simulation));
            }

            ProtocolWorkloadResult best = protocolResults.stream()
                    .min(Comparator.comparingDouble(ProtocolWorkloadResult::score))
                    .orElseThrow();
            LogFeatures features = protocolResults.stream()
                    .filter(result -> result.protocol() == SelectedProtocol.TWO_PC)
                    .findFirst()
                    .map(ProtocolWorkloadResult::features)
                    .orElseGet(() -> averageFeatures(protocolResults.stream().map(ProtocolWorkloadResult::features).toList()));

            dataset.add(new TrainingExample(split, workload.id(), features, best, List.copyOf(protocolResults)));

            System.out.println(
                    "Observed split=" + split
                            + " workload=" + workload.id()
                            + " best=" + best.protocol()
                            + " features=" + features
                            + " scores=" + formatScores(protocolResults)
            );
        }

        return dataset;
    }

    private static ProtocolWorkloadResult runWorkload(
            WorkloadRegistration workload,
            ProtocolRegistration protocol,
            DistributedSimulation simulation
    ) throws InterruptedException {
        List<SimulationResult> runs = new ArrayList<>();

        for (ScenarioRegistration scenario : workload.scenarios()) {
            runs.add(simulation.run(scenario.scenario(), protocol.protocol()));
        }

        TxMetrics aggregatedMetrics = averageMetrics(runs);
        LogFeatures aggregatedFeatures = averageFeatures(runs.stream().map(EXPERIMENTS::extractFeatures).toList());
        double commitRate = runs.stream().filter(run -> run.metrics().committed()).count() / (double) runs.size();
        double workloadScore = score(aggregatedMetrics)
                + ((1.0 - commitRate) * 120.0)
                + (aggregatedFeatures.maxNetworkLatencyMs() * 0.25)
                + (aggregatedFeatures.maxDiskIoTimeMs() * 0.25);

        return new ProtocolWorkloadResult(protocol.protocol(), aggregatedMetrics, aggregatedFeatures, workloadScore);
    }

    private static String formatScores(List<ProtocolWorkloadResult> protocolResults) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < protocolResults.size(); i++) {
            ProtocolWorkloadResult result = protocolResults.get(i);
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(result.protocol()).append('=').append(result.score());
        }
        return builder.toString();
    }

    private static TxMetrics averageMetrics(List<SimulationResult> runs) {
        long totalLatency = 0L;
        long totalMessages = 0L;
        long totalLogWrites = 0L;
        long totalBlocking = 0L;
        int committedCount = 0;

        for (SimulationResult run : runs) {
            totalLatency += run.metrics().latencyMs();
            totalMessages += run.metrics().messageCount();
            totalLogWrites += run.metrics().totalLogWrites();
            totalBlocking += run.metrics().blockingTimeMs();
            if (run.metrics().committed()) {
                committedCount++;
            }
        }

        int size = runs.size();
        return new TxMetrics(
                totalLatency / size,
                committedCount * 2 >= size,
                totalMessages / size,
                totalLogWrites / size,
                totalBlocking / size
        );
    }

    private static LogFeatures averageFeatures(List<LogFeatures> featuresList) {
        double totalAvgLatency = 0.0;
        double totalMaxLatency = 0.0;
        double totalAvgDisk = 0.0;
        double totalMaxDisk = 0.0;
        double totalAbortRate = 0.0;
        double totalParticipants = 0.0;

        for (LogFeatures features : featuresList) {
            totalAvgLatency += features.averageNetworkLatencyMs();
            totalMaxLatency += features.maxNetworkLatencyMs();
            totalAvgDisk += features.averageDiskIoTimeMs();
            totalMaxDisk += features.maxDiskIoTimeMs();
            totalAbortRate += features.abortRate();
            totalParticipants += features.participantCount();
        }

        int size = featuresList.size();
        return new LogFeatures(
                totalAvgLatency / size,
                totalMaxLatency / size,
                totalAvgDisk / size,
                totalMaxDisk / size,
                totalAbortRate / size,
                (int) Math.round(totalParticipants / size)
        );
    }

    private static EvaluationSummary evaluate(
            ProtocolSelector selector,
            String split,
            List<TrainingExample> dataset
    ) {
        int correct = 0;
        double totalRegret = 0.0;

        for (TrainingExample example : dataset) {
            SelectedProtocol predicted = selector.select(example.features());
            if (predicted == example.bestResult().protocol()) {
                correct++;
            }

            double predictedScore = example.protocolResults().stream()
                    .filter(result -> result.protocol() == predicted)
                    .findFirst()
                    .map(ProtocolWorkloadResult::score)
                    .orElseThrow();
            double bestScore = example.bestResult().score();
            totalRegret += (predictedScore - bestScore);

            System.out.println(
                    "Predicted selector=" + selector.name()
                            + " split=" + split
                            + " workload=" + example.workloadId()
                            + " best=" + example.bestResult().protocol()
                            + " predicted=" + predicted
                            + " regret=" + (predictedScore - bestScore)
                            + " features=" + example.features()
            );
        }

        if (dataset.isEmpty()) {
            return new EvaluationSummary(0.0, 0.0);
        }

        return new EvaluationSummary(
                ((double) correct) / dataset.size(),
                totalRegret / dataset.size()
        );
    }

    private record TrainingExample(
            String split,
            String workloadId,
            LogFeatures features,
            ProtocolWorkloadResult bestResult,
            List<ProtocolWorkloadResult> protocolResults
    ) {
    }

    private record EvaluationSummary(
            double accuracy,
            double averageRegret
    ) {
    }

    private record ProtocolWorkloadResult(
            SelectedProtocol protocol,
            TxMetrics metrics,
            LogFeatures features,
            double score
    ) {
    }
}
