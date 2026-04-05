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

    public static void main(String[] args) throws InterruptedException {
        List<Scenario> scenarios = defaultScenarios();
        int splitIndex = Math.max(1, (int) Math.floor(scenarios.size() * 0.7));
        List<Scenario> trainingScenarios = List.copyOf(scenarios.subList(0, splitIndex));
        List<Scenario> testScenarios = List.copyOf(scenarios.subList(splitIndex, scenarios.size()));

        VFDTProtocolPredictor predictor = new VFDTProtocolPredictor(0.0);
        DistributedSimulation simulation = new DistributedSimulation();
        List<TrainingExample> trainingDataset = collectDataset("train", trainingScenarios, simulation);
        List<TrainingExample> testDataset = collectDataset("test", testScenarios, simulation);

        trainPredictor(predictor, trainingDataset);

        EvaluationSummary trainingSummary = evaluate("train", predictor, trainingDataset);
        EvaluationSummary testSummary = evaluate("test", predictor, testDataset);

        System.out.println("\nVFDT stats:");
        System.out.println("nodes=" + predictor.getNodeCount() + ", depth=" + predictor.getTreeDepth());
        System.out.println("\nEvaluation:");
        System.out.println("train accuracy=" + trainingSummary.accuracy() + ", avg regret=" + trainingSummary.averageRegret());
        System.out.println("test accuracy=" + testSummary.accuracy() + ", avg regret=" + testSummary.averageRegret());
        System.out.println("\nLearned VFDT rules:");
        System.out.println(predictor.getLearnedRules());
    }

    static SimulationResult selectBest(List<SimulationResult> results) {
        return results.stream()
                .min(Comparator.comparingDouble(r -> score(r.metrics())))
                .orElseThrow();
    }

    static double score(TxMetrics metrics) {
        double abortPenalty = metrics.committed() ? 0.0 : 10_000.0;
        return metrics.latencyMs()
                + (2.0 * metrics.messageCount())
                + (4.0 * metrics.totalLogWrites())
                + (1.5 * metrics.blockingTimeMs())
                + abortPenalty;
    }

    private static List<Scenario> defaultScenarios() {
        List<Scenario> scenarios = new ArrayList<>();
        int index = 0;
        NodeProfile coordinator = new NodeProfile("coord", 5, 15, 5, 0.0);
        List<List<NodeProfile>> templates = List.of(
                List.of(
                        new NodeProfile("p1-fast", 5, 12, 12, 0.05),
                        new NodeProfile("p2-fast", 6, 14, 14, 0.05),
                        new NodeProfile("p3-slow", 20, 40, 35, 0.10)
                ),
                List.of(
                        new NodeProfile("p1-mid", 20, 45, 30, 0.20),
                        new NodeProfile("p2-mid", 25, 55, 45, 0.20),
                        new NodeProfile("p3-hotspot", 40, 80, 70, 0.35)
                ),
                List.of(
                        new NodeProfile("p1-fast", 10, 25, 20, 0.10),
                        new NodeProfile("p2-mid", 20, 50, 35, 0.20),
                        new NodeProfile("p3-mid", 25, 60, 45, 0.25),
                        new NodeProfile("p4-slow", 55, 100, 80, 0.45),
                        new NodeProfile("p5-tail", 70, 130, 95, 0.60)
                )
        );

        for (int templateIndex = 0; templateIndex < templates.size(); templateIndex++) {
            List<NodeProfile> template = templates.get(templateIndex);
            for (int replicate = 0; replicate < 4; replicate++) {
                scenarios.add(new Scenario(
                        "s%02d_p%d_r%d".formatted(index++, template.size(), replicate),
                        coordinator,
                        mutateProfiles(template, replicate),
                        faultPlanFor(templateIndex, replicate),
                        10_000L + index,
                        20_000
                ));
            }
        }

        return scenarios;
    }

    private static void trainPredictor(VFDTProtocolPredictor predictor, List<TrainingExample> dataset) {
        Random random = new Random(1234L);
        List<TrainingExample> workingSet = new ArrayList<>(dataset);

        for (int epoch = 0; epoch < TRAINING_EPOCHS; epoch++) {
            shuffle(workingSet, random);
            for (TrainingExample example : workingSet) {
                predictor.train(example.features(), example.result().protocol());
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

    private record TrainingExample(
            String split,
            String scenarioName,
            LogFeatures features,
            SimulationResult result,
            List<SimulationResult> allResults
    ) {
    }

    private record EvaluationSummary(
            double accuracy,
            double averageRegret
    ) {
    }

    private static List<NodeProfile> mutateProfiles(List<NodeProfile> template, int replicate) {
        List<NodeProfile> profiles = new ArrayList<>(template.size());
        int latencyDelta = replicate * 4;
        int diskDelta = replicate * 6;
        double abortDelta = replicate * 0.08;

        for (int i = 0; i < template.size(); i++) {
            NodeProfile base = template.get(i);
            profiles.add(new NodeProfile(
                    base.name() + "-r" + replicate,
                    base.minLatencyMs() + latencyDelta + i,
                    base.maxLatencyMs() + latencyDelta + (2 * i),
                    base.diskIoTimeMs() + diskDelta + (3 * i),
                    Math.min(0.95, base.abortProbability() + abortDelta)
            ));
        }

        return profiles;
    }

    private static FaultPlan faultPlanFor(int templateIndex, int replicate) {
        return switch (replicate) {
            case 1 -> new FaultPlan(
                    Map.of(MessageType.VOTE_COMMIT, 0.15),
                    Map.of(MessageType.ACK, 200),
                    List.of()
            );
            case 2 -> new FaultPlan(
                    Map.of(),
                    Map.of(),
                    List.of(
                            new CrashSpec("P1", CrashTrigger.PARTICIPANT_BEFORE_READY, 400),
                            new CrashSpec("P2", CrashTrigger.PARTICIPANT_AFTER_READY, 400)
                    )
            );
            case 3 -> new FaultPlan(
                    Map.of(),
                    Map.of(MessageType.ACK, 250),
                    List.of(
                            new CrashSpec(
                                    "COORD",
                                    templateIndex % 2 == 0
                                            ? CrashTrigger.COORDINATOR_BEFORE_DECISION
                                            : CrashTrigger.COORDINATOR_AFTER_DECISION,
                                    500
                            )
                    )
            );
            default -> FaultPlan.none();
        };
    }

    private static List<TrainingExample> collectDataset(
            String split,
            List<Scenario> scenarios,
            DistributedSimulation simulation
    ) throws InterruptedException {
        List<TrainingExample> dataset = new ArrayList<>();

        for (Scenario scenario : scenarios) {
            List<SimulationResult> results = new ArrayList<>();
            for (SelectedProtocol protocol : SelectedProtocol.values()) {
                results.add(simulation.run(scenario, protocol));
            }

            SimulationResult best = selectBest(results);
            LogFeatures features = EXPERIMENTS.extractFeatures(best);
            dataset.add(new TrainingExample(split, scenario.name(), features, best, List.copyOf(results)));

            System.out.println(
                    "Observed split=" + split
                            + " scenario=" + scenario.name()
                            + " best=" + best.protocol()
                            + " features=" + features
                            + " metrics=" + best.metrics()
            );
        }

        return dataset;
    }

    private static EvaluationSummary evaluate(
            String split,
            VFDTProtocolPredictor predictor,
            List<TrainingExample> dataset
    ) {
        int correct = 0;
        double totalRegret = 0.0;

        for (TrainingExample example : dataset) {
            SelectedProtocol predicted = predictor.predict(example.features());
            if (predicted == example.result().protocol()) {
                correct++;
            }

            double predictedScore = example.allResults().stream()
                    .filter(r -> r.protocol() == predicted)
                    .findFirst()
                    .map(r -> score(r.metrics()))
                    .orElseThrow();
            double bestScore = score(example.result().metrics());
            totalRegret += (predictedScore - bestScore);

            System.out.println(
                    "Predicted split=" + split
                            + " scenario=" + example.scenarioName()
                            + " best=" + example.result().protocol()
                            + " predicted=" + predicted
                            + " features=" + example.features()
                            + " regret=" + (predictedScore - bestScore)
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
}
