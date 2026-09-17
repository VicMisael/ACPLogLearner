package ufc.victor.experiment;

import ufc.victor.protocol.SelectedProtocol;

import java.util.Comparator;

public final class CategoricalProtocolSelectionPolicy {

    private final CategoricalPolicyWeights weights;

    public CategoricalProtocolSelectionPolicy(CategoricalPolicyWeights weights) {
        if (weights == null) throw new IllegalArgumentException("weights must not be null");
        this.weights = weights;
    }

    public SelectedProtocol select(CategoricalOutcomePrediction prediction) {
        return java.util.Arrays.stream(SelectedProtocol.values())
                .min(Comparator
                        .comparingDouble((SelectedProtocol protocol) -> score(protocol, prediction))
                        .thenComparingInt(Enum::ordinal))
                .orElseThrow();
    }

    public double score(SelectedProtocol protocol, CategoricalOutcomePrediction prediction) {
        return weights.completionWeight() * completionCost(protocol, prediction)
                + weights.latencyWeight() * ordinalCost(prediction.latency())
                + weights.redoWeight() * ordinalCost(prediction.redo())
                + weights.timeoutPenalty()
                * prediction.completion().probabilities().get(CompletionCategory.TIMEOUT);
    }

    private static double completionCost(
            SelectedProtocol protocol,
            CategoricalOutcomePrediction prediction
    ) {
        double commitCost = switch (protocol) {
            case TWO_PC_PRESUMED_COMMIT -> 0.0;
            case TWO_PC -> 0.5;
            case TWO_PC_PRESUMED_ABORT -> 1.0;
        };
        double abortCost = switch (protocol) {
            case TWO_PC_PRESUMED_ABORT -> 0.0;
            case TWO_PC -> 0.5;
            case TWO_PC_PRESUMED_COMMIT -> 1.0;
        };
        return prediction.completion().probabilities().get(CompletionCategory.COMMIT) * commitCost
                + prediction.completion().probabilities().get(CompletionCategory.ABORT) * abortCost;
    }

    private static double ordinalCost(CategoricalPrediction<CostCategory> prediction) {
        return prediction.probabilities().get(CostCategory.MEDIUM)
                + 2.0 * prediction.probabilities().get(CostCategory.HIGH);
    }
}
