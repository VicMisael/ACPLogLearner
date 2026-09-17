package ufc.victor.experiment;

import java.util.Objects;

public record CategoricalOutcomePrediction(
        CategoricalPrediction<CompletionCategory> completion,
        CategoricalPrediction<CostCategory> latency,
        CategoricalPrediction<CostCategory> redo
) {
    public CategoricalOutcomePrediction {
        Objects.requireNonNull(completion, "completion");
        Objects.requireNonNull(latency, "latency");
        Objects.requireNonNull(redo, "redo");
    }
}
