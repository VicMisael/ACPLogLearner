package ufc.victor.experiment;

import java.util.Objects;
import java.util.Optional;

public record CategoricalOutcomeObservation(
        CompletionCategory completion,
        Optional<CostCategory> latency,
        Optional<CostCategory> redo,
        OutcomeThresholds thresholds
) {
    public CategoricalOutcomeObservation {
        Objects.requireNonNull(completion, "completion");
        Objects.requireNonNull(latency, "latency");
        Objects.requireNonNull(redo, "redo");
        Objects.requireNonNull(thresholds, "thresholds");
    }
}
