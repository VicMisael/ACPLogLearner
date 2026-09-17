package ufc.victor.experiment;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

public record CategoricalPrediction<L extends Enum<L>>(
        L predictedClass,
        Map<L, Double> probabilities,
        double confidence
) {
    public CategoricalPrediction {
        Objects.requireNonNull(predictedClass, "predictedClass");
        Objects.requireNonNull(probabilities, "probabilities");
        EnumMap<L, Double> copiedProbabilities = new EnumMap<>(predictedClass.getDeclaringClass());
        copiedProbabilities.putAll(probabilities);
        probabilities = Map.copyOf(copiedProbabilities);
        if (!Double.isFinite(confidence) || confidence < 0.0 || confidence > 1.0) {
            throw new IllegalArgumentException("confidence must be between 0 and 1");
        }
    }
}
