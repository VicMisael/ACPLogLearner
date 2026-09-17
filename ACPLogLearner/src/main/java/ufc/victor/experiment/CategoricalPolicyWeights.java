package ufc.victor.experiment;

public record CategoricalPolicyWeights(
        double completionWeight,
        double latencyWeight,
        double redoWeight,
        double timeoutPenalty
) {
    public CategoricalPolicyWeights {
        if (!valid(completionWeight)
                || !valid(latencyWeight)
                || !valid(redoWeight)
                || !valid(timeoutPenalty)) {
            throw new IllegalArgumentException("policy weights must be finite and non-negative");
        }
    }

    public static CategoricalPolicyWeights balanced() {
        return new CategoricalPolicyWeights(1.0, 1.0, 1.0, 10.0);
    }

    private static boolean valid(double value) {
        return Double.isFinite(value) && value >= 0.0;
    }
}
