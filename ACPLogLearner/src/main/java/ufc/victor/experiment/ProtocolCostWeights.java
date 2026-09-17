package ufc.victor.experiment;

public record ProtocolCostWeights(
        double latencyWeight,
        double redoWeight,
        double messageWeight,
        double protocolLogWeight,
        double latencyScaleMs,
        double redoScaleBytes,
        double messageScale,
        double protocolLogScale,
        double timeoutPenalty,
        double failurePenalty
) {
    public ProtocolCostWeights {
        requireNonNegative(latencyWeight, "latencyWeight");
        requireNonNegative(redoWeight, "redoWeight");
        requireNonNegative(messageWeight, "messageWeight");
        requireNonNegative(protocolLogWeight, "protocolLogWeight");
        requirePositive(latencyScaleMs, "latencyScaleMs");
        requirePositive(redoScaleBytes, "redoScaleBytes");
        requirePositive(messageScale, "messageScale");
        requirePositive(protocolLogScale, "protocolLogScale");
        requireNonNegative(timeoutPenalty, "timeoutPenalty");
        requireNonNegative(failurePenalty, "failurePenalty");
    }

    public static ProtocolCostWeights defaults() {
        return new ProtocolCostWeights(
                1.0, 1.0, 1.0, 1.0,
                100.0, 10_000.0, 10.0, 10.0,
                100.0, 1_000.0
        );
    }

    private static void requireNonNegative(double value, String name) {
        if (!Double.isFinite(value) || value < 0.0) {
            throw new IllegalArgumentException(name + " must be finite and non-negative");
        }
    }

    private static void requirePositive(double value, String name) {
        if (!Double.isFinite(value) || value <= 0.0) {
            throw new IllegalArgumentException(name + " must be finite and positive");
        }
    }
}
