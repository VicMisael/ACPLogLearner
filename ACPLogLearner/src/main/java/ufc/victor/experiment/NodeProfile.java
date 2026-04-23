package ufc.victor.experiment;

public record NodeProfile(
        String nodeName,
        int minLatencyMs,
        int maxLatencyMs,
        int prepareDelayMs,
        double abortProbability
) {
    public NodeProfile {
        if (nodeName == null || nodeName.isBlank()) {
            throw new IllegalArgumentException("nodeName must not be blank");
        }
        if (minLatencyMs < 0 || maxLatencyMs < minLatencyMs) {
            throw new IllegalArgumentException("latency bounds are invalid");
        }
        if (prepareDelayMs < 0) {
            throw new IllegalArgumentException("prepareDelayMs must be >= 0");
        }
        if (abortProbability < 0.0 || abortProbability > 1.0) {
            throw new IllegalArgumentException("abortProbability must be between 0.0 and 1.0");
        }
    }

    public double averageLatencyMs() {
        return (minLatencyMs + maxLatencyMs) / 2.0;
    }
}
