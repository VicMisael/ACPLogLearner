package ufc.victor.experiment;

public record NodeProfile(
        String name,
        int minLatencyMs,
        int maxLatencyMs,
        int diskIoTimeMs,
        double abortProbability
) {
    public NodeProfile {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (minLatencyMs < 0 || maxLatencyMs < minLatencyMs) {
            throw new IllegalArgumentException("invalid latency bounds");
        }
        if (diskIoTimeMs < 0) {
            throw new IllegalArgumentException("diskIoTimeMs must be >= 0");
        }
        if (abortProbability < 0.0 || abortProbability > 1.0) {
            throw new IllegalArgumentException("abortProbability must be in [0, 1]");
        }
    }
}
