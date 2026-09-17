package ufc.victor.experiment;

public record OutcomeThresholds(
        long latencyLowUpperMs,
        long latencyMediumUpperMs,
        long redoLowUpperBytes,
        long redoMediumUpperBytes,
        long latencyHistoricalSamples,
        long redoHistoricalSamples
) {
    public OutcomeThresholds {
        if (latencyLowUpperMs < 0L || latencyMediumUpperMs < latencyLowUpperMs) {
            throw new IllegalArgumentException("invalid latency thresholds");
        }
        if (redoLowUpperBytes < 0L || redoMediumUpperBytes < redoLowUpperBytes) {
            throw new IllegalArgumentException("invalid redo thresholds");
        }
        if (latencyHistoricalSamples < 0L || redoHistoricalSamples < 0L) {
            throw new IllegalArgumentException("historical sample counts must not be negative");
        }
    }
}
