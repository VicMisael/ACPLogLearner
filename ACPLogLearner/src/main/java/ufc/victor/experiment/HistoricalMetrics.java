package ufc.victor.experiment;

public record HistoricalMetrics(
        long sampleCount,
        double commitRate,
        double averageLatencyMs,
        double averageRedoBytes,
        double averageLsnAdvanced,
        double averageRedoLogWrites,
        double averageRedoLogWaits,
        double averageProtocolLogWrites,
        double redoAvailabilityRate,
        double resetRate
) {
    public static HistoricalMetrics empty() {
        return new HistoricalMetrics(0L, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
    }
}
