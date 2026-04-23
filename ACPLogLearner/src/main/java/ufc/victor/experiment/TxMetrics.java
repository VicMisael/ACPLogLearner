package ufc.victor.experiment;

public record TxMetrics(
        long latencyMs,
        boolean committed,
        long messageCount,
        long totalLogWrites,
        long blockingTimeMs
) {
}
