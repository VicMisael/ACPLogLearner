package ufc.victor.experiment;

import ufc.victor.protocol.coordinator.node.NodeId;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.ToDoubleFunction;

public record ParticipantHistorySummary(
        double historyCoverage,
        double meanCommitRate,
        double minimumCommitRate,
        double meanLatencyMs,
        double maximumLatencyMs,
        double meanRedoBytes,
        double maximumRedoBytes,
        double meanLsnAdvanced,
        double maximumLsnAdvanced,
        double meanRedoLogWrites,
        double maximumRedoLogWrites,
        double meanRedoLogWaits,
        double maximumRedoLogWaits,
        double meanProtocolLogWrites,
        double maximumProtocolLogWrites,
        double minimumRedoAvailability,
        double maximumResetRate
) {
    public static ParticipantHistorySummary from(Map<NodeId, HistoricalMetrics> histories) {
        if (histories.isEmpty()) return empty();
        Collection<HistoricalMetrics> values = histories.values();
        List<HistoricalMetrics> available = values.stream()
                .filter(history -> history.sampleCount() > 0L)
                .toList();
        if (available.isEmpty()) return empty();
        return new ParticipantHistorySummary(
                available.size() / (double) values.size(),
                mean(available, HistoricalMetrics::commitRate),
                minimum(available, HistoricalMetrics::commitRate),
                mean(available, HistoricalMetrics::averageLatencyMs),
                maximum(available, HistoricalMetrics::averageLatencyMs),
                mean(available, HistoricalMetrics::averageRedoBytes),
                maximum(available, HistoricalMetrics::averageRedoBytes),
                mean(available, HistoricalMetrics::averageLsnAdvanced),
                maximum(available, HistoricalMetrics::averageLsnAdvanced),
                mean(available, HistoricalMetrics::averageRedoLogWrites),
                maximum(available, HistoricalMetrics::averageRedoLogWrites),
                mean(available, HistoricalMetrics::averageRedoLogWaits),
                maximum(available, HistoricalMetrics::averageRedoLogWaits),
                mean(available, HistoricalMetrics::averageProtocolLogWrites),
                maximum(available, HistoricalMetrics::averageProtocolLogWrites),
                minimum(available, HistoricalMetrics::redoAvailabilityRate),
                maximum(available, HistoricalMetrics::resetRate)
        );
    }

    public static ParticipantHistorySummary empty() {
        return new ParticipantHistorySummary(
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        );
    }

    private static double mean(
            Collection<HistoricalMetrics> values,
            ToDoubleFunction<HistoricalMetrics> extractor
    ) {
        return values.stream().mapToDouble(extractor).average().orElse(0.0);
    }

    private static double minimum(
            Collection<HistoricalMetrics> values,
            ToDoubleFunction<HistoricalMetrics> extractor
    ) {
        return values.stream().mapToDouble(extractor).min().orElse(0.0);
    }

    private static double maximum(
            Collection<HistoricalMetrics> values,
            ToDoubleFunction<HistoricalMetrics> extractor
    ) {
        return values.stream().mapToDouble(extractor).max().orElse(0.0);
    }
}
