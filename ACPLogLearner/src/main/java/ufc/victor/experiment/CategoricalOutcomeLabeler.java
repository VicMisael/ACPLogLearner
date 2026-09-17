package ufc.victor.experiment;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

public final class CategoricalOutcomeLabeler {

    private final int windowSize;
    private final long defaultLatencyLowUpperMs;
    private final long defaultLatencyMediumUpperMs;
    private final long defaultRedoLowUpperBytes;
    private final long defaultRedoMediumUpperBytes;
    private final Deque<Long> latenciesMs = new ArrayDeque<>();
    private final Deque<Long> redoBytes = new ArrayDeque<>();

    public CategoricalOutcomeLabeler(
            int windowSize,
            long defaultLatencyLowUpperMs,
            long defaultLatencyMediumUpperMs,
            long defaultRedoLowUpperBytes,
            long defaultRedoMediumUpperBytes
    ) {
        if (windowSize < 3) throw new IllegalArgumentException("windowSize must be at least 3");
        this.windowSize = windowSize;
        this.defaultLatencyLowUpperMs = defaultLatencyLowUpperMs;
        this.defaultLatencyMediumUpperMs = defaultLatencyMediumUpperMs;
        this.defaultRedoLowUpperBytes = defaultRedoLowUpperBytes;
        this.defaultRedoMediumUpperBytes = defaultRedoMediumUpperBytes;
        new OutcomeThresholds(
                defaultLatencyLowUpperMs,
                defaultLatencyMediumUpperMs,
                defaultRedoLowUpperBytes,
                defaultRedoMediumUpperBytes,
                0L,
                0L
        );
    }

    public synchronized OutcomeThresholds thresholds() {
        List<Long> sortedLatency = sorted(latenciesMs);
        List<Long> sortedRedo = sorted(redoBytes);
        return new OutcomeThresholds(
                threshold(sortedLatency, defaultLatencyLowUpperMs, 1.0 / 3.0),
                threshold(sortedLatency, defaultLatencyMediumUpperMs, 2.0 / 3.0),
                threshold(sortedRedo, defaultRedoLowUpperBytes, 1.0 / 3.0),
                threshold(sortedRedo, defaultRedoMediumUpperBytes, 2.0 / 3.0),
                sortedLatency.size(),
                sortedRedo.size()
        );
    }

    public CategoricalOutcomeObservation label(
            TransactionTrialRecord trial,
            OutcomeThresholds frozenThresholds
    ) {
        CompletionCategory completion = switch (trial.outcome()) {
            case COMMITTED -> CompletionCategory.COMMIT;
            case ABORTED -> CompletionCategory.ABORT;
            case TIMED_OUT -> CompletionCategory.TIMEOUT;
        };
        Optional<CostCategory> latency = trial.outcome() == TrialOutcome.TIMED_OUT
                ? Optional.empty()
                : Optional.of(category(
                        trial.totalLatency().toMillis(),
                        frozenThresholds.latencyLowUpperMs(),
                        frozenThresholds.latencyMediumUpperMs()
                ));
        OptionalLong totalRedo = trial.totalRedoBytes();
        Optional<CostCategory> redo = totalRedo.isEmpty()
                ? Optional.empty()
                : Optional.of(category(
                        totalRedo.getAsLong(),
                        frozenThresholds.redoLowUpperBytes(),
                        frozenThresholds.redoMediumUpperBytes()
                ));
        return new CategoricalOutcomeObservation(completion, latency, redo, frozenThresholds);
    }

    public synchronized void record(TransactionTrialRecord trial) {
        if (trial.outcome() != TrialOutcome.TIMED_OUT) {
            addBounded(latenciesMs, trial.totalLatency().toMillis());
        }
        trial.totalRedoBytes().ifPresent(value -> addBounded(redoBytes, value));
    }

    private void addBounded(Deque<Long> window, long value) {
        window.addLast(value);
        while (window.size() > windowSize) window.removeFirst();
    }

    private static CostCategory category(long value, long lowUpper, long mediumUpper) {
        if (value <= lowUpper) return CostCategory.LOW;
        if (value <= mediumUpper) return CostCategory.MEDIUM;
        return CostCategory.HIGH;
    }

    private static List<Long> sorted(Deque<Long> values) {
        List<Long> sorted = new ArrayList<>(values);
        sorted.sort(Long::compareTo);
        return sorted;
    }

    private static long quantile(List<Long> sorted, double probability) {
        int index = (int) Math.ceil(probability * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    private static long threshold(List<Long> sorted, long defaultValue, double probability) {
        return sorted.size() < 3 ? defaultValue : quantile(sorted, probability);
    }
}
