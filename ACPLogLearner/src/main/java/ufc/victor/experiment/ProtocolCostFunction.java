package ufc.victor.experiment;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

public final class ProtocolCostFunction {
    private final ProtocolCostWeights weights;

    public ProtocolCostFunction(ProtocolCostWeights weights) {
        if (weights == null) throw new IllegalArgumentException("weights must not be null");
        this.weights = weights;
    }

    public ProtocolTrialCost evaluate(TransactionTrialRecord trial, TrialOutcome expectedOutcome) {
        if (trial == null) throw new IllegalArgumentException("trial must not be null");
        if (expectedOutcome == null || expectedOutcome == TrialOutcome.TIMED_OUT) {
            throw new IllegalArgumentException("expectedOutcome must be COMMITTED or ABORTED");
        }

        List<String> invalidReasons = new ArrayList<>();
        if (trial.outcome() == TrialOutcome.TIMED_OUT) invalidReasons.add("timeout");
        if (trial.outcome() != expectedOutcome) invalidReasons.add("unexpected_outcome");
        if (trial.resetDetected()) invalidReasons.add("redo_counter_reset");
        OptionalLong redoBytes = trial.totalRedoBytes();
        if (weights.redoWeight() > 0.0 && redoBytes.isEmpty()) {
            invalidReasons.add("redo_unavailable");
        }

        double latency = trial.totalLatency().toNanos() / 1_000_000.0 / weights.latencyScaleMs();
        double redo = redoBytes.isPresent() ? redoBytes.getAsLong() / weights.redoScaleBytes() : 0.0;
        double messages = trial.messageCount() / weights.messageScale();
        double logs = trial.totalProtocolLogWrites() / weights.protocolLogScale();
        double total = weights.latencyWeight() * latency
                + weights.redoWeight() * redo
                + weights.messageWeight() * messages
                + weights.protocolLogWeight() * logs;
        if (trial.outcome() == TrialOutcome.TIMED_OUT) total += weights.timeoutPenalty();
        if (!invalidReasons.isEmpty()) total += weights.failurePenalty();
        return new ProtocolTrialCost(
                total, latency, redo, messages, logs,
                invalidReasons.isEmpty(), invalidReasons
        );
    }
}
