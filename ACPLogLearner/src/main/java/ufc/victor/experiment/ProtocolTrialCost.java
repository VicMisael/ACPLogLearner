package ufc.victor.experiment;

import java.util.List;

public record ProtocolTrialCost(
        double total,
        double normalizedLatency,
        double normalizedRedo,
        double normalizedMessages,
        double normalizedProtocolLogs,
        boolean valid,
        List<String> invalidReasons
) {
    public ProtocolTrialCost {
        if (!Double.isFinite(total) || total < 0.0) {
            throw new IllegalArgumentException("total must be finite and non-negative");
        }
        invalidReasons = List.copyOf(invalidReasons);
        if (valid && !invalidReasons.isEmpty()) {
            throw new IllegalArgumentException("valid cost must not have invalid reasons");
        }
        if (!valid && invalidReasons.isEmpty()) {
            throw new IllegalArgumentException("invalid cost must explain why it is invalid");
        }
    }
}
