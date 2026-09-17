package ufc.victor.experiment;

public final class OnlineExperimentSchedule {

    public enum Mode {
        STABLE,
        PHASED
    }

    public record Condition(
            String phase,
            int injectedNetworkLatencyMs,
            double abortRateFloor
    ) {
        public Condition {
            if (phase == null || phase.isBlank()) {
                throw new IllegalArgumentException("phase must not be blank");
            }
            if (injectedNetworkLatencyMs < 0) {
                throw new IllegalArgumentException("injectedNetworkLatencyMs must not be negative");
            }
            if (!Double.isFinite(abortRateFloor)
                    || abortRateFloor < 0.0
                    || abortRateFloor > 1.0) {
                throw new IllegalArgumentException("abortRateFloor must be between 0 and 1");
            }
        }
    }

    private final Mode mode;
    private final int phaseLength;
    private final int latencySpikeMs;
    private final double abortSpikeRate;

    public OnlineExperimentSchedule(
            Mode mode,
            int phaseLength,
            int latencySpikeMs,
            double abortSpikeRate
    ) {
        if (mode == null) throw new IllegalArgumentException("mode must not be null");
        if (phaseLength < 1) throw new IllegalArgumentException("phaseLength must be at least 1");
        if (latencySpikeMs < 0) {
            throw new IllegalArgumentException("latencySpikeMs must not be negative");
        }
        if (!Double.isFinite(abortSpikeRate)
                || abortSpikeRate < 0.0
                || abortSpikeRate > 1.0) {
            throw new IllegalArgumentException("abortSpikeRate must be between 0 and 1");
        }
        this.mode = mode;
        this.phaseLength = phaseLength;
        this.latencySpikeMs = latencySpikeMs;
        this.abortSpikeRate = abortSpikeRate;
    }

    public Condition conditionFor(int exampleIndex) {
        if (exampleIndex < 0) throw new IllegalArgumentException("exampleIndex must not be negative");
        if (mode == Mode.STABLE) return new Condition("STABLE", 0, 0.0);

        return switch ((exampleIndex / phaseLength) % 5) {
            case 0 -> new Condition("BASELINE", 0, 0.0);
            case 1 -> new Condition("LATENCY_SPIKE", latencySpikeMs, 0.0);
            case 2 -> new Condition("ABORT_SPIKE", 0, abortSpikeRate);
            case 3 -> new Condition("COMBINED_SPIKE", latencySpikeMs, abortSpikeRate);
            default -> new Condition("RECOVERY", 0, 0.0);
        };
    }
}
