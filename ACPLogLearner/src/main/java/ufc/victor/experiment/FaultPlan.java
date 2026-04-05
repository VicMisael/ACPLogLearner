package ufc.victor.experiment;

import ufc.victor.protocol.commom.message.MessageType;

import java.util.List;
import java.util.Map;

public record FaultPlan(
        Map<MessageType, Double> dropProbabilities,
        Map<MessageType, Integer> extraDelayMs,
        List<CrashSpec> crashes
) {
    public static FaultPlan none() {
        return new FaultPlan(Map.of(), Map.of(), List.of());
    }

    public FaultPlan {
        dropProbabilities = Map.copyOf(dropProbabilities);
        extraDelayMs = Map.copyOf(extraDelayMs);
        crashes = List.copyOf(crashes);
    }
}
