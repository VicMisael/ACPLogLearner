package ufc.victor.experiment;

import java.util.List;
import java.util.stream.Collectors;

public record Scenario(
        String id,
        String coordinatorNodeId,
        List<String> participantNodeIds,
        long randomSeed,
        long timeoutMs
) {
    public Scenario {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (coordinatorNodeId == null || coordinatorNodeId.isBlank()) {
            throw new IllegalArgumentException("coordinatorNodeId must not be blank");
        }
        if (participantNodeIds == null || participantNodeIds.isEmpty()) {
            throw new IllegalArgumentException("participantNodeIds must not be empty");
        }
        if (timeoutMs < 1L) {
            throw new IllegalArgumentException("timeoutMs must be >= 1");
        }
        participantNodeIds = List.copyOf(participantNodeIds);
    }

    public String participantSetKey() {
        return participantNodeIds.stream()
                .sorted()
                .collect(Collectors.joining("|"));
    }
}
