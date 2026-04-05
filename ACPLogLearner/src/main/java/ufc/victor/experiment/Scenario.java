package ufc.victor.experiment;

import java.util.List;

public record Scenario(
        String name,
        NodeProfile coordinatorProfile,
        List<NodeProfile> participantProfiles,
        FaultPlan faultPlan,
        long randomSeed,
        long timeoutMs
) {
    public Scenario {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (coordinatorProfile == null) {
            throw new IllegalArgumentException("coordinatorProfile must not be null");
        }
        if (participantProfiles == null || participantProfiles.isEmpty()) {
            throw new IllegalArgumentException("participantProfiles must contain at least one node");
        }
        if (faultPlan == null) {
            throw new IllegalArgumentException("faultPlan must not be null");
        }
        if (timeoutMs < 1) {
            throw new IllegalArgumentException("timeoutMs must be >= 1");
        }
        participantProfiles = List.copyOf(participantProfiles);
    }

    public int participantCount() {
        return participantProfiles.size();
    }
}
