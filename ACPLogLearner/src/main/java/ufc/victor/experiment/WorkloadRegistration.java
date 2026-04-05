package ufc.victor.experiment;

import java.util.List;

public record WorkloadRegistration(
        String id,
        List<ScenarioRegistration> scenarios
) {
    public WorkloadRegistration {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (scenarios == null || scenarios.isEmpty()) {
            throw new IllegalArgumentException("scenarios must not be empty");
        }
        scenarios = List.copyOf(scenarios);
    }
}
