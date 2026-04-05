package ufc.victor.experiment;

public record ScenarioRegistration(
        String id,
        Scenario scenario
) {
    public ScenarioRegistration {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (scenario == null) {
            throw new IllegalArgumentException("scenario must not be null");
        }
    }
}
