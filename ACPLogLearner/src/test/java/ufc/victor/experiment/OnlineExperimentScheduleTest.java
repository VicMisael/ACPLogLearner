package ufc.victor.experiment;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OnlineExperimentScheduleTest {

    @Test
    void emitsDeterministicDriftAndRecoveryPhases() {
        OnlineExperimentSchedule schedule = new OnlineExperimentSchedule(
                OnlineExperimentSchedule.Mode.PHASED, 10, 80, 0.75
        );

        assertEquals(new OnlineExperimentSchedule.Condition("BASELINE", 0, 0.0),
                schedule.conditionFor(0));
        assertEquals(new OnlineExperimentSchedule.Condition("LATENCY_SPIKE", 80, 0.0),
                schedule.conditionFor(10));
        assertEquals(new OnlineExperimentSchedule.Condition("ABORT_SPIKE", 0, 0.75),
                schedule.conditionFor(20));
        assertEquals(new OnlineExperimentSchedule.Condition("COMBINED_SPIKE", 80, 0.75),
                schedule.conditionFor(30));
        assertEquals(new OnlineExperimentSchedule.Condition("RECOVERY", 0, 0.0),
                schedule.conditionFor(40));
        assertEquals(new OnlineExperimentSchedule.Condition("BASELINE", 0, 0.0),
                schedule.conditionFor(50));
    }

    @Test
    void stableModeDoesNotInjectFaults() {
        OnlineExperimentSchedule schedule = new OnlineExperimentSchedule(
                OnlineExperimentSchedule.Mode.STABLE, 10, 80, 0.75
        );

        assertEquals(new OnlineExperimentSchedule.Condition("STABLE", 0, 0.0),
                schedule.conditionFor(999));
    }
}
