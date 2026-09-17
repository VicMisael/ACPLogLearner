package ufc.victor.experiment;

import org.junit.jupiter.api.Test;
import ufc.victor.protocol.SelectedProtocol;
import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.telemetry.redo.RedoDelta;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CategoricalOutcomeLabelerTest {

    @Test
    void freezesDefaultThresholdsBeforeAnyObservation() {
        CategoricalOutcomeLabeler labeler = new CategoricalOutcomeLabeler(10, 25, 75, 2_000, 10_000);
        OutcomeThresholds thresholds = labeler.thresholds();

        CategoricalOutcomeObservation observation = labeler.label(
                trial(TrialOutcome.COMMITTED, 50, OptionalLong.of(5_000), false),
                thresholds
        );

        assertEquals(CompletionCategory.COMMIT, observation.completion());
        assertEquals(Optional.of(CostCategory.MEDIUM), observation.latency());
        assertEquals(Optional.of(CostCategory.MEDIUM), observation.redo());
        assertEquals(0L, thresholds.latencyHistoricalSamples());
        assertEquals(0L, thresholds.redoHistoricalSamples());
    }

    @Test
    void derivesQuantilesOnlyFromPreviouslyRecordedTrials() {
        CategoricalOutcomeLabeler labeler = new CategoricalOutcomeLabeler(10, 25, 75, 2_000, 10_000);
        labeler.record(trial(TrialOutcome.COMMITTED, 10, OptionalLong.of(100), false));
        labeler.record(trial(TrialOutcome.COMMITTED, 20, OptionalLong.of(200), false));
        labeler.record(trial(TrialOutcome.ABORTED, 90, OptionalLong.of(900), false));

        OutcomeThresholds frozen = labeler.thresholds();
        labeler.record(trial(TrialOutcome.COMMITTED, 1_000, OptionalLong.of(10_000), false));
        CategoricalOutcomeObservation observation = labeler.label(
                trial(TrialOutcome.COMMITTED, 21, OptionalLong.of(201), false), frozen
        );

        assertEquals(10L, frozen.latencyLowUpperMs());
        assertEquals(20L, frozen.latencyMediumUpperMs());
        assertEquals(CostCategory.HIGH, observation.latency().orElseThrow());
        assertEquals(CostCategory.HIGH, observation.redo().orElseThrow());
    }

    @Test
    void skipsCensoredLatencyAndInvalidRedo() {
        CategoricalOutcomeLabeler labeler = new CategoricalOutcomeLabeler(10, 25, 75, 2_000, 10_000);
        CategoricalOutcomeObservation observation = labeler.label(
                trial(TrialOutcome.TIMED_OUT, 60_000, OptionalLong.empty(), true),
                labeler.thresholds()
        );

        assertEquals(CompletionCategory.TIMEOUT, observation.completion());
        assertTrue(observation.latency().isEmpty());
        assertTrue(observation.redo().isEmpty());
    }

    @Test
    void adaptsEachMetricThresholdFromItsOwnAvailableHistory() {
        CategoricalOutcomeLabeler labeler = new CategoricalOutcomeLabeler(10, 25, 75, 2_000, 10_000);
        labeler.record(trial(TrialOutcome.COMMITTED, 10, OptionalLong.empty(), false));
        labeler.record(trial(TrialOutcome.COMMITTED, 20, OptionalLong.empty(), false));
        labeler.record(trial(TrialOutcome.COMMITTED, 90, OptionalLong.empty(), false));

        OutcomeThresholds thresholds = labeler.thresholds();

        assertEquals(10L, thresholds.latencyLowUpperMs());
        assertEquals(20L, thresholds.latencyMediumUpperMs());
        assertEquals(2_000L, thresholds.redoLowUpperBytes());
        assertEquals(10_000L, thresholds.redoMediumUpperBytes());
        assertEquals(3L, thresholds.latencyHistoricalSamples());
        assertEquals(0L, thresholds.redoHistoricalSamples());
    }

    private static TransactionTrialRecord trial(
            TrialOutcome outcome,
            long latencyMs,
            OptionalLong redoBytes,
            boolean reset
    ) {
        NodeId nodeId = new NodeId("node-a");
        RedoDelta redo = new RedoDelta(
                nodeId, Duration.ofMillis(latencyMs), false, reset,
                redoBytes, redoBytes, redoBytes, redoBytes, redoBytes, redoBytes,
                redoBytes, redoBytes
        );
        return new TransactionTrialRecord(
                TransactionId.newTransaction(), UUID.randomUUID(), SelectedProtocol.TWO_PC,
                outcome, Duration.ofMillis(latencyMs), Optional.empty(), Optional.empty(),
                Map.of(nodeId, new ParticipantTrialMetrics(nodeId, redo, 1)), 1, 1
        );
    }
}
