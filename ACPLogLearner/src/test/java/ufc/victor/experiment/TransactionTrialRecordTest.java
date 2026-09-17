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
import static org.junit.jupiter.api.Assertions.assertFalse;

class TransactionTrialRecordTest {

    @Test
    void combinesCoordinatorAndParticipantProtocolLogWrites() {
        NodeId nodeId = new NodeId("P1");
        RedoDelta redo = new RedoDelta(
                nodeId, Duration.ofMillis(10), false, false,
                value(10), value(10), value(20), value(2), value(1), value(0), value(5), value(6)
        );
        TransactionTrialRecord trial = new TransactionTrialRecord(
                TransactionId.newTransaction(), UUID.randomUUID(), SelectedProtocol.TWO_PC,
                TrialOutcome.COMMITTED, Duration.ofMillis(10), Optional.of(Duration.ofMillis(7)),
                Optional.of(Duration.ofMillis(3)),
                Map.of(nodeId, new ParticipantTrialMetrics(nodeId, redo, 3L)), 7L, 2L
        );

        assertEquals(5L, trial.totalProtocolLogWrites());
        assertFalse(trial.resetDetected());
        assertFalse(trial.hasUnavailableRedoMetric());
        assertEquals(20L, trial.totalRedoBytes().orElseThrow());
    }

    private static OptionalLong value(long value) {
        return OptionalLong.of(value);
    }
}
