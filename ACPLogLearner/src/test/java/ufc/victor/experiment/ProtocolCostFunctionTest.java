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
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtocolCostFunctionTest {

    @Test
    void combinesNormalizedRawMetrics() {
        ProtocolCostFunction function = new ProtocolCostFunction(ProtocolCostWeights.defaults());

        ProtocolTrialCost cost = function.evaluate(
                trial(SelectedProtocol.TWO_PC, TrialOutcome.COMMITTED, 100, OptionalLong.of(10_000)),
                TrialOutcome.COMMITTED
        );

        assertTrue(cost.valid());
        assertEquals(3.5, cost.total(), 0.000_001);
        assertEquals(1.0, cost.normalizedLatency(), 0.000_001);
        assertEquals(1.0, cost.normalizedRedo(), 0.000_001);
        assertEquals(1.0, cost.normalizedMessages(), 0.000_001);
        assertEquals(0.5, cost.normalizedProtocolLogs(), 0.000_001);
    }

    @Test
    void invalidatesUnavailableRedoWhenRedoHasWeight() {
        ProtocolCostFunction function = new ProtocolCostFunction(ProtocolCostWeights.defaults());

        ProtocolTrialCost cost = function.evaluate(
                trial(SelectedProtocol.TWO_PC, TrialOutcome.COMMITTED, 100, OptionalLong.empty()),
                TrialOutcome.COMMITTED
        );

        assertFalse(cost.valid());
        assertTrue(cost.invalidReasons().contains("redo_unavailable"));
        assertTrue(cost.total() >= 1_000.0);
    }

    static TransactionTrialRecord trial(
            SelectedProtocol protocol,
            TrialOutcome outcome,
            long latencyMs,
            OptionalLong redoBytes
    ) {
        NodeId nodeId = new NodeId("P1");
        OptionalLong zero = OptionalLong.of(0L);
        RedoDelta redo = new RedoDelta(
                nodeId, Duration.ofMillis(latencyMs), false, false,
                zero, zero, redoBytes, zero, zero, zero, zero, zero
        );
        return new TransactionTrialRecord(
                TransactionId.newTransaction(), UUID.randomUUID(), protocol, outcome,
                Duration.ofMillis(latencyMs), Optional.of(Duration.ofMillis(latencyMs / 2)),
                Optional.of(Duration.ofMillis(latencyMs / 2)),
                Map.of(nodeId, new ParticipantTrialMetrics(nodeId, redo, 3L)),
                10L, 2L
        );
    }
}
