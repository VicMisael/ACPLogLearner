package ufc.victor.experiment;

import org.junit.jupiter.api.Test;
import ufc.victor.protocol.SelectedProtocol;
import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.telemetry.redo.RedoDelta;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RollingMetricsRegistryTest {

    @Test
    void exposesOnlyTrialsRecordedBeforeFeatureConstruction() {
        RollingMetricsRegistry registry = new RollingMetricsRegistry(3);
        TransactionContext context = context(List.of("node-a", "node-b"));

        AlpsFeatures before = registry.featuresFor(context);
        assertEquals(0L, before.globalHistory().sampleCount());
        assertEquals(0L, before.participantHistory().get(new NodeId("node-a")).sampleCount());

        registry.record(trial(context, true, 20L, 100L, false, false));

        assertEquals(0L, before.globalHistory().sampleCount());
        AlpsFeatures after = registry.featuresFor(context);
        assertEquals(1L, after.globalHistory().sampleCount());
        assertEquals(1.0, after.globalHistory().commitRate());
        assertEquals(100.0, after.globalHistory().averageRedoBytes());
        assertEquals(1L, after.participantHistory().get(new NodeId("node-b")).sampleCount());
    }

    @Test
    void boundsWindowsAndPreservesUnavailableAndResetSignals() {
        RollingMetricsRegistry registry = new RollingMetricsRegistry(1);
        TransactionContext context = context(List.of("node-a", "node-b"));
        registry.record(trial(context, true, 20L, 100L, false, false));
        registry.record(trial(context, false, 40L, 300L, true, true));

        HistoricalMetrics history = registry.featuresFor(context).globalHistory();
        assertEquals(1L, history.sampleCount());
        assertEquals(0.0, history.commitRate());
        assertEquals(40.0, history.averageLatencyMs());
        assertEquals(0.0, history.averageRedoBytes());
        assertEquals(0.0, history.redoAvailabilityRate());
        assertEquals(1.0, history.resetRate());
    }

    @Test
    void preservesGenericTransactionContext() {
        TransactionContext context = context(List.of("node-a", "node-b", "node-c"));
        AlpsFeatures features = new RollingMetricsRegistry(2).featuresFor(context);

        assertEquals(context, features.transactionContext());
        assertEquals(3, features.participantCount());
        assertTrue(features.transactionContext().distributed());
        assertEquals(7.0, features.transactionContext().workloadFeatures().get("custom_cost"));
    }

    private static TransactionTrialRecord trial(
            TransactionContext context,
            boolean committed,
            long latencyMs,
            long redoBytes,
            boolean reset,
            boolean unavailable
    ) {
        Map<NodeId, ParticipantTrialMetrics> participants = new LinkedHashMap<>();
        for (String participantId : context.participantNodeIds()) {
            NodeId nodeId = new NodeId(participantId);
            OptionalLong value = unavailable ? OptionalLong.empty() : OptionalLong.of(redoBytes);
            RedoDelta redo = new RedoDelta(
                    nodeId,
                    Duration.ofMillis(latencyMs),
                    reset,
                    reset,
                    value,
                    value,
                    value,
                    value,
                    value,
                    value,
                    value,
                    value
            );
            participants.put(nodeId, new ParticipantTrialMetrics(nodeId, redo, 2L));
        }
        return new TransactionTrialRecord(
                TransactionId.newTransaction(),
                context.specificationId(),
                SelectedProtocol.TWO_PC,
                committed ? TrialOutcome.COMMITTED : TrialOutcome.ABORTED,
                Duration.ofMillis(latencyMs),
                Optional.of(Duration.ofMillis(latencyMs / 2)),
                Optional.of(Duration.ofMillis(latencyMs / 2)),
                participants,
                5L,
                2L
        );
    }

    private static TransactionContext context(List<String> participants) {
        return new TransactionContext(
                UUID.randomUUID(), "GENERIC_TEST", participants,
                new TransactionShape(2, 1, 1, 0, 4, 0.5), Map.of("custom_cost", 7.0)
        );
    }
}
