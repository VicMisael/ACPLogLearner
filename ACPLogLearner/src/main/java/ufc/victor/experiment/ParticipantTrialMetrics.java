package ufc.victor.experiment;

import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.telemetry.redo.RedoDelta;

import java.util.Objects;

public record ParticipantTrialMetrics(
        NodeId nodeId,
        RedoDelta redo,
        long protocolLogWrites
) {
    public ParticipantTrialMetrics {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(redo, "redo");
        if (!nodeId.equals(redo.nodeId())) {
            throw new IllegalArgumentException("redo metrics must belong to the participant node");
        }
        if (protocolLogWrites < 0L) {
            throw new IllegalArgumentException("protocolLogWrites must not be negative");
        }
    }

    public boolean hasUnavailableRedoMetric() {
        return redo.lsnAdvanced().isEmpty()
                || redo.flushedLsnAdvanced().isEmpty()
                || redo.redoBytesWritten().isEmpty()
                || redo.logWriteRequests().isEmpty()
                || redo.logWrites().isEmpty()
                || redo.logWaits().isEmpty()
                || redo.checkpointAgeBeforeBytes().isEmpty()
                || redo.checkpointAgeAfterBytes().isEmpty();
    }
}
