package ufc.victor.protocol.participant.log;

import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.time.Duration;
import java.time.Instant;

public record ParticipantLogRecord(
        TransactionId txId,
        NodeId nodeId,
        ParticipantLogRecordType type,
        ParticipantPhase phase,
        Instant triggerTimestamp,
        Instant phaseStartedAt,
        Instant timestamp,
        long phaseDurationMs
) {

    public static ParticipantLogRecord of(
            TransactionId txId,
            NodeId nodeId,
            ParticipantLogRecordType type,
            ParticipantPhase phase,
            Instant triggerTimestamp,
            Instant phaseStartedAt
    ) {
        Instant writeTimestamp = Instant.now();
        Instant effectivePhaseStart = phaseStartedAt != null ? phaseStartedAt : writeTimestamp;
        Instant effectiveTrigger = triggerTimestamp != null ? triggerTimestamp : effectivePhaseStart;
        long durationMs = Math.max(0L, Duration.between(effectivePhaseStart, writeTimestamp).toMillis());

        return new ParticipantLogRecord(
                txId,
                nodeId,
                type,
                phase,
                effectiveTrigger,
                effectivePhaseStart,
                writeTimestamp,
                durationMs
        );
    }
}

