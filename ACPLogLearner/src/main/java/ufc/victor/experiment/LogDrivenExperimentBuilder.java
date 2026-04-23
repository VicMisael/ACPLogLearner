package ufc.victor.experiment;

import ufc.victor.protocol.SelectedProtocol;
import ufc.victor.protocol.coordinator.log.CoordinatorLogRecordType;
import ufc.victor.protocol.coordinator.log.LogRecord;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.protocol.participant.log.ParticipantLogRecord;
import ufc.victor.protocol.participant.log.ParticipantLogRecordType;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class LogDrivenExperimentBuilder {

    public SimulationResult build(
            Scenario scenario,
            SelectedProtocol protocol,
            long elapsedMs,
            long messageCount,
            List<LogRecord> coordinatorRecords,
            Map<NodeId, List<ParticipantLogRecord>> participantRecords
    ) {
        TxMetrics metrics = new TxMetrics(
                elapsedMs,
                isCommitted(coordinatorRecords),
                messageCount,
                totalLogWrites(coordinatorRecords, participantRecords),
                blockingTimeMs(elapsedMs, coordinatorRecords, participantRecords)
        );
        return new SimulationResult(
                scenario,
                protocol,
                metrics,
                List.copyOf(coordinatorRecords),
                Map.copyOf(participantRecords)
        );
    }

    private static boolean isCommitted(List<LogRecord> coordinatorRecords) {
        boolean sawCommit = false;

        for (LogRecord record : coordinatorRecords) {
            if (record.type() == CoordinatorLogRecordType.GLOBAL_ABORT) {
                return false;
            }
            if (record.type() == CoordinatorLogRecordType.GLOBAL_COMMIT) {
                sawCommit = true;
            }
        }

        return sawCommit;
    }

    private static long totalLogWrites(
            List<LogRecord> coordinatorRecords,
            Map<NodeId, List<ParticipantLogRecord>> participantRecords
    ) {
        long total = coordinatorRecords.size();
        for (List<ParticipantLogRecord> records : participantRecords.values()) {
            total += records.size();
        }
        return total;
    }

    private static long blockingTimeMs(
            long elapsedMs,
            List<LogRecord> coordinatorRecords,
            Map<NodeId, List<ParticipantLogRecord>> participantRecords
    ) {
        Instant fallbackEnd = coordinatorRecords.isEmpty()
                ? Instant.now()
                : coordinatorRecords.get(coordinatorRecords.size() - 1).timestamp();
        long maxBlockingTime = 0L;

        for (List<ParticipantLogRecord> records : participantRecords.values()) {
            Instant readyAt = null;
            Instant resolvedAt = null;

            for (ParticipantLogRecord record : records) {
                if (readyAt == null && record.type() == ParticipantLogRecordType.READY) {
                    readyAt = record.timestamp();
                } else if (readyAt != null
                        && (record.type() == ParticipantLogRecordType.ABORT
                        || record.type() == ParticipantLogRecordType.COMMIT)) {
                    resolvedAt = record.timestamp();
                    break;
                }
            }

            if (readyAt != null) {
                Instant end = resolvedAt != null ? resolvedAt : fallbackEnd.plusMillis(elapsedMs);
                maxBlockingTime = Math.max(
                        maxBlockingTime,
                        Math.max(0L, end.toEpochMilli() - readyAt.toEpochMilli())
                );
            }
        }

        return maxBlockingTime;
    }
}
