package ufc.victor.experiment;

import ufc.victor.LogFeatures;
import ufc.victor.protocol.SelectedProtocol;
import ufc.victor.protocol.coordinator.log.CoordinatorLogRecordType;
import ufc.victor.protocol.coordinator.log.LogRecord;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.protocol.participant.log.ParticipantPhase;
import ufc.victor.protocol.participant.log.ParticipantLogRecord;
import ufc.victor.protocol.participant.log.ParticipantLogRecordType;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
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
        List<LogRecord> coordinatorSnapshot = List.copyOf(coordinatorRecords);
        Map<NodeId, List<ParticipantLogRecord>> participantSnapshot = copyParticipantLogs(participantRecords);

        TxMetrics metrics = new TxMetrics(
                computeLatencyMs(scenario, coordinatorSnapshot, elapsedMs),
                wasCommitted(coordinatorSnapshot),
                messageCount,
                computeTotalLogWrites(coordinatorSnapshot, participantSnapshot),
                computeBlockingTimeMs(participantSnapshot)
        );

        return new SimulationResult(
                scenario,
                protocol,
                metrics,
                coordinatorSnapshot,
                participantSnapshot
        );
    }

    private static Map<NodeId, List<ParticipantLogRecord>> copyParticipantLogs(
            Map<NodeId, List<ParticipantLogRecord>> participantRecords
    ) {
        Map<NodeId, List<ParticipantLogRecord>> copy = new LinkedHashMap<>();
        for (Map.Entry<NodeId, List<ParticipantLogRecord>> entry : participantRecords.entrySet()) {
            copy.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return Map.copyOf(copy);
    }

    private static long computeLatencyMs(Scenario scenario, List<LogRecord> coordinatorRecords, long elapsedMs) {
        Instant start = null;
        Instant end = null;

        for (LogRecord record : coordinatorRecords) {
            if (record.type() == CoordinatorLogRecordType.BEGIN_COMMIT && start == null) {
                start = record.timestamp();
            }
            if (isTerminalRecord(record.type())) {
                end = record.timestamp();
            }
        }

        if (start != null && end != null) {
            return Math.max(0L, end.toEpochMilli() - start.toEpochMilli());
        }
        return Math.min(elapsedMs, scenario.timeoutMs());
    }

    private static boolean isTerminalRecord(CoordinatorLogRecordType type) {
        return type == CoordinatorLogRecordType.GLOBAL_ABORT
                || type == CoordinatorLogRecordType.GLOBAL_COMMIT
                || type == CoordinatorLogRecordType.END_TRANSACTION;
    }

    private static boolean wasCommitted(List<LogRecord> coordinatorRecords) {
        boolean hasAbort = coordinatorRecords.stream()
                .anyMatch(r -> r.type() == CoordinatorLogRecordType.GLOBAL_ABORT);
        if (hasAbort) {
            return false;
        }
        return coordinatorRecords.stream()
                .anyMatch(r -> r.type() == CoordinatorLogRecordType.GLOBAL_COMMIT);
    }

    private static long computeTotalLogWrites(
            List<LogRecord> coordinatorRecords,
            Map<NodeId, List<ParticipantLogRecord>> participantRecords
    ) {
        long total = coordinatorRecords.size();
        for (List<ParticipantLogRecord> records : participantRecords.values()) {
            total += records.size();
        }
        return total;
    }

    private static long computeBlockingTimeMs(Map<NodeId, List<ParticipantLogRecord>> participantRecords) {
        long maxBlocking = 0L;
        for (List<ParticipantLogRecord> records : participantRecords.values()) {
            Instant readyAt = null;
            Instant terminalAt = null;

            for (ParticipantLogRecord record : records) {
                if (record.type() == ParticipantLogRecordType.READY && readyAt == null) {
                    readyAt = record.timestamp();
                }
                if (readyAt != null && isTerminalParticipantRecord(record.type())) {
                    terminalAt = record.timestamp();
                    break;
                }
            }

            if (readyAt != null && terminalAt != null) {
                long blockingMs = terminalAt.toEpochMilli() - readyAt.toEpochMilli();
                if (blockingMs > maxBlocking) {
                    maxBlocking = blockingMs;
                }
            }
        }
        return Math.max(0L, maxBlocking);
    }

    private static boolean isTerminalParticipantRecord(ParticipantLogRecordType type) {
        return type == ParticipantLogRecordType.COMMIT || type == ParticipantLogRecordType.ABORT;
    }

    public double observedAbortRate(SimulationResult result) {
        int participants = result.participantLogRecords().size();
        if (participants == 0) {
            return 0.0;
        }

        int aborted = 0;
        for (List<ParticipantLogRecord> records : result.participantLogRecords().values()) {
            ParticipantLogRecord terminal = last(records);
            if (terminal != null && terminal.type() == ParticipantLogRecordType.ABORT) {
                aborted++;
            }
        }
        return ((double) aborted) / participants;
    }

    public LogFeatures extractFeatures(SimulationResult result) {
        return new LogFeatures(
                observedAverageNetworkLatencyMs(result),
                observedMaxNetworkLatencyMs(result),
                observedAverageDiskIoTimeMs(result),
                observedMaxDiskIoTimeMs(result),
                observedAbortRate(result),
                result.participantLogRecords().size()
        );
    }

    public double observedAverageNetworkLatencyMs(SimulationResult result) {
        long totalLatencyMs = 0L;
        int samples = 0;

        for (List<ParticipantLogRecord> records : result.participantLogRecords().values()) {
            for (ParticipantLogRecord record : records) {
                if (record.phase() == ParticipantPhase.TERMINATION) {
                    continue;
                }
                if (record.triggerTimestamp() == null || record.phaseStartedAt() == null) {
                    continue;
                }

                long sample = Duration.between(record.triggerTimestamp(), record.phaseStartedAt()).toMillis();
                if (sample >= 0L) {
                    totalLatencyMs += sample;
                    samples++;
                }
            }
        }

        if (samples == 0) {
            return 0.0;
        }
        return ((double) totalLatencyMs) / samples;
    }

    public double observedMaxNetworkLatencyMs(SimulationResult result) {
        long maxLatencyMs = 0L;

        for (List<ParticipantLogRecord> records : result.participantLogRecords().values()) {
            for (ParticipantLogRecord record : records) {
                if (record.phase() == ParticipantPhase.TERMINATION) {
                    continue;
                }
                if (record.triggerTimestamp() == null || record.phaseStartedAt() == null) {
                    continue;
                }

                long sample = Duration.between(record.triggerTimestamp(), record.phaseStartedAt()).toMillis();
                if (sample >= 0L && sample > maxLatencyMs) {
                    maxLatencyMs = sample;
                }
            }
        }

        return maxLatencyMs;
    }

    public double observedAverageDiskIoTimeMs(SimulationResult result) {
        long totalDiskMs = 0L;
        int samples = 0;

        for (List<ParticipantLogRecord> records : result.participantLogRecords().values()) {
            for (ParticipantLogRecord record : records) {
                if (record.phase() != ParticipantPhase.PREPARE) {
                    continue;
                }
                totalDiskMs += Math.max(0L, record.phaseDurationMs());
                samples++;
            }
        }

        if (samples == 0) {
            return 0.0;
        }
        return ((double) totalDiskMs) / samples;
    }

    public double observedMaxDiskIoTimeMs(SimulationResult result) {
        long maxDiskMs = 0L;

        for (List<ParticipantLogRecord> records : result.participantLogRecords().values()) {
            for (ParticipantLogRecord record : records) {
                if (record.phase() != ParticipantPhase.PREPARE) {
                    continue;
                }
                long sample = Math.max(0L, record.phaseDurationMs());
                if (sample > maxDiskMs) {
                    maxDiskMs = sample;
                }
            }
        }

        return maxDiskMs;
    }

    private static ParticipantLogRecord last(List<ParticipantLogRecord> records) {
        if (records.isEmpty()) {
            return null;
        }
        return records.get(records.size() - 1);
    }
}
