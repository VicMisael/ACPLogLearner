package ufc.victor.telemetry.redo;

import ufc.victor.protocol.coordinator.node.NodeId;

import java.time.Instant;
import java.util.Objects;
import java.util.OptionalLong;

/** One point-in-time view of cumulative redo-log status for a database node. */
public record RedoSnapshot(
        NodeId nodeId,
        Instant capturedAt,
        OptionalLong serverUptimeSeconds,
        OptionalLong currentLsn,
        OptionalLong flushedLsn,
        OptionalLong lastCheckpointLsn,
        OptionalLong redoBytesWritten,
        OptionalLong logWriteRequests,
        OptionalLong logWrites,
        OptionalLong logWaits
) {
    public RedoSnapshot {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(capturedAt, "capturedAt");
        Objects.requireNonNull(serverUptimeSeconds, "serverUptimeSeconds");
        Objects.requireNonNull(currentLsn, "currentLsn");
        Objects.requireNonNull(flushedLsn, "flushedLsn");
        Objects.requireNonNull(lastCheckpointLsn, "lastCheckpointLsn");
        Objects.requireNonNull(redoBytesWritten, "redoBytesWritten");
        Objects.requireNonNull(logWriteRequests, "logWriteRequests");
        Objects.requireNonNull(logWrites, "logWrites");
        Objects.requireNonNull(logWaits, "logWaits");
    }

    public OptionalLong checkpointAgeBytes() {
        return subtractIfOrdered(currentLsn, lastCheckpointLsn);
    }

    private static OptionalLong subtractIfOrdered(OptionalLong upper, OptionalLong lower) {
        if (upper.isEmpty() || lower.isEmpty() || upper.getAsLong() < lower.getAsLong()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(upper.getAsLong() - lower.getAsLong());
    }
}
