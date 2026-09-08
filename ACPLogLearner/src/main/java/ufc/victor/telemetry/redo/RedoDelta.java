package ufc.victor.telemetry.redo;

import ufc.victor.protocol.coordinator.node.NodeId;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalLong;

/** Difference between two snapshots surrounding one measured operation. */
public record RedoDelta(
        NodeId nodeId,
        Duration elapsed,
        boolean serverRestartDetected,
        boolean counterResetDetected,
        OptionalLong lsnAdvanced,
        OptionalLong flushedLsnAdvanced,
        OptionalLong redoBytesWritten,
        OptionalLong logWriteRequests,
        OptionalLong logWrites,
        OptionalLong logWaits,
        OptionalLong checkpointAgeBeforeBytes,
        OptionalLong checkpointAgeAfterBytes
) {
    public RedoDelta {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(elapsed, "elapsed");
        Objects.requireNonNull(lsnAdvanced, "lsnAdvanced");
        Objects.requireNonNull(flushedLsnAdvanced, "flushedLsnAdvanced");
        Objects.requireNonNull(redoBytesWritten, "redoBytesWritten");
        Objects.requireNonNull(logWriteRequests, "logWriteRequests");
        Objects.requireNonNull(logWrites, "logWrites");
        Objects.requireNonNull(logWaits, "logWaits");
        Objects.requireNonNull(checkpointAgeBeforeBytes, "checkpointAgeBeforeBytes");
        Objects.requireNonNull(checkpointAgeAfterBytes, "checkpointAgeAfterBytes");
        if (elapsed.isNegative()) throw new IllegalArgumentException("elapsed must not be negative");
    }

    public static RedoDelta between(RedoSnapshot before, RedoSnapshot after) {
        Objects.requireNonNull(before, "before");
        Objects.requireNonNull(after, "after");
        if (!before.nodeId().equals(after.nodeId())) {
            throw new IllegalArgumentException("snapshots must belong to the same node");
        }
        Duration elapsed = Duration.between(before.capturedAt(), after.capturedAt());
        if (elapsed.isNegative()) {
            throw new IllegalArgumentException("after snapshot precedes before snapshot");
        }

        boolean restarted = decreased(before.serverUptimeSeconds(), after.serverUptimeSeconds());
        boolean reset = restarted
                || decreased(before.currentLsn(), after.currentLsn())
                || decreased(before.flushedLsn(), after.flushedLsn())
                || decreased(before.redoBytesWritten(), after.redoBytesWritten())
                || decreased(before.logWriteRequests(), after.logWriteRequests())
                || decreased(before.logWrites(), after.logWrites())
                || decreased(before.logWaits(), after.logWaits());

        return new RedoDelta(
                before.nodeId(),
                elapsed,
                restarted,
                reset,
                delta(before.currentLsn(), after.currentLsn(), restarted),
                delta(before.flushedLsn(), after.flushedLsn(), restarted),
                delta(before.redoBytesWritten(), after.redoBytesWritten(), restarted),
                delta(before.logWriteRequests(), after.logWriteRequests(), restarted),
                delta(before.logWrites(), after.logWrites(), restarted),
                delta(before.logWaits(), after.logWaits(), restarted),
                restarted ? OptionalLong.empty() : before.checkpointAgeBytes(),
                restarted ? OptionalLong.empty() : after.checkpointAgeBytes()
        );
    }

    private static boolean decreased(OptionalLong before, OptionalLong after) {
        return before.isPresent() && after.isPresent() && after.getAsLong() < before.getAsLong();
    }

    private static OptionalLong delta(OptionalLong before, OptionalLong after, boolean invalidated) {
        if (invalidated || before.isEmpty() || after.isEmpty()
                || after.getAsLong() < before.getAsLong()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(after.getAsLong() - before.getAsLong());
    }
}
