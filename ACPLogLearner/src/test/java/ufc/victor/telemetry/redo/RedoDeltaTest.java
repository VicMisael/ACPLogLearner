package ufc.victor.telemetry.redo;

import org.junit.jupiter.api.Test;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.time.Instant;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedoDeltaTest {

    @Test
    void calculatesCounterAndCheckpointDeltas() {
        RedoDelta delta = RedoDelta.between(
                snapshot("P1", "2026-01-01T00:00:00Z", 100, 1_000, 990, 800, 5_000, 50, 20, 1),
                snapshot("P1", "2026-01-01T00:00:02Z", 102, 1_150, 1_140, 900, 5_140, 58, 23, 2)
        );

        assertEquals(2_000, delta.elapsed().toMillis());
        assertEquals(150, delta.lsnAdvanced().orElseThrow());
        assertEquals(150, delta.flushedLsnAdvanced().orElseThrow());
        assertEquals(140, delta.redoBytesWritten().orElseThrow());
        assertEquals(8, delta.logWriteRequests().orElseThrow());
        assertEquals(3, delta.logWrites().orElseThrow());
        assertEquals(1, delta.logWaits().orElseThrow());
        assertEquals(200, delta.checkpointAgeBeforeBytes().orElseThrow());
        assertEquals(250, delta.checkpointAgeAfterBytes().orElseThrow());
        assertFalse(delta.counterResetDetected());
    }

    @Test
    void marksRestartAndInvalidatesAllCrossRestartDeltas() {
        RedoDelta delta = RedoDelta.between(
                snapshot("P1", "2026-01-01T00:00:00Z", 100, 1_000, 990, 800, 5_000, 50, 20, 1),
                snapshot("P1", "2026-01-01T00:00:02Z", 2, 1_100, 1_090, 900, 5_100, 55, 22, 1)
        );

        assertTrue(delta.serverRestartDetected());
        assertTrue(delta.counterResetDetected());
        assertTrue(delta.lsnAdvanced().isEmpty());
        assertTrue(delta.redoBytesWritten().isEmpty());
        assertTrue(delta.checkpointAgeAfterBytes().isEmpty());
    }

    @Test
    void invalidatesOnlyAnIndividualCounterThatWasReset() {
        RedoDelta delta = RedoDelta.between(
                snapshot("P1", "2026-01-01T00:00:00Z", 100, 1_000, 990, 800, 5_000, 50, 20, 3),
                snapshot("P1", "2026-01-01T00:00:02Z", 102, 1_100, 1_090, 850, 5_090, 55, 22, 0)
        );

        assertTrue(delta.counterResetDetected());
        assertTrue(delta.logWaits().isEmpty());
        assertEquals(100, delta.lsnAdvanced().orElseThrow());
        assertEquals(90, delta.redoBytesWritten().orElseThrow());
    }

    @Test
    void rejectsSnapshotsFromDifferentNodes() {
        assertThrows(IllegalArgumentException.class, () -> RedoDelta.between(
                snapshot("P1", "2026-01-01T00:00:00Z", 1, 1, 1, 1, 1, 1, 1, 1),
                snapshot("P2", "2026-01-01T00:00:01Z", 2, 2, 2, 2, 2, 2, 2, 2)
        ));
    }

    private static RedoSnapshot snapshot(
            String node,
            String instant,
            long uptime,
            long current,
            long flushed,
            long checkpoint,
            long bytes,
            long requests,
            long writes,
            long waits
    ) {
        return new RedoSnapshot(
                new NodeId(node), Instant.parse(instant), OptionalLong.of(uptime),
                OptionalLong.of(current), OptionalLong.of(flushed), OptionalLong.of(checkpoint),
                OptionalLong.of(bytes), OptionalLong.of(requests), OptionalLong.of(writes),
                OptionalLong.of(waits)
        );
    }
}
