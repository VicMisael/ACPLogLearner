package ufc.victor.db.mariadb;

import org.junit.jupiter.api.Test;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.telemetry.redo.RedoMetricsCollectionException;
import ufc.victor.telemetry.redo.RedoSnapshot;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MariaDbWalMetricsCollectorTest {

    private static final NodeId P1 = new NodeId("P1");
    private static final MariaDbXaConfig CONFIG = new MariaDbXaConfig(
            "jdbc:mariadb://example/acp", "user", "password"
    );

    @Test
    void mapsMariaDbStatusCaseInsensitivelyAndPreservesMissingMetrics() {
        Map<String, String> status = new LinkedHashMap<>();
        status.put("UPTIME", "42");
        status.put("INNODB_LSN_CURRENT", "1200");
        status.put("Innodb_lsn_flushed", "1180");
        status.put("Innodb_lsn_last_checkpoint", "1000");
        status.put("Innodb_os_log_written", "777");
        status.put("Innodb_log_write_requests", "12");
        status.put("Innodb_log_writes", "4");

        MariaDbWalMetricsCollector collector = collector(config -> status);
        RedoSnapshot snapshot = collector.capture(P1);

        assertEquals(Instant.parse("2026-01-01T00:00:00Z"), snapshot.capturedAt());
        assertEquals(42, snapshot.serverUptimeSeconds().orElseThrow());
        assertEquals(1200, snapshot.currentLsn().orElseThrow());
        assertEquals(200, snapshot.checkpointAgeBytes().orElseThrow());
        assertEquals(777, snapshot.redoBytesWritten().orElseThrow());
        assertTrue(snapshot.logWaits().isEmpty());
    }

    @Test
    void wrapsSqlFailuresWithNodeContext() {
        MariaDbWalMetricsCollector collector = collector(config -> {
            throw new SQLException("offline");
        });

        RedoMetricsCollectionException exception = assertThrows(
                RedoMetricsCollectionException.class,
                () -> collector.capture(P1)
        );
        assertTrue(exception.getMessage().contains("P1"));
        assertTrue(exception.getCause() instanceof SQLException);
    }

    @Test
    void rejectsUnknownNodeBeforeReadingStatus() {
        MariaDbWalMetricsCollector collector = collector(config -> Map.of());
        assertThrows(IllegalArgumentException.class, () -> collector.capture(new NodeId("P2")));
    }

    private static MariaDbWalMetricsCollector collector(
            MariaDbWalMetricsCollector.StatusReader reader
    ) {
        return new MariaDbWalMetricsCollector(
                Map.of(P1, CONFIG),
                reader,
                Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC)
        );
    }
}
