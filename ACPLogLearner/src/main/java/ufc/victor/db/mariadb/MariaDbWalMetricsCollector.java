package ufc.victor.db.mariadb;

import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.telemetry.redo.RedoMetricsCollectionException;
import ufc.victor.telemetry.redo.RedoMetricsCollector;
import ufc.victor.telemetry.redo.RedoSnapshot;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/** Reads MariaDB global InnoDB redo counters over a dedicated JDBC connection. */
public final class MariaDbWalMetricsCollector implements RedoMetricsCollector {

    private static final String STATUS_QUERY = """
            SHOW GLOBAL STATUS WHERE Variable_name IN (
                'Uptime',
                'Innodb_lsn_current',
                'Innodb_lsn_flushed',
                'Innodb_lsn_last_checkpoint',
                'Innodb_os_log_written',
                'Innodb_log_write_requests',
                'Innodb_log_writes',
                'Innodb_log_waits'
            )
            """;

    private final Map<NodeId, MariaDbXaConfig> configs;
    private final StatusReader statusReader;
    private final Clock clock;

    public MariaDbWalMetricsCollector(Map<NodeId, MariaDbXaConfig> configs) {
        this(configs, MariaDbWalMetricsCollector::readGlobalStatus, Clock.systemUTC());
    }

    MariaDbWalMetricsCollector(
            Map<NodeId, MariaDbXaConfig> configs,
            StatusReader statusReader,
            Clock clock
    ) {
        this.configs = Map.copyOf(Objects.requireNonNull(configs, "configs"));
        this.statusReader = Objects.requireNonNull(statusReader, "statusReader");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public RedoSnapshot capture(NodeId nodeId) {
        MariaDbXaConfig config = configs.get(Objects.requireNonNull(nodeId, "nodeId"));
        if (config == null) throw new IllegalArgumentException("No MariaDB config for " + nodeId);

        try {
            Map<String, String> status = normalize(statusReader.read(config));
            return new RedoSnapshot(
                    nodeId,
                    clock.instant(),
                    numeric(status, "uptime"),
                    numeric(status, "innodb_lsn_current"),
                    numeric(status, "innodb_lsn_flushed"),
                    numeric(status, "innodb_lsn_last_checkpoint"),
                    numeric(status, "innodb_os_log_written"),
                    numeric(status, "innodb_log_write_requests"),
                    numeric(status, "innodb_log_writes"),
                    numeric(status, "innodb_log_waits")
            );
        } catch (SQLException | NumberFormatException exception) {
            throw new RedoMetricsCollectionException(
                    "Could not capture MariaDB redo metrics for " + nodeId,
                    exception
            );
        }
    }

    private static Map<String, String> readGlobalStatus(MariaDbXaConfig config) throws SQLException {
        Map<String, String> result = new LinkedHashMap<>();
        try (Connection connection = DriverManager.getConnection(
                config.jdbcUrl(), config.user(), config.password()
        ); Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(STATUS_QUERY)) {
            while (rows.next()) result.put(rows.getString(1), rows.getString(2));
        }
        return result;
    }

    private static Map<String, String> normalize(Map<String, String> raw) {
        Map<String, String> normalized = new LinkedHashMap<>();
        raw.forEach((name, value) -> normalized.put(name.toLowerCase(Locale.ROOT), value));
        return normalized;
    }

    private static OptionalLong numeric(Map<String, String> status, String name) {
        String value = status.get(name);
        return value == null || value.isBlank()
                ? OptionalLong.empty()
                : OptionalLong.of(Long.parseLong(value));
    }

    @FunctionalInterface
    interface StatusReader {
        Map<String, String> read(MariaDbXaConfig config) throws SQLException;
    }
}
