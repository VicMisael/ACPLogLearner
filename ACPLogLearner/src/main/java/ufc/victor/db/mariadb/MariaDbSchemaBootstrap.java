package ufc.victor.db.mariadb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public final class MariaDbSchemaBootstrap {

    private MariaDbSchemaBootstrap() {
    }

    public static void ensureJournalTable(MariaDbXaConfig config, String tableName) throws SQLException {
        try (Connection connection = DriverManager.getConnection(
                config.jdbcUrl(),
                config.user(),
                config.password()
        ); Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                        tx_id VARCHAR(36) NOT NULL,
                        participant_id VARCHAR(64) NOT NULL,
                        status VARCHAR(32) NOT NULL,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (tx_id, participant_id)
                    )
                    """.formatted(tableName));
        }
    }
}
