package ufc.victor.db.mariadb;

import ufc.victor.protocol.commom.TransactionId;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class MariaDbTransactionJournalWorkload implements MariaDbXaWorkload {

    private final String participantId;
    private final String tableName;

    public MariaDbTransactionJournalWorkload(String participantId, String tableName) {
        if (participantId == null || participantId.isBlank()) {
            throw new IllegalArgumentException("participantId must not be blank");
        }
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName must not be blank");
        }
        this.participantId = participantId;
        this.tableName = tableName;
    }

    @Override
    public void executeWithinXa(Connection connection, TransactionId txId) throws SQLException {
        upsertStatus(connection, txId, "PREPARED");
    }

    @Override
    public void afterCommit(Connection connection, TransactionId txId) throws SQLException {
        upsertStatus(connection, txId, "COMMITTED");
    }

    @Override
    public void afterAbort(Connection connection, TransactionId txId) throws SQLException {
        upsertStatus(connection, txId, "ABORTED");
    }

    private void upsertStatus(Connection connection, TransactionId txId, String status) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO %s (tx_id, participant_id, status)
                VALUES (?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    status = VALUES(status),
                    updated_at = CURRENT_TIMESTAMP
                """.formatted(tableName))) {
            statement.setString(1, txId.toString());
            statement.setString(2, participantId);
            statement.setString(3, status);
            statement.executeUpdate();
        }
    }
}
