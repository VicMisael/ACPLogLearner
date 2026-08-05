package ufc.victor.db.mariadb;

import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.participant.TransactionalResource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public final class MariaDbXaTransactionalResource implements TransactionalResource {

    private final MariaDbXaConfig config;
    private final String participantId;
    private final MariaDbXaWorkload workload;

    public MariaDbXaTransactionalResource(
            MariaDbXaConfig config,
            String participantId,
            MariaDbXaWorkload workload
    ) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (participantId == null || participantId.isBlank()) {
            throw new IllegalArgumentException("participantId must not be blank");
        }
        if (workload == null) {
            throw new IllegalArgumentException("workload must not be null");
        }
        this.config = config;
        this.participantId = participantId;
        this.workload = workload;
    }

    @Override
    public boolean prepare(TransactionId txId) {
        boolean started = false;
        boolean ended = false;
        try (Connection connection = openConnection()) {
            executeXa(connection, "XA START %s".formatted(xaIdLiteral(txId)));
            started = true;
            workload.executeWithinXa(connection, txId);
            executeXa(connection, "XA END %s".formatted(xaIdLiteral(txId)));
            ended = true;
            executeXa(connection, "XA PREPARE %s".formatted(xaIdLiteral(txId)));
            return true;
        } catch (SQLException e) {
            recoverPrepareFailure(txId, started, ended);
            return false;
        }
    }

    @Override
    public void commit(TransactionId txId) {
        try (Connection connection = openConnection()) {
            executeXa(connection, "XA COMMIT %s".formatted(xaIdLiteral(txId)));
        } catch (SQLException e) {
            if (!isUnknownBranch(e)) {
                throw new IllegalStateException("Failed to XA COMMIT txId=" + txId, e);
            }
        }

        try (Connection connection = openConnection()) {
            workload.afterCommit(connection, txId);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to record COMMITTED state for txId=" + txId, e);
        }
    }

    @Override
    public void abort(TransactionId txId) {
        try (Connection connection = openConnection()) {
            executeXa(connection, "XA ROLLBACK %s".formatted(xaIdLiteral(txId)));
        } catch (SQLException e) {
            if (!isUnknownBranch(e)) {
                throw new IllegalStateException("Failed to XA ROLLBACK txId=" + txId, e);
            }
        }

        try (Connection connection = openConnection()) {
            workload.afterAbort(connection, txId);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to record ABORTED state for txId=" + txId, e);
        }
    }

    private void recoverPrepareFailure(TransactionId txId, boolean started, boolean ended) {
        if (!started) {
            return;
        }

        try (Connection recoveryConnection = openConnection()) {
            if (!ended) {
                try {
                    executeXa(recoveryConnection, "XA END %s".formatted(xaIdLiteral(txId)));
                } catch (SQLException ignored) {
                    // best effort
                }
            }
            try {
                executeXa(recoveryConnection, "XA ROLLBACK %s".formatted(xaIdLiteral(txId)));
            } catch (SQLException ignored) {
                // best effort
            }
        } catch (SQLException ignored) {
            // best effort
        }
    }

    private Connection openConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(
                config.jdbcUrl(),
                config.user(),
                config.password()
        );
        connection.setAutoCommit(true);
        return connection;
    }

    private static void executeXa(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private String xaIdLiteral(TransactionId txId) {
        return "'%s','%s',1".formatted(escapeSql(txId.toString()), escapeSql(participantId));
    }

    private static boolean isUnknownBranch(SQLException exception) {
        String message = exception.getMessage();
        return message != null && (message.contains("XAER_NOTA") || message.contains("Unknown XID"));
    }

    private static String escapeSql(String value) {
        return value.replace("'", "''");
    }
}
