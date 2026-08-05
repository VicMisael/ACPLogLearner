package ufc.victor.db.mariadb;

import ufc.victor.protocol.commom.TransactionId;

import java.sql.Connection;
import java.sql.SQLException;

public interface MariaDbXaWorkload {

    void executeWithinXa(Connection connection, TransactionId txId) throws SQLException;

    default void afterCommit(Connection connection, TransactionId txId) throws SQLException {
        // optional hook
    }

    default void afterAbort(Connection connection, TransactionId txId) throws SQLException {
        // optional hook
    }
}
