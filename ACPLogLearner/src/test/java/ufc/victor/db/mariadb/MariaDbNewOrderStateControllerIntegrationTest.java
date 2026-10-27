package ufc.victor.db.mariadb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "ACP_MARIADB_INTEGRATION_TEST", matches = "true")
class MariaDbNewOrderStateControllerIntegrationTest {

    @Test
    void reusesPhysicalConnectionsAndResetsSessionAfterFailedOperation() throws Exception {
        try (MariaDbNewOrderStateController controller = controller()) {
            Set<Long> connectionIds = new HashSet<>();
            for (int index = 0; index < 200; index++) {
                try (Connection connection = controller.open(1)) {
                    connectionIds.add(scalar(connection, "SELECT CONNECTION_ID()"));
                }
            }
            assertTrue(connectionIds.size() <= 2, "connections must remain bounded by the pool");

            long original;
            try (Connection connection = controller.open(1)) {
                original = scalar(connection,
                        "SELECT d_next_o_id FROM bmsql_district WHERE d_w_id = 1 AND d_id = 1");
            }
            assertThrows(IllegalStateException.class, () -> {
                try (Connection connection = controller.open(1);
                     Statement statement = connection.createStatement()) {
                    connection.setAutoCommit(false);
                    statement.executeUpdate("UPDATE bmsql_district SET d_next_o_id = d_next_o_id + 1 "
                            + "WHERE d_w_id = 1 AND d_id = 1");
                    statement.execute("SET @acp_pool_test = 123");
                    throw new IllegalStateException("simulated operation failure");
                }
            });

            // Borrow both slots so a second clean connection cannot hide a dirty returned session.
            try (Connection first = controller.open(1);
                 Connection second = controller.open(1)) {
                for (Connection connection : new Connection[]{first, second}) {
                    assertTrue(connection.getAutoCommit());
                    assertEquals(1, scalar(connection, "SELECT @acp_pool_test IS NULL"));
                    assertEquals(original, scalar(connection,
                            "SELECT d_next_o_id FROM bmsql_district WHERE d_w_id = 1 AND d_id = 1"));
                }
            }
        }
    }

    @Test
    void closesPoolsOnExceptionalExitAndRejectsFurtherBorrowing() throws Exception {
        MariaDbNewOrderStateController controller = controller();
        assertThrows(IllegalStateException.class, () -> {
            try (controller; Connection connection = controller.open(1)) {
                assertTrue(connection.isValid(2));
                throw new IllegalStateException("simulated experiment failure");
            }
        });
        assertThrows(SQLException.class, () -> controller.open(1));
        assertDoesNotThrow(controller::close);
    }

    private static MariaDbNewOrderStateController controller() throws SQLException {
        return new MariaDbNewOrderStateController(Map.of(1, new MariaDbXaConfig(
                System.getenv().getOrDefault("ACP_MARIADB_1_URL", "jdbc:mariadb://127.0.0.1:33061/acp"),
                System.getenv().getOrDefault("ACP_MARIADB_USER", "acp"),
                System.getenv().getOrDefault("ACP_MARIADB_PASSWORD", "acp_secret")
        )));
    }

    private static long scalar(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getLong(1);
        }
    }
}
