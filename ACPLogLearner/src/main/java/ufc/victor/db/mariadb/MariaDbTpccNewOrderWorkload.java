package ufc.victor.db.mariadb;

import ufc.victor.protocol.commom.TransactionId;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/** A compact BenchmarkSQL-schema implementation of the TPC-C New-Order transaction. */
public final class MariaDbTpccNewOrderWorkload implements MariaDbXaWorkload {

    private final int warehouseId;
    private final int districtId;
    private final int customerId;
    private final List<OrderLine> lines;

    public MariaDbTpccNewOrderWorkload(
            int warehouseId,
            int districtId,
            int customerId,
            List<OrderLine> lines
    ) {
        if (warehouseId < 1) throw new IllegalArgumentException("warehouseId must be positive");
        if (districtId < 1 || districtId > 10) throw new IllegalArgumentException("districtId must be between 1 and 10");
        if (customerId < 1) throw new IllegalArgumentException("customerId must be positive");
        if (lines == null || lines.size() < 5 || lines.size() > 15) {
            throw new IllegalArgumentException("New-Order requires between 5 and 15 order lines");
        }
        this.warehouseId = warehouseId;
        this.districtId = districtId;
        this.customerId = customerId;
        this.lines = List.copyOf(lines);
    }

    @Override
    public void executeWithinXa(Connection connection, TransactionId txId) throws SQLException {
        requireCustomer(connection);
        int orderId = reserveOrderId(connection);
        insertOrder(connection, orderId);
        insertNewOrder(connection, orderId);

        for (int lineNumber = 1; lineNumber <= lines.size(); lineNumber++) {
            applyOrderLine(connection, orderId, lineNumber, lines.get(lineNumber - 1));
        }
    }

    private void requireCustomer(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT c_id FROM bmsql_customer
                WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
                """)) {
            statement.setInt(1, warehouseId);
            statement.setInt(2, districtId);
            statement.setInt(3, customerId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new SQLException("TPC-C customer does not exist: w=" + warehouseId
                            + ", d=" + districtId + ", c=" + customerId);
                }
            }
        }
    }

    private int reserveOrderId(Connection connection) throws SQLException {
        int orderId;
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT d_next_o_id FROM bmsql_district
                WHERE d_w_id = ? AND d_id = ? FOR UPDATE
                """)) {
            statement.setInt(1, warehouseId);
            statement.setInt(2, districtId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new SQLException("TPC-C district does not exist: w=" + warehouseId + ", d=" + districtId);
                }
                orderId = result.getInt(1);
            }
        }
        try (PreparedStatement statement = connection.prepareStatement("""
                UPDATE bmsql_district SET d_next_o_id = d_next_o_id + 1
                WHERE d_w_id = ? AND d_id = ?
                """)) {
            statement.setInt(1, warehouseId);
            statement.setInt(2, districtId);
            requireOneRow(statement.executeUpdate(), "district order-id update");
        }
        return orderId;
    }

    private void insertOrder(Connection connection, int orderId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO bmsql_oorder
                    (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
                VALUES (?, ?, ?, ?, NULL, ?, 1, ?)
                """)) {
            statement.setInt(1, warehouseId);
            statement.setInt(2, districtId);
            statement.setInt(3, orderId);
            statement.setInt(4, customerId);
            statement.setInt(5, lines.size());
            statement.setTimestamp(6, Timestamp.from(Instant.now()));
            requireOneRow(statement.executeUpdate(), "order insert");
        }
    }

    private void insertNewOrder(Connection connection, int orderId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO bmsql_new_order (no_w_id, no_d_id, no_o_id) VALUES (?, ?, ?)
                """)) {
            statement.setInt(1, warehouseId);
            statement.setInt(2, districtId);
            statement.setInt(3, orderId);
            requireOneRow(statement.executeUpdate(), "new-order insert");
        }
    }

    private void applyOrderLine(Connection connection, int orderId, int lineNumber, OrderLine line)
            throws SQLException {
        BigDecimal itemPrice = itemPrice(connection, line.itemId());
        Stock stock = lockStock(connection, line.itemId());
        int adjustedQuantity = stock.quantity() >= line.quantity() + 10
                ? stock.quantity() - line.quantity()
                : stock.quantity() + 91 - line.quantity();

        try (PreparedStatement statement = connection.prepareStatement("""
                UPDATE bmsql_stock
                SET s_quantity = ?, s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1
                WHERE s_w_id = ? AND s_i_id = ?
                """)) {
            statement.setInt(1, adjustedQuantity);
            statement.setInt(2, line.quantity());
            statement.setInt(3, warehouseId);
            statement.setInt(4, line.itemId());
            requireOneRow(statement.executeUpdate(), "stock update");
        }

        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO bmsql_order_line
                    (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,
                     ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
                VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
                """)) {
            statement.setInt(1, warehouseId);
            statement.setInt(2, districtId);
            statement.setInt(3, orderId);
            statement.setInt(4, lineNumber);
            statement.setInt(5, line.itemId());
            statement.setBigDecimal(6, itemPrice.multiply(BigDecimal.valueOf(line.quantity())));
            statement.setInt(7, warehouseId);
            statement.setInt(8, line.quantity());
            statement.setString(9, stock.districtInfo());
            requireOneRow(statement.executeUpdate(), "order-line insert");
        }
    }

    private BigDecimal itemPrice(Connection connection, int itemId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT i_price FROM bmsql_item WHERE i_id = ?")) {
            statement.setInt(1, itemId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) throw new SQLException("TPC-C item does not exist: " + itemId);
                return result.getBigDecimal(1);
            }
        }
    }

    private Stock lockStock(Connection connection, int itemId) throws SQLException {
        String districtColumn = "s_dist_%02d".formatted(districtId);
        String sql = "SELECT s_quantity, " + districtColumn
                + " FROM bmsql_stock WHERE s_w_id = ? AND s_i_id = ? FOR UPDATE";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, warehouseId);
            statement.setInt(2, itemId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) throw new SQLException("TPC-C stock does not exist: w="
                        + warehouseId + ", item=" + itemId);
                return new Stock(result.getInt(1), result.getString(2));
            }
        }
    }

    private static void requireOneRow(int affectedRows, String operation) throws SQLException {
        if (affectedRows != 1) throw new SQLException(operation + " affected " + affectedRows + " rows");
    }

    public record OrderLine(int itemId, int quantity) {
        public OrderLine {
            if (itemId < 1) throw new IllegalArgumentException("itemId must be positive");
            if (quantity < 1 || quantity > 10) throw new IllegalArgumentException("quantity must be between 1 and 10");
        }
    }

    private record Stock(int quantity, String districtInfo) {
    }
}
