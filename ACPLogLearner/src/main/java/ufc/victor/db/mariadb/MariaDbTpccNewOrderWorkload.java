package ufc.victor.db.mariadb;

import ufc.victor.protocol.commom.TransactionId;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Executes either the home or remote-stock XA branch of one TPC-C New-Order. */
public final class MariaDbTpccNewOrderWorkload implements MariaDbXaWorkload {
    private enum BranchRole { HOME, REMOTE_STOCK }

    private final BranchRole role;
    private final TpccNewOrderSpec spec;
    private final int branchWarehouseId;
    private final List<TpccNewOrderSpec.OrderLineSpec> branchLines;
    private final Map<Integer, String> remoteDistrictInfoByLine;

    /** Compatibility constructor for a New-Order whose lines are all locally supplied. */
    public MariaDbTpccNewOrderWorkload(
            int warehouseId,
            int districtId,
            int customerId,
            List<OrderLine> lines
    ) {
        this(localSpec(warehouseId, districtId, customerId, lines));
    }

    private MariaDbTpccNewOrderWorkload(TpccNewOrderSpec localSpec) {
        this(BranchRole.HOME, localSpec, localSpec.homeWarehouseId(), localSpec.lines(), Map.of());
    }

    private MariaDbTpccNewOrderWorkload(
            BranchRole role,
            TpccNewOrderSpec spec,
            int branchWarehouseId,
            List<TpccNewOrderSpec.OrderLineSpec> branchLines,
            Map<Integer, String> remoteDistrictInfoByLine
    ) {
        this.role = role;
        this.spec = spec;
        this.branchWarehouseId = branchWarehouseId;
        this.branchLines = List.copyOf(branchLines);
        this.remoteDistrictInfoByLine = Map.copyOf(remoteDistrictInfoByLine);
    }

    static MariaDbTpccNewOrderWorkload homeBranch(
            TpccNewOrderSpec spec,
            Map<Integer, String> remoteDistrictInfoByLine
    ) {
        return new MariaDbTpccNewOrderWorkload(
                BranchRole.HOME,
                spec,
                spec.homeWarehouseId(),
                spec.lines(),
                remoteDistrictInfoByLine
        );
    }

    static MariaDbTpccNewOrderWorkload remoteStockBranch(
            TpccNewOrderSpec spec,
            int supplierWarehouseId,
            List<TpccNewOrderSpec.OrderLineSpec> lines
    ) {
        if (lines == null || lines.isEmpty()) {
            throw new IllegalArgumentException("remote stock branch lines must not be empty");
        }
        if (lines.stream().anyMatch(line -> line.supplierWarehouseId() != supplierWarehouseId)) {
            throw new IllegalArgumentException("remote stock lines must belong to their branch warehouse");
        }
        return new MariaDbTpccNewOrderWorkload(
                BranchRole.REMOTE_STOCK,
                spec,
                supplierWarehouseId,
                lines,
                Map.of()
        );
    }

    @Override
    public void executeWithinXa(Connection connection, TransactionId txId) throws SQLException {
        if (role == BranchRole.REMOTE_STOCK) {
            applyRemoteStockLines(connection);
        } else {
            executeHomeBranch(connection);
        }
    }

    private void executeHomeBranch(Connection connection) throws SQLException {
        requireCustomer(connection);
        int orderId = reserveOrderId(connection);
        insertOrder(connection, orderId);
        insertNewOrder(connection, orderId);

        for (int lineNumber = 1; lineNumber <= spec.lines().size(); lineNumber++) {
            applyHomeOrderLine(connection, orderId, lineNumber, spec.lines().get(lineNumber - 1));
        }
    }

    private void applyRemoteStockLines(Connection connection) throws SQLException {
        for (TpccNewOrderSpec.OrderLineSpec line : branchLines) {
            Stock stock = lockStock(
                    connection, branchWarehouseId, spec.districtId(), line.itemId()
            );
            updateStock(connection, branchWarehouseId, line, stock, true);
        }
    }

    private void requireCustomer(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT c_id FROM bmsql_customer
                WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
                """)) {
            statement.setInt(1, spec.homeWarehouseId());
            statement.setInt(2, spec.districtId());
            statement.setInt(3, spec.customerId());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new SQLException("TPC-C customer does not exist: w=" + spec.homeWarehouseId()
                            + ", d=" + spec.districtId() + ", c=" + spec.customerId());
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
            statement.setInt(1, spec.homeWarehouseId());
            statement.setInt(2, spec.districtId());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new SQLException("TPC-C district does not exist: w="
                            + spec.homeWarehouseId() + ", d=" + spec.districtId());
                }
                orderId = result.getInt(1);
            }
        }
        try (PreparedStatement statement = connection.prepareStatement("""
                UPDATE bmsql_district SET d_next_o_id = d_next_o_id + 1
                WHERE d_w_id = ? AND d_id = ?
                """)) {
            statement.setInt(1, spec.homeWarehouseId());
            statement.setInt(2, spec.districtId());
            requireOneRow(statement.executeUpdate(), "district order-id update");
        }
        return orderId;
    }

    private void insertOrder(Connection connection, int orderId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO bmsql_oorder
                    (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
                VALUES (?, ?, ?, ?, NULL, ?, ?, ?)
                """)) {
            statement.setInt(1, spec.homeWarehouseId());
            statement.setInt(2, spec.districtId());
            statement.setInt(3, orderId);
            statement.setInt(4, spec.customerId());
            statement.setInt(5, spec.lines().size());
            statement.setInt(6, spec.allLocal() ? 1 : 0);
            statement.setTimestamp(7, java.sql.Timestamp.from(spec.entryTime()));
            requireOneRow(statement.executeUpdate(), "order insert");
        }
    }

    private void insertNewOrder(Connection connection, int orderId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO bmsql_new_order (no_w_id, no_d_id, no_o_id) VALUES (?, ?, ?)
                """)) {
            statement.setInt(1, spec.homeWarehouseId());
            statement.setInt(2, spec.districtId());
            statement.setInt(3, orderId);
            requireOneRow(statement.executeUpdate(), "new-order insert");
        }
    }

    private void applyHomeOrderLine(
            Connection connection,
            int orderId,
            int lineNumber,
            TpccNewOrderSpec.OrderLineSpec line
    ) throws SQLException {
        BigDecimal itemPrice = itemPrice(connection, line.itemId());
        String districtInfo;
        if (line.supplierWarehouseId() == spec.homeWarehouseId()) {
            Stock stock = lockStock(
                    connection, spec.homeWarehouseId(), spec.districtId(), line.itemId()
            );
            updateStock(connection, spec.homeWarehouseId(), line, stock, false);
            districtInfo = stock.districtInfo();
        } else {
            districtInfo = remoteDistrictInfoByLine.get(lineNumber);
            if (districtInfo == null) {
                throw new SQLException("TPC-C remote stock does not exist: w="
                        + line.supplierWarehouseId() + ", item=" + line.itemId());
            }
        }

        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO bmsql_order_line
                    (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,
                     ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
                VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
                """)) {
            statement.setInt(1, spec.homeWarehouseId());
            statement.setInt(2, spec.districtId());
            statement.setInt(3, orderId);
            statement.setInt(4, lineNumber);
            statement.setInt(5, line.itemId());
            statement.setBigDecimal(6, itemPrice.multiply(BigDecimal.valueOf(line.quantity())));
            statement.setInt(7, line.supplierWarehouseId());
            statement.setInt(8, line.quantity());
            statement.setString(9, districtInfo);
            requireOneRow(statement.executeUpdate(), "order-line insert");
        }
    }

    private static BigDecimal itemPrice(Connection connection, int itemId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT i_price FROM bmsql_item WHERE i_id = ?")) {
            statement.setInt(1, itemId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) throw new SQLException("TPC-C item does not exist: " + itemId);
                return result.getBigDecimal(1);
            }
        }
    }

    private static Stock lockStock(
            Connection connection,
            int warehouseId,
            int districtId,
            int itemId
    ) throws SQLException {
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

    private static void updateStock(
            Connection connection,
            int warehouseId,
            TpccNewOrderSpec.OrderLineSpec line,
            Stock stock,
            boolean remote
    ) throws SQLException {
        int adjustedQuantity = stock.quantity() >= line.quantity() + 10
                ? stock.quantity() - line.quantity()
                : stock.quantity() + 91 - line.quantity();
        try (PreparedStatement statement = connection.prepareStatement("""
                UPDATE bmsql_stock
                SET s_quantity = ?,
                    s_ytd = s_ytd + ?,
                    s_order_cnt = s_order_cnt + 1,
                    s_remote_cnt = s_remote_cnt + ?
                WHERE s_w_id = ? AND s_i_id = ?
                """)) {
            statement.setInt(1, adjustedQuantity);
            statement.setInt(2, line.quantity());
            statement.setInt(3, remote ? 1 : 0);
            statement.setInt(4, warehouseId);
            statement.setInt(5, line.itemId());
            requireOneRow(statement.executeUpdate(), "stock update");
        }
    }

    private static TpccNewOrderSpec localSpec(
            int warehouseId,
            int districtId,
            int customerId,
            List<OrderLine> lines
    ) {
        List<TpccNewOrderSpec.OrderLineSpec> localLines = lines == null ? null : lines.stream()
                .map(line -> new TpccNewOrderSpec.OrderLineSpec(
                        line.itemId(), warehouseId, line.quantity()
                ))
                .toList();
        return new TpccNewOrderSpec(
                UUID.randomUUID(), 0L, warehouseId, districtId, customerId, Instant.now(), localLines
        );
    }

    private static void requireOneRow(int affectedRows, String operation) throws SQLException {
        if (affectedRows != 1) throw new SQLException(operation + " affected " + affectedRows + " rows");
    }

    public record OrderLine(int itemId, int quantity) {
        public OrderLine {
            if (itemId < 1) throw new IllegalArgumentException("itemId must be positive");
            if (quantity < 1 || quantity > 10) {
                throw new IllegalArgumentException("quantity must be between 1 and 10");
            }
        }
    }

    private record Stock(int quantity, String districtInfo) {
    }
}
