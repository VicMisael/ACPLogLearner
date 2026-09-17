package ufc.victor.db.mariadb;

import ufc.victor.experiment.TrialStateController;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Restores every row changed by a controlled New-Order after a protocol trial. */
public final class MariaDbNewOrderStateController
        implements TrialStateController<TpccNewOrderSpec, MariaDbNewOrderStateController.Snapshot> {

    private final Map<Integer, MariaDbXaConfig> configs;

    public MariaDbNewOrderStateController(Map<Integer, MariaDbXaConfig> configs) {
        if (configs == null || configs.isEmpty()) {
            throw new IllegalArgumentException("configs must not be empty");
        }
        this.configs = Map.copyOf(new LinkedHashMap<>(configs));
    }

    @Override
    public Snapshot capture(TpccNewOrderSpec spec) throws SQLException {
        Objects.requireNonNull(spec, "spec");
        requireNoPreparedBranches(spec.participantWarehouseIds());
        int nextOrderId = readNextOrderId(spec);
        Map<StockKey, StockState> stocks = new LinkedHashMap<>();
        for (TpccNewOrderSpec.OrderLineSpec line : spec.lines()) {
            if (line.itemId() == TpccNewOrderSpec.INVALID_ITEM_ID) continue;
            StockKey key = new StockKey(line.supplierWarehouseId(), line.itemId());
            if (!stocks.containsKey(key)) stocks.put(key, readStock(key));
        }
        return new Snapshot(spec, nextOrderId, stocks);
    }

    @Override
    public void restoreAndVerify(Snapshot snapshot) throws SQLException {
        Objects.requireNonNull(snapshot, "snapshot");
        requireNoPreparedBranches(snapshot.spec().participantWarehouseIds());
        restoreHomeRows(snapshot);
        restoreStocks(snapshot);
        verify(snapshot);
    }

    private void restoreHomeRows(Snapshot snapshot) throws SQLException {
        TpccNewOrderSpec spec = snapshot.spec();
        MariaDbXaConfig config = config(spec.homeWarehouseId());
        try (Connection connection = open(config)) {
            connection.setAutoCommit(false);
            try {
                deleteOrderRows(
                        connection, "bmsql_order_line",
                        "ol_w_id", "ol_d_id", "ol_o_id", spec, snapshot.nextOrderId()
                );
                deleteOrderRows(
                        connection, "bmsql_new_order",
                        "no_w_id", "no_d_id", "no_o_id", spec, snapshot.nextOrderId()
                );
                deleteOrderRows(
                        connection, "bmsql_oorder",
                        "o_w_id", "o_d_id", "o_id", spec, snapshot.nextOrderId()
                );
                try (PreparedStatement statement = connection.prepareStatement("""
                        UPDATE bmsql_district SET d_next_o_id = ?
                        WHERE d_w_id = ? AND d_id = ?
                        """)) {
                    statement.setInt(1, snapshot.nextOrderId());
                    statement.setInt(2, spec.homeWarehouseId());
                    statement.setInt(3, spec.districtId());
                    requireOne(statement.executeUpdate(), "district restore");
                }
                connection.commit();
            } catch (SQLException exception) {
                connection.rollback();
                throw exception;
            }
        }
    }

    private static void deleteOrderRows(
            Connection connection,
            String table,
            String warehouseColumn,
            String districtColumn,
            String orderColumn,
            TpccNewOrderSpec spec,
            int orderId
    ) throws SQLException {
        String sql = "DELETE FROM " + table + " WHERE " + warehouseColumn + " = ? AND "
                + districtColumn + " = ? AND " + orderColumn + " = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, spec.homeWarehouseId());
            statement.setInt(2, spec.districtId());
            statement.setInt(3, orderId);
            statement.executeUpdate();
        }
    }

    private void restoreStocks(Snapshot snapshot) throws SQLException {
        Map<Integer, Map<StockKey, StockState>> byWarehouse = new LinkedHashMap<>();
        snapshot.stocks().forEach((key, state) -> byWarehouse
                .computeIfAbsent(key.warehouseId(), ignored -> new LinkedHashMap<>())
                .put(key, state));
        for (Map.Entry<Integer, Map<StockKey, StockState>> warehouse : byWarehouse.entrySet()) {
            try (Connection connection = open(config(warehouse.getKey()))) {
                connection.setAutoCommit(false);
                try (PreparedStatement statement = connection.prepareStatement("""
                        UPDATE bmsql_stock
                        SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ?
                        WHERE s_w_id = ? AND s_i_id = ?
                        """)) {
                    for (Map.Entry<StockKey, StockState> entry : warehouse.getValue().entrySet()) {
                        StockKey key = entry.getKey();
                        StockState state = entry.getValue();
                        statement.setInt(1, state.quantity());
                        statement.setBigDecimal(2, state.ytd());
                        statement.setInt(3, state.orderCount());
                        statement.setInt(4, state.remoteCount());
                        statement.setInt(5, key.warehouseId());
                        statement.setInt(6, key.itemId());
                        statement.addBatch();
                    }
                    for (int affected : statement.executeBatch()) {
                        requireOne(affected, "stock restore");
                    }
                    connection.commit();
                } catch (SQLException exception) {
                    connection.rollback();
                    throw exception;
                }
            }
        }
    }

    private void verify(Snapshot snapshot) throws SQLException {
        int actualNextOrderId = readNextOrderId(snapshot.spec());
        if (actualNextOrderId != snapshot.nextOrderId()) {
            throw new SQLException("district state was not restored: expected d_next_o_id="
                    + snapshot.nextOrderId() + ", actual=" + actualNextOrderId);
        }
        for (Map.Entry<StockKey, StockState> entry : snapshot.stocks().entrySet()) {
            StockState actual = readStock(entry.getKey());
            if (!entry.getValue().equals(actual)) {
                throw new SQLException("stock state was not restored for " + entry.getKey()
                        + ": expected=" + entry.getValue() + ", actual=" + actual);
            }
        }
        requireNoOrderRows(snapshot);
        requireNoPreparedBranches(snapshot.spec().participantWarehouseIds());
    }

    private void requireNoOrderRows(Snapshot snapshot) throws SQLException {
        TpccNewOrderSpec spec = snapshot.spec();
        try (Connection connection = open(config(spec.homeWarehouseId()));
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT
                       (SELECT COUNT(*) FROM bmsql_oorder
                        WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?) +
                       (SELECT COUNT(*) FROM bmsql_new_order
                        WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?) +
                       (SELECT COUNT(*) FROM bmsql_order_line
                        WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?)
                     """)) {
            for (int offset = 0; offset < 3; offset++) {
                statement.setInt(offset * 3 + 1, spec.homeWarehouseId());
                statement.setInt(offset * 3 + 2, spec.districtId());
                statement.setInt(offset * 3 + 3, snapshot.nextOrderId());
            }
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next() || result.getLong(1) != 0L) {
                    throw new SQLException("New-Order rows remain after state restoration");
                }
            }
        }
    }

    private int readNextOrderId(TpccNewOrderSpec spec) throws SQLException {
        try (Connection connection = open(config(spec.homeWarehouseId()));
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT d_next_o_id FROM bmsql_district
                     WHERE d_w_id = ? AND d_id = ?
                     """)) {
            statement.setInt(1, spec.homeWarehouseId());
            statement.setInt(2, spec.districtId());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) throw new SQLException("district not found for state snapshot");
                return result.getInt(1);
            }
        }
    }

    private StockState readStock(StockKey key) throws SQLException {
        try (Connection connection = open(config(key.warehouseId()));
             PreparedStatement statement = connection.prepareStatement("""
                     SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt
                     FROM bmsql_stock WHERE s_w_id = ? AND s_i_id = ?
                     """)) {
            statement.setInt(1, key.warehouseId());
            statement.setInt(2, key.itemId());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) throw new SQLException("stock not found for state snapshot: " + key);
                return new StockState(
                        result.getInt(1), result.getBigDecimal(2),
                        result.getInt(3), result.getInt(4)
                );
            }
        }
    }

    private void requireNoPreparedBranches(Iterable<Integer> warehouseIds) throws SQLException {
        Set<Integer> unique = new LinkedHashSet<>();
        warehouseIds.forEach(unique::add);
        for (int warehouseId : unique) {
            try (Connection connection = open(config(warehouseId));
                 Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery("XA RECOVER")) {
                if (result.next()) {
                    throw new SQLException("prepared XA branch exists on warehouse " + warehouseId
                            + "; refusing unsafe state restoration");
                }
            }
        }
    }

    private MariaDbXaConfig config(int warehouseId) {
        MariaDbXaConfig config = configs.get(warehouseId);
        if (config == null) throw new IllegalArgumentException("no config for warehouse " + warehouseId);
        return config;
    }

    private static Connection open(MariaDbXaConfig config) throws SQLException {
        return DriverManager.getConnection(config.jdbcUrl(), config.user(), config.password());
    }

    private static void requireOne(int affected, String operation) throws SQLException {
        if (affected != 1 && affected != Statement.SUCCESS_NO_INFO) {
            throw new SQLException(operation + " affected " + affected + " rows");
        }
    }

    public record Snapshot(
            TpccNewOrderSpec spec,
            int nextOrderId,
            Map<StockKey, StockState> stocks
    ) {
        public Snapshot {
            Objects.requireNonNull(spec, "spec");
            stocks = Map.copyOf(new LinkedHashMap<>(stocks));
        }
    }

    public record StockKey(int warehouseId, int itemId) {
    }

    public record StockState(int quantity, BigDecimal ytd, int orderCount, int remoteCount) {
        public StockState {
            Objects.requireNonNull(ytd, "ytd");
            ytd = ytd.stripTrailingZeros();
        }
    }
}
