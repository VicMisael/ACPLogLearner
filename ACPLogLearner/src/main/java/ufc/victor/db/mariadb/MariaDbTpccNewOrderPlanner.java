package ufc.victor.db.mariadb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Converts one logical New-Order into XA work for the shards it touches. */
public final class MariaDbTpccNewOrderPlanner {
    private final WarehouseRouter router;
    private final StockDistrictInfoReader districtInfoReader;

    public MariaDbTpccNewOrderPlanner(WarehouseRouter router) {
        this(router, MariaDbTpccNewOrderPlanner::readDistrictInfo);
    }

    MariaDbTpccNewOrderPlanner(WarehouseRouter router, StockDistrictInfoReader districtInfoReader) {
        if (router == null) throw new IllegalArgumentException("router must not be null");
        if (districtInfoReader == null) {
            throw new IllegalArgumentException("districtInfoReader must not be null");
        }
        this.router = router;
        this.districtInfoReader = districtInfoReader;
    }

    public MariaDbTpccNewOrderPlan plan(TpccNewOrderSpec spec) throws SQLException {
        if (spec == null) throw new IllegalArgumentException("spec must not be null");

        Map<Integer, String> remoteDistrictInfoByLine = new LinkedHashMap<>();
        Map<Integer, List<TpccNewOrderSpec.OrderLineSpec>> remoteLinesByWarehouse =
                new LinkedHashMap<>();
        for (int index = 0; index < spec.lines().size(); index++) {
            TpccNewOrderSpec.OrderLineSpec line = spec.lines().get(index);
            router.configFor(line.supplierWarehouseId());
            if (line.supplierWarehouseId() == spec.homeWarehouseId()) continue;

            remoteLinesByWarehouse.computeIfAbsent(
                    line.supplierWarehouseId(), ignored -> new ArrayList<>()
            ).add(line);
            if (line.itemId() != TpccNewOrderSpec.INVALID_ITEM_ID) {
                String districtInfo = districtInfoReader.read(
                        router.configFor(line.supplierWarehouseId()),
                        line.supplierWarehouseId(),
                        spec.districtId(),
                        line.itemId()
                );
                if (districtInfo != null) remoteDistrictInfoByLine.put(index + 1, districtInfo);
            }
        }

        List<MariaDbTpccNewOrderPlan.Branch> branches = new ArrayList<>();
        int homeWarehouseId = spec.homeWarehouseId();
        branches.add(new MariaDbTpccNewOrderPlan.Branch(
                homeWarehouseId,
                router.participantIdFor(homeWarehouseId),
                router.configFor(homeWarehouseId),
                MariaDbTpccNewOrderWorkload.homeBranch(spec, remoteDistrictInfoByLine),
                true
        ));
        for (Map.Entry<Integer, List<TpccNewOrderSpec.OrderLineSpec>> entry
                : remoteLinesByWarehouse.entrySet()) {
            int supplierWarehouseId = entry.getKey();
            branches.add(new MariaDbTpccNewOrderPlan.Branch(
                    supplierWarehouseId,
                    router.participantIdFor(supplierWarehouseId),
                    router.configFor(supplierWarehouseId),
                    MariaDbTpccNewOrderWorkload.remoteStockBranch(
                            spec, supplierWarehouseId, entry.getValue()
                    ),
                    false
            ));
        }
        return new MariaDbTpccNewOrderPlan(spec, branches);
    }

    private static String readDistrictInfo(
            MariaDbXaConfig config,
            int warehouseId,
            int districtId,
            int itemId
    ) throws SQLException {
        String districtColumn = "s_dist_%02d".formatted(districtId);
        String sql = "SELECT " + districtColumn
                + " FROM bmsql_stock WHERE s_w_id = ? AND s_i_id = ?";
        try (Connection connection = DriverManager.getConnection(
                config.jdbcUrl(), config.user(), config.password());
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, warehouseId);
            statement.setInt(2, itemId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? result.getString(1) : null;
            }
        }
    }

    @FunctionalInterface
    interface StockDistrictInfoReader {
        String read(MariaDbXaConfig config, int warehouseId, int districtId, int itemId)
                throws SQLException;
    }
}
