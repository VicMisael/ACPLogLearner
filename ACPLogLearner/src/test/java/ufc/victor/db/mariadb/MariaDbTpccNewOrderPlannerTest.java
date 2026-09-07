package ufc.victor.db.mariadb;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MariaDbTpccNewOrderPlannerTest {

    @Test
    void createsOneHomeBranchAndOneBranchPerRemoteSupplier() throws SQLException {
        WarehouseRouter router = new WarehouseRouter(configs(1, 2, 3, 4));
        MariaDbTpccNewOrderPlanner planner = new MariaDbTpccNewOrderPlanner(
                router,
                (config, warehouseId, districtId, itemId) -> "DIST-" + warehouseId
        );
        TpccNewOrderSpec spec = TpccNewOrderSpecFactory.controlled(List.of(1, 3, 2), 9);

        MariaDbTpccNewOrderPlan plan = planner.plan(spec);

        assertEquals(List.of(1, 3, 2), plan.branches().stream()
                .map(MariaDbTpccNewOrderPlan.Branch::warehouseId)
                .toList());
        assertTrue(plan.branches().getFirst().home());
        assertTrue(plan.branches().stream().skip(1).noneMatch(MariaDbTpccNewOrderPlan.Branch::home));
    }

    @Test
    void failsBeforeExecutionWhenARequestTargetsAnUnknownShard() {
        WarehouseRouter router = new WarehouseRouter(configs(1));
        MariaDbTpccNewOrderPlanner planner = new MariaDbTpccNewOrderPlanner(
                router,
                (config, warehouseId, districtId, itemId) -> "DIST"
        );
        TpccNewOrderSpec spec = TpccNewOrderSpecFactory.controlled(List.of(1, 2), 0);

        assertThrows(IllegalArgumentException.class, () -> planner.plan(spec));
    }

    private static Map<Integer, MariaDbXaConfig> configs(int... warehouses) {
        Map<Integer, MariaDbXaConfig> configs = new LinkedHashMap<>();
        for (int warehouseId : warehouses) {
            configs.put(warehouseId, new MariaDbXaConfig(
                    "jdbc:mariadb://localhost:" + (33_060 + warehouseId) + "/acp",
                    "acp",
                    "secret"
            ));
        }
        return configs;
    }
}
