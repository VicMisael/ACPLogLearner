package ufc.victor.db.mariadb;

import java.util.Map;

/** Resolves a global warehouse identifier to its MariaDB XA participant. */
public final class WarehouseRouter {
    private final Map<Integer, MariaDbXaConfig> configsByWarehouse;

    public WarehouseRouter(Map<Integer, MariaDbXaConfig> configsByWarehouse) {
        if (configsByWarehouse == null || configsByWarehouse.isEmpty()) {
            throw new IllegalArgumentException("configsByWarehouse must not be empty");
        }
        for (Map.Entry<Integer, MariaDbXaConfig> entry : configsByWarehouse.entrySet()) {
            if (entry.getKey() == null || entry.getKey() < 1) {
                throw new IllegalArgumentException("warehouse IDs must be positive");
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("warehouse config must not be null");
            }
        }
        this.configsByWarehouse = Map.copyOf(configsByWarehouse);
    }

    public MariaDbXaConfig configFor(int warehouseId) {
        MariaDbXaConfig config = configsByWarehouse.get(warehouseId);
        if (config == null) {
            throw new IllegalArgumentException("No shard owns global warehouse " + warehouseId);
        }
        return config;
    }

    public String participantIdFor(int warehouseId) {
        configFor(warehouseId);
        return "P" + warehouseId;
    }
}
