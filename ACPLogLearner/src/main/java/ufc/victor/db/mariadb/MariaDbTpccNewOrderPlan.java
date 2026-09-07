package ufc.victor.db.mariadb;

import java.util.List;

/** Per-shard XA work produced from one logical New-Order specification. */
public record MariaDbTpccNewOrderPlan(TpccNewOrderSpec spec, List<Branch> branches) {
    public MariaDbTpccNewOrderPlan {
        if (spec == null) throw new IllegalArgumentException("spec must not be null");
        if (branches == null || branches.isEmpty()) {
            throw new IllegalArgumentException("branches must not be empty");
        }
        branches = List.copyOf(branches);
    }

    public record Branch(
            int warehouseId,
            String participantId,
            MariaDbXaConfig config,
            MariaDbXaWorkload workload,
            boolean home
    ) {
        public Branch {
            if (warehouseId < 1) throw new IllegalArgumentException("warehouseId must be positive");
            if (participantId == null || participantId.isBlank()) {
                throw new IllegalArgumentException("participantId must not be blank");
            }
            if (config == null) throw new IllegalArgumentException("config must not be null");
            if (workload == null) throw new IllegalArgumentException("workload must not be null");
        }
    }
}
