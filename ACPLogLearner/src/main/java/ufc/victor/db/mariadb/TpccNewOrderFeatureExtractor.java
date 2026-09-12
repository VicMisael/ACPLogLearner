package ufc.victor.db.mariadb;

import ufc.victor.experiment.TransactionContext;
import ufc.victor.experiment.TransactionContextExtractor;
import ufc.victor.experiment.TransactionShape;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TpccNewOrderFeatureExtractor
        implements TransactionContextExtractor<TpccNewOrderSpec> {

    public static final String TRANSACTION_TYPE = "TPCC_NEW_ORDER";
    public static final List<String> WORKLOAD_FEATURE_NAMES = List.of(
            "home_warehouse_id",
            "district_id",
            "order_line_count",
            "remote_line_count",
            "remote_line_ratio",
            "all_local"
    );

    @Override
    public TransactionContext extract(TpccNewOrderSpec specification) {
        List<String> participantIds = specification.participantWarehouseIds().stream()
                .map(warehouse -> "P" + warehouse)
                .toList();
        long remoteLines = specification.lines().stream()
                .filter(line -> line.supplierWarehouseId() != specification.homeWarehouseId())
                .count();
        Map<String, Double> workloadFeatures = new LinkedHashMap<>();
        workloadFeatures.put("home_warehouse_id", (double) specification.homeWarehouseId());
        workloadFeatures.put("district_id", (double) specification.districtId());
        workloadFeatures.put("order_line_count", (double) specification.lines().size());
        workloadFeatures.put("remote_line_count", (double) remoteLines);
        workloadFeatures.put("remote_line_ratio", remoteLines / (double) specification.lines().size());
        workloadFeatures.put("all_local", specification.allLocal() ? 1.0 : 0.0);
        long lineCount = specification.lines().size();
        TransactionShape shape = new TransactionShape(
                2L + 2L * lineCount,
                2L + lineCount,
                1L + lineCount,
                0L,
                5L + 4L * lineCount,
                remoteLines / (double) lineCount
        );
        return new TransactionContext(
                specification.specId(),
                TRANSACTION_TYPE,
                participantIds,
                shape,
                workloadFeatures
        );
    }
}
