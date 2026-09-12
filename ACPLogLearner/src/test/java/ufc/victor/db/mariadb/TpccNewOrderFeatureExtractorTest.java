package ufc.victor.db.mariadb;

import org.junit.jupiter.api.Test;
import ufc.victor.experiment.TransactionContext;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpccNewOrderFeatureExtractorTest {

    @Test
    void mapsNewOrderDetailsIntoGenericWorkloadFeatures() {
        TpccNewOrderSpec specification = TpccNewOrderSpecFactory.controlled(List.of(1, 2, 3), 2);

        TransactionContext context = new TpccNewOrderFeatureExtractor().extract(specification);

        long remoteLines = specification.lines().stream()
                .filter(line -> line.supplierWarehouseId() != specification.homeWarehouseId())
                .count();
        assertEquals(specification.specId(), context.specificationId());
        assertEquals(TpccNewOrderFeatureExtractor.TRANSACTION_TYPE, context.transactionType());
        assertEquals(List.of("P1", "P2", "P3"), context.participantNodeIds());
        assertTrue(context.distributed());
        assertEquals((double) specification.lines().size(), context.workloadFeatures().get("order_line_count"));
        assertEquals((double) remoteLines, context.workloadFeatures().get("remote_line_count"));
        assertEquals(remoteLines / (double) specification.lines().size(),
                context.workloadFeatures().get("remote_line_ratio"));
        assertFalse(context.workloadFeatures().isEmpty());
    }
}
