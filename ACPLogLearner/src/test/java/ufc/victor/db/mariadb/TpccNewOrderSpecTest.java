package ufc.victor.db.mariadb;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpccNewOrderSpecTest {

    @Test
    void derivesOrderedParticipantsAndLocalityFromSuppliers() {
        TpccNewOrderSpec spec = spec(List.of(
                line(1, 1),
                line(2, 3),
                line(3, 1),
                line(4, 2),
                line(5, 3)
        ));

        assertFalse(spec.allLocal());
        assertEquals(List.of(1, 3, 2), List.copyOf(spec.participantWarehouseIds()));
    }

    @Test
    void reportsAllLocalWhenEverySupplierIsHome() {
        assertTrue(spec(List.of(
                line(1, 1), line(2, 1), line(3, 1), line(4, 1), line(5, 1)
        )).allLocal());
    }

    @Test
    void rejectsDuplicateItemsBecauseTpccRequiresDistinctOrderLines() {
        assertThrows(IllegalArgumentException.class, () -> spec(List.of(
                line(1, 1), line(2, 1), line(3, 2), line(4, 2), line(1, 1)
        )));
    }

    @Test
    void rollbackVariantChangesOnlyTheFinalItem() {
        TpccNewOrderSpec original = TpccNewOrderSpecFactory.controlled(List.of(1, 2), 12);

        TpccNewOrderSpec rollback = TpccNewOrderSpecFactory.withInvalidFinalItem(original);

        assertEquals(original.lines().subList(0, original.lines().size() - 1),
                rollback.lines().subList(0, rollback.lines().size() - 1));
        assertEquals(TpccNewOrderSpec.INVALID_ITEM_ID, rollback.lines().getLast().itemId());
        assertEquals(original.lines().getLast().supplierWarehouseId(),
                rollback.lines().getLast().supplierWarehouseId());
    }

    private static TpccNewOrderSpec spec(List<TpccNewOrderSpec.OrderLineSpec> lines) {
        return new TpccNewOrderSpec(
                UUID.randomUUID(), 7L, 1, 2, 3, Instant.parse("2026-01-01T00:00:00Z"), lines
        );
    }

    private static TpccNewOrderSpec.OrderLineSpec line(int itemId, int supplierWarehouseId) {
        return new TpccNewOrderSpec.OrderLineSpec(itemId, supplierWarehouseId, 1);
    }
}
