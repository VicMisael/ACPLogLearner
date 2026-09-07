package ufc.victor.db.mariadb;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** Immutable, replayable description of one logical TPC-C New-Order. */
public record TpccNewOrderSpec(
        UUID specId,
        long randomSeed,
        int homeWarehouseId,
        int districtId,
        int customerId,
        Instant entryTime,
        List<OrderLineSpec> lines
) {
    public static final int INVALID_ITEM_ID = 999_999;

    public TpccNewOrderSpec {
        if (specId == null) throw new IllegalArgumentException("specId must not be null");
        if (homeWarehouseId < 1) throw new IllegalArgumentException("homeWarehouseId must be positive");
        if (districtId < 1 || districtId > 10) {
            throw new IllegalArgumentException("districtId must be between 1 and 10");
        }
        if (customerId < 1 || customerId > 3_000) {
            throw new IllegalArgumentException("customerId must be between 1 and 3000");
        }
        if (entryTime == null) throw new IllegalArgumentException("entryTime must not be null");
        if (lines == null || lines.size() < 5 || lines.size() > 15) {
            throw new IllegalArgumentException("New-Order requires between 5 and 15 order lines");
        }
        lines = List.copyOf(lines);

        Set<Integer> itemIds = new LinkedHashSet<>();
        for (OrderLineSpec line : lines) {
            if (!itemIds.add(line.itemId())) {
                throw new IllegalArgumentException("New-Order item IDs must be unique: " + line.itemId());
            }
        }
    }

    public boolean allLocal() {
        return lines.stream().allMatch(line -> line.supplierWarehouseId() == homeWarehouseId);
    }

    public Set<Integer> participantWarehouseIds() {
        LinkedHashSet<Integer> participants = new LinkedHashSet<>();
        participants.add(homeWarehouseId);
        for (OrderLineSpec line : lines) participants.add(line.supplierWarehouseId());
        return Collections.unmodifiableSet(participants);
    }

    public record OrderLineSpec(int itemId, int supplierWarehouseId, int quantity) {
        public OrderLineSpec {
            if (itemId < 1) throw new IllegalArgumentException("itemId must be positive");
            if (supplierWarehouseId < 1) {
                throw new IllegalArgumentException("supplierWarehouseId must be positive");
            }
            if (quantity < 1 || quantity > 10) {
                throw new IllegalArgumentException("quantity must be between 1 and 10");
            }
        }
    }
}
