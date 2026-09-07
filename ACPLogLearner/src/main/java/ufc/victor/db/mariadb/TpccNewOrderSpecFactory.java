package ufc.victor.db.mariadb;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.UUID;

/** Deterministic specifications used by the current controlled and smoke workloads. */
public final class TpccNewOrderSpecFactory {
    private static final long BASE_EPOCH_SECOND = 1_700_000_000L;

    private TpccNewOrderSpecFactory() {
    }

    public static TpccNewOrderSpec controlled(
            List<Integer> participantWarehouseIds,
            int transactionIndex
    ) {
        if (participantWarehouseIds == null || participantWarehouseIds.isEmpty()) {
            throw new IllegalArgumentException("participantWarehouseIds must not be empty");
        }
        if (transactionIndex < 0) throw new IllegalArgumentException("transactionIndex must not be negative");

        List<Integer> participants = List.copyOf(new LinkedHashSet<>(participantWarehouseIds));
        if (participants.stream().anyMatch(warehouseId -> warehouseId == null || warehouseId < 1)) {
            throw new IllegalArgumentException("participant warehouse IDs must be positive");
        }

        int homeWarehouseId = participants.getFirst();
        int districtId = ((transactionIndex + homeWarehouseId - 1) % 10) + 1;
        int customerId = ((transactionIndex * 37 + homeWarehouseId - 1) % 3_000) + 1;
        List<TpccNewOrderSpec.OrderLineSpec> lines = new ArrayList<>();
        int lineCount = Math.max(5, participants.size());
        for (int line = 0; line < lineCount; line++) {
            int itemId = ((transactionIndex * 101 + homeWarehouseId * 17 + line * 997) % 100_000) + 1;
            int supplierWarehouseId = participants.get(line % participants.size());
            lines.add(new TpccNewOrderSpec.OrderLineSpec(
                    itemId, supplierWarehouseId, (line % 10) + 1
            ));
        }

        String identity = "controlled-new-order-" + transactionIndex + "-" + participants;
        return new TpccNewOrderSpec(
                UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)),
                transactionIndex,
                homeWarehouseId,
                districtId,
                customerId,
                Instant.ofEpochSecond(BASE_EPOCH_SECOND + transactionIndex),
                lines
        );
    }

    public static TpccNewOrderSpec withInvalidFinalItem(TpccNewOrderSpec spec) {
        if (spec == null) throw new IllegalArgumentException("spec must not be null");
        List<TpccNewOrderSpec.OrderLineSpec> lines = new ArrayList<>(spec.lines());
        TpccNewOrderSpec.OrderLineSpec finalLine = lines.getLast();
        lines.set(lines.size() - 1, new TpccNewOrderSpec.OrderLineSpec(
                TpccNewOrderSpec.INVALID_ITEM_ID,
                finalLine.supplierWarehouseId(),
                finalLine.quantity()
        ));
        return new TpccNewOrderSpec(
                spec.specId(),
                spec.randomSeed(),
                spec.homeWarehouseId(),
                spec.districtId(),
                spec.customerId(),
                spec.entryTime(),
                lines
        );
    }
}
