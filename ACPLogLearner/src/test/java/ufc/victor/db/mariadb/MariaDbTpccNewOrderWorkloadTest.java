package ufc.victor.db.mariadb;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MariaDbTpccNewOrderWorkloadTest {

    private static final List<MariaDbTpccNewOrderWorkload.OrderLine> FIVE_LINES = List.of(
            new MariaDbTpccNewOrderWorkload.OrderLine(1, 1),
            new MariaDbTpccNewOrderWorkload.OrderLine(2, 2),
            new MariaDbTpccNewOrderWorkload.OrderLine(3, 3),
            new MariaDbTpccNewOrderWorkload.OrderLine(4, 4),
            new MariaDbTpccNewOrderWorkload.OrderLine(5, 5)
    );

    @Test
    void acceptsValidNewOrderShape() {
        assertDoesNotThrow(() -> new MariaDbTpccNewOrderWorkload(1, 10, 3_000, FIVE_LINES));
    }

    @Test
    void rejectsTooFewOrderLines() {
        assertThrows(IllegalArgumentException.class,
                () -> new MariaDbTpccNewOrderWorkload(1, 1, 1, FIVE_LINES.subList(0, 4)));
    }

    @Test
    void rejectsDistrictOutsideTpccRange() {
        assertThrows(IllegalArgumentException.class,
                () -> new MariaDbTpccNewOrderWorkload(1, 11, 1, FIVE_LINES));
    }
}
