package ufc.victor.db.mariadb;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WarehouseRouterTest {

    @Test
    void resolvesGlobalWarehouseToItsParticipantAndConfig() {
        MariaDbXaConfig config = new MariaDbXaConfig("jdbc:mariadb://node-3/acp", "acp", "secret");
        WarehouseRouter router = new WarehouseRouter(Map.of(3, config));

        assertEquals(config, router.configFor(3));
        assertEquals("P3", router.participantIdFor(3));
    }

    @Test
    void rejectsAnUnownedWarehouse() {
        WarehouseRouter router = new WarehouseRouter(Map.of(
                1, new MariaDbXaConfig("jdbc:mariadb://node-1/acp", "acp", "secret")
        ));

        assertThrows(IllegalArgumentException.class, () -> router.configFor(4));
    }
}
