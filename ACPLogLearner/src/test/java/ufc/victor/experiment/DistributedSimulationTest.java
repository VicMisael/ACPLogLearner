package ufc.victor.experiment;

import org.junit.jupiter.api.Test;
import ufc.victor.protocol.SelectedProtocol;
import ufc.victor.protocol.coordinator.log.CoordinatorLogRecordType;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DistributedSimulationTest {

    @Test
    void presumedCommitCompletesAbortWithoutWaitingForTimeout() throws InterruptedException {
        NodeCatalog catalog = new NodeCatalog(List.of(
                new NodeProfile("coord", 1, 1, 1, 0.0),
                new NodeProfile("p1", 1, 1, 1, 1.0),
                new NodeProfile("p2", 1, 1, 1, 0.0)
        ));
        Scenario scenario = new Scenario("abort", "coord", List.of("p1", "p2"), 1L, 1_000L);

        SimulationResult result = new DistributedSimulation().run(
                scenario,
                catalog,
                SelectedProtocol.TWO_PC_PRESUMED_COMMIT,
                new TransactionPlan(List.of(false, true))
        );

        assertFalse(result.metrics().committed());
        assertTrue(result.metrics().latencyMs() < scenario.timeoutMs());
        assertEquals(
                CoordinatorLogRecordType.END_TRANSACTION,
                result.coordinatorRecords().getLast().type()
        );
    }
}
