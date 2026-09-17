package ufc.victor.experiment;

import org.junit.jupiter.api.Test;
import ufc.victor.protocol.SelectedProtocol;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PairedProtocolTrialRunnerTest {

    @Test
    void executesEveryProtocolAndRestoresAfterEveryCandidate() throws Exception {
        AtomicInteger captures = new AtomicInteger();
        AtomicInteger restores = new AtomicInteger();
        List<SelectedProtocol> executionOrder = new ArrayList<>();
        TrialStateController<String, String> state = new TrialStateController<>() {
            @Override public String capture(String specification) {
                captures.incrementAndGet();
                return "snapshot";
            }

            @Override public void restoreAndVerify(String snapshot) {
                restores.incrementAndGet();
            }
        };
        PairedProtocolTrialRunner<String, String> runner = new PairedProtocolTrialRunner<>(
                state,
                (specification, protocol) -> {
                    executionOrder.add(protocol);
                    long latency = switch (protocol) {
                        case TWO_PC -> 300L;
                        case TWO_PC_PRESUMED_ABORT -> 200L;
                        case TWO_PC_PRESUMED_COMMIT -> 100L;
                    };
                    return ProtocolCostFunctionTest.trial(
                            protocol, TrialOutcome.COMMITTED, latency, OptionalLong.of(10_000L)
                    );
                },
                new ProtocolCostFunction(ProtocolCostWeights.defaults())
        );

        PairedProtocolTrial result = runner.run("spec", TrialOutcome.COMMITTED, 42L);

        assertEquals(1, captures.get());
        assertEquals(3, restores.get());
        assertEquals(3, executionOrder.stream().distinct().count());
        assertEquals(SelectedProtocol.TWO_PC_PRESUMED_COMMIT, result.oracleProtocol());
        assertEquals(0.0, result.regret(SelectedProtocol.TWO_PC_PRESUMED_COMMIT), 0.000_001);
    }

    @Test
    void failsClosedWhenStateCannotBeRestored() {
        TrialStateController<String, String> state = new TrialStateController<>() {
            @Override public String capture(String specification) {
                return "snapshot";
            }

            @Override public void restoreAndVerify(String snapshot) throws Exception {
                throw new Exception("state mismatch");
            }
        };
        PairedProtocolTrialRunner<String, String> runner = new PairedProtocolTrialRunner<>(
                state,
                (specification, protocol) -> ProtocolCostFunctionTest.trial(
                        protocol, TrialOutcome.COMMITTED, 10L, OptionalLong.of(1L)
                ),
                new ProtocolCostFunction(ProtocolCostWeights.defaults())
        );

        Exception exception = assertThrows(
                Exception.class,
                () -> runner.run("spec", TrialOutcome.COMMITTED, 1L)
        );
        assertEquals("state mismatch", exception.getMessage());
    }
}
