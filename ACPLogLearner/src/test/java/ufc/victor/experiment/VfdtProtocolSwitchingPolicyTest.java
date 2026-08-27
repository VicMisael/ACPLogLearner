package ufc.victor.experiment;

import org.junit.jupiter.api.Test;
import ufc.victor.protocol.SelectedProtocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VfdtProtocolSwitchingPolicyTest {

    private final VfdtProtocolSwitchingPolicy policy = new VfdtProtocolSwitchingPolicy();

    @Test
    void selectsPresumedCommitForLikelyCommit() {
        assertEquals(SelectedProtocol.TWO_PC_PRESUMED_COMMIT,
                policy.select(TransactionOutcomeClass.LIKELY_COMMIT));
    }

    @Test
    void selectsPresumedAbortForLikelyAbort() {
        assertEquals(SelectedProtocol.TWO_PC_PRESUMED_ABORT,
                policy.select(TransactionOutcomeClass.LIKELY_ABORT));
    }

    @Test
    void keepsClassicTwoPcWhenOutcomeIsUncertain() {
        assertEquals(SelectedProtocol.TWO_PC,
                policy.select(TransactionOutcomeClass.UNCERTAIN));
    }
}
