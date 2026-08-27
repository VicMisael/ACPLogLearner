package ufc.victor.experiment;

import ufc.victor.protocol.SelectedProtocol;

/** Maps the outcome predicted before commit to the protocol optimized for it. */
public final class VfdtProtocolSwitchingPolicy {

    public SelectedProtocol select(TransactionOutcomeClass predictedOutcome) {
        return switch (predictedOutcome) {
            case LIKELY_COMMIT -> SelectedProtocol.TWO_PC_PRESUMED_COMMIT;
            case LIKELY_ABORT -> SelectedProtocol.TWO_PC_PRESUMED_ABORT;
            case UNCERTAIN -> SelectedProtocol.TWO_PC;
        };
    }
}
