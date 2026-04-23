package ufc.victor.experiment;

public final class TransactionOutcomeLabeler {

    private final double likelyCommitAbortThreshold;
    private final double likelyAbortThreshold;

    public TransactionOutcomeLabeler(double likelyCommitAbortThreshold, double likelyAbortThreshold) {
        if (likelyCommitAbortThreshold < 0.0 || likelyCommitAbortThreshold > 1.0) {
            throw new IllegalArgumentException("likelyCommitAbortThreshold must be between 0.0 and 1.0");
        }
        if (likelyAbortThreshold < 0.0 || likelyAbortThreshold > 1.0) {
            throw new IllegalArgumentException("likelyAbortThreshold must be between 0.0 and 1.0");
        }
        if (likelyCommitAbortThreshold > likelyAbortThreshold) {
            throw new IllegalArgumentException("likelyCommitAbortThreshold must be <= likelyAbortThreshold");
        }
        this.likelyCommitAbortThreshold = likelyCommitAbortThreshold;
        this.likelyAbortThreshold = likelyAbortThreshold;
    }

    public TransactionOutcomeClass label(double abortRate) {
        if (abortRate < 0.0 || abortRate > 1.0) {
            throw new IllegalArgumentException("abortRate must be between 0.0 and 1.0");
        }
        if (abortRate < likelyCommitAbortThreshold) {
            return TransactionOutcomeClass.LIKELY_COMMIT;
        }
        if (abortRate > likelyAbortThreshold) {
            return TransactionOutcomeClass.LIKELY_ABORT;
        }
        return TransactionOutcomeClass.UNCERTAIN;
    }
}
