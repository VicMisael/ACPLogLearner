package ufc.victor.experiment;

public record TransactionShape(
        long readOperationCount,
        long insertOperationCount,
        long updateOperationCount,
        long deleteOperationCount,
        long estimatedRowsTouched,
        double remoteOperationRatio
) {
    public TransactionShape {
        if (readOperationCount < 0L
                || insertOperationCount < 0L
                || updateOperationCount < 0L
                || deleteOperationCount < 0L
                || estimatedRowsTouched < 0L) {
            throw new IllegalArgumentException("operation and row counts must not be negative");
        }
        if (!Double.isFinite(remoteOperationRatio)
                || remoteOperationRatio < 0.0
                || remoteOperationRatio > 1.0) {
            throw new IllegalArgumentException("remoteOperationRatio must be between 0 and 1");
        }
    }

    public long writeOperationCount() {
        return insertOperationCount + updateOperationCount + deleteOperationCount;
    }

    public boolean readOnly() {
        return writeOperationCount() == 0L;
    }
}
