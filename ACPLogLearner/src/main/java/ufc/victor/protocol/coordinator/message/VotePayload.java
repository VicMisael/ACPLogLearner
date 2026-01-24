package ufc.victor.protocol.coordinator.message;

public class VotePayload implements MessagePayload{
    private final boolean canCommit;
    private final long localExecutionTimeMs;

    public VotePayload(boolean canCommit, long localExecutionTimeMs) {
        this.canCommit = canCommit;
        this.localExecutionTimeMs = localExecutionTimeMs;
    }

    public boolean canCommit() {
        return canCommit;
    }

    public long localExecutionTimeMs() {
        return localExecutionTimeMs;
    }
}
