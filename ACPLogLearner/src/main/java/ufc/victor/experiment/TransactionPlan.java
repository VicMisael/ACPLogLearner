package ufc.victor.experiment;

import java.util.List;

public record TransactionPlan(
        List<Boolean> participantShouldCommit
) {
    public TransactionPlan {
        if (participantShouldCommit == null || participantShouldCommit.isEmpty()) {
            throw new IllegalArgumentException("participantShouldCommit must not be empty");
        }
        participantShouldCommit = List.copyOf(participantShouldCommit);
    }
}
