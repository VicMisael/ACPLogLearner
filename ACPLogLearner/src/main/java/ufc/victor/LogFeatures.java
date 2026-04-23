package ufc.victor;

import java.util.List;

/**
 * Node-membership view of the participant set that is about to execute a transaction.
 */
public record LogFeatures(
        List<String> participantNodeIds
) {
    public LogFeatures {
        participantNodeIds = List.copyOf(participantNodeIds);
    }

    public String explanationSummary() {
        return "participants=" + participantNodeIds.size()
                + ", nodes=" + participantNodeIds;
    }
}
