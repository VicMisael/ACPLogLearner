package ufc.victor.experiment;

import ufc.victor.protocol.coordinator.node.NodeId;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record AlpsFeatures(
        TransactionContext transactionContext,
        HistoricalMetrics globalHistory,
        Map<NodeId, HistoricalMetrics> participantHistory
) {
    public AlpsFeatures {
        Objects.requireNonNull(transactionContext, "transactionContext");
        Objects.requireNonNull(globalHistory, "globalHistory");
        participantHistory = Map.copyOf(new LinkedHashMap<>(
                Objects.requireNonNull(participantHistory, "participantHistory")
        ));
    }

    public List<String> participantNodeIds() {
        return transactionContext.participantNodeIds();
    }

    public int participantCount() {
        return transactionContext.participantCount();
    }

    public ParticipantHistorySummary selectedParticipantHistory() {
        return ParticipantHistorySummary.from(participantHistory);
    }
}
