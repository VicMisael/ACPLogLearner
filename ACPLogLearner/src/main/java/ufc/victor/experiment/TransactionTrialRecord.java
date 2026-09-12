package ufc.victor.experiment;

import ufc.victor.protocol.SelectedProtocol;
import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public record TransactionTrialRecord(
        TransactionId transactionId,
        UUID specificationId,
        SelectedProtocol protocol,
        TrialOutcome outcome,
        Duration totalLatency,
        Optional<Duration> preparePhaseLatency,
        Optional<Duration> completionPhaseLatency,
        Map<NodeId, ParticipantTrialMetrics> participants,
        long messageCount,
        long coordinatorLogWrites
) {
    public TransactionTrialRecord {
        Objects.requireNonNull(transactionId, "transactionId");
        Objects.requireNonNull(specificationId, "specificationId");
        Objects.requireNonNull(protocol, "protocol");
        Objects.requireNonNull(outcome, "outcome");
        Objects.requireNonNull(totalLatency, "totalLatency");
        Objects.requireNonNull(preparePhaseLatency, "preparePhaseLatency");
        Objects.requireNonNull(completionPhaseLatency, "completionPhaseLatency");
        Objects.requireNonNull(participants, "participants");
        if (totalLatency.isNegative()) throw new IllegalArgumentException("totalLatency must not be negative");
        preparePhaseLatency.ifPresent(value -> requireNonNegative(value, "preparePhaseLatency"));
        completionPhaseLatency.ifPresent(value -> requireNonNegative(value, "completionPhaseLatency"));
        if (messageCount < 0L) throw new IllegalArgumentException("messageCount must not be negative");
        if (coordinatorLogWrites < 0L) {
            throw new IllegalArgumentException("coordinatorLogWrites must not be negative");
        }
        participants = Map.copyOf(new LinkedHashMap<>(participants));
    }

    public boolean committed() {
        return outcome == TrialOutcome.COMMITTED;
    }

    public long totalProtocolLogWrites() {
        return coordinatorLogWrites + participants.values().stream()
                .mapToLong(ParticipantTrialMetrics::protocolLogWrites)
                .sum();
    }

    public boolean resetDetected() {
        return participants.values().stream()
                .anyMatch(metrics -> metrics.redo().counterResetDetected());
    }

    public boolean hasUnavailableRedoMetric() {
        return participants.values().stream()
                .anyMatch(ParticipantTrialMetrics::hasUnavailableRedoMetric);
    }

    private static void requireNonNegative(Duration value, String name) {
        if (value.isNegative()) throw new IllegalArgumentException(name + " must not be negative");
    }
}
