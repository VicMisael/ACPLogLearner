package ufc.victor.experiment;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record TransactionContext(
        UUID specificationId,
        String transactionType,
        List<String> participantNodeIds,
        TransactionShape shape,
        Map<String, Double> workloadFeatures
) {
    public TransactionContext {
        Objects.requireNonNull(specificationId, "specificationId");
        if (transactionType == null || transactionType.isBlank()) {
            throw new IllegalArgumentException("transactionType must not be blank");
        }
        Objects.requireNonNull(shape, "shape");
        participantNodeIds = List.copyOf(Objects.requireNonNull(
                participantNodeIds, "participantNodeIds"
        ));
        if (participantNodeIds.isEmpty()) {
            throw new IllegalArgumentException("participantNodeIds must not be empty");
        }
        if (participantNodeIds.stream().anyMatch(id -> id == null || id.isBlank())) {
            throw new IllegalArgumentException("participantNodeIds must not contain blank IDs");
        }
        if (participantNodeIds.stream().distinct().count() != participantNodeIds.size()) {
            throw new IllegalArgumentException("participantNodeIds must not contain duplicates");
        }
        Map<String, Double> copiedFeatures = new LinkedHashMap<>();
        Objects.requireNonNull(workloadFeatures, "workloadFeatures").forEach((name, value) -> {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("workload feature names must not be blank");
            }
            if (value == null || !Double.isFinite(value)) {
                throw new IllegalArgumentException("workload feature values must be finite");
            }
            copiedFeatures.put(name, value);
        });
        workloadFeatures = Map.copyOf(copiedFeatures);
    }

    public int participantCount() {
        return participantNodeIds.size();
    }

    public boolean distributed() {
        return participantNodeIds.size() > 1;
    }
}
