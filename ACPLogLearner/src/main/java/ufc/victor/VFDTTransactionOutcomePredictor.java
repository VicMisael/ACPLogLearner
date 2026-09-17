package ufc.victor;

import ufc.victor.experiment.AlpsFeatureProfile;
import ufc.victor.experiment.AlpsFeatures;
import ufc.victor.experiment.HistoricalMetrics;
import ufc.victor.experiment.TransactionContext;
import ufc.victor.experiment.TransactionOutcomeClass;
import ufc.victor.experiment.TransactionShape;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class VFDTTransactionOutcomePredictor {

    private final VfdtCategoricalModel<TransactionOutcomeClass> model;

    public VFDTTransactionOutcomePredictor(List<String> knownParticipantNodeIds) {
        this(knownParticipantNodeIds, List.of(), List.of(), AlpsFeatureProfile.GENERIC);
    }

    public VFDTTransactionOutcomePredictor(
            List<String> knownParticipantNodeIds,
            List<String> knownTransactionTypes,
            List<String> workloadFeatureNames,
            AlpsFeatureProfile featureProfile
    ) {
        model = new VfdtCategoricalModel<>(
                knownParticipantNodeIds,
                knownTransactionTypes,
                workloadFeatureNames,
                featureProfile,
                TransactionOutcomeClass.class,
                "transaction_outcome"
        );
    }

    public TransactionOutcomeClass predict(LogFeatures features) {
        return predict(legacyFeatures(features));
    }

    public TransactionOutcomeClass predict(AlpsFeatures features) {
        return model.predict(features).predictedClass();
    }

    public void train(LogFeatures features, TransactionOutcomeClass label) {
        train(legacyFeatures(features), label);
    }

    public void train(AlpsFeatures features, TransactionOutcomeClass label) {
        model.train(features, label);
    }

    public String getLearnedRules() {
        return model.learnedRules();
    }

    public int getNodeCount() {
        return model.nodeCount();
    }

    public int getTreeDepth() {
        return model.treeDepth();
    }

    public List<String> getFeatureNames() {
        return model.featureNames();
    }

    private static AlpsFeatures legacyFeatures(LogFeatures features) {
        TransactionContext context = new TransactionContext(
                UUID.randomUUID(),
                "LEGACY_SIMULATION",
                features.participantNodeIds(),
                new TransactionShape(0L, 0L, 0L, 0L, 0L, 0.0),
                Map.of()
        );
        Map<NodeId, HistoricalMetrics> participantHistory = new LinkedHashMap<>();
        features.participantNodeIds().forEach(nodeId -> participantHistory.put(
                new NodeId(nodeId), HistoricalMetrics.empty()
        ));
        return new AlpsFeatures(context, HistoricalMetrics.empty(), participantHistory);
    }
}
