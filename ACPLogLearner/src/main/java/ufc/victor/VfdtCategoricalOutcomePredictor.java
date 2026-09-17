package ufc.victor;

import ufc.victor.experiment.AlpsFeatureProfile;
import ufc.victor.experiment.AlpsFeatures;
import ufc.victor.experiment.CategoricalOutcomePrediction;
import ufc.victor.experiment.CategoricalOutcomeObservation;
import ufc.victor.experiment.CompletionCategory;
import ufc.victor.experiment.CostCategory;

import java.util.List;
import java.util.Map;

public final class VfdtCategoricalOutcomePredictor {

    private final VfdtCategoricalModel<CompletionCategory> completionModel;
    private final VfdtCategoricalModel<CostCategory> latencyModel;
    private final VfdtCategoricalModel<CostCategory> redoModel;

    public VfdtCategoricalOutcomePredictor(
            List<String> knownParticipantNodeIds,
            List<String> knownTransactionTypes,
            List<String> workloadFeatureNames,
            AlpsFeatureProfile featureProfile
    ) {
        completionModel = model(
                knownParticipantNodeIds, knownTransactionTypes, workloadFeatureNames,
                featureProfile, CompletionCategory.class, "completion_outcome"
        );
        latencyModel = model(
                knownParticipantNodeIds, knownTransactionTypes, workloadFeatureNames,
                featureProfile, CostCategory.class, "latency_class"
        );
        redoModel = model(
                knownParticipantNodeIds, knownTransactionTypes, workloadFeatureNames,
                featureProfile, CostCategory.class, "redo_class"
        );
    }

    public CategoricalOutcomePrediction predict(AlpsFeatures features) {
        return new CategoricalOutcomePrediction(
                completionModel.predict(features),
                latencyModel.predict(features),
                redoModel.predict(features)
        );
    }

    public void train(AlpsFeatures features, CategoricalOutcomeObservation observation) {
        completionModel.train(features, observation.completion());
        observation.latency().ifPresent(label -> latencyModel.train(features, label));
        observation.redo().ifPresent(label -> redoModel.train(features, label));
    }

    public List<String> featureNames() {
        return completionModel.featureNames();
    }

    public Map<String, String> learnedRules() {
        return Map.of(
                "completion", completionModel.learnedRules(),
                "latency", latencyModel.learnedRules(),
                "redo", redoModel.learnedRules()
        );
    }

    public Map<String, Integer> nodeCounts() {
        return Map.of(
                "completion", completionModel.nodeCount(),
                "latency", latencyModel.nodeCount(),
                "redo", redoModel.nodeCount()
        );
    }

    public Map<String, Integer> treeDepths() {
        return Map.of(
                "completion", completionModel.treeDepth(),
                "latency", latencyModel.treeDepth(),
                "redo", redoModel.treeDepth()
        );
    }

    private static <L extends Enum<L>> VfdtCategoricalModel<L> model(
            List<String> knownParticipantNodeIds,
            List<String> knownTransactionTypes,
            List<String> workloadFeatureNames,
            AlpsFeatureProfile featureProfile,
            Class<L> labelType,
            String labelName
    ) {
        return new VfdtCategoricalModel<>(
                knownParticipantNodeIds,
                knownTransactionTypes,
                workloadFeatureNames,
                featureProfile,
                labelType,
                labelName
        );
    }
}
