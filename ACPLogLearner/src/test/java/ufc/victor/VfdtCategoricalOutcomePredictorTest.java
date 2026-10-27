package ufc.victor;

import org.junit.jupiter.api.Test;
import ufc.victor.experiment.AlpsFeatures;
import ufc.victor.experiment.AlpsFeatureProfile;
import ufc.victor.experiment.CategoricalOutcomeObservation;
import ufc.victor.experiment.CompletionCategory;
import ufc.victor.experiment.CostCategory;
import ufc.victor.experiment.HistoricalMetrics;
import ufc.victor.experiment.OutcomeThresholds;
import ufc.victor.experiment.TransactionContext;
import ufc.victor.experiment.TransactionShape;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VfdtCategoricalOutcomePredictorTest {

    @Test
    void acceptsConfiguredWorkloadFeaturesWithoutKnowingTheirDomain() {
        VfdtCategoricalOutcomePredictor predictor = new VfdtCategoricalOutcomePredictor(
                List.of("node-a", "node-b"),
                List.of("CUSTOM_TRANSACTION"),
                List.of("custom_cost"),
                AlpsFeatureProfile.COMPLETE
        );
        TransactionContext context = new TransactionContext(
                UUID.randomUUID(),
                "CUSTOM_TRANSACTION",
                List.of("node-a", "node-b"),
                new TransactionShape(2, 1, 1, 0, 4, 0.5),
                Map.of("custom_cost", 42.0)
        );
        AlpsFeatures features = new AlpsFeatures(
                context,
                HistoricalMetrics.empty(),
                Map.of(
                        new NodeId("node-a"), HistoricalMetrics.empty(),
                        new NodeId("node-b"), HistoricalMetrics.empty()
                )
        );

        predictor.train(features, new CategoricalOutcomeObservation(
                CompletionCategory.COMMIT,
                Optional.of(CostCategory.LOW),
                Optional.of(CostCategory.HIGH),
                new OutcomeThresholds(10, 20, 100, 200, 0, 0)
        ));

        var prediction = predictor.predict(features);
        assertEquals(CompletionCategory.COMMIT, prediction.completion().predictedClass());
        assertEquals(CostCategory.LOW, prediction.latency().predictedClass());
        assertEquals(CostCategory.HIGH, prediction.redo().predictedClass());
        assertTrue(predictor.featureNames().contains("has_node-a"));
        assertTrue(predictor.featureNames().contains("workload_custom_cost"));
    }

    @Test
    void genericProfileExcludesTopologyAndWorkloadIdentity() {
        VfdtCategoricalOutcomePredictor predictor = new VfdtCategoricalOutcomePredictor(
                List.of("node-a"),
                List.of("CUSTOM_TRANSACTION"),
                List.of("custom_cost"),
                AlpsFeatureProfile.GENERIC
        );

        assertTrue(predictor.featureNames().contains("remote_operation_ratio"));
        assertTrue(predictor.featureNames().contains("participants_max_latency_ms"));
        assertFalse(predictor.featureNames().contains("has_node-a"));
        assertFalse(predictor.featureNames().contains("type_CUSTOM_TRANSACTION"));
        assertFalse(predictor.featureNames().contains("workload_custom_cost"));
    }

    @Test
    void multiHeadPredictorLearnsSeparateTargetsWithoutProtocolIdentity() {
        VfdtCategoricalOutcomePredictor predictor = new VfdtCategoricalOutcomePredictor(
                List.of("node-a", "node-b"),
                List.of("CUSTOM_TRANSACTION"),
                List.of("custom_cost"),
                AlpsFeatureProfile.GENERIC
        );
        AlpsFeatures features = features();
        CategoricalOutcomeObservation observation = new CategoricalOutcomeObservation(
                CompletionCategory.ABORT,
                Optional.of(CostCategory.HIGH),
                Optional.of(CostCategory.MEDIUM),
                new OutcomeThresholds(10, 20, 100, 200, 0, 0)
        );

        predictor.train(features, observation);

        var prediction = predictor.predict(features);
        assertEquals(CompletionCategory.ABORT, prediction.completion().predictedClass());
        assertEquals(CostCategory.HIGH, prediction.latency().predictedClass());
        assertEquals(CostCategory.MEDIUM, prediction.redo().predictedClass());
        assertFalse(predictor.featureNames().stream().anyMatch(
                feature -> feature.startsWith("candidate_protocol_")
        ));
    }

    private static AlpsFeatures features() {
        TransactionContext context = new TransactionContext(
                UUID.randomUUID(),
                "CUSTOM_TRANSACTION",
                List.of("node-a", "node-b"),
                new TransactionShape(2, 1, 1, 0, 4, 0.5),
                Map.of("custom_cost", 42.0)
        );
        return new AlpsFeatures(
                context,
                HistoricalMetrics.empty(),
                Map.of(
                        new NodeId("node-a"), HistoricalMetrics.empty(),
                        new NodeId("node-b"), HistoricalMetrics.empty()
                )
        );
    }
}
