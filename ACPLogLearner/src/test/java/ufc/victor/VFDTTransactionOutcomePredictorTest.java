package ufc.victor;

import org.junit.jupiter.api.Test;
import ufc.victor.experiment.AlpsFeatures;
import ufc.victor.experiment.AlpsFeatureProfile;
import ufc.victor.experiment.HistoricalMetrics;
import ufc.victor.experiment.TransactionContext;
import ufc.victor.experiment.TransactionOutcomeClass;
import ufc.victor.experiment.TransactionShape;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VFDTTransactionOutcomePredictorTest {

    @Test
    void acceptsConfiguredWorkloadFeaturesWithoutKnowingTheirDomain() {
        VFDTTransactionOutcomePredictor predictor = new VFDTTransactionOutcomePredictor(
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

        predictor.train(features, TransactionOutcomeClass.UNCERTAIN);

        assertEquals(TransactionOutcomeClass.UNCERTAIN, predictor.predict(features));
        assertTrue(predictor.getFeatureNames().contains("has_node-a"));
        assertTrue(predictor.getFeatureNames().contains("workload_custom_cost"));
    }

    @Test
    void genericProfileExcludesTopologyAndWorkloadIdentity() {
        VFDTTransactionOutcomePredictor predictor = new VFDTTransactionOutcomePredictor(
                List.of("node-a"),
                List.of("CUSTOM_TRANSACTION"),
                List.of("custom_cost"),
                AlpsFeatureProfile.GENERIC
        );

        assertTrue(predictor.getFeatureNames().contains("remote_operation_ratio"));
        assertTrue(predictor.getFeatureNames().contains("participants_max_latency_ms"));
        assertFalse(predictor.getFeatureNames().contains("has_node-a"));
        assertFalse(predictor.getFeatureNames().contains("type_CUSTOM_TRANSACTION"));
        assertFalse(predictor.getFeatureNames().contains("workload_custom_cost"));
    }
}
