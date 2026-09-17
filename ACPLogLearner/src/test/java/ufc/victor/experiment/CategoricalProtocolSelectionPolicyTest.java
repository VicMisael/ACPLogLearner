package ufc.victor.experiment;

import org.junit.jupiter.api.Test;
import ufc.victor.protocol.SelectedProtocol;

import java.util.EnumMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CategoricalProtocolSelectionPolicyTest {

    private final CategoricalProtocolSelectionPolicy policy =
            new CategoricalProtocolSelectionPolicy(CategoricalPolicyWeights.balanced());

    @Test
    void selectsPresumedCommitForLikelyCommitAtEqualResourceCost() {
        assertEquals(
                SelectedProtocol.TWO_PC_PRESUMED_COMMIT,
                policy.select(prediction(CompletionCategory.COMMIT))
        );
    }

    @Test
    void selectsPresumedAbortForLikelyAbortAtEqualResourceCost() {
        assertEquals(
                SelectedProtocol.TWO_PC_PRESUMED_ABORT,
                policy.select(prediction(CompletionCategory.ABORT))
        );
    }

    @Test
    void resolvesEqualScoresInProtocolDeclarationOrder() {
        assertEquals(SelectedProtocol.TWO_PC, policy.select(new CategoricalOutcomePrediction(
                uniformPrediction(CompletionCategory.COMMIT, CompletionCategory.class),
                uniformPrediction(CostCategory.LOW, CostCategory.class),
                uniformPrediction(CostCategory.LOW, CostCategory.class)
        )));
    }

    private static CategoricalOutcomePrediction prediction(
            CompletionCategory completion
    ) {
        return new CategoricalOutcomePrediction(
                classPrediction(completion, CompletionCategory.class),
                classPrediction(CostCategory.LOW, CostCategory.class),
                classPrediction(CostCategory.LOW, CostCategory.class)
        );
    }

    private static <L extends Enum<L>> CategoricalPrediction<L> classPrediction(
            L predicted,
            Class<L> labelType
    ) {
        Map<L, Double> probabilities = new EnumMap<>(labelType);
        for (L label : labelType.getEnumConstants()) {
            probabilities.put(label, label == predicted ? 1.0 : 0.0);
        }
        return new CategoricalPrediction<>(predicted, probabilities, 1.0);
    }

    private static <L extends Enum<L>> CategoricalPrediction<L> uniformPrediction(
            L predicted,
            Class<L> labelType
    ) {
        Map<L, Double> probabilities = new EnumMap<>(labelType);
        double probability = 1.0 / labelType.getEnumConstants().length;
        for (L label : labelType.getEnumConstants()) probabilities.put(label, probability);
        return new CategoricalPrediction<>(predicted, probabilities, probability);
    }
}
