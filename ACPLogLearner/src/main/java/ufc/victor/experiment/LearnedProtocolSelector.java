package ufc.victor.experiment;

import ufc.victor.LogFeatures;
import ufc.victor.VFDTProtocolPredictor;
import ufc.victor.protocol.SelectedProtocol;

public record LearnedProtocolSelector(
        String name,
        VFDTProtocolPredictor predictor
) implements ProtocolSelector {
    public LearnedProtocolSelector {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (predictor == null) {
            throw new IllegalArgumentException("predictor must not be null");
        }
    }

    @Override
    public SelectedProtocol select(LogFeatures features) {
        return predictor.predict(features);
    }
}
