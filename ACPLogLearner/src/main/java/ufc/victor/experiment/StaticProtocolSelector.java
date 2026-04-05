package ufc.victor.experiment;

import ufc.victor.LogFeatures;
import ufc.victor.protocol.SelectedProtocol;

public record StaticProtocolSelector(
        String name,
        SelectedProtocol protocol
) implements ProtocolSelector {
    public StaticProtocolSelector {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (protocol == null) {
            throw new IllegalArgumentException("protocol must not be null");
        }
    }

    @Override
    public SelectedProtocol select(LogFeatures features) {
        return protocol;
    }
}
