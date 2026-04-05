package ufc.victor.experiment;

import ufc.victor.protocol.SelectedProtocol;

public record ProtocolRegistration(
        String id,
        SelectedProtocol protocol
) {
    public ProtocolRegistration {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (protocol == null) {
            throw new IllegalArgumentException("protocol must not be null");
        }
    }
}
