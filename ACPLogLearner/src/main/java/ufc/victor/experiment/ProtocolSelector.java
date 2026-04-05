package ufc.victor.experiment;

import ufc.victor.LogFeatures;
import ufc.victor.protocol.SelectedProtocol;

public interface ProtocolSelector {
    String name();

    SelectedProtocol select(LogFeatures features);
}
