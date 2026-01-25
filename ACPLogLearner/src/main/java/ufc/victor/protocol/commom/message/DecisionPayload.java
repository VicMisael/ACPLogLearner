package ufc.victor.protocol.commom.message;

import java.io.Serializable;

public class DecisionPayload implements Serializable {
    private final MessageType decision; // GLOBAL_COMMIT or GLOBAL_ABORT

    public DecisionPayload(MessageType decision) {
        this.decision = decision;
    }

    public MessageType decision() {
        return decision;
    }

}

