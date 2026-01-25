package ufc.victor.protocol.coordinator;

enum NodeState {
    NONE,
    VOTED_COMMIT,
    ABORTED,
    ACKED;

    boolean canTransitionTo(NodeState next) {
        return ordinal() <= next.ordinal();
    }
}
