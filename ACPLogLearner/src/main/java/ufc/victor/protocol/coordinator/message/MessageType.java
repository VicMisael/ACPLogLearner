package ufc.victor.protocol.coordinator.message;

public enum MessageType {
    // Client / Scheduler
    COMMIT_REQUEST,
    ABORT_REQUEST,

    // Phase 1
    PREPARE,
    VOTE_COMMIT,
    VOTE_ABORT,

    // Phase 2
    GLOBAL_COMMIT,
    GLOBAL_ABORT,

    // Completion
    ACK,

    // Recovery / termination
    DECISION_REQUEST,
    DECISION_RESPONSE
}
