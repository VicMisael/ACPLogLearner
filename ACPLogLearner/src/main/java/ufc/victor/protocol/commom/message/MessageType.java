package ufc.victor.protocol.commom.message;

public enum MessageType {
    // Client / Scheduler
    COMMIT_REQUEST,

    // Phase 1
    PREPARE_2PC,
    PREPARE_2PC_PAB,
    PREPARE_2PC_PCO,
    VOTE_COMMIT,
    VOTE_ABORT,

    // Phase 2
    GLOBAL_COMMIT,
    GLOBAL_ABORT,

    // Completion
    ACK
}
