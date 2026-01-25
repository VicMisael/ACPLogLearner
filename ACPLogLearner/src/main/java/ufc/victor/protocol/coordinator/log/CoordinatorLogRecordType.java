package ufc.victor.protocol.coordinator.log;

public  enum CoordinatorLogRecordType {
    BEGIN_COMMIT,
    GLOBAL_COMMIT,
    GLOBAL_ABORT,
    END_TRANSACTION
}