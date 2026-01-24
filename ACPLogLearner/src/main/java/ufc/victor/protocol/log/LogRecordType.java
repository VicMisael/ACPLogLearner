package ufc.victor.protocol.log;

public  enum LogRecordType {
    BEGIN_COMMIT,
    READY,
    GLOBAL_COMMIT,
    GLOBAL_ABORT,
    COMMIT,
    ABORT,
    END_TRANSACTION
}