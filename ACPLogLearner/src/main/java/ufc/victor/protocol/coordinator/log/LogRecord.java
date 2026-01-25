package ufc.victor.protocol.coordinator.log;

import ufc.victor.protocol.commom.TransactionId;

import java.time.Instant;

public final class LogRecord {

    private final TransactionId txId;
    private final CoordinatorLogRecordType type;
    private final Instant timestamp;

    public LogRecord(TransactionId txId, CoordinatorLogRecordType type, Instant timestamp) {
        this.txId = txId;
        this.type = type;
        this.timestamp = timestamp;
    }

    public TransactionId txId() {
        return txId;
    }

    public CoordinatorLogRecordType type() {
        return type;
    }

    public Instant timestamp() {
        return timestamp;
    }
}

