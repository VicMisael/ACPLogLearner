package ufc.victor.protocol.participant.log;

import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.coordinator.log.CoordinatorLogRecordType;

import java.time.Instant;

public final class ParticipantLogRecord {

    private final TransactionId txId;
    private final ParticipantLogRecordType type;
    private final Instant timestamp;

    public ParticipantLogRecord(TransactionId txId, ParticipantLogRecordType type, Instant timestamp) {
        this.txId = txId;
        this.type = type;
        this.timestamp = timestamp;
    }

    public TransactionId txId() {
        return txId;
    }

    public ParticipantLogRecordType type() {
        return type;
    }

    public Instant timestamp() {
        return timestamp;
    }
}

