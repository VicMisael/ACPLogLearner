package ufc.victor.protocol.participant.log;

import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.coordinator.log.CoordinatorLogRecordType;

import java.time.Instant;

public record ParticipantLogRecord(TransactionId txId,
                                   ParticipantLogRecordType type,
                                   Instant timestamp) {

}

