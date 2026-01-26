package ufc.victor.protocol.coordinator.log;

import ufc.victor.protocol.commom.TransactionId;

import java.time.Instant;

public record LogRecord(TransactionId txId, CoordinatorLogRecordType type, Instant timestamp) {

}

