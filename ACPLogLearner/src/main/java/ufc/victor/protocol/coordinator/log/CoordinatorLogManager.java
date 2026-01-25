package ufc.victor.protocol.coordinator.log;

import ufc.victor.protocol.commom.TransactionId;

import java.util.List;

public interface CoordinatorLogManager {
    void write(LogRecord record);

    List<LogRecord> read(TransactionId txId);

    boolean contains(TransactionId txId, CoordinatorLogRecordType type);
}
