package ufc.victor.protocol.log;

import ufc.victor.protocol.commom.TransactionId;

import java.util.List;

public interface LogManager {
    void write(LogRecord record);

    List<LogRecord> read(TransactionId txId);

    boolean contains(TransactionId txId, LogRecordType type);
}
