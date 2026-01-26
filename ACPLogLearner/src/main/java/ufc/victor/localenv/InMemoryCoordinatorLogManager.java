package ufc.victor.localenv;

import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.coordinator.log.CoordinatorLogManager;
import ufc.victor.protocol.coordinator.log.LogRecord;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class InMemoryCoordinatorLogManager implements CoordinatorLogManager {

    private final Map<TransactionId, List<LogRecord>> logs = new ConcurrentHashMap<>();


    @Override
    public void write(LogRecord record) {
        logs.computeIfAbsent(record.txId(), k -> new ArrayList<>())
                .add(record);
    }

    @Override
    public List<LogRecord> read(TransactionId txId) {
        return logs.get(txId);
    }
}
