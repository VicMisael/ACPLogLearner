package ufc.victor.localenv;

import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.participant.log.ParticipantLogManager;
import ufc.victor.protocol.participant.log.ParticipantLogRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryParticipantLogManager implements ParticipantLogManager {

    private final Map<TransactionId, List<ParticipantLogRecord>> logs = new ConcurrentHashMap<>();
    @Override
    public List<ParticipantLogRecord> read(TransactionId txId) {
        return logs.get(txId);
    }

    @Override
    public void write(ParticipantLogRecord record) {
        logs.computeIfAbsent(record.txId(), k -> new ArrayList<>())
                .add(record);
    }
}
