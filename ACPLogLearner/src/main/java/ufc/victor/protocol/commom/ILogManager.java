package ufc.victor.protocol.commom;

import ufc.victor.protocol.coordinator.log.LogRecord;
import ufc.victor.protocol.participant.log.ParticipantLogRecord;

import java.util.List;

public interface ILogManager<T> {
    void write(T log);

    List<T> read(TransactionId txId);

    T getLast(TransactionId txId);
}
