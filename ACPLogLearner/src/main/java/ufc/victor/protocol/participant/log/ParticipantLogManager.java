package ufc.victor.protocol.participant.log;

import ufc.victor.protocol.commom.TransactionId;

import java.util.List;


public interface ParticipantLogManager {
     List<ParticipantLogRecord> read(TransactionId txId);

    void write(ParticipantLogRecord logRecord);
}
