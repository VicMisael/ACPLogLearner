package ufc.victor.experiment;

import ufc.victor.protocol.SelectedProtocol;
import ufc.victor.protocol.coordinator.log.LogRecord;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.protocol.participant.log.ParticipantLogRecord;

import java.util.List;
import java.util.Map;

public record SimulationResult(
        Scenario scenario,
        SelectedProtocol protocol,
        TxMetrics metrics,
        List<LogRecord> coordinatorLogRecords,
        Map<NodeId, List<ParticipantLogRecord>> participantLogRecords
) {
}
