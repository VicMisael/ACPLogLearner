package ufc.victor.experiment;

import ufc.victor.protocol.SelectedProtocol;

@FunctionalInterface
public interface ProtocolTrialExecutor<SPEC> {
    TransactionTrialRecord execute(SPEC specification, SelectedProtocol protocol) throws Exception;
}
