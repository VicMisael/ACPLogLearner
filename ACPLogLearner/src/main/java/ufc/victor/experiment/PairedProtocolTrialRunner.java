package ufc.victor.experiment;

import ufc.victor.protocol.SelectedProtocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public final class PairedProtocolTrialRunner<SPEC, SNAPSHOT> {
    private final TrialStateController<SPEC, SNAPSHOT> stateController;
    private final ProtocolTrialExecutor<SPEC> executor;
    private final ProtocolCostFunction costFunction;

    public PairedProtocolTrialRunner(
            TrialStateController<SPEC, SNAPSHOT> stateController,
            ProtocolTrialExecutor<SPEC> executor,
            ProtocolCostFunction costFunction
    ) {
        if (stateController == null || executor == null || costFunction == null) {
            throw new IllegalArgumentException("paired trial dependencies must not be null");
        }
        this.stateController = stateController;
        this.executor = executor;
        this.costFunction = costFunction;
    }

    public PairedProtocolTrial run(SPEC specification, TrialOutcome expectedOutcome, long orderSeed)
            throws Exception {
        SNAPSHOT snapshot = stateController.capture(specification);
        List<SelectedProtocol> order = new ArrayList<>(List.of(SelectedProtocol.values()));
        Collections.shuffle(order, new Random(orderSeed));
        Map<SelectedProtocol, TransactionTrialRecord> trials = new EnumMap<>(SelectedProtocol.class);
        Exception failure = null;

        for (SelectedProtocol protocol : order) {
            try {
                trials.put(protocol, executor.execute(specification, protocol));
            } catch (Exception exception) {
                failure = exception;
            } finally {
                try {
                    stateController.restoreAndVerify(snapshot);
                } catch (Exception restoreFailure) {
                    if (failure != null) failure.addSuppressed(restoreFailure);
                    else failure = restoreFailure;
                }
            }
            if (failure != null) throw failure;
        }

        Map<SelectedProtocol, ProtocolTrialCost> costs = new EnumMap<>(SelectedProtocol.class);
        trials.forEach((protocol, trial) ->
                costs.put(protocol, costFunction.evaluate(trial, expectedOutcome)));
        return new PairedProtocolTrial(trials, costs, PairedProtocolTrial.chooseOracle(costs));
    }
}
