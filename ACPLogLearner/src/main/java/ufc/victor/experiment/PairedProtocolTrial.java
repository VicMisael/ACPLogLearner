package ufc.victor.experiment;

import ufc.victor.protocol.SelectedProtocol;

import java.util.Comparator;
import java.util.EnumMap;
import java.util.Map;

public record PairedProtocolTrial(
        Map<SelectedProtocol, TransactionTrialRecord> trials,
        Map<SelectedProtocol, ProtocolTrialCost> costs,
        SelectedProtocol oracleProtocol
) {
    public PairedProtocolTrial {
        EnumMap<SelectedProtocol, TransactionTrialRecord> copiedTrials =
                new EnumMap<>(SelectedProtocol.class);
        copiedTrials.putAll(trials);
        EnumMap<SelectedProtocol, ProtocolTrialCost> copiedCosts =
                new EnumMap<>(SelectedProtocol.class);
        copiedCosts.putAll(costs);
        for (SelectedProtocol protocol : SelectedProtocol.values()) {
            if (!copiedTrials.containsKey(protocol) || !copiedCosts.containsKey(protocol)) {
                throw new IllegalArgumentException("missing paired result for " + protocol);
            }
        }
        if (!copiedCosts.get(oracleProtocol).valid()) {
            throw new IllegalArgumentException("oracle protocol must be valid");
        }
        trials = Map.copyOf(copiedTrials);
        costs = Map.copyOf(copiedCosts);
    }

    public static SelectedProtocol chooseOracle(Map<SelectedProtocol, ProtocolTrialCost> costs) {
        return costs.entrySet().stream()
                .filter(entry -> entry.getValue().valid())
                .min(Comparator
                        .comparingDouble((Map.Entry<SelectedProtocol, ProtocolTrialCost> entry) ->
                                entry.getValue().total())
                        .thenComparingInt(entry -> entry.getKey().ordinal()))
                .map(Map.Entry::getKey)
                .orElseThrow(() -> new IllegalStateException("no valid protocol trial for oracle"));
    }

    public double regret(SelectedProtocol selected) {
        return costs.get(selected).total() - costs.get(oracleProtocol).total();
    }
}
