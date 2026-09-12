package ufc.victor.experiment;

import ufc.victor.protocol.coordinator.node.NodeId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

public final class RollingMetricsRegistry {

    private final int windowSize;
    private final Deque<TransactionTrialRecord> globalWindow = new ArrayDeque<>();
    private final Map<NodeId, Deque<TransactionTrialRecord>> participantWindows = new LinkedHashMap<>();

    public RollingMetricsRegistry(int windowSize) {
        if (windowSize < 1) throw new IllegalArgumentException("windowSize must be at least 1");
        this.windowSize = windowSize;
    }

    public synchronized AlpsFeatures featuresFor(TransactionContext context) {
        Map<NodeId, HistoricalMetrics> histories = new LinkedHashMap<>();
        for (String participantId : context.participantNodeIds()) {
            NodeId nodeId = new NodeId(participantId);
            histories.put(nodeId, aggregate(participantWindows.getOrDefault(nodeId, new ArrayDeque<>()), nodeId));
        }
        return new AlpsFeatures(context, aggregate(globalWindow, null), histories);
    }

    public synchronized void record(TransactionTrialRecord trial) {
        addBounded(globalWindow, trial);
        for (NodeId nodeId : trial.participants().keySet()) {
            addBounded(participantWindows.computeIfAbsent(nodeId, ignored -> new ArrayDeque<>()), trial);
        }
    }

    private void addBounded(Deque<TransactionTrialRecord> window, TransactionTrialRecord trial) {
        window.addLast(trial);
        while (window.size() > windowSize) window.removeFirst();
    }

    private static HistoricalMetrics aggregate(
            Deque<TransactionTrialRecord> trials,
            NodeId participantFilter
    ) {
        if (trials.isEmpty()) return HistoricalMetrics.empty();

        long committed = 0L;
        long totalLatencyMs = 0L;
        long protocolLogWrites = 0L;
        long resets = 0L;
        long participantObservations = 0L;
        List<Long> redoBytes = new ArrayList<>();
        List<Long> lsnAdvanced = new ArrayList<>();
        List<Long> redoLogWrites = new ArrayList<>();
        List<Long> redoLogWaits = new ArrayList<>();
        long redoMetricSlots = 0L;
        long availableRedoMetricSlots = 0L;

        for (TransactionTrialRecord trial : trials) {
            committed += trial.committed() ? 1 : 0;
            totalLatencyMs += trial.totalLatency().toMillis();
            protocolLogWrites += participantFilter == null
                    ? trial.totalProtocolLogWrites()
                    : trial.participants().get(participantFilter).protocolLogWrites();

            var participants = participantFilter == null
                    ? trial.participants().values()
                    : List.of(trial.participants().get(participantFilter));
            for (ParticipantTrialMetrics metrics : participants) {
                participantObservations++;
                resets += metrics.redo().counterResetDetected() ? 1 : 0;
                redoMetricSlots += 4L;
                availableRedoMetricSlots += add(metrics.redo().redoBytesWritten(), redoBytes);
                availableRedoMetricSlots += add(metrics.redo().lsnAdvanced(), lsnAdvanced);
                availableRedoMetricSlots += add(metrics.redo().logWrites(), redoLogWrites);
                availableRedoMetricSlots += add(metrics.redo().logWaits(), redoLogWaits);
            }
        }

        return new HistoricalMetrics(
                trials.size(),
                committed / (double) trials.size(),
                totalLatencyMs / (double) trials.size(),
                average(redoBytes),
                average(lsnAdvanced),
                average(redoLogWrites),
                average(redoLogWaits),
                protocolLogWrites / (double) trials.size(),
                redoMetricSlots == 0L ? 0.0 : availableRedoMetricSlots / (double) redoMetricSlots,
                participantObservations == 0L ? 0.0 : resets / (double) participantObservations
        );
    }

    private static int add(OptionalLong value, List<Long> destination) {
        if (value.isEmpty()) return 0;
        destination.add(value.getAsLong());
        return 1;
    }

    private static double average(List<Long> values) {
        return values.stream().mapToLong(Long::longValue).average().orElse(0.0);
    }
}
