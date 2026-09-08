package ufc.victor.telemetry.redo;

import ufc.victor.protocol.coordinator.node.NodeId;

/** Captures cumulative redo-log counters without participating in the measured transaction. */
public interface RedoMetricsCollector {

    RedoSnapshot capture(NodeId nodeId);
}
