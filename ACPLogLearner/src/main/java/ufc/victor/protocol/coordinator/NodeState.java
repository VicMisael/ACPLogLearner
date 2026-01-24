package ufc.victor.protocol.coordinator;

import ufc.victor.protocol.coordinator.node.Node;

public record NodeState(
        Node node,
        NodeState state
) {
}
