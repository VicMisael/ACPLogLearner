package ufc.victor.protocol.coordinator;

import ufc.victor.protocol.coordinator.node.Node;


public final class NodeStatus {

    private final Node node;
    private NodeState state;

    public NodeStatus(Node node, NodeState state) {
        this.node = node;
        this.state = state;
    }

    public Node node() { return node; }

    public NodeState state() { return state; }

    public void setState(NodeState state) {
        this.state = state;
    }
}

