package ufc.victor.experiment;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class NodeCatalog {

    private final Map<String, NodeProfile> nodesById;

    public NodeCatalog(List<NodeProfile> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }

        Map<String, NodeProfile> ordered = new LinkedHashMap<>();
        for (NodeProfile node : nodes) {
            NodeProfile previous = ordered.putIfAbsent(node.nodeName(), node);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate node name: " + node.nodeName());
            }
        }
        this.nodesById = Map.copyOf(ordered);
    }

    public NodeProfile getRequired(String nodeId) {
        NodeProfile profile = nodesById.get(nodeId);
        if (profile == null) {
            throw new IllegalArgumentException("unknown node id: " + nodeId);
        }
        return profile;
    }

    public List<NodeProfile> resolve(List<String> nodeIds) {
        return nodeIds.stream()
                .map(this::getRequired)
                .toList();
    }

    public List<String> nodeIds() {
        return nodesById.keySet().stream().toList();
    }
}
