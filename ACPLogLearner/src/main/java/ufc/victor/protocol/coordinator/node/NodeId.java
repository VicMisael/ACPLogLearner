package ufc.victor.protocol.coordinator.node;

public abstract class NodeId {
    private final String id;

    public NodeId(String id) {
        this.id = id;
    }

    public String value() {
        return id;
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof NodeId other) && id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
