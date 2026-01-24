package ufc.victor.protocol.commom;

import java.io.Serializable;
import java.util.UUID;

public final class TransactionId implements Serializable {

    private final UUID id;

    private TransactionId(UUID id) {
        this.id = id;
    }

    public static TransactionId newTransaction() {
        return new TransactionId(UUID.randomUUID());
    }

    public UUID value() {
        return id;
    }

    @Override
    public String toString() {
        return id.toString();
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof TransactionId other) && id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}