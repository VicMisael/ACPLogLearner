package ufc.victor.protocol.coordinator.message;


import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.time.Instant;

public final class Message {

    private final MessageType type;
    private final TransactionId transactionId;
    private final NodeId from;
    private final NodeId to;
    private final MessagePayload payload;
    private final Instant timestamp;

    private Message(
            MessageType type,
            TransactionId transactionId,
            NodeId from,
            NodeId to,
            MessagePayload payload
    ) {
        this.type = type;
        this.transactionId = transactionId;
        this.from = from;
        this.to = to;
        this.payload = payload;
        this.timestamp = Instant.now();
    }

    public static Message of(
            MessageType type,
            TransactionId txId,
            NodeId from,
            NodeId to,
            MessagePayload payload
    ) {
        return new Message(type, txId, from, to, payload);
    }

    public MessageType type() {
        return type;
    }

    public TransactionId transactionId() {
        return transactionId;
    }

    public NodeId from() {
        return from;
    }

    public NodeId to() {
        return to;
    }

    public MessagePayload payload() {
        return payload;
    }

    public Instant timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Message[" +
                "type=" + type +
                ", tx=" + transactionId +
                ", from=" + from +
                ", to=" + to +
                ", payload=" + payload.getClass().getSimpleName() +
                ", ts=" + timestamp +
                "]";
    }
}