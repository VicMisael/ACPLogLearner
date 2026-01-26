package ufc.victor.protocol.commom.message;


import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.coordinator.node.Node;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.time.Instant;

public final class Message {

    private final MessageType type;
    private final TransactionId transactionId;
    private final Node from;
    private final Node to;
    private final MessagePayload payload;
    private final Instant timestamp;

    private Message(
            MessageType type,
            TransactionId transactionId,
            Node from,
            Node to,
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
            Node from,
            Node to,
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

    public Node from() {
        return from;
    }

    public Node to() {
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