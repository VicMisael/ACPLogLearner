package ufc.victor.protocol.commom;

import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.coordinator.node.NodeId;

public interface Network {
    void register(NodeId node, IMessageHandler handler);
    void send(Message msg);

}
