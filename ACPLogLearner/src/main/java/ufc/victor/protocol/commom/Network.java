package ufc.victor.protocol.commom;

import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.coordinator.node.Node;
import ufc.victor.protocol.coordinator.node.NodeId;

public interface Network {
    void register(NodeId node, MessageHandler handler);
    void send(Message msg);

}
