package ufc.victor.localenv;

import ufc.victor.protocol.commom.MessageHandler;
import ufc.victor.protocol.commom.Network;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LocalNetwork implements Network {
    private final Map<NodeId, MessageHandler> handlers = new ConcurrentHashMap<>();

    @Override
    public void register(NodeId nodeId, MessageHandler handler) {
        handlers.put(nodeId, handler);
    }


    @Override
    public void send(Message msg) {
        MessageHandler handler = handlers.get(msg.to().id);

        if (handler == null) {
            // Node down or unreachable
            return;
        }

        handler.onMessage(msg);
    }
}
