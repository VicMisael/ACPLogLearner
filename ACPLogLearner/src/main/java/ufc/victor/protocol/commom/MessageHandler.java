package ufc.victor.protocol.commom;

import ufc.victor.protocol.commom.message.Message;

@FunctionalInterface
public interface MessageHandler {
    void onMessage(Message msg);
}