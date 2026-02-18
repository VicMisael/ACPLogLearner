package ufc.victor.protocol.abstractions;

import ufc.victor.localenv.actors.ActorNode;
import ufc.victor.protocol.commom.message.Message;

public  interface IMessageHandler {
    void onMessage(Message msg);
}