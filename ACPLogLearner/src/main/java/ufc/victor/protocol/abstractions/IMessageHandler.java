package ufc.victor.protocol.abstractions;

import ufc.victor.protocol.commom.message.Message;

public sealed interface IMessageHandler permits ICoordinator, IParticipant, IProtocol {
    void onMessage(Message msg);
}