package ufc.victor.protocol.abstractions;

import ufc.victor.protocol.participant.AdaptiveParticipantDispatcher;

public sealed interface IProtocol extends IMessageHandler, ITimeoutHandler permits ICoordinator, IParticipant {
    void recover();
}
