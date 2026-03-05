package ufc.victor.protocol.abstractions;

public sealed interface IProtocol extends IMessageHandler, ITimeoutHandler permits ICoordinator,IParticipant{
    void recover();
}
