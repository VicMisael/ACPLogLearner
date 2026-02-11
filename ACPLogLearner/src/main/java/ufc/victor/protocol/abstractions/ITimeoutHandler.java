package ufc.victor.protocol.abstractions;


public sealed interface ITimeoutHandler permits  IProtocol {
    void onTimeout();
}
