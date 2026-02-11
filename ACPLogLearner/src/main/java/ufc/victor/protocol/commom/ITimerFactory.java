package ufc.victor.protocol.commom;

import ufc.victor.protocol.abstractions.ITimeoutHandler;

public interface ITimerFactory {
    public ITimer createOrGetTimer(ITimeoutHandler timeoutHandler);
}
