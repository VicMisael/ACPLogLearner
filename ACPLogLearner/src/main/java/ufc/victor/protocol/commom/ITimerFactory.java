package ufc.victor.protocol.commom;

public interface ITimerFactory {
    public ITimer createTimer(TimeoutHandler timeoutHandler);
}
