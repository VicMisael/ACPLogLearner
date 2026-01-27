package ufc.victor.localenv;

import ufc.victor.protocol.commom.TimeoutHandler;
import ufc.victor.protocol.commom.ITimer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public final class LocalTimer implements ITimer {

    private  final TimeoutHandler handler;
    public LocalTimer(TimeoutHandler handler) {
        this.handler = handler;
    }

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    private ScheduledFuture<?> task;

    @Override
    public synchronized void set() {
        reset();

        task = scheduler.schedule(
                handler::onTimeout,
                1, // seconds (tune later)
                TimeUnit.SECONDS
        );
    }

    @Override
    public synchronized void reset() {
        if (task != null) {
            task.cancel(false);
            task = null;
        }
    }
}

