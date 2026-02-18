package ufc.victor.localenv.actors;

import ufc.victor.localenv.actors.ActorNode;
import ufc.victor.protocol.abstractions.ITimeoutHandler;
import ufc.victor.protocol.abstractions.events.TimeoutEvent;
import ufc.victor.protocol.commom.ITimer;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ActorTimer implements ITimer {

    private final ITimeoutHandler handler;
    private final ActorNode node;
    private final ScheduledExecutorService scheduler;

    private ScheduledFuture<?> task;

    public ActorTimer(ITimeoutHandler handler, ActorNode node, ScheduledExecutorService scheduler) {
        this.handler = handler;
        this.node = node;
        this.scheduler = scheduler;
    }

    @Override
    public synchronized void set() {
        // 1. Cancel previous task if this timer is being reused
        reset();

        // 2. Schedule the event injection
        // TODO: For your thesis, make this 3000ms configurable!
        task = scheduler.schedule(() -> {

            // 3. Inject event into Actor's inbox
            node.onEvent(new TimeoutEvent());

        }, 6_000, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void reset() {
        if (task != null && !task.isDone()) {
            task.cancel(false);
            task = null;
        }
    }
}