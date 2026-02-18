package ufc.victor.localenv.actors;

import ufc.victor.localenv.actors.ActorNode;
import ufc.victor.protocol.abstractions.ITimeoutHandler;
import ufc.victor.protocol.commom.ITimer;
import ufc.victor.protocol.commom.ITimerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ActorTimerFactory implements ITimerFactory {

    // Shared Scheduler for the whole simulation (efficient)
    private static final ScheduledExecutorService SHARED_SCHEDULER =
            Executors.newScheduledThreadPool(4, new ThreadFactory() {
                private final AtomicInteger count = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "Sim-Timer-" + count.incrementAndGet());
                    t.setDaemon(true); // Allow JVM to exit
                    return t;
                }
            });

    // The physical node this factory serves
    private final ActorNode node;

    // CACHE: Map<Handler, Timer> to implement "createOrGet"
    private final Map<ITimeoutHandler, ITimer> timerCache = new ConcurrentHashMap<>();

    public ActorTimerFactory(ActorNode node) {
        this.node = node;
    }

    @Override
    public ITimer createOrGetTimer(ITimeoutHandler timeoutHandler) {
        // Atomic "Get or Create" operation
        return timerCache.computeIfAbsent(timeoutHandler, handler ->
                new ActorTimer(handler, node, SHARED_SCHEDULER)
        );
    }

    // Helper to stop everything when simulation ends
    public static void shutdown() {
        SHARED_SCHEDULER.shutdownNow();
    }
}