package ufc.victor.localenv.actors;

import ufc.victor.protocol.abstractions.IMessageHandler;
import ufc.victor.protocol.commom.Network;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.util.Map;
import java.util.concurrent.*;

public class SimulationMessageBus implements Network {


    private final Map<NodeId, IMessageHandler> routes = new ConcurrentHashMap<>();


    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(4, r -> new Thread(r, "Sim-Net-Transport"));

    private final int minLatencyMs;
    private final int maxLatencyMs;

    public SimulationMessageBus(int minLatencyMs, int maxLatencyMs) {
        this.minLatencyMs = minLatencyMs;
        this.maxLatencyMs = maxLatencyMs;
    }

    // --------------------------------------------------------
    // INTERFACE IMPLEMENTATION
    // --------------------------------------------------------

    @Override
    public void register(NodeId nodeId, IMessageHandler handler) {
        routes.put(nodeId, handler);
    }

    @Override
    public void send(Message msg) {
        // SAFETY CHECK: Is the network plugged in?
        if (scheduler.isShutdown()) {
            // Silently drop, or log a warning. Do not crash.
            // System.out.println("[Network] Dropped packet (Network Down): " + msg.type());
            return;
        }

        NodeId targetId = msg.to().id;
        IMessageHandler target = routes.get(targetId);

        if (target == null) {
            System.err.println("NETWORK ERROR: Host unreachable " + targetId);
            return;
        }

        long latency = ThreadLocalRandom.current().nextInt(minLatencyMs, maxLatencyMs + 1);

        try {
            scheduler.schedule(() -> {
                try {
                    target.onMessage(msg);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, latency, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // Double-check catch in case of race condition right at the 'schedule' call
        }
    }

    // --------------------------------------------------------
    // LIFECYCLE
    // --------------------------------------------------------
    public void shutdown() {
        scheduler.shutdownNow();
    }
}