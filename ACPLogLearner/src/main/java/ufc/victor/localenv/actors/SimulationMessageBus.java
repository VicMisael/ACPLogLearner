package ufc.victor.localenv.actors;

import ufc.victor.protocol.abstractions.IMessageHandler;
import ufc.victor.protocol.commom.Network;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.util.Map;
import java.util.concurrent.*;

public class SimulationMessageBus implements Network {

    // 1. REGISTRY
    // We map IDs to handlers. In your case, the handler IS the ActorNode.
    private final Map<NodeId, IMessageHandler> routes = new ConcurrentHashMap<>();

    // 2. SIMULATION ENGINE
    // Handles the "travel time" of packets so they don't arrive instantly
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(4, r -> new Thread(r, "Sim-Net-Transport"));

    // 3. CONFIGURATION (Latency)
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
        NodeId targetId = msg.to().id;
        IMessageHandler target = routes.get(targetId);

        if (target == null) {
            System.err.println("NETWORK ERROR: Host unreachable " + targetId);
            return;
        }

        // 1. Calculate Wire Time (Latency)
        // This is crucial for your thesis to simulate "Slow Networks"
        long latency = ThreadLocalRandom.current().nextInt(minLatencyMs, maxLatencyMs + 1);

        // 2. Schedule Delivery (Async)
        // We do NOT call target.onMessage() immediately. We wait.
        scheduler.schedule(() -> {
            try {
                // 3. Physical Delivery
                target.onMessage(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, latency, TimeUnit.MILLISECONDS);
    }

    // --------------------------------------------------------
    // LIFECYCLE
    // --------------------------------------------------------
    public void shutdown() {
        scheduler.shutdownNow();
    }
}