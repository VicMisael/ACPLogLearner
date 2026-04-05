package ufc.victor.localenv.actors;

import ufc.victor.protocol.abstractions.IMessageHandler;
import ufc.victor.protocol.commom.Network;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.commom.message.MessageType;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class SimulationMessageBus implements Network {


    private final Map<NodeId, IMessageHandler> routes = new ConcurrentHashMap<>();
    private final Map<NodeId, LatencyProfile> nodeLatencies = new ConcurrentHashMap<>();
    private final Map<MessageType, Double> dropProbabilities = new ConcurrentHashMap<>();
    private final Map<MessageType, Integer> extraDelayMs = new ConcurrentHashMap<>();


    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(4, r -> new Thread(r, "Sim-Net-Transport"));

    private final int minLatencyMs;
    private final int maxLatencyMs;
    private final AtomicLong sentMessageCount = new AtomicLong(0);

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

    public void setNodeLatency(NodeId nodeId, int minLatencyMs, int maxLatencyMs) {
        nodeLatencies.put(nodeId, new LatencyProfile(minLatencyMs, maxLatencyMs));
    }

    public void setDropProbability(MessageType type, double probability) {
        if (probability < 0.0 || probability > 1.0) {
            throw new IllegalArgumentException("probability must be in [0, 1]");
        }
        dropProbabilities.put(type, probability);
    }

    public void setExtraDelay(MessageType type, int delayMs) {
        if (delayMs < 0) {
            throw new IllegalArgumentException("delayMs must be >= 0");
        }
        extraDelayMs.put(type, delayMs);
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

        double dropProbability = dropProbabilities.getOrDefault(msg.type(), 0.0);
        if (ThreadLocalRandom.current().nextDouble() < dropProbability) {
            return;
        }

        sentMessageCount.incrementAndGet();
        LatencyProfile profile = nodeLatencies.getOrDefault(targetId, new LatencyProfile(minLatencyMs, maxLatencyMs));
        long latency = ThreadLocalRandom.current().nextInt(profile.minLatencyMs(), profile.maxLatencyMs() + 1)
                + extraDelayMs.getOrDefault(msg.type(), 0);

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

    public long getSentMessageCount() {
        return sentMessageCount.get();
    }

    public void resetSentMessageCount() {
        sentMessageCount.set(0);
    }

    private record LatencyProfile(int minLatencyMs, int maxLatencyMs) {
        private LatencyProfile {
            if (minLatencyMs < 0 || maxLatencyMs < minLatencyMs) {
                throw new IllegalArgumentException("invalid latency bounds");
            }
        }
    }
}
