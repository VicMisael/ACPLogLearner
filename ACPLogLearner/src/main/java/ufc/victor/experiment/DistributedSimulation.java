package ufc.victor.experiment;

import ufc.victor.localenv.InMemoryCoordinatorLogManager;
import ufc.victor.localenv.InMemoryParticipantLogManager;
import ufc.victor.localenv.LocalNode;
import ufc.victor.localenv.actors.ActorNode;
import ufc.victor.localenv.actors.ActorTimerFactory;
import ufc.victor.localenv.actors.SimulationMessageBus;
import ufc.victor.protocol.SelectedProtocol;
import ufc.victor.protocol.abstractions.ICoordinator;
import ufc.victor.protocol.abstractions.IParticipant;
import ufc.victor.protocol.commom.ITimerFactory;
import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.commom.message.MessageType;
import ufc.victor.protocol.coordinator.AdaptiveCoordinatorDispatcher;
import ufc.victor.protocol.coordinator.log.CoordinatorLogRecordType;
import ufc.victor.protocol.coordinator.log.LogRecord;
import ufc.victor.protocol.coordinator.node.Node;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.protocol.participant.AdaptiveParticipantDispatcher;
import ufc.victor.protocol.participant.TransactionalResource;
import ufc.victor.protocol.participant.log.ParticipantLogRecord;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DistributedSimulation {

    private final LogDrivenExperimentBuilder builder = new LogDrivenExperimentBuilder();

    public SimulationResult run(
            Scenario scenario,
            NodeCatalog nodeCatalog,
            SelectedProtocol protocol
    ) throws InterruptedException {
        return run(scenario, nodeCatalog, protocol, null);
    }

    public SimulationResult run(
            Scenario scenario,
            NodeCatalog nodeCatalog,
            SelectedProtocol protocol,
            TransactionPlan transactionPlan
    ) throws InterruptedException {
        NodeProfile coordinatorProfile = nodeCatalog.getRequired(scenario.coordinatorNodeId());
        List<NodeProfile> participantProfiles = nodeCatalog.resolve(scenario.participantNodeIds());

        SimulationMessageBus network = new SimulationMessageBus(
                coordinatorProfile.minLatencyMs(),
                coordinatorProfile.maxLatencyMs()
        );
        TransactionId txId = TransactionId.newTransaction();

        NodeId coordinatorId = new NodeId("COORD[" + coordinatorProfile.nodeName() + "]");
        LocalNode coordinatorNode = new LocalNode(coordinatorId);
        ActorNode coordinatorActor = new ActorNode(coordinatorId);
        InMemoryCoordinatorLogManager coordinatorLog = new InMemoryCoordinatorLogManager();
        ITimerFactory coordinatorTimerFactory = new ActorTimerFactory(coordinatorActor);

        network.register(coordinatorId, coordinatorActor);
        network.setNodeLatency(
                coordinatorId,
                coordinatorProfile.minLatencyMs(),
                coordinatorProfile.maxLatencyMs()
        );

        Map<NodeId, ActorNode> participantActors = new LinkedHashMap<>();
        Map<NodeId, InMemoryParticipantLogManager> participantLogs = new LinkedHashMap<>();
        Set<Node> participants = new LinkedHashSet<>();

        for (int i = 0; i < participantProfiles.size(); i++) {
            NodeProfile profile = participantProfiles.get(i);
            NodeId participantId = new NodeId("P" + (i + 1) + "[" + profile.nodeName() + "]");
            LocalNode participantNode = new LocalNode(participantId);
            ActorNode participantActor = new ActorNode(participantId);
            InMemoryParticipantLogManager participantLog = new InMemoryParticipantLogManager();
            ITimerFactory participantTimerFactory = new ActorTimerFactory(participantActor);
            boolean shouldCommit = shouldCommit(transactionPlan, i, profile);

            IParticipant participantLogic = new AdaptiveParticipantDispatcher(
                    participantNode,
                    participantLog,
                    participantTimerFactory,
                    new ProfiledTransactionalResource(shouldCommit, profile.prepareDelayMs()),
                    network
            );

            participantActor.setProtocolLogic(participantLogic);
            participantActors.put(participantId, participantActor);
            participantLogs.put(participantId, participantLog);
            participants.add(participantNode);

            network.register(participantId, participantActor);
            network.setNodeLatency(participantId, profile.minLatencyMs(), profile.maxLatencyMs());
        }

        ICoordinator coordinatorLogic = new AdaptiveCoordinatorDispatcher(
                txId,
                coordinatorNode,
                participants,
                coordinatorLog,
                network,
                coordinatorTimerFactory,
                protocol
        );
        coordinatorActor.setProtocolLogic(coordinatorLogic);

        coordinatorActor.start();
        for (ActorNode participantActor : participantActors.values()) {
            participantActor.start();
        }

        Instant startedAt = Instant.now();
        network.send(Message.of(MessageType.COMMIT_REQUEST, txId, coordinatorNode, coordinatorNode));
        boolean finished = waitUntilFinished(protocol, coordinatorLog, txId, scenario.timeoutMs());
        long elapsedMs = finished
                ? Math.max(1L, Instant.now().toEpochMilli() - startedAt.toEpochMilli())
                : scenario.timeoutMs();

        List<LogRecord> coordinatorRecords = List.copyOf(coordinatorLog.read(txId));
        Map<NodeId, List<ParticipantLogRecord>> participantRecords = new LinkedHashMap<>();
        for (Map.Entry<NodeId, InMemoryParticipantLogManager> entry : participantLogs.entrySet()) {
            participantRecords.put(entry.getKey(), List.copyOf(entry.getValue().read(txId)));
        }

        shutdown(coordinatorActor, participantActors.values(), network);

        return builder.build(
                scenario,
                protocol,
                elapsedMs,
                network.getSentMessageCount(),
                coordinatorRecords,
                participantRecords
        );
    }

    private static boolean shouldCommit(TransactionPlan transactionPlan, int participantIndex, NodeProfile profile) {
        if (transactionPlan != null) {
            return transactionPlan.participantShouldCommit().get(participantIndex);
        }
        return profile.abortProbability() <= 0.0;
    }

    private static boolean waitUntilFinished(
            SelectedProtocol protocol,
            InMemoryCoordinatorLogManager coordinatorLog,
            TransactionId txId,
            long timeoutMs
    ) throws InterruptedException {
        long startedAt = System.currentTimeMillis();

        while (System.currentTimeMillis() - startedAt < timeoutMs) {
            LogRecord last = coordinatorLog.getLast(txId);
            if (last != null && isTerminal(protocol, last.type())) {
                return true;
            }
            Thread.sleep(10L);
        }

        return false;
    }

    private static boolean isTerminal(SelectedProtocol protocol, CoordinatorLogRecordType type) {
        if (type == CoordinatorLogRecordType.END_TRANSACTION) {
            return true;
        }
        if (protocol == SelectedProtocol.TWO_PC_PRESUMED_ABORT && type == CoordinatorLogRecordType.GLOBAL_ABORT) {
            return true;
        }
        return protocol == SelectedProtocol.TWO_PC_PRESUMED_COMMIT && type == CoordinatorLogRecordType.GLOBAL_COMMIT;
    }

    private static void shutdown(
            ActorNode coordinatorActor,
            Iterable<ActorNode> participantActors,
            SimulationMessageBus network
    ) throws InterruptedException {
        network.shutdown();
        coordinatorActor.stop();
        coordinatorActor.getWorkerThread().join(500L);

        for (ActorNode participantActor : participantActors) {
            participantActor.stop();
            participantActor.getWorkerThread().join(500L);
        }
    }

    private static final class ProfiledTransactionalResource implements TransactionalResource {

        private final boolean shouldCommit;
        private final int prepareDelayMs;

        private ProfiledTransactionalResource(boolean shouldCommit, int prepareDelayMs) {
            this.shouldCommit = shouldCommit;
            this.prepareDelayMs = prepareDelayMs;
        }

        @Override
        public boolean prepare(TransactionId txId) {
            sleep(prepareDelayMs);
            return shouldCommit;
        }

        @Override
        public void commit(TransactionId txId) {
            // No-op for simulation
        }

        @Override
        public void abort(TransactionId txId) {
            // No-op for simulation
        }

        private static void sleep(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
