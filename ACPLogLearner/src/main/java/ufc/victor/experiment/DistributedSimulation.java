package ufc.victor.experiment;

import ufc.victor.localenv.InMemoryCoordinatorLogManager;
import ufc.victor.localenv.InMemoryParticipantLogManager;
import ufc.victor.localenv.LocalNode;
import ufc.victor.localenv.actors.ActorNode;
import ufc.victor.localenv.actors.ActorTimerFactory;
import ufc.victor.localenv.actors.SimulationMessageBus;
import ufc.victor.protocol.SelectedProtocol;
import ufc.victor.protocol.coordinator.AdaptiveCoordinatorDispatcher;
import ufc.victor.protocol.abstractions.ICoordinator;
import ufc.victor.protocol.abstractions.IParticipant;
import ufc.victor.protocol.commom.ITimerFactory;
import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.commom.message.MessageType;
import ufc.victor.protocol.coordinator.log.CoordinatorLogRecordType;
import ufc.victor.protocol.coordinator.log.LogRecord;
import ufc.victor.protocol.coordinator.node.Node;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.protocol.participant.AdaptiveParticipantDispatcher;
import ufc.victor.protocol.participant.TransactionalResource;
import ufc.victor.protocol.participant.log.ParticipantLogRecord;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DistributedSimulation {

    private final LogDrivenExperimentBuilder experimentBuilder = new LogDrivenExperimentBuilder();

    public SimulationResult run(Scenario scenario, SelectedProtocol protocol) throws InterruptedException {
        SimulationMessageBus network = new SimulationMessageBus(
                scenario.coordinatorProfile().minLatencyMs(),
                scenario.coordinatorProfile().maxLatencyMs()
        );
        applyFaultPlan(network, scenario.faultPlan());
        TransactionId txId = TransactionId.newTransaction();

        NodeId coordinatorId = new NodeId("COORD");
        LocalNode coordinatorNode = new LocalNode(coordinatorId);
        InMemoryCoordinatorLogManager coordinatorLog = new InMemoryCoordinatorLogManager();

        Map<NodeId, ParticipantRuntime> participantRuntimes = new LinkedHashMap<>();
        Map<NodeId, LocalNode> participantNodes = new LinkedHashMap<>();
        Map<NodeId, InMemoryParticipantLogManager> participantLogs = new LinkedHashMap<>();

        Random voteRandom = new Random(scenario.randomSeed());

        for (int i = 1; i <= scenario.participantProfiles().size(); i++) {
            NodeProfile profile = scenario.participantProfiles().get(i - 1);
            NodeId participantId = new NodeId("P" + i);
            LocalNode node = new LocalNode(participantId);
            InMemoryParticipantLogManager logManager = new InMemoryParticipantLogManager();
            CrashSpec beforeReadyCrash = findCrashSpec(scenario.faultPlan(), participantId.value(), CrashTrigger.PARTICIPANT_BEFORE_READY);
            boolean shouldCommit = voteRandom.nextDouble() >= profile.abortProbability();

            participantNodes.put(participantId, node);
            participantLogs.put(participantId, logManager);
            participantRuntimes.put(
                    participantId,
                    new ParticipantRuntime(
                            participantId,
                            node,
                            profile,
                            protocol,
                            txId,
                            coordinatorNode,
                            logManager,
                            shouldCommit,
                            beforeReadyCrash,
                            network
                    )
            );
        }

        Set<Node> participantNodeSet = participantNodes.values().stream()
                .map(n -> (Node) n)
                .collect(Collectors.toSet());

        CoordinatorRuntime coordinatorRuntime = new CoordinatorRuntime(
                coordinatorId,
                coordinatorNode,
                scenario.coordinatorProfile(),
                protocol,
                txId,
                participantNodeSet,
                coordinatorLog,
                network
        );
        coordinatorRuntime.start();
        for (ParticipantRuntime runtime : participantRuntimes.values()) {
            runtime.start();
        }

        long startNanos = System.nanoTime();
        Message start = Message.of(MessageType.COMMIT_REQUEST, txId, coordinatorNode, coordinatorNode);
        network.send(start);

        boolean completed = waitUntilFinished(
                protocol,
                coordinatorLog,
                txId,
                scenario.timeoutMs(),
                scenario.faultPlan(),
                coordinatorRuntime,
                participantRuntimes
        );
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;

        List<LogRecord> coordinatorRecords = List.copyOf(coordinatorLog.read(txId));
        Map<NodeId, List<ParticipantLogRecord>> participantRecords = new LinkedHashMap<>();
        for (Map.Entry<NodeId, InMemoryParticipantLogManager> entry : participantLogs.entrySet()) {
            participantRecords.put(entry.getKey(), List.copyOf(entry.getValue().read(txId)));
        }

        coordinatorRuntime.shutdown();
        for (ParticipantRuntime runtime : participantRuntimes.values()) {
            runtime.shutdown();
        }
        network.shutdown();

        long boundedElapsedMs = completed ? elapsedMs : scenario.timeoutMs();
        return experimentBuilder.build(
                scenario,
                protocol,
                boundedElapsedMs,
                network.getSentMessageCount(),
                coordinatorRecords,
                participantRecords
        );
    }

    private static void applyFaultPlan(SimulationMessageBus network, FaultPlan faultPlan) {
        for (Map.Entry<MessageType, Double> entry : faultPlan.dropProbabilities().entrySet()) {
            network.setDropProbability(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<MessageType, Integer> entry : faultPlan.extraDelayMs().entrySet()) {
            network.setExtraDelay(entry.getKey(), entry.getValue());
        }
    }

    private static CrashSpec findCrashSpec(FaultPlan plan, String nodeName, CrashTrigger trigger) {
        for (CrashSpec crash : plan.crashes()) {
            if (crash.nodeName().equals(nodeName) && crash.trigger() == trigger) {
                return crash;
            }
        }
        return null;
    }

    private static ICoordinator buildCoordinator(
            SelectedProtocol protocol,
            TransactionId txId,
            Node coordinator,
            Set<Node> participants,
            InMemoryCoordinatorLogManager log,
            SimulationMessageBus network,
            ITimerFactory timerFactory
    ) {
        return new AdaptiveCoordinatorDispatcher(
                txId,
                coordinator,
                participants,
                log,
                network,
                timerFactory,
                protocol
        );
    }

    private static IParticipant buildParticipant(
            LocalNode participant,
            InMemoryParticipantLogManager log,
            ITimerFactory timerFactory,
            TransactionalResource resource,
            SimulationMessageBus network,
            SelectedProtocol protocol,
            TransactionId txId,
            Node coordinator
    ) {
        return new AdaptiveParticipantDispatcher(
                participant,
                log,
                timerFactory,
                resource,
                network,
                protocol,
                txId,
                coordinator
        );
    }

    private static boolean waitUntilFinished(
            SelectedProtocol protocol,
            InMemoryCoordinatorLogManager coordinatorLog,
            TransactionId txId,
            long timeoutMs,
            FaultPlan faultPlan,
            CoordinatorRuntime coordinatorRuntime,
            Map<NodeId, ParticipantRuntime> participantRuntimes
    ) throws InterruptedException {
        long start = System.currentTimeMillis();
        Map<String, Boolean> firedCrashes = new ConcurrentHashMap<>();
        while (System.currentTimeMillis() - start < timeoutMs) {
            long now = System.currentTimeMillis();
            maybeTriggerCrashes(faultPlan, coordinatorLog, txId, coordinatorRuntime, participantRuntimes, firedCrashes);
            coordinatorRuntime.tickRecovery(now);
            for (ParticipantRuntime runtime : participantRuntimes.values()) {
                runtime.tickRecovery(now);
            }

            LogRecord last = coordinatorLog.getLast(txId);
            if (last != null && isTerminal(protocol, last.type())) {
                return true;
            }
            Thread.sleep(5);
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

    private static void maybeTriggerCrashes(
            FaultPlan faultPlan,
            InMemoryCoordinatorLogManager coordinatorLog,
            TransactionId txId,
            CoordinatorRuntime coordinatorRuntime,
            Map<NodeId, ParticipantRuntime> participantRuntimes,
            Map<String, Boolean> firedCrashes
    ) {
        List<LogRecord> coordinatorRecords = coordinatorLog.read(txId);

        for (CrashSpec crash : faultPlan.crashes()) {
            String key = crash.nodeName() + "-" + crash.trigger();
            if (firedCrashes.putIfAbsent(key, Boolean.FALSE) == null) {
                // initialized
            }
            if (Boolean.TRUE.equals(firedCrashes.get(key))) {
                continue;
            }

            boolean triggered = switch (crash.trigger()) {
                case COORDINATOR_BEFORE_DECISION -> crash.nodeName().equals("COORD")
                        && coordinatorRuntime.isOnline()
                        && containsCoordinatorType(coordinatorRecords, CoordinatorLogRecordType.BEGIN_COMMIT)
                        && !hasDecision(coordinatorRecords);
                case COORDINATOR_AFTER_DECISION -> crash.nodeName().equals("COORD")
                        && coordinatorRuntime.isOnline()
                        && hasDecision(coordinatorRecords);
                case PARTICIPANT_AFTER_READY -> {
                    ParticipantRuntime runtime = participantRuntimes.get(new NodeId(crash.nodeName()));
                    yield runtime != null && runtime.isOnline() && runtime.hasReadyRecord(txId);
                }
                case PARTICIPANT_BEFORE_READY -> false;
            };

            if (triggered) {
                if (crash.nodeName().equals("COORD")) {
                    coordinatorRuntime.crashFor(crash.downtimeMs());
                } else {
                    ParticipantRuntime runtime = participantRuntimes.get(new NodeId(crash.nodeName()));
                    if (runtime != null) {
                        runtime.crashFor(crash.downtimeMs());
                    }
                }
                firedCrashes.put(key, Boolean.TRUE);
            }
        }
    }

    private static boolean containsCoordinatorType(List<LogRecord> records, CoordinatorLogRecordType type) {
        for (LogRecord record : records) {
            if (record.type() == type) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasDecision(List<LogRecord> records) {
        return containsCoordinatorType(records, CoordinatorLogRecordType.GLOBAL_ABORT)
                || containsCoordinatorType(records, CoordinatorLogRecordType.GLOBAL_COMMIT);
    }

    private static final class DeterministicResource implements TransactionalResource {
        private final boolean shouldCommit;
        private final int diskIoTimeMs;

        private DeterministicResource(boolean shouldCommit, int diskIoTimeMs) {
            this.shouldCommit = shouldCommit;
            this.diskIoTimeMs = diskIoTimeMs;
        }

        @Override
        public boolean prepare(TransactionId txId) {
            sleep(diskIoTimeMs);
            return shouldCommit;
        }

        @Override
        public void commit(TransactionId txId) {
            // no-op
        }

        @Override
        public void abort(TransactionId txId) {
            // no-op
        }

        private static void sleep(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private abstract static class RecoverableNodeRuntime {
        private final NodeId nodeId;
        private final LocalNode node;
        private final NodeProfile profile;
        private final SimulationMessageBus network;
        private ActorNode actor;
        private ITimerFactory timerFactory;
        private long recoverAtMs = -1L;
        private boolean online = false;

        protected RecoverableNodeRuntime(NodeId nodeId, LocalNode node, NodeProfile profile, SimulationMessageBus network) {
            this.nodeId = nodeId;
            this.node = node;
            this.profile = profile;
            this.network = network;
        }

        public void start() {
            actor = new ActorNode(nodeId);
            timerFactory = new ActorTimerFactory(actor);
            network.register(nodeId, actor);
            network.setNodeLatency(nodeId, profile.minLatencyMs(), profile.maxLatencyMs());
            actor.setProtocolLogic(createProtocol(timerFactory));
            actor.start();
            online = true;
        }

        public void crashFor(long downtimeMs) {
            if (!online) {
                return;
            }
            actor.crash();
            online = false;
            recoverAtMs = System.currentTimeMillis() + downtimeMs;
        }

        public void tickRecovery(long now) {
            if (!online && recoverAtMs > 0L && now >= recoverAtMs) {
                start();
                actorProtocol().recover();
                recoverAtMs = -1L;
            }
        }

        public void shutdown() throws InterruptedException {
            if (actor != null) {
                actor.stop();
                actor.getWorkerThread().join(200);
            }
            online = false;
        }

        public boolean isOnline() {
            return online;
        }

        protected LocalNode node() {
            return node;
        }

        protected SimulationMessageBus network() {
            return network;
        }

        protected abstract ufc.victor.protocol.abstractions.IProtocol createProtocol(ITimerFactory timerFactory);

        protected abstract ufc.victor.protocol.abstractions.IProtocol actorProtocol();

        protected ActorNode actor() {
            return actor;
        }
    }

    private static final class CoordinatorRuntime extends RecoverableNodeRuntime {
        private final SelectedProtocol protocol;
        private final TransactionId txId;
        private final Set<Node> participants;
        private final InMemoryCoordinatorLogManager log;
        private ICoordinator activeProtocol;

        private CoordinatorRuntime(
                NodeId nodeId,
                LocalNode node,
                NodeProfile profile,
                SelectedProtocol protocol,
                TransactionId txId,
                Set<Node> participants,
                InMemoryCoordinatorLogManager log,
                SimulationMessageBus network
        ) {
            super(nodeId, node, profile, network);
            this.protocol = protocol;
            this.txId = txId;
            this.participants = participants;
            this.log = log;
        }

        @Override
        protected ufc.victor.protocol.abstractions.IProtocol createProtocol(ITimerFactory timerFactory) {
            activeProtocol = buildCoordinator(protocol, txId, node(), participants, log, network(), timerFactory);
            return activeProtocol;
        }

        @Override
        protected ufc.victor.protocol.abstractions.IProtocol actorProtocol() {
            return activeProtocol;
        }
    }

    private static final class ParticipantRuntime extends RecoverableNodeRuntime {
        private final SelectedProtocol protocol;
        private final TransactionId txId;
        private final LocalNode coordinatorNode;
        private final InMemoryParticipantLogManager log;
        private final boolean shouldCommit;
        private final int diskIoTimeMs;
        private final CrashSpec beforeReadyCrash;
        private boolean beforeReadyTriggered = false;
        private IParticipant activeProtocol;

        private ParticipantRuntime(
                NodeId nodeId,
                LocalNode node,
                NodeProfile profile,
                SelectedProtocol protocol,
                TransactionId txId,
                LocalNode coordinatorNode,
                InMemoryParticipantLogManager log,
                boolean shouldCommit,
                CrashSpec beforeReadyCrash,
                SimulationMessageBus network
        ) {
            super(nodeId, node, profile, network);
            this.protocol = protocol;
            this.txId = txId;
            this.coordinatorNode = coordinatorNode;
            this.log = log;
            this.shouldCommit = shouldCommit;
            this.diskIoTimeMs = profile.diskIoTimeMs();
            this.beforeReadyCrash = beforeReadyCrash;
        }

        @Override
        protected ufc.victor.protocol.abstractions.IProtocol createProtocol(ITimerFactory timerFactory) {
            activeProtocol = buildParticipant(
                    node(),
                    log,
                    timerFactory,
                    createResource(),
                    network(),
                    protocol,
                    txId,
                    coordinatorNode
            );
            return activeProtocol;
        }

        @Override
        protected ufc.victor.protocol.abstractions.IProtocol actorProtocol() {
            return activeProtocol;
        }

        public boolean hasReadyRecord(TransactionId txId) {
            for (ParticipantLogRecord record : log.read(txId)) {
                if (record.type() == ufc.victor.protocol.participant.log.ParticipantLogRecordType.READY) {
                    return true;
                }
            }
            return false;
        }

        private TransactionalResource createResource() {
            TransactionalResource base = new DeterministicResource(shouldCommit, diskIoTimeMs);
            if (beforeReadyCrash == null || beforeReadyTriggered) {
                return base;
            }
            return new TransactionalResource() {
                @Override
                public boolean prepare(TransactionId txId) {
                    boolean decision = base.prepare(txId);
                    beforeReadyTriggered = true;
                    crashFor(beforeReadyCrash.downtimeMs());
                    throw new IllegalStateException("Simulated participant crash before READY");
                }

                @Override
                public void commit(TransactionId txId) {
                    base.commit(txId);
                }

                @Override
                public void abort(TransactionId txId) {
                    base.abort(txId);
                }
            };
        }
    }
}
