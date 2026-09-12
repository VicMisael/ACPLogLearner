package ufc.victor.db.mariadb;

import ufc.victor.experiment.ParticipantTrialMetrics;
import ufc.victor.experiment.TransactionTrialRecord;
import ufc.victor.experiment.TrialOutcome;
import ufc.victor.localenv.InMemoryCoordinatorLogManager;
import ufc.victor.localenv.InMemoryParticipantLogManager;
import ufc.victor.localenv.LocalNode;
import ufc.victor.localenv.actors.ActorNode;
import ufc.victor.localenv.actors.ActorTimerFactory;
import ufc.victor.localenv.actors.SimulationMessageBus;
import ufc.victor.protocol.SelectedProtocol;
import ufc.victor.protocol.abstractions.ICoordinator;
import ufc.victor.protocol.abstractions.IParticipant;
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
import ufc.victor.telemetry.redo.RedoDelta;
import ufc.victor.telemetry.redo.RedoMetricsCollector;
import ufc.victor.telemetry.redo.RedoSnapshot;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public final class MariaDbTpccTransactionRunner {

    private static final NodeId COORDINATOR_ID = new NodeId("COORD");

    private final MariaDbTpccNewOrderPlanner planner;
    private final RedoMetricsCollector redoCollector;
    private final long timeoutMs;

    public MariaDbTpccTransactionRunner(
            MariaDbTpccNewOrderPlanner planner,
            RedoMetricsCollector redoCollector,
            long timeoutMs
    ) {
        this.planner = Objects.requireNonNull(planner, "planner");
        this.redoCollector = Objects.requireNonNull(redoCollector, "redoCollector");
        if (timeoutMs < 1L) throw new IllegalArgumentException("timeoutMs must be at least 1");
        this.timeoutMs = timeoutMs;
    }

    public TransactionTrialRecord run(TpccNewOrderSpec spec, SelectedProtocol protocol)
            throws InterruptedException, SQLException {
        Map<Integer, Boolean> commitVotes = new LinkedHashMap<>();
        spec.participantWarehouseIds().forEach(warehouse -> commitVotes.put(warehouse, true));
        return run(spec, protocol, commitVotes);
    }

    public TransactionTrialRecord run(
            TpccNewOrderSpec spec,
            SelectedProtocol protocol,
            Map<Integer, Boolean> commitVotes
    ) throws InterruptedException, SQLException {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(protocol, "protocol");
        Objects.requireNonNull(commitVotes, "commitVotes");
        MariaDbTpccNewOrderPlan plan = planner.plan(spec);
        validateVotes(plan, commitVotes);

        SimulationMessageBus network = new SimulationMessageBus(1, 3);
        TransactionId transactionId = TransactionId.newTransaction();
        LocalNode coordinatorNode = new LocalNode(COORDINATOR_ID);
        ActorNode coordinatorActor = new ActorNode(COORDINATOR_ID);
        InMemoryCoordinatorLogManager coordinatorLog = new InMemoryCoordinatorLogManager();
        Map<NodeId, InMemoryParticipantLogManager> participantLogs = new LinkedHashMap<>();
        List<ActorNode> participantActors = new ArrayList<>();
        List<CompletionTrackingTransactionalResource> participantResources = new ArrayList<>();
        Set<Node> participantNodes = new LinkedHashSet<>();

        network.register(COORDINATOR_ID, coordinatorActor);
        for (MariaDbTpccNewOrderPlan.Branch branch : plan.branches()) {
            NodeId participantId = new NodeId(branch.participantId());
            LocalNode participantNode = new LocalNode(participantId);
            ActorNode participantActor = new ActorNode(participantId);
            InMemoryParticipantLogManager participantLog = new InMemoryParticipantLogManager();
            TransactionalResource databaseResource = new MariaDbXaTransactionalResource(
                    branch.config(), participantId.value(), branch.workload()
            );
            TransactionalResource votingResource = new VotingResource(
                    commitVotes.get(branch.warehouseId()), databaseResource
            );
            CompletionTrackingTransactionalResource participantResource =
                    new CompletionTrackingTransactionalResource(votingResource);
            IParticipant participant = new AdaptiveParticipantDispatcher(
                    participantNode,
                    participantLog,
                    new ActorTimerFactory(participantActor),
                    participantResource,
                    network
            );
            participantActor.setProtocolLogic(participant);
            participantActors.add(participantActor);
            participantResources.add(participantResource);
            participantNodes.add(participantNode);
            participantLogs.put(participantId, participantLog);
            network.register(participantId, participantActor);
        }

        ICoordinator coordinator = new AdaptiveCoordinatorDispatcher(
                transactionId,
                coordinatorNode,
                participantNodes,
                coordinatorLog,
                network,
                new ActorTimerFactory(coordinatorActor),
                protocol
        );
        coordinatorActor.setProtocolLogic(coordinator);
        coordinatorActor.start();
        participantActors.forEach(ActorNode::start);

        try {
            Map<NodeId, RedoSnapshot> before = captureRedo(participantNodes);
            Instant startedAt = Instant.now();
            long startedNanos = System.nanoTime();
            network.send(Message.of(
                    MessageType.COMMIT_REQUEST, transactionId, coordinatorNode, coordinatorNode
            ));
            boolean finished = waitForTerminal(
                    protocol, transactionId, coordinatorLog, participantResources
            );
            Duration totalLatency = Duration.ofNanos(System.nanoTime() - startedNanos);
            Instant completedAt = startedAt.plus(totalLatency);
            List<LogRecord> coordinatorRecords = List.copyOf(coordinatorLog.read(transactionId));
            TrialOutcome outcome = outcome(finished, coordinatorRecords);
            Optional<Instant> decidedAt = coordinatorRecords.stream()
                    .filter(record -> record.type() == CoordinatorLogRecordType.GLOBAL_COMMIT
                            || record.type() == CoordinatorLogRecordType.GLOBAL_ABORT)
                    .map(LogRecord::timestamp)
                    .findFirst();
            Map<NodeId, RedoSnapshot> after = captureRedo(participantNodes);
            Map<NodeId, ParticipantTrialMetrics> participants = new LinkedHashMap<>();
            before.forEach((nodeId, snapshot) -> participants.put(nodeId, new ParticipantTrialMetrics(
                    nodeId,
                    RedoDelta.between(snapshot, after.get(nodeId)),
                    participantLogs.get(nodeId).read(transactionId).size()
            )));

            return new TransactionTrialRecord(
                    transactionId,
                    spec.specId(),
                    protocol,
                    outcome,
                    totalLatency,
                    decidedAt.map(value -> nonNegativeDuration(startedAt, value)),
                    decidedAt.map(value -> nonNegativeDuration(value, completedAt)),
                    participants,
                    network.getSentMessageCount(),
                    coordinatorRecords.size()
            );
        } finally {
            network.shutdown();
            coordinatorActor.stop();
            coordinatorActor.getWorkerThread().join(500L);
            for (ActorNode actor : participantActors) {
                actor.stop();
                actor.getWorkerThread().join(500L);
            }
        }
    }

    private Map<NodeId, RedoSnapshot> captureRedo(Set<Node> participantNodes) {
        Map<NodeId, RedoSnapshot> snapshots = new LinkedHashMap<>();
        participantNodes.forEach(node -> snapshots.put(node.id, redoCollector.capture(node.id)));
        return snapshots;
    }

    private boolean waitForTerminal(
            SelectedProtocol protocol,
            TransactionId transactionId,
            InMemoryCoordinatorLogManager log,
            List<CompletionTrackingTransactionalResource> participantResources
    ) throws InterruptedException {
        long startedAt = System.nanoTime();
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() - startedAt < timeoutNanos) {
            LogRecord last = log.getLast(transactionId);
            boolean coordinatorFinished = last != null && (
                    last.type() == CoordinatorLogRecordType.END_TRANSACTION
                            || protocol == SelectedProtocol.TWO_PC_PRESUMED_ABORT
                            && last.type() == CoordinatorLogRecordType.GLOBAL_ABORT
                            || protocol == SelectedProtocol.TWO_PC_PRESUMED_COMMIT
                            && last.type() == CoordinatorLogRecordType.GLOBAL_COMMIT
            );
            if (coordinatorFinished && participantResources.stream()
                    .allMatch(CompletionTrackingTransactionalResource::isResolved)) {
                return true;
            }
            Thread.sleep(10L);
        }
        return false;
    }

    private static TrialOutcome outcome(boolean finished, List<LogRecord> records) {
        if (!finished) return TrialOutcome.TIMED_OUT;
        return records.stream().anyMatch(record -> record.type() == CoordinatorLogRecordType.GLOBAL_COMMIT)
                ? TrialOutcome.COMMITTED
                : TrialOutcome.ABORTED;
    }

    private static Duration nonNegativeDuration(Instant start, Instant end) {
        Duration duration = Duration.between(start, end);
        return duration.isNegative() ? Duration.ZERO : duration;
    }

    private static void validateVotes(
            MariaDbTpccNewOrderPlan plan,
            Map<Integer, Boolean> commitVotes
    ) {
        for (MariaDbTpccNewOrderPlan.Branch branch : plan.branches()) {
            if (commitVotes.get(branch.warehouseId()) == null) {
                throw new IllegalArgumentException(
                        "Missing commit vote for warehouse " + branch.warehouseId()
                );
            }
        }
    }

    private static final class VotingResource implements TransactionalResource {
        private final boolean shouldCommit;
        private final TransactionalResource delegate;

        private VotingResource(boolean shouldCommit, TransactionalResource delegate) {
            this.shouldCommit = shouldCommit;
            this.delegate = delegate;
        }

        @Override
        public boolean prepare(TransactionId transactionId) {
            return shouldCommit && delegate.prepare(transactionId);
        }

        @Override
        public void commit(TransactionId transactionId) {
            delegate.commit(transactionId);
        }

        @Override
        public void abort(TransactionId transactionId) {
            delegate.abort(transactionId);
        }
    }
}
