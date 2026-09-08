package ufc.victor;

import ufc.victor.db.mariadb.CompletionTrackingTransactionalResource;
import ufc.victor.db.mariadb.MariaDbTpccNewOrderPlan;
import ufc.victor.db.mariadb.MariaDbTpccNewOrderPlanner;
import ufc.victor.db.mariadb.MariaDbWalMetricsCollector;
import ufc.victor.db.mariadb.MariaDbXaConfig;
import ufc.victor.db.mariadb.MariaDbXaTransactionalResource;
import ufc.victor.db.mariadb.TpccNewOrderSpec;
import ufc.victor.db.mariadb.TpccNewOrderSpecFactory;
import ufc.victor.db.mariadb.WarehouseRouter;
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
import ufc.victor.protocol.coordinator.node.Node;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.protocol.participant.AdaptiveParticipantDispatcher;
import ufc.victor.telemetry.redo.RedoDelta;
import ufc.victor.telemetry.redo.RedoMetricsCollector;
import ufc.victor.telemetry.redo.RedoSnapshot;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/** Runs one logical New-Order with XA work split across the four warehouse shards. */
public final class MariaDbTpccClusterMain {

    private static final int NODE_COUNT = 4;

    private MariaDbTpccClusterMain() {
    }

    public static void main(String[] args) throws InterruptedException, SQLException {
        String user = envOrDefault("ACP_MARIADB_USER", "acp");
        String password = envOrDefault("ACP_MARIADB_PASSWORD", "acp_secret");
        String database = envOrDefault("ACP_MARIADB_DATABASE", "acp");
        SelectedProtocol protocol = SelectedProtocol.valueOf(
                envOrDefault("ACP_PROTOCOL", SelectedProtocol.TWO_PC.name()).toUpperCase()
        );
        int transactions = positiveIntEnv("ACP_TPCC_TRANSACTIONS", 1);
        boolean forceRollback = Boolean.parseBoolean(
                envOrDefault("ACP_TPCC_FORCE_ROLLBACK", "false")
        );
        Map<Integer, MariaDbXaConfig> configs = configs(user, password, database);
        MariaDbTpccNewOrderPlanner planner = new MariaDbTpccNewOrderPlanner(
                new WarehouseRouter(configs)
        );
        RedoMetricsCollector redoCollector = new MariaDbWalMetricsCollector(
                redoConfigs(configs)
        );

        System.out.println("=== MARIADB CLUSTER / BENCHMARKSQL NEW-ORDER ===");
        System.out.println("protocol=" + protocol + ", transactions=" + transactions
                + ", forceRollback=" + forceRollback);
        System.out.println("Shard map: warehouse N is stored exclusively on mariadb-N.");
        System.out.println("One home order is split into local and remote-supplier XA branches.");

        int committed = 0;
        long totalLatencyMs = 0L;
        for (int index = 0; index < transactions; index++) {
            TpccNewOrderSpec spec = TpccNewOrderSpecFactory.controlled(
                    List.of(1, 2, 3, 4), index
            );
            if (forceRollback) spec = TpccNewOrderSpecFactory.withInvalidFinalItem(spec);
            RunResult result = runTransaction(planner.plan(spec), protocol, redoCollector);
            committed += result.committed() ? 1 : 0;
            totalLatencyMs += result.latencyMs();
            System.out.println("tx=" + (index + 1) + ", committed=" + result.committed()
                    + ", latencyMs=" + result.latencyMs());
            result.redoByNode().forEach((node, delta) ->
                    System.out.println("  redo node=" + node + formatRedo(delta))
            );
        }

        System.out.println("Summary: committed=" + committed + "/" + transactions
                + ", averageLatencyMs=" + (totalLatencyMs / (double) transactions));
    }

    private static RunResult runTransaction(
            MariaDbTpccNewOrderPlan plan,
            SelectedProtocol protocol,
            RedoMetricsCollector redoCollector
    ) throws InterruptedException {
        SimulationMessageBus network = new SimulationMessageBus(1, 3);
        TransactionId txId = TransactionId.newTransaction();
        NodeId coordinatorId = new NodeId("COORD");
        LocalNode coordinatorNode = new LocalNode(coordinatorId);
        ActorNode coordinatorActor = new ActorNode(coordinatorId);
        InMemoryCoordinatorLogManager coordinatorLog = new InMemoryCoordinatorLogManager();
        List<ActorNode> participantActors = new ArrayList<>();
        List<CompletionTrackingTransactionalResource> participantResources = new ArrayList<>();
        Set<Node> participantNodes = new java.util.LinkedHashSet<>();

        network.register(coordinatorId, coordinatorActor);
        for (MariaDbTpccNewOrderPlan.Branch branch : plan.branches()) {
            NodeId participantId = new NodeId(branch.participantId());
            LocalNode participantNode = new LocalNode(participantId);
            ActorNode participantActor = new ActorNode(participantId);
            CompletionTrackingTransactionalResource participantResource =
                    new CompletionTrackingTransactionalResource(new MariaDbXaTransactionalResource(
                            branch.config(),
                            participantId.value(),
                            branch.workload()
                    ));
            IParticipant participant = new AdaptiveParticipantDispatcher(
                    participantNode,
                    new InMemoryParticipantLogManager(),
                    new ActorTimerFactory(participantActor),
                    participantResource,
                    network
            );
            participantActor.setProtocolLogic(participant);
            participantActors.add(participantActor);
            participantResources.add(participantResource);
            participantNodes.add(participantNode);
            network.register(participantId, participantActor);
        }

        ICoordinator coordinator = new AdaptiveCoordinatorDispatcher(
                txId,
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
            Map<NodeId, RedoSnapshot> before = captureRedo(participantNodes, redoCollector);
            long startedAt = System.currentTimeMillis();
            network.send(Message.of(MessageType.COMMIT_REQUEST, txId, coordinatorNode, coordinatorNode));
            boolean finished = waitForTerminal(
                    protocol, txId, coordinatorLog, participantResources, 60_000L
            );
            long latencyMs = System.currentTimeMillis() - startedAt;
            boolean committed = finished && coordinatorLog.read(txId).stream()
                    .anyMatch(record -> record.type() == CoordinatorLogRecordType.GLOBAL_COMMIT);
            Map<NodeId, RedoSnapshot> after = captureRedo(participantNodes, redoCollector);
            Map<NodeId, RedoDelta> redoByNode = new LinkedHashMap<>();
            before.forEach((node, snapshot) -> redoByNode.put(
                    node, RedoDelta.between(snapshot, after.get(node))
            ));
            return new RunResult(
                    committed,
                    latencyMs,
                    Collections.unmodifiableMap(new LinkedHashMap<>(redoByNode))
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

    private static Map<NodeId, RedoSnapshot> captureRedo(
            Set<Node> participantNodes,
            RedoMetricsCollector collector
    ) {
        Map<NodeId, RedoSnapshot> snapshots = new LinkedHashMap<>();
        participantNodes.forEach(node -> snapshots.put(node.id, collector.capture(node.id)));
        return snapshots;
    }

    private static Map<NodeId, MariaDbXaConfig> redoConfigs(Map<Integer, MariaDbXaConfig> configs) {
        Map<NodeId, MariaDbXaConfig> byNode = new LinkedHashMap<>();
        configs.forEach((warehouse, config) -> byNode.put(new NodeId("P" + warehouse), config));
        return byNode;
    }

    private static String formatRedo(RedoDelta delta) {
        return ", lsnAdvanced=" + value(delta.lsnAdvanced())
                + ", flushedLsnAdvanced=" + value(delta.flushedLsnAdvanced())
                + ", bytesWritten=" + value(delta.redoBytesWritten())
                + ", writeRequests=" + value(delta.logWriteRequests())
                + ", logWrites=" + value(delta.logWrites())
                + ", logWaits=" + value(delta.logWaits())
                + ", checkpointAgeBefore=" + value(delta.checkpointAgeBeforeBytes())
                + ", checkpointAgeAfter=" + value(delta.checkpointAgeAfterBytes())
                + ", resetDetected=" + delta.counterResetDetected();
    }

    private static String value(OptionalLong value) {
        return value.isPresent() ? Long.toString(value.getAsLong()) : "unavailable";
    }

    private static boolean waitForTerminal(
            SelectedProtocol protocol,
            TransactionId txId,
            InMemoryCoordinatorLogManager log,
            List<CompletionTrackingTransactionalResource> participantResources,
            long timeoutMs
    ) throws InterruptedException {
        long startedAt = System.currentTimeMillis();
        while (System.currentTimeMillis() - startedAt < timeoutMs) {
            var last = log.getLast(txId);
            boolean coordinatorFinished = last != null && (last.type() == CoordinatorLogRecordType.END_TRANSACTION
                    || protocol == SelectedProtocol.TWO_PC_PRESUMED_ABORT
                    && last.type() == CoordinatorLogRecordType.GLOBAL_ABORT
                    || protocol == SelectedProtocol.TWO_PC_PRESUMED_COMMIT
                    && last.type() == CoordinatorLogRecordType.GLOBAL_COMMIT);
            if (coordinatorFinished && participantResources.stream()
                    .allMatch(CompletionTrackingTransactionalResource::isResolved)) {
                return true;
            }
            Thread.sleep(10L);
        }
        return false;
    }

    private static Map<Integer, MariaDbXaConfig> configs(String user, String password, String database) {
        Map<Integer, MariaDbXaConfig> configs = new LinkedHashMap<>();
        for (int node = 1; node <= NODE_COUNT; node++) {
            String defaultUrl = "jdbc:mariadb://127.0.0.1:" + (33_060 + node) + "/" + database;
            configs.put(node, new MariaDbXaConfig(
                    envOrDefault("ACP_MARIADB_" + node + "_URL", defaultUrl),
                    user,
                    password
            ));
        }
        return Map.copyOf(configs);
    }

    private static int positiveIntEnv(String name, int defaultValue) {
        int value = Integer.parseInt(envOrDefault(name, Integer.toString(defaultValue)));
        if (value < 1) throw new IllegalArgumentException(name + " must be at least 1");
        return value;
    }

    private static String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private record RunResult(
            boolean committed,
            long latencyMs,
            Map<NodeId, RedoDelta> redoByNode
    ) {
    }
}
