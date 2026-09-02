package ufc.victor;

import ufc.victor.db.mariadb.MariaDbTpccNewOrderWorkload;
import ufc.victor.db.mariadb.CompletionTrackingTransactionalResource;
import ufc.victor.db.mariadb.MariaDbXaConfig;
import ufc.victor.db.mariadb.MariaDbXaTransactionalResource;
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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Runs distributed XA New-Order work against the four-node MariaDB cluster. */
public final class MariaDbTpccClusterMain {

    private static final int NODE_COUNT = 4;

    private MariaDbTpccClusterMain() {
    }

    public static void main(String[] args) throws InterruptedException {
        String user = envOrDefault("ACP_MARIADB_USER", "acp");
        String password = envOrDefault("ACP_MARIADB_PASSWORD", "acp_secret");
        String database = envOrDefault("ACP_MARIADB_DATABASE", "acp");
        SelectedProtocol protocol = SelectedProtocol.valueOf(
                envOrDefault("ACP_PROTOCOL", SelectedProtocol.TWO_PC.name()).toUpperCase()
        );
        int transactions = positiveIntEnv("ACP_TPCC_TRANSACTIONS", 1);
        List<MariaDbXaConfig> configs = configs(user, password, database);

        System.out.println("=== MARIADB CLUSTER / BENCHMARKSQL NEW-ORDER ===");
        System.out.println("protocol=" + protocol + ", transactions=" + transactions);
        System.out.println("Shard map: warehouse N is stored exclusively on mariadb-N.");

        int committed = 0;
        long totalLatencyMs = 0L;
        for (int index = 0; index < transactions; index++) {
            RunResult result = runTransaction(configs, protocol, index);
            committed += result.committed() ? 1 : 0;
            totalLatencyMs += result.latencyMs();
            System.out.println("tx=" + (index + 1) + ", committed=" + result.committed()
                    + ", latencyMs=" + result.latencyMs());
        }

        System.out.println("Summary: committed=" + committed + "/" + transactions
                + ", averageLatencyMs=" + (totalLatencyMs / (double) transactions));
    }

    private static RunResult runTransaction(
            List<MariaDbXaConfig> configs,
            SelectedProtocol protocol,
            int transactionIndex
    ) throws InterruptedException {
        SimulationMessageBus network = new SimulationMessageBus(1, 3);
        TransactionId txId = TransactionId.newTransaction();
        NodeId coordinatorId = new NodeId("COORD");
        LocalNode coordinatorNode = new LocalNode(coordinatorId);
        ActorNode coordinatorActor = new ActorNode(coordinatorId);
        InMemoryCoordinatorLogManager coordinatorLog = new InMemoryCoordinatorLogManager();
        List<ActorNode> participantActors = new ArrayList<>();
        List<CompletionTrackingTransactionalResource> participantResources = new ArrayList<>();
        Set<Node> participantNodes = new LinkedHashSet<>();

        network.register(coordinatorId, coordinatorActor);
        for (int index = 0; index < configs.size(); index++) {
            int nodeNumber = index + 1;
            NodeId participantId = new NodeId("P" + nodeNumber);
            LocalNode participantNode = new LocalNode(participantId);
            ActorNode participantActor = new ActorNode(participantId);
            CompletionTrackingTransactionalResource participantResource =
                    new CompletionTrackingTransactionalResource(new MariaDbXaTransactionalResource(
                            configs.get(index),
                            participantId.value(),
                            workload(nodeNumber, transactionIndex)
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

        long startedAt = System.currentTimeMillis();
        network.send(Message.of(MessageType.COMMIT_REQUEST, txId, coordinatorNode, coordinatorNode));
        boolean finished = waitForTerminal(
                protocol, txId, coordinatorLog, participantResources, 60_000L
        );
        long latencyMs = System.currentTimeMillis() - startedAt;
        boolean committed = finished && coordinatorLog.read(txId).stream()
                .anyMatch(record -> record.type() == CoordinatorLogRecordType.GLOBAL_COMMIT);

        network.shutdown();
        coordinatorActor.stop();
        coordinatorActor.getWorkerThread().join(500L);
        for (ActorNode actor : participantActors) {
            actor.stop();
            actor.getWorkerThread().join(500L);
        }
        return new RunResult(committed, latencyMs);
    }

    private static MariaDbTpccNewOrderWorkload workload(int nodeNumber, int transactionIndex) {
        int districtId = ((transactionIndex + nodeNumber - 1) % 10) + 1;
        int customerId = ((transactionIndex * 37 + nodeNumber - 1) % 3_000) + 1;
        List<MariaDbTpccNewOrderWorkload.OrderLine> lines = new ArrayList<>();
        for (int line = 0; line < 5; line++) {
            int itemId = ((transactionIndex * 101 + nodeNumber * 17 + line * 997) % 100_000) + 1;
            lines.add(new MariaDbTpccNewOrderWorkload.OrderLine(itemId, (line % 10) + 1));
        }
        return new MariaDbTpccNewOrderWorkload(nodeNumber, districtId, customerId, lines);
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

    private static List<MariaDbXaConfig> configs(String user, String password, String database) {
        List<MariaDbXaConfig> configs = new ArrayList<>(NODE_COUNT);
        for (int node = 1; node <= NODE_COUNT; node++) {
            String defaultUrl = "jdbc:mariadb://127.0.0.1:" + (33_060 + node) + "/" + database;
            configs.add(new MariaDbXaConfig(
                    envOrDefault("ACP_MARIADB_" + node + "_URL", defaultUrl),
                    user,
                    password
            ));
        }
        return List.copyOf(configs);
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

    private record RunResult(boolean committed, long latencyMs) {
    }
}
