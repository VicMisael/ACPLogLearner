package ufc.victor;

import ufc.victor.db.mariadb.MariaDbTpccNewOrderWorkload;
import ufc.victor.db.mariadb.CompletionTrackingTransactionalResource;
import ufc.victor.db.mariadb.MariaDbXaConfig;
import ufc.victor.db.mariadb.MariaDbXaTransactionalResource;
import ufc.victor.experiment.TransactionOutcomeClass;
import ufc.victor.experiment.TransactionOutcomeLabeler;
import ufc.victor.experiment.VfdtProtocolSwitchingPolicy;
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
import ufc.victor.protocol.participant.TransactionalResource;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Prequential ALPS experiment backed by real MariaDB XA New-Order transactions. */
public final class MariaDbAlpsMain {

    private static final List<List<Integer>> PARTICIPANT_SETS = List.of(
            List.of(1),
            List.of(2),
            List.of(3),
            List.of(4),
            List.of(1, 2),
            List.of(1, 3),
            List.of(1, 4),
            List.of(2, 3),
            List.of(2, 4),
            List.of(3, 4),
            List.of(1, 2, 3),
            List.of(1, 2, 4),
            List.of(1, 3, 4),
            List.of(2, 3, 4),
            List.of(1, 2, 3, 4)
    );

    private MariaDbAlpsMain() {
    }

    public static void main(String[] args) throws InterruptedException {
        int examples = positiveIntEnv("ACP_ALPS_EXAMPLES", 30);
        int windowSize = positiveIntEnv("ACP_ALPS_WINDOW_SIZE", 3);
        long timeoutMs = positiveLongEnv("ACP_ALPS_TIMEOUT_MS", 60_000L);
        Map<Integer, MariaDbXaConfig> configs = configs();
        Map<Integer, Double> abortProbabilities = abortProbabilities();

        VFDTTransactionOutcomePredictor model =
                new VFDTTransactionOutcomePredictor(List.of("P1", "P2", "P3", "P4"));
        TransactionOutcomeLabeler labeler = new TransactionOutcomeLabeler(0.25, 0.60);
        VfdtProtocolSwitchingPolicy policy = new VfdtProtocolSwitchingPolicy();
        Map<TransactionOutcomeClass, Integer> labels = new EnumMap<>(TransactionOutcomeClass.class);
        Map<SelectedProtocol, Integer> selections = new EnumMap<>(SelectedProtocol.class);

        int correct = 0;
        long totalTransactions = 0L;
        long totalCommitted = 0L;
        long totalLatencyMs = 0L;
        long totalMessages = 0L;
        long totalLogWrites = 0L;

        System.out.println("=== ALPS / REAL MARIADB XA PREQUENTIAL EXPERIMENT ===");
        System.out.println("examples=" + examples + ", windowSize=" + windowSize
                + ", abortProbabilities=" + abortProbabilities);

        int transactionIndex = 0;
        for (int example = 0; example < examples; example++) {
            List<Integer> participantNumbers = PARTICIPANT_SETS.get(example % PARTICIPANT_SETS.size());
            List<String> participantIds = participantNumbers.stream().map(number -> "P" + number).toList();
            LogFeatures features = new LogFeatures(participantIds);
            TransactionOutcomeClass prediction = model.predict(features);
            SelectedProtocol selectedProtocol = policy.select(prediction);

            int committedInWindow = 0;
            long windowLatencyMs = 0L;
            long windowMessages = 0L;
            long windowLogWrites = 0L;
            for (int offset = 0; offset < windowSize; offset++) {
                Map<Integer, Boolean> votes = votes(
                        participantNumbers,
                        abortProbabilities,
                        example,
                        offset
                );
                RealTransactionResult result = runTransaction(
                        configs,
                        participantNumbers,
                        votes,
                        selectedProtocol,
                        transactionIndex++,
                        timeoutMs
                );
                committedInWindow += result.committed() ? 1 : 0;
                windowLatencyMs += result.latencyMs();
                windowMessages += result.messageCount();
                windowLogWrites += result.logWrites();
            }

            double commitRate = committedInWindow / (double) windowSize;
            double abortRate = 1.0 - commitRate;
            TransactionOutcomeClass observed = labeler.label(abortRate);
            if (prediction == observed) correct++;
            model.train(features, observed);
            labels.merge(observed, 1, Integer::sum);
            selections.merge(selectedProtocol, 1, Integer::sum);

            totalTransactions += windowSize;
            totalCommitted += committedInWindow;
            totalLatencyMs += windowLatencyMs;
            totalMessages += windowMessages;
            totalLogWrites += windowLogWrites;

            System.out.println("example=" + (example + 1)
                    + ", participants=" + participantIds
                    + ", predicted=" + prediction
                    + ", selected=" + selectedProtocol
                    + ", observed=" + observed
                    + ", commitRate=" + commitRate
                    + ", avgLatencyMs=" + (windowLatencyMs / (double) windowSize)
                    + ", avgMessages=" + (windowMessages / (double) windowSize)
                    + ", avgLogWrites=" + (windowLogWrites / (double) windowSize));
        }

        System.out.println("\n=== ALPS REAL-DATABASE SUMMARY ===");
        System.out.println("contextAccuracy=" + (correct / (double) examples)
                + ", labels=" + labels
                + ", selections=" + selections);
        System.out.println("transactions=" + totalTransactions
                + ", commitRate=" + (totalCommitted / (double) totalTransactions)
                + ", avgLatencyMs=" + (totalLatencyMs / (double) totalTransactions)
                + ", avgMessages=" + (totalMessages / (double) totalTransactions)
                + ", avgLogWrites=" + (totalLogWrites / (double) totalTransactions));
        System.out.println("model=VFDT, nodes=" + model.getNodeCount() + ", depth=" + model.getTreeDepth());
        System.out.println(model.getLearnedRules());
    }

    private static RealTransactionResult runTransaction(
            Map<Integer, MariaDbXaConfig> configs,
            List<Integer> participantNumbers,
            Map<Integer, Boolean> votes,
            SelectedProtocol protocol,
            int transactionIndex,
            long timeoutMs
    ) throws InterruptedException {
        SimulationMessageBus network = new SimulationMessageBus(1, 3);
        TransactionId txId = TransactionId.newTransaction();
        NodeId coordinatorId = new NodeId("COORD");
        LocalNode coordinatorNode = new LocalNode(coordinatorId);
        ActorNode coordinatorActor = new ActorNode(coordinatorId);
        InMemoryCoordinatorLogManager coordinatorLog = new InMemoryCoordinatorLogManager();
        Map<NodeId, InMemoryParticipantLogManager> participantLogs = new LinkedHashMap<>();
        List<ActorNode> participantActors = new ArrayList<>();
        List<CompletionTrackingTransactionalResource> participantResources = new ArrayList<>();
        Set<Node> participantNodes = new LinkedHashSet<>();

        network.register(coordinatorId, coordinatorActor);
        for (int nodeNumber : participantNumbers) {
            NodeId participantId = new NodeId("P" + nodeNumber);
            LocalNode participantNode = new LocalNode(participantId);
            ActorNode participantActor = new ActorNode(participantId);
            InMemoryParticipantLogManager participantLog = new InMemoryParticipantLogManager();
            TransactionalResource databaseResource = new MariaDbXaTransactionalResource(
                    configs.get(nodeNumber),
                    participantId.value(),
                    workload(nodeNumber, transactionIndex)
            );
            TransactionalResource votingResource = new VotingResource(
                    votes.get(nodeNumber),
                    databaseResource
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
                protocol, txId, coordinatorLog, participantResources, timeoutMs
        );
        long latencyMs = System.currentTimeMillis() - startedAt;
        boolean committed = finished && coordinatorLog.read(txId).stream()
                .anyMatch(record -> record.type() == CoordinatorLogRecordType.GLOBAL_COMMIT);
        long logWrites = coordinatorLog.read(txId).size();
        for (InMemoryParticipantLogManager participantLog : participantLogs.values()) {
            logWrites += participantLog.read(txId).size();
        }

        network.shutdown();
        coordinatorActor.stop();
        coordinatorActor.getWorkerThread().join(500L);
        for (ActorNode actor : participantActors) {
            actor.stop();
            actor.getWorkerThread().join(500L);
        }
        return new RealTransactionResult(
                committed,
                latencyMs,
                network.getSentMessageCount(),
                logWrites
        );
    }

    private static Map<Integer, Boolean> votes(
            List<Integer> participantNumbers,
            Map<Integer, Double> abortProbabilities,
            int example,
            int offset
    ) {
        Map<Integer, Boolean> votes = new LinkedHashMap<>();
        for (int nodeNumber : participantNumbers) {
            long mixed = 31L * (example + 1L) + 17L * (offset + 1L) + 13L * nodeNumber;
            double deterministicSample = Math.floorMod(mixed * 1_103_515_245L + 12_345L, 10_000L) / 10_000.0;
            votes.put(nodeNumber, deterministicSample >= abortProbabilities.get(nodeNumber));
        }
        return Map.copyOf(votes);
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

    private static Map<Integer, MariaDbXaConfig> configs() {
        String user = envOrDefault("ACP_MARIADB_USER", "acp");
        String password = envOrDefault("ACP_MARIADB_PASSWORD", "acp_secret");
        String database = envOrDefault("ACP_MARIADB_DATABASE", "acp");
        Map<Integer, MariaDbXaConfig> configs = new LinkedHashMap<>();
        for (int node = 1; node <= 4; node++) {
            String defaultUrl = "jdbc:mariadb://127.0.0.1:" + (33_060 + node) + "/" + database;
            configs.put(node, new MariaDbXaConfig(
                    envOrDefault("ACP_MARIADB_" + node + "_URL", defaultUrl),
                    user,
                    password
            ));
        }
        return Map.copyOf(configs);
    }

    private static Map<Integer, Double> abortProbabilities() {
        Map<Integer, Double> probabilities = new LinkedHashMap<>();
        for (int node = 1; node <= 4; node++) {
            double probability = Double.parseDouble(envOrDefault("ACP_ALPS_P" + node + "_ABORT_RATE", "0.0"));
            if (probability < 0.0 || probability > 1.0) {
                throw new IllegalArgumentException("Abort probability must be between 0 and 1 for P" + node);
            }
            probabilities.put(node, probability);
        }
        return Map.copyOf(probabilities);
    }

    private static int positiveIntEnv(String name, int defaultValue) {
        int value = Integer.parseInt(envOrDefault(name, Integer.toString(defaultValue)));
        if (value < 1) throw new IllegalArgumentException(name + " must be at least 1");
        return value;
    }

    private static long positiveLongEnv(String name, long defaultValue) {
        long value = Long.parseLong(envOrDefault(name, Long.toString(defaultValue)));
        if (value < 1L) throw new IllegalArgumentException(name + " must be at least 1");
        return value;
    }

    private static String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private record RealTransactionResult(
            boolean committed,
            long latencyMs,
            long messageCount,
            long logWrites
    ) {
    }

    private static final class VotingResource implements TransactionalResource {
        private final boolean shouldCommit;
        private final TransactionalResource delegate;

        private VotingResource(boolean shouldCommit, TransactionalResource delegate) {
            this.shouldCommit = shouldCommit;
            this.delegate = delegate;
        }

        @Override
        public boolean prepare(TransactionId txId) {
            return shouldCommit && delegate.prepare(txId);
        }

        @Override
        public void commit(TransactionId txId) {
            delegate.commit(txId);
        }

        @Override
        public void abort(TransactionId txId) {
            delegate.abort(txId);
        }
    }
}
