package ufc.victor;

import ufc.victor.db.mariadb.MariaDbSchemaBootstrap;
import ufc.victor.db.mariadb.MariaDbTransactionJournalWorkload;
import ufc.victor.db.mariadb.MariaDbXaConfig;
import ufc.victor.db.mariadb.MariaDbXaTransactionalResource;
import ufc.victor.localenv.InMemoryCoordinatorLogManager;
import ufc.victor.localenv.InMemoryParticipantLogManager;
import ufc.victor.localenv.LocalNode;
import ufc.victor.localenv.actors.ActorNode;
import ufc.victor.localenv.actors.ActorTimerFactory;
import ufc.victor.localenv.actors.SimulationMessageBus;
import ufc.victor.protocol.abstractions.ICoordinator;
import ufc.victor.protocol.abstractions.IParticipant;
import ufc.victor.protocol.commom.ITimerFactory;
import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.commom.message.MessageType;
import ufc.victor.protocol.coordinator.TwoPhaseCommitCoordinator;
import ufc.victor.protocol.coordinator.log.CoordinatorLogManager;
import ufc.victor.protocol.coordinator.log.CoordinatorLogRecordType;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.protocol.participant.TwoPhaseCommitParticipant;
import ufc.victor.protocol.participant.log.ParticipantLogManager;

import java.sql.SQLException;
import java.util.Set;

public final class MariaDbMain {

    private MariaDbMain() {
    }

    public static void main(String[] args) throws InterruptedException, SQLException {
        MariaDbXaConfig dbConfig = new MariaDbXaConfig(
                requireEnv("ACP_MARIADB_URL"),
                requireEnv("ACP_MARIADB_USER"),
                requireEnv("ACP_MARIADB_PASSWORD")
        );
        String journalTable = envOrDefault("ACP_MARIADB_JOURNAL_TABLE", "tx_journal");
        MariaDbSchemaBootstrap.ensureJournalTable(dbConfig, journalTable);

        System.out.println("=== INITIALIZING DISTRIBUTED SYSTEM WITH MARIADB XA ===");

        SimulationMessageBus network = new SimulationMessageBus(10, 50);
        TransactionId txId = TransactionId.newTransaction();

        NodeId cId = new NodeId("COORD");
        NodeId p1Id = new NodeId("P1");
        NodeId p2Id = new NodeId("P2");
        NodeId p3Id = new NodeId("P3");

        ActorNode coordActor = new ActorNode(cId);
        ActorNode p1Actor = new ActorNode(p1Id);
        ActorNode p2Actor = new ActorNode(p2Id);
        ActorNode p3Actor = new ActorNode(p3Id);

        network.register(cId, coordActor);
        network.register(p1Id, p1Actor);
        network.register(p2Id, p2Actor);
        network.register(p3Id, p3Actor);

        CoordinatorLogManager cLog = new InMemoryCoordinatorLogManager();
        ParticipantLogManager p1Log = new InMemoryParticipantLogManager();
        ParticipantLogManager p2Log = new InMemoryParticipantLogManager();
        ParticipantLogManager p3Log = new InMemoryParticipantLogManager();

        ITimerFactory cTimerFactory = new ActorTimerFactory(coordActor);
        ITimerFactory p1TimerFactory = new ActorTimerFactory(p1Actor);
        ITimerFactory p2TimerFactory = new ActorTimerFactory(p2Actor);
        ITimerFactory p3TimerFactory = new ActorTimerFactory(p3Actor);

        LocalNode coordNode = new LocalNode(cId);
        LocalNode p1Node = new LocalNode(p1Id);
        LocalNode p2Node = new LocalNode(p2Id);
        LocalNode p3Node = new LocalNode(p3Id);

        ICoordinator coordLogic = new TwoPhaseCommitCoordinator(
                txId,
                coordNode,
                Set.of(p1Node, p2Node, p3Node),
                cLog,
                network,
                cTimerFactory
        );

        IParticipant p1Logic = new TwoPhaseCommitParticipant(
                txId,
                p1Node,
                coordNode,
                p1Log,
                p1TimerFactory,
                new MariaDbXaTransactionalResource(
                        dbConfig,
                        p1Id.value(),
                        new MariaDbTransactionJournalWorkload(p1Id.value(), journalTable)
                ),
                network
        );
        IParticipant p2Logic = new TwoPhaseCommitParticipant(
                txId,
                p2Node,
                coordNode,
                p2Log,
                p2TimerFactory,
                new MariaDbXaTransactionalResource(
                        dbConfig,
                        p2Id.value(),
                        new MariaDbTransactionJournalWorkload(p2Id.value(), journalTable)
                ),
                network
        );
        IParticipant p3Logic = new TwoPhaseCommitParticipant(
                txId,
                p3Node,
                coordNode,
                p3Log,
                p3TimerFactory,
                new MariaDbXaTransactionalResource(
                        dbConfig,
                        p3Id.value(),
                        new MariaDbTransactionJournalWorkload(p3Id.value(), journalTable)
                ),
                network
        );

        coordActor.setProtocolLogic(coordLogic);
        p1Actor.setProtocolLogic(p1Logic);
        p2Actor.setProtocolLogic(p2Logic);
        p3Actor.setProtocolLogic(p3Logic);

        coordActor.start();
        p1Actor.start();
        p2Actor.start();
        p3Actor.start();

        network.send(Message.of(
                MessageType.COMMIT_REQUEST,
                txId,
                coordNode,
                coordNode
        ));

        waitForCompletion(txId, cLog, 60_000L);

        network.shutdown();
        coordActor.stop();
        p1Actor.stop();
        p2Actor.stop();
        p3Actor.stop();

        System.out.println("\n=== MARIADB XA REPORT ===");
        System.out.println("--- Coordinator Log ---");
        cLog.read(txId).forEach(System.out::println);
        System.out.println("\n--- P1 Log ---");
        p1Log.read(txId).forEach(System.out::println);
        System.out.println("\n--- P2 Log ---");
        p2Log.read(txId).forEach(System.out::println);
        System.out.println("\n--- P3 Log ---");
        p3Log.read(txId).forEach(System.out::println);
        System.out.println("\nMariaDB journal table: " + journalTable);
    }

    private static void waitForCompletion(
            TransactionId txId,
            CoordinatorLogManager cLog,
            long timeoutMs
    ) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (true) {
            var log = cLog.getLast(txId);
            if (log != null && log.type() == CoordinatorLogRecordType.END_TRANSACTION) {
                System.out.println("Transaction finished with state: " + log.type());
                return;
            }
            if (System.currentTimeMillis() - startTime > timeoutMs) {
                throw new IllegalStateException("Transaction TIMED OUT or BLOCKED");
            }
            Thread.sleep(10L);
        }
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required environment variable: " + name);
        }
        return value;
    }

    private static String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }
}
