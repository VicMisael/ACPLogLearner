package ufc.victor;

import ufc.victor.localenv.*;
import ufc.victor.localenv.actors.ActorNode;
import ufc.victor.localenv.actors.ActorTimerFactory;
import ufc.victor.localenv.actors.SimulationMessageBus;
import ufc.victor.protocol.abstractions.ICoordinator;
import ufc.victor.protocol.abstractions.IParticipant;
import ufc.victor.protocol.commom.*;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.commom.message.MessageType;
import ufc.victor.protocol.coordinator.TwoPhasePresumedAbortCoordinator;
import ufc.victor.protocol.coordinator.log.CoordinatorLogManager;
import ufc.victor.protocol.coordinator.log.CoordinatorLogRecordType;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.protocol.participant.TransactionalResource;
import ufc.victor.protocol.participant.TwoPhaseCommitPresumedAbortParticipant;
import ufc.victor.protocol.participant.log.ParticipantLogManager;

import java.util.Set;

public class Main {

    // ----------------------------------------------------------
    // MOCK RESOURCES (Your Database Stubs)
    // ----------------------------------------------------------
    private static class FakeResource implements TransactionalResource {
        private final String name;
        public FakeResource(String name) { this.name = name; }

        @Override
        public boolean prepare(TransactionId txId) {
            // Simulate disk I/O latency
            sleep(50);
            return true; // Vote COMMIT
        }

        @Override
        public void commit(TransactionId txId) {
            System.out.println("  [" + name + "] DB COMMITTED " + txId);
        }

        @Override
        public void abort(TransactionId txId) {
            System.out.println("  [" + name + "] DB ABORTED " + txId);
        }
    }

    private static class SlowResource implements TransactionalResource {
        @Override
        public boolean prepare(TransactionId txId) {
            System.out.println("  [SlowDB] Disk spin-up...");
            sleep(4000); // Takes 4s to vote (Simulates a Straggler)
            return false;
        }

        @Override
        public void commit(TransactionId txId) { System.out.println("  [SlowDB] COMMITTED"); }
        @Override
        public void abort(TransactionId txId) { System.out.println("  [SlowDB] ABORTED"); }
    }

    // ----------------------------------------------------------
    // MAIN SIMULATION
    // ----------------------------------------------------------
    public static void main(String[] args) throws InterruptedException {

        System.out.println("=== INITIALIZING DISTRIBUTED SYSTEM ===");

        // 1. ENVIRONMENT ( The Internet )
        // Latency: 10ms to 50ms (Simulating a WAN)
        SimulationMessageBus network = new SimulationMessageBus(10, 50);
        TransactionId txId = TransactionId.newTransaction();


        // 2. HARDWARE ( The Servers / Actors )
        // We create the physical nodes first so we can link them to network & timers
        NodeId cId = new NodeId("COORD");
        NodeId p1Id = new NodeId("P1");
        NodeId p2Id = new NodeId("P2");
        NodeId p3Id = new NodeId("P3"); // The Straggler

        ActorNode coordActor = new ActorNode(cId);
        ActorNode p1Actor = new ActorNode(p1Id);
        ActorNode p2Actor = new ActorNode(p2Id);
        ActorNode p3Actor = new ActorNode(p3Id);

        // Plug servers into the network switch
        network.register(cId, coordActor);
        network.register(p1Id, p1Actor);
        network.register(p2Id, p2Actor);
        network.register(p3Id, p3Actor);


        // 3. INFRASTRUCTURE ( Logs & Timers )
        // Each node gets its own Log Manager (InMemory for now)
        CoordinatorLogManager cLog = new InMemoryCoordinatorLogManager();
        ParticipantLogManager p1Log = new InMemoryParticipantLogManager();
        ParticipantLogManager p2Log = new InMemoryParticipantLogManager();
        ParticipantLogManager p3Log = new InMemoryParticipantLogManager();

        // CRITICAL: Each node gets a TimerFactory bound to its specific Actor
        ITimerFactory cTimerFactory = new ActorTimerFactory(coordActor);
        ITimerFactory p1TimerFactory = new ActorTimerFactory(p1Actor);
        ITimerFactory p2TimerFactory = new ActorTimerFactory(p2Actor);
        ITimerFactory p3TimerFactory = new ActorTimerFactory(p3Actor);


        // 4. SOFTWARE ( The Protocol Logic )
        // We instantiate the logic, injecting the specific infrastructure for each node

        // Data Objects for logic identification
        LocalNode coordNode = new LocalNode(cId);
        LocalNode p1Node = new LocalNode(p1Id);
        LocalNode p2Node = new LocalNode(p2Id);
        LocalNode p3Node = new LocalNode(p3Id);

        // -- Coordinator Logic --
        ICoordinator coordLogic = new TwoPhasePresumedAbortCoordinator(
                txId,
                coordNode,
                Set.of(p1Node, p2Node, p3Node), // The cluster membership
                cLog,
                network,
                cTimerFactory
        );

        // -- Participant Logic --
        IParticipant p1Logic = new TwoPhaseCommitPresumedAbortParticipant(
                txId, p1Node, coordNode, p1Log, p1TimerFactory, new FakeResource("DB-1"), network
        );

        IParticipant p2Logic = new TwoPhaseCommitPresumedAbortParticipant(
                txId, p2Node, coordNode, p2Log, p2TimerFactory, new FakeResource("DB-2"), network
        );

        // P3 is the "Bad Node" (Slow Resource)
        IParticipant p3Logic = new TwoPhaseCommitPresumedAbortParticipant(
                txId, p3Node, coordNode, p3Log, p3TimerFactory, new SlowResource(), network
        );


        // 5. INSTALLATION ( Loading OS )
        // Bind the logic to the physical actors
        coordActor.setProtocolLogic(coordLogic);
        p1Actor.setProtocolLogic(p1Logic);
        p2Actor.setProtocolLogic(p2Logic);
        p3Actor.setProtocolLogic(p3Logic);


        // 6. BOOT SEQUENCE
        System.out.println(">>> Powering on nodes...");
        coordActor.start();
        p1Actor.start();
        p2Actor.start();
        p3Actor.start();


        // 7. EXECUTION ( Run Experiment )
        System.out.println(">>> Client sends COMMIT_REQUEST to Coordinator...");

        // We inject the start message directly into the Coordinator's inbox
        Message startMsg = Message.of(
                MessageType.COMMIT_REQUEST,
                txId,
                coordNode, // From (Self/Client)
                coordNode
        );

        network.send(startMsg); // Or coordActor.onMessage(startMsg);


        // 8. OBSERVATION
        // We wait long enough for the slow node (4s) + network delay
        System.out.println(">>> Simulation running (Waiting 6s for completion)...");

        boolean loop = true;

        long startTime = System.currentTimeMillis();
        long timeoutMs = 60_000;

        while (loop) {
            var log = cLog.getLast(txId);

            if (log != null) {
                // Did we reach a terminal state?
                if (log.type() == CoordinatorLogRecordType.END_TRANSACTION){ // (PrC might stop here)

                    System.out.println("Transaction finished with state: " + log.type());
                    loop = false;
                }
            }

            // Did we wait too long? (Simulated 2PC Block)
            if (System.currentTimeMillis() - startTime > timeoutMs) {
                System.err.println("Transaction TIMED OUT or BLOCKED!");
                loop = false;
            }

            // Let the CPU breathe and give the Actor threads time to work
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                loop = false;
            }
        }

        // 9. SHUTDOWN & REPORT
        System.out.println(">>> Shutting down...");
        network.shutdown(); // Stops the message bus threads
        coordActor.stop();
        p1Actor.stop();
        p2Actor.stop();
        p3Actor.stop();

        System.out.println("\n=== EXPERIMENT REPORT ===");

        System.out.println("--- Coordinator Log ---");
        cLog.read(txId).forEach(System.out::println);

        System.out.println("\n--- P1 Log (Fast) ---");
        p1Log.read(txId).forEach(System.out::println);

        System.out.println("\n--- P2 Log (Fast) ---");
        p1Log.read(txId).forEach(System.out::println);

        System.out.println("\n--- P3 Log (Straggler) ---");
        p3Log.read(txId).forEach(System.out::println);
    }

    // Helper for sleep
    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }
}