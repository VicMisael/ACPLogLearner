package ufc.victor;

import ufc.victor.localenv.*;
import ufc.victor.protocol.abstractions.ITimeoutHandler;
import ufc.victor.protocol.commom.*;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.commom.message.MessageType;
import ufc.victor.protocol.coordinator.TwoPhaseCommitCoordinator;
import ufc.victor.protocol.coordinator.log.CoordinatorLogManager;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.protocol.participant.TransactionalResource;
import ufc.victor.protocol.participant.TwoPhaseCommitParticipant;
import ufc.victor.protocol.participant.log.ParticipantLogManager;

import java.util.Set;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {

    private static class LocalTimerFactory implements ITimerFactory{
        @Override
        public ITimer createOrGetTimer(ITimeoutHandler timeoutHandler) {
            return new LocalTimer(timeoutHandler);
        }
    }


    public static void main(String[] args) {
        // -----------------------------
        // Infrastructure
        // -----------------------------
        Network network = new LocalNetwork();
        ITimerFactory timerFactory = new LocalTimerFactory();

        TransactionId txId = TransactionId.newTransaction();


        // -----------------------------
        // Nodes
        // -----------------------------
        LocalNode coordinatorNode = new LocalNode(new NodeId("C"));
        LocalNode p1Node = new LocalNode(new NodeId("P1"));
        LocalNode p2Node = new LocalNode(new NodeId("P2"));
        LocalNode p3Node = new LocalNode(new NodeId("P3"));


        // -----------------------------
        // Logs
        // -----------------------------
        CoordinatorLogManager coordinatorLog = new InMemoryCoordinatorLogManager();
        ParticipantLogManager p1Log = new InMemoryParticipantLogManager();
        ParticipantLogManager p2Log = new InMemoryParticipantLogManager();
        ParticipantLogManager p3Log = new InMemoryParticipantLogManager();

        // -----------------------------
        // Resources
        // -----------------------------
        TransactionalResource r1 = new FakeResource();
        TransactionalResource r2 = new FakeResource();
        TransactionalResource r3 = new AbortResource();

        // -----------------------------
        // Protocol instances
        // -----------------------------


        TwoPhaseCommitParticipant p1 =
                new TwoPhaseCommitParticipant(
                        txId,
                        p1Node,
                        coordinatorNode,
                        p1Log,
                        timerFactory,
                        r1,
                        network
                );

        TwoPhaseCommitParticipant p2 =
                new TwoPhaseCommitParticipant(
                        txId,
                        p2Node,
                        coordinatorNode,
                        p2Log,
                        timerFactory,
                        r2,
                        network
                );

        TwoPhaseCommitParticipant p3 = new TwoPhaseCommitParticipant(
                txId,
                p3Node,
                coordinatorNode,
                p3Log,
                timerFactory,
                r3,
                network
        );

        TwoPhaseCommitCoordinator coordinator =
                new TwoPhaseCommitCoordinator(
                        txId,
                        coordinatorNode,
                        Set.of(p1Node, p2Node,p3Node),
                        coordinatorLog,
                        network,
                        timerFactory
                );


        // -----------------------------
        // Network registration
        // -----------------------------
        network.register(coordinatorNode.id, coordinator);
        network.register(p1Node.id, p1);
        network.register(p2Node.id, p2);
        network.register(p3Node.id, p3);

        // -----------------------------
        // Start 2PC
        // -----------------------------
        System.out.println("=== BEGIN 2PC ===");
        coordinator.onMessage(
                Message.of(
                        MessageType.COMMIT_REQUEST,
                        txId,
                        coordinatorNode,
                        coordinatorNode )
        );

        // -----------------------------
        // Wait for async timers/messages
        // -----------------------------

        // -----------------------------
        // Dump logs
        // -----------------------------
        System.out.println("\n=== COORDINATOR LOG ===");
        coordinatorLog.read(txId).forEach(System.out::println);

        System.out.println("\n=== PARTICIPANT P1 LOG ===");
        p1Log.read(txId).forEach(System.out::println);

        System.out.println("\n=== PARTICIPANT P2 LOG ===");
        p2Log.read(txId).forEach(System.out::println);

        System.out.println("\n=== PARTICIPANT P3 LOG ===");
            p3Log.read(txId).forEach(System.out::println);
        return ;
    }

}