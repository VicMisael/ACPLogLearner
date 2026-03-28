package ufc.victor.protocol.coordinator;

import ufc.victor.protocol.SelectedProtocol;
import ufc.victor.protocol.abstractions.ICoordinator;
import ufc.victor.protocol.commom.ITimerFactory;
import ufc.victor.protocol.commom.Network;
import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.coordinator.log.CoordinatorLogManager;
import ufc.victor.protocol.coordinator.node.Node;


import java.util.Set;

public class AdaptiveCoordinatorDispatcher implements ICoordinator {

    // The actual Valduriez state machine doing the heavy lifting
    private final ICoordinator activeProtocol;

    public AdaptiveCoordinatorDispatcher(
            TransactionId txId,
            Node coordinatorId,
            Set<Node> participants,
            CoordinatorLogManager log,
            Network network,
            ITimerFactory timerFactory,
            SelectedProtocol selectedProtocol
    ) {
        // 1. Immediately spawn the correct protocol based on the ML's choice
        this.activeProtocol = spawnProtocol(
                selectedProtocol, txId, coordinatorId, participants, log, network, timerFactory
        );

        System.out.println("[Coordinator " + coordinatorId.id+ "] Initialized "
                + activeProtocol.getClass().getSimpleName() + " for Tx " + txId);
    }

    // ----------------------------------------------------------
    // FACTORY ROUTER
    // ----------------------------------------------------------
    private ICoordinator spawnProtocol(
            SelectedProtocol protocol, TransactionId txId, Node coordinatorId,
            Set<Node> participants, CoordinatorLogManager log, Network network, ITimerFactory timerFactory) {

        return switch (protocol) {
            case TWO_PC ->
                    new TwoPhaseCommitCoordinator(txId, coordinatorId, participants, log, network, timerFactory);
            case TWO_PC_PRESUMED_ABORT ->
                    new TwoPhasePresumedAbortCoordinator(txId, coordinatorId, participants, log, network, timerFactory);
            case TWO_PC_PRESUMED_COMMIT ->
                    new TwoPhasePresumedCommitCoordinator(txId, coordinatorId, participants, log, network, timerFactory);
        };
    }

    // ----------------------------------------------------------
    // DELEGATION (Transparent Proxy)
    // ----------------------------------------------------------
    @Override
    public void recover() {
        activeProtocol.recover();
    }

    @Override
    public void onMessage(Message msg) {
        activeProtocol.onMessage(msg);
    }

    @Override
    public void onTimeout() {
        activeProtocol.onTimeout();
    }
}