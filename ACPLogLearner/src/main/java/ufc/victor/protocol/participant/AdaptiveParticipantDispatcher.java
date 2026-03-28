package ufc.victor.protocol.participant;

import ufc.victor.localenv.LocalNode;
import ufc.victor.protocol.abstractions.IParticipant;
import ufc.victor.protocol.commom.ITimerFactory;
import ufc.victor.protocol.commom.Network;
import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.commom.message.MessageType;
import ufc.victor.protocol.coordinator.node.Node;
import ufc.victor.protocol.participant.log.ParticipantLogManager;


public final class AdaptiveParticipantDispatcher implements IParticipant {

    private IParticipant activeProtocol = null;

    private final Node myNode;
    private final ParticipantLogManager logManager;
    private final ITimerFactory timerFactory;
    private final TransactionalResource resource;
    private final Network network;

    public AdaptiveParticipantDispatcher(
            LocalNode myNode,
            ParticipantLogManager logManager,
            ITimerFactory timerFactory,
            TransactionalResource resource,
            Network network) {
        this.myNode = myNode;
        this.logManager = logManager;
        this.timerFactory = timerFactory;
        this.resource = resource;
        this.network = network;
    }

    @Override
    public void onMessage(Message msg) {
        // 1. Lazy-load the protocol on the very first message
        if (activeProtocol == null) {
            activeProtocol = spawnProtocol(msg.type(), msg.transactionId(), msg.from());
            if (activeProtocol != null) {
                System.out.println("[Participant " + myNode.id + "] Bound to " + activeProtocol.getClass().getSimpleName() + " for Tx " + msg.transactionId());
            } else {
                System.err.println("[Participant " + myNode.id + "] Unknown protocol signal: " + msg.type());
                return;
            }
        }

        // 2. Pass the message down to the active Valduriez protocol
        activeProtocol.onMessage(msg);
    }

    @Override
    public void onTimeout() {
        if (activeProtocol != null) {
            activeProtocol.onTimeout();
        }
    }

    /**
     * Call this at the end of your Main simulation loop to wipe the brain
     * clean for the next transaction in the dataset generation.
     */
    public void resetForNextTransaction() {
        this.activeProtocol = null;
    }

    private IParticipant spawnProtocol(MessageType type, TransactionId txId, Node coordinatorNode) {
        return switch (type) {
            case PREPARE_2PC ->
                    new TwoPhaseCommitParticipant(txId, myNode, coordinatorNode, logManager, timerFactory, resource, network);
            case PREPARE_2PC_PAB ->
                    new TwoPhaseCommitPresumedAbortParticipant(txId, myNode, coordinatorNode, logManager, timerFactory, resource, network);
            case PREPARE_2PC_PCO ->
                    new TwoPhaseCommitPresumedCommitParticipant(txId, myNode, coordinatorNode, logManager, timerFactory, resource, network);
            default -> null;
        };
    }

    @Override
    public void recover() {
        if(activeProtocol != null){
            activeProtocol.onTimeout();
        }
    }
}