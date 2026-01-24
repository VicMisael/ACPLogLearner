package ufc.victor.protocol.coordinator;

import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.coordinator.message.EmptyPayload;
import ufc.victor.protocol.coordinator.message.Message;
import ufc.victor.protocol.coordinator.message.MessageType;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.protocol.log.LogManager;
import ufc.victor.protocol.log.LogRecord;
import ufc.victor.protocol.log.LogRecordType;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

public final class TwoPhaseCommitCoordinator {

    // ----------------------------
    // Valduriez coordinator states
    // ----------------------------
    private enum State {
        INIT,
        WAIT,
        COMMIT,
        ABORT
    }

    private final TransactionId txId;
    private final NodeId coordinatorId;
    private final Set<NodeId> participants;

    private final Set<NodeId> voted;
    private final Set<NodeId> acknowledged;

    private final LogManager log;
    private final CoordinatorTerminationProtocol terminationProtocol;

    private State state = State.INIT;
    private MessageType globalDecision = null;

    public TwoPhaseCommitCoordinator(
            TransactionId txId,
            NodeId coordinatorId,
            Set<NodeId> participants,
            LogManager log,
            CoordinatorTerminationProtocol terminationProtocol
    ) {
        this.txId = txId;
        this.coordinatorId = coordinatorId;
        this.participants = participants;
        this.log = log;
        this.terminationProtocol = terminationProtocol;

        this.voted = new HashSet<>();
        this.acknowledged = new HashSet<>();
    }

    // =========================================================
    // Event: Message Arrival
    // =========================================================
    public void onMessage(Message msg) {

        switch (msg.type()) {

            // -----------------------------------------
            // Commit command from scheduler
            // -----------------------------------------
            case COMMIT_REQUEST -> onCommitRequest();

            // -----------------------------------------
            // One participant voted abort (unilateral)
            // -----------------------------------------
            case VOTE_ABORT -> onVoteAbort(msg.from());

            // -----------------------------------------
            // One participant voted commit
            // -----------------------------------------
            case VOTE_COMMIT -> onVoteCommit(msg.from());

            // -----------------------------------------
            // Participant acknowledged final decision
            // -----------------------------------------
            case ACK -> onAck(msg.from());

            default -> {
                // ignore
            }
        }
    }

    // =========================================================
    // Event: Timeout
    // =========================================================
    public void onTimeout() {
        terminationProtocol.execute(txId);
    }

    // =========================================================
    // Handlers (Valduriez mapping)
    // =========================================================

    /**
     * case Commit do
     *   write begin_commit
     *   send PREPARE to all participants
     *   set timer
     */
    private void onCommitRequest() {
        if (state != State.INIT) return;

        log.write(new LogRecord(
                txId,
                LogRecordType.BEGIN_COMMIT,
                Instant.now()
        ));

        broadcast(MessageType.PREPARE);
        state = State.WAIT;
    }

    /**
     * case Vote-abort do
     *   write abort
     *   send global-abort
     *   set timer
     */
    private void onVoteAbort(NodeId from) {
        if (state != State.WAIT) return;

        globalDecision = MessageType.GLOBAL_ABORT;

        log.write(new LogRecord(
                txId,
                LogRecordType.GLOBAL_ABORT,
                Instant.now()
        ));

        broadcast(MessageType.GLOBAL_ABORT);
        state = State.ABORT;
    }

    /**
     * case Vote-commit do
     *   update list of participants
     *   if all answered then
     *     write commit
     *     send global-commit
     *     set timer
     */
    private void onVoteCommit(NodeId from) {
        if (state != State.WAIT) return;

        voted.add(from);

        if (voted.containsAll(participants)) {
            globalDecision = MessageType.GLOBAL_COMMIT;

            log.write(new LogRecord(
                    txId,
                    LogRecordType.GLOBAL_COMMIT,
                    Instant.now()
            ));

            broadcast(MessageType.GLOBAL_COMMIT);
            state = State.COMMIT;
        }
    }

    /**
     * case Ack do
     *   update list of acknowledgements
     *   if all acknowledged then
     *     write end_of_transaction
     *   else
     *     resend decision
     */
    private void onAck(NodeId from) {
        if (state != State.COMMIT && state != State.ABORT) return;

        acknowledged.add(from);

        if (acknowledged.containsAll(participants)) {

            log.write(new LogRecord(
                    txId,
                    LogRecordType.END_TRANSACTION,
                    Instant.now()
            ));

        } else {
            resendDecisionToMissing();
        }
    }

    // =========================================================
    // Helpers
    // =========================================================

    private void broadcast(MessageType type) {
        for (NodeId p : participants) {
            Message msg = Message.of(
                    type,
                    txId,
                    coordinatorId,
                    p,
                    EmptyPayload.INSTANCE
            );
            send(msg);
        }
    }

    private void resendDecisionToMissing() {
        for (NodeId p : participants) {
            if (!acknowledged.contains(p)) {
                Message msg = Message.of(
                        globalDecision,
                        txId,
                        coordinatorId,
                        p,
                        EmptyPayload.INSTANCE
                );
                send(msg);
            }
        }
    }

    // Stub for now (LocalNetwork will call this)
    private void send(Message msg) {
        // network.send(msg.to(), msg);
    }
}
