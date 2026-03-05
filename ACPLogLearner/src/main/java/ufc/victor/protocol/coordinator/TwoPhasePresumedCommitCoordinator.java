package ufc.victor.protocol.coordinator;

import ufc.victor.protocol.abstractions.ICoordinator;
import ufc.victor.protocol.commom.ITimer;
import ufc.victor.protocol.commom.ITimerFactory;
import ufc.victor.protocol.commom.Network;
import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.commom.message.MessageType;
import ufc.victor.protocol.coordinator.log.CoordinatorLogManager;
import ufc.victor.protocol.coordinator.log.CoordinatorLogRecordType;
import ufc.victor.protocol.coordinator.log.LogRecord;
import ufc.victor.protocol.coordinator.node.Node;

import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;

public final class TwoPhasePresumedCommitCoordinator implements ICoordinator {

    // ----------------------------
    // States adapted for PrC
    // ----------------------------
    private enum State {
        INIT,
        COLLECTING, // Coordinator force-writes participant list
        WAIT,
        ABORT,      // Only Aborts wait for ACKs
        FINISHED    // Reached immediately on Commit, or after ACKs on Abort
    }

    private final TransactionId txId;
    private final Node coordinatorId;
    private final Set<NodeStatus> participants;

    private final CoordinatorLogManager log;
    private final ITimer timer;
    private final Network network;

    public TwoPhasePresumedCommitCoordinator(
            TransactionId txId,
            Node coordinatorId,
            Set<Node> participants,
            CoordinatorLogManager log,
            Network network,
            ITimerFactory timerFactory
    ) {
        this.txId = txId;
        this.coordinatorId = coordinatorId;
        this.participants = participants.stream()
                .map(n -> new NodeStatus(n, NodeState.NONE))
                .collect(Collectors.toSet());

        this.log = log;
        this.network = network;
        this.timer = timerFactory.createOrGetTimer(this);
    }

    public void recover() {
        State state = getState();

        switch (state) {
            case INIT, FINISHED -> {
                // Nothing to do
            }
            case COLLECTING, WAIT -> {
                // PrC Rule: If we crash after writing the collecting record but before deciding,
                // we must abort upon recovery to prevent inconsistency.
                writeGlobalAbort();
                broadcast(MessageType.GLOBAL_ABORT);
                timer.set(); // We expect ACKs for aborts
            }
            case ABORT -> {
                // Crashed while waiting for Abort ACKs. Must resend.
                resendDecisionToMissing();
                timer.set();
            }
        }
    }

    private State getState() {
        LogRecord last = log.getLast(txId);

        if (last == null) return State.INIT;

        return switch (last.type()) {
            case BEGIN_COMMIT -> State.WAIT; // Acts as the collecting record
            case GLOBAL_ABORT -> State.ABORT;
            // In PrC, GLOBAL_COMMIT or END_TRANSACTION means we are completely done.
            case GLOBAL_COMMIT, END_TRANSACTION -> State.FINISHED;
        };
    }

    // =========================================================
    // Event: Message Arrival
    // =========================================================
    public void onMessage(Message msg) {
        switch (msg.type()) {
            case COMMIT_REQUEST -> onCommitRequest();
            case VOTE_ABORT -> onVoteAbort(msg.from());
            case VOTE_COMMIT -> onVoteCommit(msg.from());
            case ACK -> onAck(msg.from());
            default -> { /* ignore */ }
        }
    }

    private MessageType globalDecision() {
        return switch (getState()) {
            // We only return GLOBAL_ABORT here because the COMMIT state doesn't wait for ACKs
            case ABORT -> MessageType.GLOBAL_ABORT;
            default -> null;
        };
    }

    // =========================================================
    // Event: Timeout
    // =========================================================
    public void onTimeout() {
        Terminate();
    }

    // =========================================================
    // Handlers
    // =========================================================

    private void onCommitRequest() {
        if (getState() != State.INIT) return;

        // PrC RULE: Force-write the collecting record BEFORE sending prepare messages.
        // We use BEGIN_COMMIT to represent this.
        writeBeginCommit();

        broadcast(MessageType.PREPARE_2PC_PCO);
        timer.set();
    }

    private void onVoteAbort(Node from) {
        if (getState() != State.WAIT) return;

        mark(from, NodeState.ABORTED);

        writeGlobalAbort();
        broadcast(MessageType.GLOBAL_ABORT);

        // PrC RULE: We DO expect ACKs for aborts.
        timer.set();
    }

    private void onVoteCommit(Node from) {
        if (getState() != State.WAIT) return;

        mark(from, NodeState.VOTED_COMMIT);

        if (allVotedCommit()) {
            // Write the commit record (can be async)
            WriteGlobalCommit();
            broadcast(MessageType.GLOBAL_COMMIT);

            // PrC RULE: Do NOT set the timer. We do not expect ACKs for commits.
            // The coordinator forgets the transaction immediately.
        }
    }

    private void onAck(Node from) {
        State state = getState();

        // PrC RULE: We only process ACKs for the ABORT path.
        if (state != State.ABORT) return;

        mark(from, NodeState.ACKED);

        if (allAcknowledged()) {
            // Write end_of_transaction to forget the aborted transaction
            log.write(new LogRecord(
                    txId,
                    CoordinatorLogRecordType.END_TRANSACTION,
                    Instant.now()
            ));
            // Finished. No timer.
        } else {
            resendDecisionToMissing();
            timer.set();
        }
    }

    private void Terminate() {
        State state = getState();

        if (state == State.WAIT) {
            // Timeout waiting for votes -> Decide to Abort
            writeGlobalAbort();
            broadcast(MessageType.GLOBAL_ABORT);
            timer.set(); // Wait for Abort ACKs
        } else if (state == State.ABORT) {
            // Timeout waiting for Abort ACKs -> Resend
            resendDecisionToMissing();
            timer.set();
        }
    }

    // =========================================================
    // Helpers
    // =========================================================

    private void writeGlobalAbort() {
        log.write(new LogRecord(
                txId,
                CoordinatorLogRecordType.GLOBAL_ABORT,
                Instant.now()
        ));
    }

    private void writeBeginCommit() {
        // This acts as the "collecting record" which must be force-written.
        log.write(new LogRecord(
                txId,
                CoordinatorLogRecordType.BEGIN_COMMIT,
                Instant.now()
        ));
    }

    private void WriteGlobalCommit() {
        // Optimization: This write does not need to be forced to disk synchronously.
        log.write(new LogRecord(
                txId,
                CoordinatorLogRecordType.GLOBAL_COMMIT,
                Instant.now()
        ));
    }

    private void mark(Node node, NodeState newState) {
        participants.stream()
                .filter(ns -> ns.node().id.equals(node.id))
                .forEach(ns -> {
                    if (ns.state().canTransitionTo(newState)) {
                        ns.setState(newState);
                    }
                });
    }

    private boolean allVotedCommit() {
        return participants.stream()
                .allMatch(ns -> ns.state() == NodeState.VOTED_COMMIT);
    }

    private boolean allAcknowledged() {
        return participants.stream()
                .allMatch(ns -> ns.state() == NodeState.ACKED);
    }

    private void broadcast(MessageType type) {
        for (NodeStatus ns : participants) {
            Message msg = Message.of(
                    type,
                    txId,
                    coordinatorId,
                    ns.node());
            send(msg);
        }
    }

    private void resendDecisionToMissing() {
        MessageType decision = globalDecision();
        if (decision == null) return;

        for (NodeStatus ns : participants) {
            if (ns.state() != NodeState.ACKED) {
                send(Message.of(
                        decision,
                        txId,
                        coordinatorId,
                        ns.node()
                ));
            }
        }
    }

    private void send(Message msg) {
        network.send(msg);
    }
}