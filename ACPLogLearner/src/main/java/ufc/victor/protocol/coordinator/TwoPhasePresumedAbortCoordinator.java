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

public final class TwoPhasePresumedAbortCoordinator implements ICoordinator {

    // ----------------------------
    // Valduriez coordinator states (Adapted for PrA)
    // ----------------------------
    private enum State {
        INIT,
        WAIT,
        COMMIT,
        FINISHED // ABORT is merged into FINISHED for PrA
    }

    private final TransactionId txId;
    private final Node coordinatorId;
    private final Set<NodeStatus> participants;

    private final CoordinatorLogManager log;
    private final ITimer timer;
    private final Network network;

    public TwoPhasePresumedAbortCoordinator(
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
            case WAIT -> {
                // PrA Rule: If we crashed before deciding, we PRESUME ABORT.
                writeGlobalAbort();
                broadcast(MessageType.GLOBAL_ABORT);
            }
            case COMMIT -> {
                resendDecisionToMissing();
                timer.set();
            }
        }
    }

    private State getState() {
        LogRecord last = log.getLast(txId);

        // No logs written, which means the protocol has not started
        if (last == null) return State.INIT;

        return switch (last.type()) {
            case BEGIN_COMMIT -> State.WAIT;
            case GLOBAL_COMMIT -> State.COMMIT;
            case GLOBAL_ABORT, END_TRANSACTION -> State.FINISHED;
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
            case COMMIT -> MessageType.GLOBAL_COMMIT;
            // We never return GLOBAL_ABORT here because the ABORT state doesn't wait for ACKs
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
    // Handlers (Valduriez mapping for PrA)
    // =========================================================

    private void onCommitRequest() {
        if (getState() != State.INIT) return;

        writeBeginCommit();
        broadcast(MessageType.PREPARE_2PC_PAB);
        timer.set();
    }

    private void onVoteAbort(Node from) {
        if (getState() != State.WAIT) return;

        mark(from, NodeState.ABORTED);

        writeGlobalAbort();
        broadcast(MessageType.GLOBAL_ABORT);

        // PrA RULE: Do NOT set the timer. We do not expect ACKs for aborts.
        // The transaction is now forgotten.
    }

    private void onVoteCommit(Node from) {
        if (getState() != State.WAIT) return;

        mark(from, NodeState.VOTED_COMMIT);

        if (allVotedCommit()) {
            WriteGlobalCommit();
            broadcast(MessageType.GLOBAL_COMMIT);

            // We DO expect ACKs for Commits in PrA.
            timer.set();
        }
    }

    private void onAck(Node from) {
        State state = getState();

        // PrA RULE: We only process ACKs for the COMMIT path.
        if (state != State.COMMIT) return;

        mark(from, NodeState.ACKED);

        if (allAcknowledged()) {
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
            // Timeout waiting for votes -> Presume Abort
            writeGlobalAbort();
            broadcast(MessageType.GLOBAL_ABORT);
            // PrA RULE: Do not set timer. We are finished.
        } else if (state == State.COMMIT) {
            // Timeout waiting for Commit ACKs -> Resend
            resendDecisionToMissing();
            timer.set();
        }
    }

    // =========================================================
    // Helpers
    // =========================================================

    private void writeGlobalAbort() {
        // Optimization: In a real DB, this write does not need to be forced to disk synchronously.
        log.write(new LogRecord(
                txId,
                CoordinatorLogRecordType.GLOBAL_ABORT,
                Instant.now()
        ));
    }

    private void writeBeginCommit() {
        log.write(new LogRecord(
                txId,
                CoordinatorLogRecordType.BEGIN_COMMIT,
                Instant.now()
        ));
    }

    private void WriteGlobalCommit() {
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
            Message msg = Message.of(type, txId, coordinatorId, ns.node());
            send(msg);
        }
    }

    private void resendDecisionToMissing() {
        MessageType decision = globalDecision();
        if (decision == null) return;

        for (NodeStatus ns : participants) {
            if (ns.state() != NodeState.ACKED) {
                send(Message.of(decision, txId, coordinatorId, ns.node()));
            }
        }
    }

    private void send(Message msg) {
        network.send(msg);
    }
}