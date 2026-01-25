package ufc.victor.protocol.coordinator;

import ufc.victor.protocol.commom.Timer;
import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.commom.message.EmptyPayload;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.commom.message.MessageType;
import ufc.victor.protocol.coordinator.node.Node;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.protocol.coordinator.log.CoordinatorLogManager;
import ufc.victor.protocol.coordinator.log.LogRecord;
import ufc.victor.protocol.coordinator.log.CoordinatorLogRecordType;

import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;

public final class TwoPhaseCommitCoordinator {

    // ----------------------------
    // Valduriez coordinator states
    // ----------------------------
    private enum State {
        INIT,
        WAIT,
        COMMIT,
        ABORT,
        FINISHED
    }

    private final TransactionId txId;
    private final NodeId coordinatorId;
    private final Set<NodeStatus> participants;

    private final CoordinatorLogManager log;
    private final Timer timer;

    public TwoPhaseCommitCoordinator(
            TransactionId txId,
            NodeId coordinatorId,
            Set<Node> participants,
            CoordinatorLogManager log,
            Timer timer
    ) {
        this.txId = txId;
        this.coordinatorId = coordinatorId;
        this.participants = participants.stream()
                .map(n -> new NodeStatus(n, NodeState.NONE))
                .collect(Collectors.toSet());

        this.log = log;
        this.timer = timer;
    }


    private State getState() {
        LogRecord last = log.read(txId).getLast();

        if (last == null) return State.INIT;

        return switch (last.type()) {
            case BEGIN_COMMIT -> State.WAIT;
            case GLOBAL_ABORT -> State.ABORT;
            case GLOBAL_COMMIT -> State.COMMIT;
            case END_TRANSACTION -> State.FINISHED; // protocol finished
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

            default -> {
                // ignore
            }
        }
    }

    private MessageType globalDecision() {
        return switch (getState()) {
            case COMMIT -> MessageType.GLOBAL_COMMIT;
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
    // Handlers (Valduriez mapping)
    // =========================================================

    /**
     * Valduriez:
     * case Commit do
     *   write begin_commit
     *   send PREPARE
     *   set timer
     */
    private void onCommitRequest() {
        if (getState() != State.INIT) return;

        writeBeginCommit();

        broadcast(MessageType.PREPARE);
        timer.set();
    }



    /**
     * Valduriez:
     * case Vote-abort do
     *   write abort
     *   send global-abort
     */
    private void onVoteAbort(NodeId from) {
        if (getState() != State.WAIT) return;

        mark(from, NodeState.ABORTED);


        writeGlobalAbort();

        broadcast(MessageType.GLOBAL_ABORT);
        timer.set();
    }



    /**
     * Valduriez:
     * case Vote-commit do
     *   update list
     *   if all answered then
     *      write commit
     *      send global-commit
     */
    private void onVoteCommit(NodeId from) {
        if (getState() != State.WAIT) return;

        mark(from, NodeState.VOTED_COMMIT);

        if (allVotedCommit()) {

            WriteGlobalCommit();

            broadcast(MessageType.GLOBAL_COMMIT);
            timer.set();
        }
    }



    /**
     * Valduriez:
     * case Ack do
     *   update acknowledgements
     *   if all acknowledged then
     *      write end_of_transaction
     *   else
     *      resend decision
     */
    private void onAck(NodeId from) {
        State state = getState();
        if (state != State.COMMIT && state != State.ABORT) return;

        mark(from, NodeState.ACKED);

        if (allAcknowledged()) {
            log.write(new LogRecord(
                    txId,
                    CoordinatorLogRecordType.END_TRANSACTION,
                    Instant.now()
            ));
        } else {
            resendDecisionToMissing();
        }
    }

    private void Terminate() {
        LogRecord lastRecord = log.read(txId).getLast();

        if (lastRecord == null || lastRecord.type() == CoordinatorLogRecordType.BEGIN_COMMIT) {
            // INIT or WAIT → unilateral abort
            writeGlobalAbort();
            broadcast(MessageType.GLOBAL_ABORT);
        } else if (lastRecord.type() == CoordinatorLogRecordType.GLOBAL_ABORT) {
            broadcast(MessageType.GLOBAL_ABORT);
        } else if (lastRecord.type() == CoordinatorLogRecordType.GLOBAL_COMMIT) {
            broadcast(MessageType.GLOBAL_COMMIT);
        }

        timer.set();
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

    private void mark(NodeId id, NodeState newState) {
        participants.stream()
                .filter(ns -> ns.node().id.equals(id))
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
                    ns.node().id,
                    EmptyPayload.INSTANCE
            );
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
                        ns.node().id,
                        EmptyPayload.INSTANCE
                ));
            }
        }
    }


    // Stub (LocalNetwork will call this)
    private void send(Message msg) {
        // network.send(msg.to(), msg);
    }
}
