package ufc.victor.protocol.coordinator;

import ufc.victor.protocol.commom.*;
import ufc.victor.protocol.commom.message.EmptyPayload;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.commom.message.MessageType;
import ufc.victor.protocol.coordinator.node.Node;
import ufc.victor.protocol.coordinator.log.CoordinatorLogManager;
import ufc.victor.protocol.coordinator.log.LogRecord;
import ufc.victor.protocol.coordinator.log.CoordinatorLogRecordType;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class TwoPhaseCommitCoordinator implements IMessageHandler, TimeoutHandler {

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
    private final Node coordinatorId;
    private final Set<NodeStatus> participants;

    private final CoordinatorLogManager log;
    private final ITimer timer;
    private final Network network;

    public TwoPhaseCommitCoordinator(
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
        this.timer = timerFactory.createTimer(this);
    }

    public void recover() {
        State state = getState();

        switch (state) {
            case INIT -> {
                // Nothing to do
            }
            case WAIT -> {
                // Votes may be missing → start timeout
                timer.set();
            }
            case COMMIT -> {
                broadcast(MessageType.GLOBAL_COMMIT);
                timer.set();
            }
            case ABORT -> {
                broadcast(MessageType.GLOBAL_ABORT);
                timer.set();
            }
            case FINISHED -> {
                // Protocol complete
            }
        }
    }


    private State getState() {
         LogRecord last = log.getLast(txId);

                 //No logs written, which means the protocol has not started;
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
    private void onVoteAbort(Node from) {
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
    private void onVoteCommit(Node from) {
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
    private void onAck(Node from) {
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
    /**
    Algorithm 5.8: 2PC Coordinator Terminate
            begin
if in WAIT state then {coordinator is in ABORT state}
    write abort record in the log
            send “Global-abort” message to all the participants
else {coordinator is in COMMIT state}
    check for the last log record
if last log record = abort then
    send “Global-abort” to all participants that have not responded
else
    send “Global-commit” to all the participants that have not responded
    end if
    end if
    set timer
    end*/
    private void Terminate() {
        var state = getState();
        if (state == State.WAIT){
            writeGlobalAbort();
            broadcast(MessageType.GLOBAL_ABORT);
        }else{
            var last = log.getLast(txId);
            if (last != null){
                resendDecisionToMissing();
            }
            //Last log will never be null since
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
                    ns.node(),
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
                        ns.node(),
                        EmptyPayload.INSTANCE
                ));
            }
        }
    }


    // Stub (LocalNetwork will call this)
    private void send(Message msg) {

        network.send(msg);
    }
}
