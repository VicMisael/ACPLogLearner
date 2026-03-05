package ufc.victor.protocol.participant;

import ufc.victor.protocol.abstractions.IParticipant;
import ufc.victor.protocol.commom.ITimer;
import ufc.victor.protocol.commom.ITimerFactory;
import ufc.victor.protocol.commom.Network;
import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.commom.message.MessageType;
import ufc.victor.protocol.coordinator.node.Node;
import ufc.victor.protocol.participant.log.ParticipantLogManager;
import ufc.victor.protocol.participant.log.ParticipantLogRecord;
import ufc.victor.protocol.participant.log.ParticipantLogRecordType;

import java.time.Instant;

import static ufc.victor.protocol.commom.message.MessageType.VOTE_ABORT;
import static ufc.victor.protocol.commom.message.MessageType.VOTE_COMMIT;

public final class TwoPhaseCommitPresumedCommitParticipant implements IParticipant {

    // ----------------------------
    // Valduriez participant states
    // ----------------------------
    private enum State {
        INIT,
        READY,
        COMMIT,
        ABORT
    }

    private final TransactionId txId;
    private final Node participantId;
    private final Node coordinatorId;

    private final ParticipantLogManager log;
    private final ITimer timer;
    private final TransactionalResource resource;
    private final Network network;

    public TwoPhaseCommitPresumedCommitParticipant(
            TransactionId txId,
            Node participant,
            Node coordinator,
            ParticipantLogManager log,
            ITimerFactory timerFactory,
            TransactionalResource resource, Network network
    ) {
        this.txId = txId;
        this.participantId = participant;
        this.coordinatorId = coordinator;
        this.log = log;
        this.resource = resource;
        this.network = network;
        this.timer = timerFactory.createOrGetTimer(this);
    }

    @Override
    public void recover() {
        State state = getState();

        switch (state) {
            case INIT -> {
                // If we crashed before voting, we unilaterally abort.
                voteAbort();
            }
            case READY -> {
                // We voted YES but crashed. We are blocked and must ask the coordinator.
                // In PrC, if the coordinator responds with "I don't know this transaction",
                // we will presume it committed.
                Terminate();
            }
            default -> {
                // COMMIT or ABORT: Already handled locally.
            }
        }
    }

    // =========================================================
    // State derived ONLY from log (crash-safe)
    // =========================================================
    private State getState() {
        ParticipantLogRecord last = log.getLast(txId);

        if (last == null) return State.INIT;

        return switch (last.type()) {
            case READY -> State.READY;
            case ABORT -> State.ABORT;
            case COMMIT -> State.COMMIT;
        };
    }

    // =========================================================
    // Event: Message Arrival
    // =========================================================
    public void onMessage(Message msg) {
        switch (msg.type()) {
            // PrC Activation Signal!
            case PREPARE_2PC_PCO -> onPrepare();
            case GLOBAL_ABORT -> onGlobalAbort();
            case GLOBAL_COMMIT -> onGlobalCommit();
            default -> { /* ignore */ }
        }
    }

    // =========================================================
    // Event: Timeout
    // =========================================================
    public void onTimeout() {
        Terminate();
    }

    // =========================================================
    // Valduriez handlers (Adapted for PrC)
    // =========================================================

    private void onPrepare() {
        if (getState() != State.INIT) return;

        if (resource.prepare(txId)) {
            voteCommit();
        } else {
            voteAbort();
        }
    }

    private void voteCommit() {
        // MUST be force-written
        log.write(new ParticipantLogRecord(
                txId,
                ParticipantLogRecordType.READY,
                Instant.now()
        ));

        send(VOTE_COMMIT);
        timer.set();
    }

    private void voteAbort() {
        // PrC RULE: Aborts must be safely logged before sending VOTE_ABORT
        log.write(new ParticipantLogRecord(
                txId,
                ParticipantLogRecordType.ABORT,
                Instant.now()
        ));

        resource.abort(txId);
        send(VOTE_ABORT);
        // Protocol done locally.
    }

    private void onGlobalAbort() {
        if (getState() == State.ABORT || getState() == State.INIT) return;

        // PrC RULE: We must log the abort so we don't accidentally presume commit later
        log.write(new ParticipantLogRecord(
                txId,
                ParticipantLogRecordType.ABORT,
                Instant.now()
        ));

        resource.abort(txId);

        // PrC RULE: MUST send ACK for Global-Abort. Coordinator is waiting for it.
        send(MessageType.ACK);
        timer.reset();
    }

    private void onGlobalCommit() {
        if (getState() == State.COMMIT) return;

        // PrC Optimization: This commit log does NOT need to be strictly force-written.
        // If we crash and lose it, recovery sees READY and asks the coordinator.
        // Coordinator won't know the Tx, so we will presume commit anyway!
        log.write(new ParticipantLogRecord(
                txId,
                ParticipantLogRecordType.COMMIT,
                Instant.now()
        ));

        resource.commit(txId);

        // PrC RULE: Do NOT send ACK! The coordinator has already forgotten the transaction.
        timer.reset();
    }

    private void Terminate() {
        if (getState() == State.INIT) {
            voteAbort();
        } else if (getState() == State.READY) {
            // We are BLOCKED. Query the coordinator.
            send(VOTE_COMMIT);

            // CRITICAL: We must re-set the timer to keep polling if the message drops.
            timer.set();
        }
    }

    // =========================================================
    // Messaging
    // =========================================================
    private void send(MessageType type) {
        Message msg = Message.of(
                type,
                txId,
                participantId,
                coordinatorId
        );
        send(msg);
    }

    private void send(Message msg) {
        network.send(msg);
    }
}