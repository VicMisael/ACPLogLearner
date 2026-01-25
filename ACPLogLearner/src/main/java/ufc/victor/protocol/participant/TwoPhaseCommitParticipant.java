package ufc.victor.protocol.participant;

import ufc.victor.protocol.commom.Timer;
import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.commom.message.EmptyPayload;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.commom.message.MessageType;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.protocol.participant.log.ParticipantLogManager;
import ufc.victor.protocol.participant.log.ParticipantLogRecord;
import ufc.victor.protocol.participant.log.ParticipantLogRecordType;


import java.time.Instant;

import static ufc.victor.protocol.commom.message.MessageType.VOTE_ABORT;
import static ufc.victor.protocol.commom.message.MessageType.VOTE_COMMIT;


public final class TwoPhaseCommitParticipant {

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
    private final NodeId participantId;
    private final NodeId coordinatorId;

    private final ParticipantLogManager log;
    private final Timer timer;
    private final TransactionalResource resource;

    public TwoPhaseCommitParticipant(
            TransactionId txId,
            NodeId participantId,
            NodeId coordinatorId,
            ParticipantLogManager log,
            Timer timer,
            TransactionalResource resource
    ) {
        this.txId = txId;
        this.participantId = participantId;
        this.coordinatorId = coordinatorId;
        this.log = log;
        this.timer = timer;
        this.resource = resource;
    }

    // =========================================================
    // State derived ONLY from log (crash-safe)
    // =========================================================
    private State getState() {
        ParticipantLogRecord last = log.read(txId).getLast();

        if (last == null) return State.INIT;

        return switch (last.type()) {
            case VOTE_COMMIT -> State.READY;
            case VOTE_ABORT -> State.ABORT;
            case GLOBAL_COMMIT -> State.COMMIT;
            case GLOBAL_ABORT -> State.ABORT;
        };
    }

    // =========================================================
    // Event: Message Arrival
    // =========================================================
    public void onMessage(Message msg) {
        switch (msg.type()) {

            case PREPARE -> onPrepare();

            case GLOBAL_ABORT -> onGlobalAbort();

            case GLOBAL_COMMIT -> onGlobalCommit();

            default -> {
                // ignore
            }
        }
    }

    // =========================================================
    // Event: Timeout
    // =========================================================
    public void onTimeout() {
        executeTerminationProtocol();
    }

    // =========================================================
    // Valduriez handlers
    // =========================================================

    /**
     * Valduriez:
     * case Prepare do
     *   if ready to commit then
     *      write ready
     *      send Vote-commit
     *      set timer
     *   else
     *      write abort
     *      send Vote-abort
     *      abort transaction
     */
    private void onPrepare() {
        if (getState() != State.INIT) return;

        if (resource.prepare(txId)) {
            log.write(new ParticipantLogRecord(
                    txId,
                    ParticipantLogRecordType.VOTE_COMMIT,
                    Instant.now()
            ));

            send(VOTE_COMMIT);
            timer.set();

        } else {
            log.write(new ParticipantLogRecord(
                    txId,
                    ParticipantLogRecordType.VOTE_ABORT,
                    Instant.now()
            ));

            send(VOTE_ABORT);
            resource.abort(txId);
        }
    }

    /**
     * Valduriez:
     * case Global-abort do
     *   write abort
     *   abort the transaction
     */
    private void onGlobalAbort() {
        State state = getState();
        if (state == State.ABORT || state == State.COMMIT) return;

        log.write(new ParticipantLogRecord(
                txId,
                ParticipantLogRecordType.GLOBAL_ABORT,
                Instant.now()
        ));

        resource.abort(txId);
        send(MessageType.ACK);
    }

    /**
     * Valduriez:
     * case Global-commit do
     *   write commit
     *   commit the transaction
     */
    private void onGlobalCommit() {
        if (getState() != State.READY) return;

        log.write(new ParticipantLogRecord(
                txId,
                ParticipantLogRecordType.GLOBAL_COMMIT,
                Instant.now()
        ));

        resource.commit(txId);
        send(MessageType.ACK);
    }

    /**
     * Valduriez:
     * case Timeout do
     *   execute the termination protocol
     */
    private void executeTerminationProtocol() {
        if (getState() != State.READY) return;

        // Conservative termination:
        // If READY and coordinator unreachable → abort
        log.write(new ParticipantLogRecord(
                txId,
                ParticipantLogRecordType.GLOBAL_ABORT,
                Instant.now()
        ));

        resource.abort(txId);
        send(MessageType.ACK);
    }

    // =========================================================
    // Messaging
    // =========================================================
    private void send(MessageType type) {
        Message msg = Message.of(
                type,
                txId,
                participantId,
                coordinatorId,
                EmptyPayload.INSTANCE
        );
        send(msg);
    }

    // Stub — wired by LocalNetwork later
    private void send(Message msg) {
        // network.send(msg.to(), msg);
    }
}
