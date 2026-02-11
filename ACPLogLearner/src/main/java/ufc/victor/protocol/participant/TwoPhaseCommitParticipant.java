package ufc.victor.protocol.participant;

import ufc.victor.protocol.abstractions.IParticipant;
import ufc.victor.protocol.commom.*;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.commom.message.MessageType;
import ufc.victor.protocol.coordinator.node.Node;
import ufc.victor.protocol.participant.log.ParticipantLogManager;
import ufc.victor.protocol.participant.log.ParticipantLogRecord;
import ufc.victor.protocol.participant.log.ParticipantLogRecordType;


import java.time.Instant;

import static ufc.victor.protocol.commom.message.MessageType.VOTE_ABORT;
import static ufc.victor.protocol.commom.message.MessageType.VOTE_COMMIT;


public final class TwoPhaseCommitParticipant implements IParticipant {

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

    public TwoPhaseCommitParticipant(
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
        Terminate();
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
                    ParticipantLogRecordType.READY,
                    Instant.now()
            ));

            send(VOTE_COMMIT);
            timer.set();

        } else {
            log.write(new ParticipantLogRecord(
                    txId,
                    ParticipantLogRecordType.ABORT,
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
        log.write(new ParticipantLogRecord(
                txId,
                ParticipantLogRecordType.ABORT,
                Instant.now()
        ));

        resource.abort(txId);
        send(MessageType.ACK);
        timer.reset();
    }

    /**
     * Valduriez:
     * case Global-commit do
     *   write commit
     *   commit the transaction
     */
    private void onGlobalCommit() {

        log.write(new ParticipantLogRecord(
                txId,
                ParticipantLogRecordType.COMMIT,
                Instant.now()
        ));

        resource.commit(txId);
        send(MessageType.ACK); // Necessary for Coordinator Algo 5.6 "Case Ack"
        timer.reset();
    }

    /**
     * Valduriez:
     * case Timeout do
     *   execute the termination protocol
     */
    private void Terminate() {
        if (getState() == State.INIT) {
            log.write(new ParticipantLogRecord(
                    txId,
                    ParticipantLogRecordType.ABORT,
                    Instant.now()
            ));
        } else {
            send(VOTE_COMMIT);
            timer.reset();
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

    // Stub — wired by LocalNetwork later
    private void send(Message msg) {
        network.send(msg);
    }
}
