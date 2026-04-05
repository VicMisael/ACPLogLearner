package ufc.victor.protocol.participant;

import ufc.victor.protocol.abstractions.IParticipant;
import ufc.victor.protocol.commom.*;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.commom.message.MessageType;
import ufc.victor.protocol.coordinator.node.Node;
import ufc.victor.protocol.participant.log.ParticipantLogManager;
import ufc.victor.protocol.participant.log.ParticipantPhase;
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
    private Instant preparePhaseStartedAt;

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


    @Override
    public void recover() {
        State state = getState();

        switch (state){
            case INIT:
                Instant phaseStart = Instant.now();
                abort(phaseStart, ParticipantPhase.TERMINATION, phaseStart);
                break;
                case READY:
                    Terminate();
                    break;
            default:
                //nothing

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

            case PREPARE_2PC -> onPrepare(msg);

            case GLOBAL_ABORT -> onGlobalAbort(msg);

            case GLOBAL_COMMIT -> onGlobalCommit(msg);

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
    private void onPrepare(Message msg) {
        if (getState() != State.INIT) return;

        preparePhaseStartedAt = Instant.now();
        if (resource.prepare(txId)) {
            commit(msg.timestamp());

        } else {
            abort(msg.timestamp(), ParticipantPhase.PREPARE, preparePhaseStartedAt);
        }
    }

    private void commit(Instant triggerTimestamp) {
        log.write(ParticipantLogRecord.of(
                txId,
                participantId.id,
                ParticipantLogRecordType.READY,
                ParticipantPhase.PREPARE,
                triggerTimestamp,
                preparePhaseStartedAt
        ));

        send(VOTE_COMMIT);
        timer.set();
    }

    private void abort(Instant triggerTimestamp, ParticipantPhase phase, Instant phaseStartedAt) {
        log.write(ParticipantLogRecord.of(
                txId,
                participantId.id,
                ParticipantLogRecordType.ABORT,
                phase,
                triggerTimestamp,
                phaseStartedAt
        ));

        send(VOTE_ABORT);
        resource.abort(txId);
    }

    /**
     * Valduriez:
     * case Global-abort do
     *   write abort
     *   abort the transaction
     */
    private void onGlobalAbort(Message msg) {
        Instant phaseStart = Instant.now();
        log.write(ParticipantLogRecord.of(
                txId,
                participantId.id,
                ParticipantLogRecordType.ABORT,
                ParticipantPhase.DECISION,
                msg.timestamp(),
                phaseStart
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
    private void onGlobalCommit(Message msg) {
        Instant phaseStart = Instant.now();
        log.write(ParticipantLogRecord.of(
                txId,
                participantId.id,
                ParticipantLogRecordType.COMMIT,
                ParticipantPhase.DECISION,
                msg.timestamp(),
                phaseStart
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
            Instant phaseStart = Instant.now();
            log.write(ParticipantLogRecord.of(
                    txId,
                    participantId.id,
                    ParticipantLogRecordType.ABORT,
                    ParticipantPhase.TERMINATION,
                    phaseStart,
                    phaseStart
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
