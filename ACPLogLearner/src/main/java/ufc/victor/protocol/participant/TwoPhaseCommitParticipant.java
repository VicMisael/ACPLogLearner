package ufc.victor.protocol.participant;

import ufc.victor.protocol.commom.MessageHandler;
import ufc.victor.protocol.commom.Network;
import ufc.victor.protocol.commom.Timer;
import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.commom.message.EmptyPayload;
import ufc.victor.protocol.commom.message.Message;
import ufc.victor.protocol.commom.message.MessageType;
import ufc.victor.protocol.coordinator.node.Node;
import ufc.victor.protocol.coordinator.node.NodeId;
import ufc.victor.protocol.participant.log.ParticipantLogManager;
import ufc.victor.protocol.participant.log.ParticipantLogRecord;
import ufc.victor.protocol.participant.log.ParticipantLogRecordType;


import java.time.Instant;

import static ufc.victor.protocol.commom.message.MessageType.VOTE_ABORT;
import static ufc.victor.protocol.commom.message.MessageType.VOTE_COMMIT;


public final class TwoPhaseCommitParticipant implements MessageHandler {

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
    private final Timer timer;
    private final TransactionalResource resource;
    private final Network network;

    public TwoPhaseCommitParticipant(
            TransactionId txId,
            Node participantId,
            Node coordinatorId,
            ParticipantLogManager log,
            Timer timer,
            TransactionalResource resource, Network network
    ) {
        this.txId = txId;
        this.participantId = participantId;
        this.coordinatorId = coordinatorId;
        this.log = log;
        this.timer = timer;
        this.resource = resource;
        this.network = network;
    }



    // =========================================================
    // State derived ONLY from log (crash-safe)
    // =========================================================
    private State getState() {
        ParticipantLogRecord last = log.read(txId).getLast();

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
        State state = getState();
        if (state == State.ABORT || state == State.COMMIT) return;

        log.write(new ParticipantLogRecord(
                txId,
                ParticipantLogRecordType.ABORT,
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
                ParticipantLogRecordType.COMMIT,
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
                ParticipantLogRecordType.ABORT,
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
        network.send(msg);
    }
}
