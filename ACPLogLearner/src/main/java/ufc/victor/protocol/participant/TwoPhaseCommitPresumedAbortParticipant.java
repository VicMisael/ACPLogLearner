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
import ufc.victor.protocol.participant.log.ParticipantPhase;
import ufc.victor.protocol.participant.log.ParticipantLogRecord;
import ufc.victor.protocol.participant.log.ParticipantLogRecordType;

import java.time.Instant;

import static ufc.victor.protocol.commom.message.MessageType.VOTE_ABORT;
import static ufc.victor.protocol.commom.message.MessageType.VOTE_COMMIT;

public final class TwoPhaseCommitPresumedAbortParticipant implements IParticipant {

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

    public TwoPhaseCommitPresumedAbortParticipant(
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
                // PrA Rule: If we crashed before logging READY, we can unilaterally abort.
                // The coordinator will presume abort anyway if it doesn't hear from us.
                Instant phaseStart = Instant.now();
                voteAbort(phaseStart, ParticipantPhase.TERMINATION, phaseStart);
            }
            case READY -> {
                // We voted YES but crashed before receiving the decision.
                // We are blocked. We must ask the coordinator.
                Terminate();
            }
            default -> {
                // COMMIT or ABORT: The transaction is already finished locally.
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
            case PREPARE_2PC_PAB -> onPrepare(msg);
            case GLOBAL_ABORT -> onGlobalAbort(msg);
            case GLOBAL_COMMIT -> onGlobalCommit(msg);
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
    // Valduriez handlers (Adapted for PrA)
    // =========================================================

    private void onPrepare(Message msg) {
        if (getState() != State.INIT) return;

        preparePhaseStartedAt = Instant.now();
        if (resource.prepare(txId)) {
            voteCommit(msg.timestamp());
        } else {
            Instant phaseStart = preparePhaseStartedAt;
            voteAbort(msg.timestamp(), ParticipantPhase.PREPARE, phaseStart);
        }
    }

    private void voteCommit(Instant triggerTimestamp) {
        // MUST be force-written to disk
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

    private void voteAbort(Instant triggerTimestamp, ParticipantPhase phase, Instant phaseStartedAt) {
        // PrA Optimization: This log does NOT need to be force-written.
        log.write(ParticipantLogRecord.of(
                txId,
                participantId.id,
                ParticipantLogRecordType.ABORT,
                phase,
                triggerTimestamp,
                phaseStartedAt
        ));

        resource.abort(txId);
        send(VOTE_ABORT);
        // Protocol finished locally. No timer needed.
    }

    private void onGlobalAbort(Message msg) {
        if (getState() == State.ABORT || getState() == State.INIT) return;

        // PrA Optimization: This log does NOT need to be force-written.
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

        // PrA RULE: Do NOT send ACK. The coordinator does not expect it and has already forgotten the transaction.
        timer.reset();
    }

    private void onGlobalCommit(Message msg) {
        if (getState() == State.COMMIT) return;

        // MUST be force-written to disk
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

        // PrA RULE: We MUST send ACK for Global-Commit.
        send(MessageType.ACK);
        timer.reset();
    }

    private void Terminate() {
        if (getState() == State.INIT) {
            // Timed out waiting for PREPARE. Safe to unilaterally abort.
            Instant phaseStart = Instant.now();
            voteAbort(phaseStart, ParticipantPhase.TERMINATION, phaseStart);
        } else if (getState() == State.READY) {
            // Send VOTE_COMMIT again as a way to query the coordinator (or use a STATUS_REQUEST).
            send(VOTE_COMMIT);

            // CRITICAL FIX: Restart the timer! If we reset it, we stop waiting.
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
