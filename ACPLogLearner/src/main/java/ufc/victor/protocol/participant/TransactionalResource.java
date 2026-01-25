package ufc.victor.protocol.participant;

import ufc.victor.protocol.commom.TransactionId;

public interface TransactionalResource {
    /**
     * Phase 1:
     * Check if the resource can commit.
     * Must be durable if it returns true.
     */
    boolean prepare(TransactionId txId);

    /**
     * Phase 2 (commit path)
     * Must be idempotent.
     */
    void commit(TransactionId txId);

    /**
     * Phase 2 (abort path)
     * Must be idempotent.
     */
    void abort(TransactionId txId);
}
