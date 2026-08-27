package ufc.victor.db.mariadb;

import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.participant.TransactionalResource;

import java.util.concurrent.atomic.AtomicBoolean;

/** Tracks completion after the underlying database commit or rollback returns. */
public final class CompletionTrackingTransactionalResource implements TransactionalResource {

    private final TransactionalResource delegate;
    private final AtomicBoolean resolved = new AtomicBoolean();

    public CompletionTrackingTransactionalResource(TransactionalResource delegate) {
        if (delegate == null) throw new IllegalArgumentException("delegate must not be null");
        this.delegate = delegate;
    }

    @Override
    public boolean prepare(TransactionId txId) {
        return delegate.prepare(txId);
    }

    @Override
    public void commit(TransactionId txId) {
        delegate.commit(txId);
        resolved.set(true);
    }

    @Override
    public void abort(TransactionId txId) {
        delegate.abort(txId);
        resolved.set(true);
    }

    public boolean isResolved() {
        return resolved.get();
    }
}
