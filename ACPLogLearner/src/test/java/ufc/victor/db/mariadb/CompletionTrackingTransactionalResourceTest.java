package ufc.victor.db.mariadb;

import org.junit.jupiter.api.Test;
import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.participant.TransactionalResource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompletionTrackingTransactionalResourceTest {

    @Test
    void resolvesOnlyAfterCommitReturnsSuccessfully() {
        CompletionTrackingTransactionalResource resource =
                new CompletionTrackingTransactionalResource(new StubResource(false));

        resource.commit(TransactionId.newTransaction());

        assertTrue(resource.isResolved());
    }

    @Test
    void doesNotResolveWhenDatabaseCompletionFails() {
        CompletionTrackingTransactionalResource resource =
                new CompletionTrackingTransactionalResource(new StubResource(true));

        assertThrows(IllegalStateException.class,
                () -> resource.commit(TransactionId.newTransaction()));
        assertFalse(resource.isResolved());
    }

    private static final class StubResource implements TransactionalResource {
        private final boolean fail;

        private StubResource(boolean fail) {
            this.fail = fail;
        }

        @Override
        public boolean prepare(TransactionId txId) {
            return true;
        }

        @Override
        public void commit(TransactionId txId) {
            if (fail) throw new IllegalStateException("commit failed");
        }

        @Override
        public void abort(TransactionId txId) {
            if (fail) throw new IllegalStateException("abort failed");
        }
    }
}
