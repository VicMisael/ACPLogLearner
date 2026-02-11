package ufc.victor.localenv;

import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.participant.TransactionalResource;

public class AbortResource implements TransactionalResource {
    @Override
    public boolean prepare(TransactionId txId) {
        return false;
    }

    @Override
    public void commit(TransactionId txId) {
        System.out.println(this.getClass().getName() + "commited" + txId);
    }

    @Override
    public void abort(TransactionId txId) {
        System.out.println(this.getClass().getName() + "aborted" + txId);
    }
}
