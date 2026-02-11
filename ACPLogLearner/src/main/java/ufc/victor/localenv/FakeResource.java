package ufc.victor.localenv;

import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.participant.TransactionalResource;

public class FakeResource implements TransactionalResource {

    @Override
    public boolean prepare(TransactionId txId) {
//            sleep(3500);
        return true;
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
