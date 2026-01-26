package ufc.victor;

import ufc.victor.protocol.commom.TransactionId;
import ufc.victor.protocol.participant.TransactionalResource;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    private static class FakeResources implements TransactionalResource{

        @Override
        public boolean prepare(TransactionId txId) {
            return true;
        }

        @Override
        public void commit(TransactionId txId) {
            System.out.println("commited"+txId);
        }

        @Override
        public void abort(TransactionId txId) {
            System.out.println("aborted"+txId);
        }
    }


    public static void main(String[] args) {

    }
}