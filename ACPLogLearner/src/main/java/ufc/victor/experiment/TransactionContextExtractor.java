package ufc.victor.experiment;

@FunctionalInterface
public interface TransactionContextExtractor<S> {
    TransactionContext extract(S specification);
}
