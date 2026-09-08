package ufc.victor.telemetry.redo;

/** Signals that a database node could not provide a redo metrics snapshot. */
public final class RedoMetricsCollectionException extends RuntimeException {

    public RedoMetricsCollectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
