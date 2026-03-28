package ufc.victor;

/**
 * Represents the current "State" of the distributed system
 * just before a new transaction begins.
 */
public record LogFeatures(
        double networkLatencyMs,
        double diskIoTimeMs,
        double abortRate,
        int participantCount
) {

}