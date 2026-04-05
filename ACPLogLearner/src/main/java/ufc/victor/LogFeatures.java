package ufc.victor;

/**
 * Represents the current "State" of the distributed system
 * just before a new transaction begins.
 */
public record LogFeatures(
        double averageNetworkLatencyMs,
        double maxNetworkLatencyMs,
        double averageDiskIoTimeMs,
        double maxDiskIoTimeMs,
        double abortRate,
        int participantCount
) {

}
