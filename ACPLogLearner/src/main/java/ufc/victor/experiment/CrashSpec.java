package ufc.victor.experiment;

public record CrashSpec(
        String nodeName,
        CrashTrigger trigger,
        long downtimeMs
) {
    public CrashSpec {
        if (nodeName == null || nodeName.isBlank()) {
            throw new IllegalArgumentException("nodeName must not be blank");
        }
        if (trigger == null) {
            throw new IllegalArgumentException("trigger must not be null");
        }
        if (downtimeMs < 1L) {
            throw new IllegalArgumentException("downtimeMs must be >= 1");
        }
    }
}
