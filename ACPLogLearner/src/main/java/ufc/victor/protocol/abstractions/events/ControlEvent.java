package ufc.victor.protocol.abstractions.events;

// Event Type 3: Simulation Control (Stop/Crash)
public record ControlEvent(Type type) implements Event {
    public enum Type { SHUTDOWN }
}