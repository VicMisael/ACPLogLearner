package ufc.victor.localenv;

import ufc.victor.protocol.commom.Timer;

import java.util.function.Consumer;

public final class LocalTimer implements Timer {

    private final Consumer<Void> onTimeout;
    private boolean armed = false;

    public LocalTimer(Runnable onTimeout) {
        this.onTimeout = v -> onTimeout.run();
    }

    @Override
    public void set() {
        armed = true;
    }

    @Override
    public void reset() {
        armed = false;
    }

    // Manually triggered by the test harness / network
    public void trigger() {
        if (!armed) return;
        armed = false;
        onTimeout.accept(null);
    }
}

