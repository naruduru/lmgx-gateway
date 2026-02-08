package com.lmgx.gateway.monitor;

import java.util.concurrent.atomic.AtomicBoolean;

public class InstanceControlStore {
    private final AtomicBoolean paused = new AtomicBoolean(false);

    public boolean isPaused() {
        return paused.get();
    }

    public void setPaused(boolean value) {
        paused.set(value);
    }
}
