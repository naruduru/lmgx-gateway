package com.lmgx.gateway.instance;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class InstanceControlStore {
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicInteger haState = new AtomicInteger(1);

    public boolean isPaused() {
        return paused.get();
    }

    public void setPaused(boolean value) {
        paused.set(value);
    }

    public int getHaState() {
        return haState.get();
    }

    public void setHaState(int value) {
        haState.set(normalizeHaState(value));
    }

    private static int normalizeHaState(int value) {
        return value == 2 ? 2 : 1;
    }
}
