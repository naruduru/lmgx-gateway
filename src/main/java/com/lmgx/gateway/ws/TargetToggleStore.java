package com.lmgx.gateway.ws;

import java.util.concurrent.ConcurrentHashMap;

public class TargetToggleStore {
  private final ConcurrentHashMap<String, Boolean> ackEnabled = new ConcurrentHashMap<>();

  public void setAckEnabled(String targetId, boolean enabled) {
    ackEnabled.put(targetId, enabled);
  }

  public boolean isAckEnabled(String targetId) {
    return ackEnabled.getOrDefault(targetId, true);
  }
}
