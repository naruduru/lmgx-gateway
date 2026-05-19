package com.lmgx.gateway.message;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class SessionInFlightStore {
  private final ConcurrentMap<String, InFlight> inFlights = new ConcurrentHashMap<>();
  private final long ttlMs;

  public SessionInFlightStore(@Value("${gateway.uq.session-inflight-ttl-ms:300000}") long ttlMs) {
    this.ttlMs = Math.max(1000L, ttlMs);
  }

  public boolean tryAcquire(String sessionKey, String requestId) {
    if (sessionKey == null || sessionKey.isBlank()) {
      return true;
    }

    long now = System.currentTimeMillis();
    InFlight next = new InFlight(requestId, now);

    return inFlights.compute(sessionKey, (key, current) -> {
      if (current == null || isExpired(current, now)) {
        return next;
      }
      return current;
    }) == next;
  }

  public void release(String sessionKey) {
    if (sessionKey == null || sessionKey.isBlank()) {
      return;
    }
    inFlights.remove(sessionKey);
  }

  private boolean isExpired(InFlight current, long now) {
    return now - current.startedAt > ttlMs;
  }

  private record InFlight(String requestId, long startedAt) {
  }
}
