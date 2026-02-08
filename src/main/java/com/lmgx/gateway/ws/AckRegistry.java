package com.lmgx.gateway.ws;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class AckRegistry {
  private final ConcurrentHashMap<String, CompletableFuture<Void>> inflight = new ConcurrentHashMap<>();

  public String newId(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }

  public CompletableFuture<Void> register(String ackId) {
    CompletableFuture<Void> f = new CompletableFuture<>();
    inflight.put(ackId, f);
    return f;
  }

  public void ack(String ackId) {
    CompletableFuture<Void> f = inflight.remove(ackId);
    if (f != null) f.complete(null);
  }
}
