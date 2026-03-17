package com.lmgx.gateway.message;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Component
public class UqRequestTracker {
  private static final Logger log = LoggerFactory.getLogger(UqRequestTracker.class);

  private final UqMessageService messageService;
  private final long timeoutMs;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "uq-request-timeout");
    t.setDaemon(true);
    return t;
  });
  private final ConcurrentMap<String, TrackedRequest> requests = new ConcurrentHashMap<>();

  public UqRequestTracker(
      UqMessageService messageService,
      @Value("${gateway.uq.response-timeout-ms:300000}") long timeoutMs
  ) {
    this.messageService = messageService;
    this.timeoutMs = Math.max(1L, timeoutMs);
  }

  public void arm(int command, Map<String, Object> payload) {
    String ucid = ucidOf(payload);
    if (ucid == null) {
      return;
    }

    Map<String, Object> snapshot = new LinkedHashMap<>();
    if (payload != null) {
      snapshot.putAll(payload);
    }
    snapshot.put("Command", command);

    TrackedRequest next = new TrackedRequest(command, ucid, snapshot);
    TrackedRequest prev = requests.put(ucid, next);
    if (prev != null) {
      prev.cancel();
    }

    ScheduledFuture<?> future = scheduler.schedule(() -> onTimeout(next), timeoutMs, TimeUnit.MILLISECONDS);
    next.future = future;
    log.debug("uq request armed: ucid={}, command={}, timeoutMs={}", ucid, command, timeoutMs);
  }

  public void complete(Map<String, Object> message) {
    String ucid = ucidOf(message);
    if (ucid == null) {
      return;
    }
    TrackedRequest tracked = requests.remove(ucid);
    if (tracked == null) {
      return;
    }
    tracked.cancel();
    log.debug("uq request completed: ucid={}, requestCommand={}, responseCommand={}",
        ucid, tracked.command, commandOf(message));
  }

  private void onTimeout(TrackedRequest tracked) {
    if (!requests.remove(tracked.ucid, tracked)) {
      return;
    }

    Map<String, Object> timeoutMessage = new LinkedHashMap<>(tracked.payload);
    timeoutMessage.put("TimedOut", true);
    timeoutMessage.put("TimeoutMs", timeoutMs);
    timeoutMessage.put("TimeoutCommand", tracked.command);
    log.warn("uq request timeout: ucid={}, command={}, timeoutMs={}", tracked.ucid, tracked.command, timeoutMs);
    try {
      messageService.onRequestTimeout(timeoutMessage);
    } catch (Exception e) {
      log.warn("uq timeout handler failed: ucid={}, command={}, cause={}",
          tracked.ucid, tracked.command, e.getMessage(), e);
    }
  }

  private static String ucidOf(Map<String, Object> message) {
    if (message == null) {
      return null;
    }
    Object ucid = message.get("UCID");
    if (ucid == null) {
      ucid = message.get("ucid");
    }
    if (ucid == null) {
      return null;
    }
    String value = String.valueOf(ucid).trim();
    return value.isEmpty() ? null : value;
  }

  private static Object commandOf(Map<String, Object> message) {
    if (message == null) {
      return null;
    }
    Object command = message.get("Command");
    return command != null ? command : message.get("command");
  }

  @PreDestroy
  public void shutdown() {
    scheduler.shutdownNow();
  }

  private static final class TrackedRequest {
    private final int command;
    private final String ucid;
    private final Map<String, Object> payload;
    private volatile ScheduledFuture<?> future;

    private TrackedRequest(int command, String ucid, Map<String, Object> payload) {
      this.command = command;
      this.ucid = ucid;
      this.payload = payload;
    }

    private void cancel() {
      ScheduledFuture<?> current = future;
      if (current != null) {
        current.cancel(false);
      }
    }
  }
}
