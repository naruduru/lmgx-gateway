package com.lmgx.gateway.message;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
  private final SessionRequestKeyResolver sessionRequestKeyResolver;
  private final long timeoutMs;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "uq-request-timeout");
    t.setDaemon(true);
    return t;
  });
  private final ConcurrentMap<String, TrackedRequest> requests = new ConcurrentHashMap<>();

  public UqRequestTracker(
      UqMessageService messageService,
      SessionRequestKeyResolver sessionRequestKeyResolver,
      @Value("${gateway.uq.response-timeout-ms:300000}") long timeoutMs
  ) {
    this.messageService = messageService;
    this.sessionRequestKeyResolver = sessionRequestKeyResolver;
    this.timeoutMs = Math.max(1L, timeoutMs);
  }

  public CompletableFuture<Map<String, Object>> arm(int command, Map<String, Object> payload) {
    String sessionKey = sessionRequestKeyResolver.resolve(payload);
    if (sessionKey == null) {
      return CompletableFuture.completedFuture(Map.of());
    }
    return arm(command, payload, sessionKey);
  }

  public CompletableFuture<Map<String, Object>> armGlobal(int command, Map<String, Object> payload) {
    return arm(command, payload, UqSerializationKeys.lockKey(command));
  }

  private CompletableFuture<Map<String, Object>> arm(int command, Map<String, Object> payload, String sessionKey) {
    Map<String, Object> snapshot = new LinkedHashMap<>();
    if (payload != null) {
      snapshot.putAll(payload);
    }
    snapshot.put("Command", command);

    String expectedResponseCommand = expectedResponseCommand(command);
    String key = requestKey(sessionKey, expectedResponseCommand);
    TrackedRequest next = new TrackedRequest(command, expectedResponseCommand, sessionKey, snapshot);
    TrackedRequest prev = requests.put(key, next);
    if (prev != null) {
      prev.cancel();
    }

    ScheduledFuture<?> future = scheduler.schedule(() -> onTimeout(next), timeoutMs, TimeUnit.MILLISECONDS);
    next.future = future;
    log.debug("uq request armed: sessionKey={}, command={}, expectedResponseCommand={}, timeoutMs={}",
        sessionKey, command, expectedResponseCommand, timeoutMs);
    return next.response;
  }

  public void complete(Map<String, Object> message) {
    String sessionKey = sessionRequestKeyResolver.resolve(message);
    Object responseCommand = commandOf(message);
    String responseCommandText = stringOf(responseCommand);
    TrackedRequest tracked = null;
    if (sessionKey != null) {
      tracked = requests.remove(requestKey(sessionKey, responseCommandText));
    }
    if (tracked == null) {
      tracked = requests.remove(requestKey(globalResponseSessionKey(responseCommandText), responseCommandText));
    }
    if (tracked == null) {
      return;
    }
    tracked.cancel();
    tracked.response.complete(new LinkedHashMap<>(message));
    log.debug("uq request completed: sessionKey={}, requestCommand={}, responseCommand={}",
        sessionKey, tracked.command, responseCommand);
  }

  private void onTimeout(TrackedRequest tracked) {
    if (!requests.remove(requestKey(tracked.sessionKey, tracked.expectedResponseCommand), tracked)) {
      return;
    }

    Map<String, Object> timeoutMessage = new LinkedHashMap<>(tracked.payload);
    timeoutMessage.put("TimedOut", true);
    timeoutMessage.put("TimeoutMs", timeoutMs);
    timeoutMessage.put("TimeoutCommand", tracked.command);
    timeoutMessage.put("ExpectedResponseCommand", tracked.expectedResponseCommand);
    tracked.response.complete(timeoutMessage);
    log.warn("uq request timeout: sessionKey={}, command={}, expectedResponseCommand={}, timeoutMs={}",
        tracked.sessionKey, tracked.command, tracked.expectedResponseCommand, timeoutMs);
    try {
      messageService.onRequestTimeout(timeoutMessage);
    } catch (Exception e) {
      log.warn("uq timeout handler failed: sessionKey={}, command={}, cause={}",
          tracked.sessionKey, tracked.command, e.getMessage(), e);
    }
  }

  private static Object commandOf(Map<String, Object> message) {
    if (message == null) {
      return null;
    }
    Object command = message.get("Command");
    return command != null ? command : message.get("command");
  }

  private static String expectedResponseCommand(int requestCommand) {
    return switch (requestCommand) {
      case 1537 -> "1538";
      case 1539 -> "1540";
      case 1541 -> "1542";
      case 1543 -> "1544";
      case 1545 -> "1546";
      default -> String.valueOf(requestCommand + 1);
    };
  }

  private static String requestKey(String sessionKey, String responseCommand) {
    return sessionKey + ":" + responseCommand;
  }

  private static String globalResponseSessionKey(String responseCommand) {
    if (responseCommand == null) {
      return null;
    }
    try {
      return UqSerializationKeys.lockKey(Integer.parseInt(responseCommand) - 1);
    } catch (Exception e) {
      return null;
    }
  }

  private static String stringOf(Object value) {
    if (value == null) {
      return null;
    }
    String text = String.valueOf(value).trim();
    return text.isEmpty() ? null : text;
  }

  @PreDestroy
  public void shutdown() {
    scheduler.shutdownNow();
  }

  public long timeoutMs() {
    return timeoutMs;
  }

  private static final class TrackedRequest {
    private final int command;
    private final String expectedResponseCommand;
    private final String sessionKey;
    private final Map<String, Object> payload;
    private final CompletableFuture<Map<String, Object>> response = new CompletableFuture<>();
    private volatile ScheduledFuture<?> future;

    private TrackedRequest(int command, String expectedResponseCommand, String sessionKey, Map<String, Object> payload) {
      this.command = command;
      this.expectedResponseCommand = expectedResponseCommand;
      this.sessionKey = sessionKey;
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
