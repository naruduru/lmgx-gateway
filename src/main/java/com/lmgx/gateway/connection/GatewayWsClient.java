package com.lmgx.gateway.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class GatewayWsClient {

  private static final Logger log = LoggerFactory.getLogger(GatewayWsClient.class);

  private final ObjectMapper om = new ObjectMapper();
  private final StandardWebSocketClient client;
  private final IncomingMessageDispatcher dispatcher;
  private final ExecutorService connector = Executors.newSingleThreadExecutor(r -> {
    Thread t = new Thread(r, "gw-ws-connector");
    t.setDaemon(true);
    return t;
  });
  private final AtomicBoolean connecting = new AtomicBoolean(false);
  private final AtomicInteger connectFailures = new AtomicInteger(0);
  private final AtomicReference<CompletableFuture<Void>> pendingHeartbeat = new AtomicReference<>();
  private volatile long nextConnectAllowedAt = 0L;

  private volatile WebSocketSession chat;   // type C
  private volatile WebSocketSession email;  // type I
  private volatile String currentUrl;
  private volatile boolean lastPingOk = false;
  private volatile long lastPingAt = 0L;
  private final AtomicInteger pingFailures = new AtomicInteger(0);

  private static final long PING_STALE_MS = 5000;
  private static final int PING_FAIL_THRESHOLD = 3;
  private static final int DEFAULT_HB_PERIOD_SEC = 10;
  private static final int DEFAULT_HA_STATE = 1;

  private volatile int hbPeriodSec = DEFAULT_HB_PERIOD_SEC;
  private volatile int haState = DEFAULT_HA_STATE;
  private final long ackTimeoutMs;

  public GatewayWsClient(IncomingMessageDispatcher dispatcher, StandardWebSocketClient client, long ackTimeoutMs) {
    this.dispatcher = dispatcher;
    this.client = Objects.requireNonNull(client, "StandardWebSocketClient must not be null");
    this.ackTimeoutMs = Math.max(1, ackTimeoutMs);
  }

  public void connect(String wsUrl) {
    connectInternal(wsUrl, false);
  }

  public void connectForce(String wsUrl) {
    connectInternal(wsUrl, true);
  }

  private void connectInternal(String wsUrl, boolean force) {
    if (wsUrl == null || wsUrl.isBlank()) {
      return;
    }
    this.currentUrl = wsUrl;
    long now = System.currentTimeMillis();
    if (!force && now < nextConnectAllowedAt) {
      return;
    }
    if (force) {
      nextConnectAllowedAt = 0L;
      connectFailures.set(0);
    }
    if (!connecting.compareAndSet(false, true)) {
      return;
    }
    connector.submit(() -> {
      synchronized (this) {
        closeQuiet(chat);
        closeQuiet(email);
        pendingHeartbeat.set(null);
        try {
          log.info("connect: {}", wsUrl);
          chat = open(wsUrl, "C");
          email = open(wsUrl, "I");
          connectFailures.set(0);
          nextConnectAllowedAt = 0L;
        } catch (Exception e) {
          chat = null;
          email = null;
          int failures = connectFailures.incrementAndGet();
          long backoff = calcBackoffMs(failures);
          nextConnectAllowedAt = System.currentTimeMillis() + backoff;
          log.warn("connect failed: {}", describe(e), e);
        } finally {
          connecting.set(false);
        }
      }
    });
  }

  private long calcBackoffMs(int failures) {
    int capped = Math.min(failures, 9);
    return 1000L;
  }

  public String currentUrl() { return currentUrl; }

  public long lastPingAt() { return lastPingAt; }
  public boolean lastPingOk() { return lastPingOk; }
  public int pingFailures() { return pingFailures.get(); }
  public boolean isConnecting() { return connecting.get(); }
  public String readinessDebug() {
    long age = lastPingAt <= 0 ? -1 : (System.currentTimeMillis() - lastPingAt);
    return "chatOpen=" + isChatOpen()
        + ", emailOpen=" + isEmailOpen()
        + ", connecting=" + isConnecting()
        + ", currentUrl=" + currentUrl
        + ", lastPingOk=" + lastPingOk
        + ", pingFailures=" + pingFailures.get()
        + ", pingAgeMs=" + age
        + ", nextConnectAllowedInMs=" + Math.max(0L, nextConnectAllowedAt - System.currentTimeMillis());
  }

  public boolean isChatOpen() { return chat != null && chat.isOpen(); }
  public boolean isEmailOpen() { return email != null && email.isOpen(); }
  public boolean isReady() { return isChatOpen() && isEmailOpen(); }
  public boolean isPingHealthy() {
    long age = System.currentTimeMillis() - lastPingAt;
    if (age > PING_STALE_MS) {
      return false;
    }
    return lastPingOk || pingFailures.get() < PING_FAIL_THRESHOLD;
  }
  public boolean isHealthy() { return isChatOpen() && isEmailOpen() && isPingHealthy(); }

  public void disconnectAll() {
    closeQuiet(chat);
    closeQuiet(email);
    chat = null;
    email = null;
    pendingHeartbeat.set(null);
  }

  public boolean pingChat() {
    long now = System.currentTimeMillis();
    try {
      if (chat == null || !chat.isOpen()) {
        lastPingOk = false;
        lastPingAt = now;
        pingFailures.incrementAndGet();
        return false;
      }

      CompletableFuture<Void> hb = new CompletableFuture<>();
      if (!pendingHeartbeat.compareAndSet(null, hb)) {
        log.debug("heartbeat already pending");
        return false;
      }
      log.debug("hb send: command=3, haState={}, url={}", haState, currentUrl);
      if (!sendJson(chat, Map.of(
          "Command", "3",
          "HaState", haState
      ))) {
        throw new IllegalStateException("session closed while sending heartbeat");
      }
      log.debug("hb sent: command=3, url={}", currentUrl);
      hb.get(ackTimeoutMs, TimeUnit.MILLISECONDS);
      lastPingOk = true;
      lastPingAt = System.currentTimeMillis();
      pingFailures.set(0);
      return true;
    } catch (TimeoutException e) {
      log.warn("heartbeat timeout waiting command=4 after {}ms", ackTimeoutMs);
      lastPingOk = false;
      lastPingAt = System.currentTimeMillis();
      pingFailures.incrementAndGet();
      return false;
    } catch (Exception e) {
      log.debug("pingChat failed: {}", e.getMessage());
      lastPingOk = false;
      lastPingAt = System.currentTimeMillis();
      pingFailures.incrementAndGet();
      return false;
    } finally {
      pendingHeartbeat.set(null);
    }
  }

  public String sendChat(Map<String, Object> payload) throws Exception {
    return send(chat, payload);
  }

  public String sendEmail(Map<String, Object> payload) throws Exception {
    return send(email, payload);
  }

  private String send(WebSocketSession s, Map<String, Object> payload) throws Exception {
    if (s == null || !s.isOpen()) throw new IllegalStateException("ws not open");
    if (payload == null) throw new IllegalArgumentException("payload is required");

    Map<String, Object> data = new LinkedHashMap<>(payload);
    Object command = data.get("Command");
    if (command == null) {
      command = data.remove("command");
    }
    data.remove("cmd");
    if (command == null) {
      throw new IllegalArgumentException("payload.Command is required");
    }
    data.put("Command", String.valueOf(command));

    log.debug("msg send: command={}, url={}", data.get("Command"), currentUrl);
    synchronized (s) {
      s.sendMessage(new TextMessage(om.writeValueAsString(data)));
    }
    log.debug("msg sent: command={}, url={}", data.get("Command"), currentUrl);
    return "REQ-" + UUID.randomUUID();
  }

  private WebSocketSession open(String url, String type) {
    CompletableFuture<Void> initDone = new CompletableFuture<>();
    MessageSender.Channel channel = "I".equals(type) ? MessageSender.Channel.EMAIL : MessageSender.Channel.CHAT;

    WebSocketHandler h = new TextWebSocketHandler() {
      @Override
      public void afterConnectionEstablished(WebSocketSession session) {
        log.info("ws established: type={}, sessionId={}, url={}", type, session.getId(), url);
      }

      @Override
      public void afterConnectionClosed(WebSocketSession session, org.springframework.web.socket.CloseStatus status) {
        log.warn("ws closed: type={}, sessionId={}, url={}, code={}, reason={}, readyState={}",
            type, session.getId(), url, status.getCode(), status.getReason(), readinessDebug());
      }

      @Override
      public void handleTransportError(WebSocketSession session, Throwable exception) {
        String sessionId = session != null ? session.getId() : "null";
        log.warn("ws transport error: type={}, sessionId={}, url={}, cause={}, readyState={}",
            type, sessionId, url, describe(exception), readinessDebug(), exception);
      }

      @Override
      protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
          String p = message.getPayload();
          @SuppressWarnings("unchecked")
          Map<String, Object> msg = om.readValue(p, Map.class);

          Object command = commandOf(msg);
          log.debug("recv: type={}, command={}, payload={}", type, command, msg);
          if (isInitCommand(command)) {
            try {
              hbPeriodSec = readInt(msg, "HBPeriod", DEFAULT_HB_PERIOD_SEC);
              haState = readInt(msg, "HaState", DEFAULT_HA_STATE);
              log.debug("init recv: hbPeriodSec={}, haState={}, type={}", hbPeriodSec, haState, type);
              if (!sendJson(session, Map.of(
                  "Command", "2",
                  "HostKind", hostKindOf(type),
                  "HBPeriod", hbPeriodSec,
                  "HaState", haState,
                  "ResultCode", 1
              ))) {
                throw new IllegalStateException("session closed while sending init response");
              }
              log.debug("init sent: command=2, type={}, hbPeriodSec={}, haState={}", type, hbPeriodSec, haState);
              initDone.complete(null);
            } catch (Exception e) {
              initDone.completeExceptionally(e);
            }
            return;
          }

          if (isHeartbeatAckCommand(command)) {
            int ackHaState = readInt(msg, "HaState", haState);
            Object nodeRole1 = msg.getOrDefault("NodeRole1", msg.get("nodeRole1"));
            Object nodeRole2 = msg.getOrDefault("NodeRole2", msg.get("nodeRole2"));
            log.debug("heartbeat ack: haState={}, nodeRole1={}, nodeRole2={}", ackHaState, nodeRole1, nodeRole2);
            CompletableFuture<Void> hb = pendingHeartbeat.getAndSet(null);
            if (hb != null && !hb.isDone()) {
              hb.complete(null);
            }
            return;
          }

          if (dispatcher != null) {
            dispatcher.dispatch(channel, msg);
          }
        } catch (Exception e) {
          log.debug("ws message handling error: {}", e.getMessage());
        }
      }
    };

    try {
      WebSocketSession session = client.execute(h, url).get(1, TimeUnit.SECONDS);
      initDone.get(ackTimeoutMs, TimeUnit.MILLISECONDS);
      log.debug("open ok: url={}, type={}", url, type);
      return session;
    } catch (Exception e) {
      log.warn("open failed: url={}, type={}, cause={}", url, type, describe(e), e);
      throw new RuntimeException("handshake/init fail: " + url + " type=" + type + " cause=" + describe(e), e);
    }
  }

  private boolean sendJson(WebSocketSession session, Map<String, Object> payload) {
    if (session == null || !session.isOpen()) {
      return false;
    }
    try {
      synchronized (session) {
        if (!session.isOpen()) {
          return false;
        }
        session.sendMessage(new TextMessage(om.writeValueAsString(payload)));
      }
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static int readInt(Map<String, Object> msg, String key, int defaultValue) {
    Object v = msg.get(key);
    if (v == null) {
      String lower = Character.toLowerCase(key.charAt(0)) + key.substring(1);
      v = msg.get(lower);
    }
    if (v == null) return defaultValue;
    try {
      if (v instanceof Number n) {
        return n.intValue();
      }
      return Integer.parseInt(String.valueOf(v));
    } catch (Exception e) {
      return defaultValue;
    }
  }

  private static Integer hostKindOf(String type) {
    return "I".equals(type) ? 2 : 1;
  }

  private static Object commandOf(Map<String, Object> msg) {
    Object c = msg.get("command");
    return c != null ? c : msg.get("Command");
  }

  private static boolean isInitCommand(Object command) {
    return Integer.valueOf(1).equals(command) || "1".equals(String.valueOf(command));
  }

  private static boolean isHeartbeatAckCommand(Object command) {
    return Integer.valueOf(4).equals(command) || "4".equals(String.valueOf(command));
  }

  private static String describe(Throwable t) {
    Throwable cur = t;
    while (cur.getCause() != null) {
      cur = cur.getCause();
    }
    String msg = cur.getMessage();
    if (msg == null || msg.isBlank()) {
      return cur.getClass().getName();
    }
    return cur.getClass().getName() + ": " + msg;
  }

  private static void closeQuiet(WebSocketSession s) {
    if (s != null && s.isOpen()) {
      try { s.close(); } catch (Exception ignore) {}
    }
  }
}
