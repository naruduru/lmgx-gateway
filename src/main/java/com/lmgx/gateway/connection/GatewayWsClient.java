package com.lmgx.gateway.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;

public class GatewayWsClient {

  private static final Logger log = LoggerFactory.getLogger(GatewayWsClient.class);

  private final ObjectMapper om = new ObjectMapper();
  private final StandardWebSocketClient client = new StandardWebSocketClient();
  private final AckRegistry ack = new AckRegistry();
  private final IncomingMessageDispatcher dispatcher;
  private final ExecutorService connector = Executors.newSingleThreadExecutor(r -> {
    Thread t = new Thread(r, "gw-ws-connector");
    t.setDaemon(true);
    return t;
  });
  private final AtomicBoolean connecting = new AtomicBoolean(false);
  private final AtomicInteger connectFailures = new AtomicInteger(0);
  private volatile long nextConnectAllowedAt = 0L;

  private volatile WebSocketSession chat;   // type C
  private volatile WebSocketSession email;  // type I
  private volatile String currentUrl;
  private volatile boolean lastPingOk = false;
  private volatile long lastPingAt = 0L;
  private final AtomicInteger pingFailures = new AtomicInteger(0);

  private static final long ACK_TIMEOUT_MS = 1000;
  private static final long PING_STALE_MS = 5000;
  private static final int PING_FAIL_THRESHOLD = 3;

  public GatewayWsClient(IncomingMessageDispatcher dispatcher) {
    this.dispatcher = dispatcher;
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
          log.warn("connect failed: {}", e.getMessage());
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

      String id = ack.newId("H");
      CompletableFuture<Void> f = ack.register(id);

      synchronized (chat) {
        chat.sendMessage(new TextMessage(om.writeValueAsString(Map.of(
            "command", "PING",
            "ackId", id
        ))));
      }

      f.get(ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      lastPingOk = true;
      lastPingAt = System.currentTimeMillis();
      pingFailures.set(0);
      return true;
    } catch (Exception e) {
      log.debug("pingChat failed: {}", e.getMessage());
      lastPingOk = false;
      lastPingAt = System.currentTimeMillis();
      pingFailures.incrementAndGet();
      return false;
    }
  }

  public String sendChat(Map<String, Object> payload) throws Exception {
    return send(chat, "CHAT_SEND", payload);
  }

  public String sendEmail(Map<String, Object> payload) throws Exception {
    return send(email, "EMAIL_SEND", payload);
  }

  private String send(WebSocketSession s, String cmd, Map<String, Object> payload) throws Exception {
    if (s == null || !s.isOpen()) throw new IllegalStateException("ws not open");

    String id = ack.newId("B");
    CompletableFuture<Void> f = ack.register(id);

    synchronized (s) {
      s.sendMessage(new TextMessage(om.writeValueAsString(Map.of(
          "command", cmd,
          "ackId", id,
          "payload", payload
      ))));
    }

    f.get(ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    return id;
  }

  private WebSocketSession open(String url, String type) {
    CompletableFuture<Void> initDone = new CompletableFuture<>();
    AtomicReference<String> initAckRef = new AtomicReference<>();
    MessageSender.Channel channel = "I".equals(type) ? MessageSender.Channel.EMAIL : MessageSender.Channel.CHAT;

    WebSocketHandler h = new TextWebSocketHandler() {
      @Override
      protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String p = message.getPayload();

        if (p.contains("\"command\":1")) {
          String initAckId = ack.newId("I");
          initAckRef.set(initAckId);
          ack.register(initAckId);

          synchronized (session) {
            session.sendMessage(new TextMessage(om.writeValueAsString(Map.of(
                "command", 2,
                "type", type,
                "ackId", initAckId
            ))));
          }
          return;
        }

        if (p.contains("\"command\":\"ACK\"") && p.contains("\"ackId\"")) {
          String ackId = pick(p, "\"ackId\":\"", "\"");
          if (ackId != null) {
            ack.ack(ackId);
            if (ackId.equals(initAckRef.get())) {
              initDone.complete(null);
            }
          }
          return;
        }

        if (dispatcher != null) {
          try {
            @SuppressWarnings("unchecked")
            Map<String, Object> msg = om.readValue(p, Map.class);
            dispatcher.dispatch(channel, msg);
          } catch (Exception ignore) {
          }
        }
      }
    };

    try {
      WebSocketSession session = client.execute(h, url).get(1, TimeUnit.SECONDS);
      initDone.get(ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      log.debug("open ok: url={}, type={}", url, type);
      return session;
    } catch (Exception e) {
      log.warn("open failed: url={}, type={}", url, type, e);
      throw new RuntimeException("handshake/init fail: " + url + " type=" + type, e);
    }
  }

  private static void closeQuiet(WebSocketSession s) {
    if (s != null && s.isOpen()) {
      try { s.close(); } catch (Exception ignore) {}
    }
  }

  private static String pick(String src, String prefix, String until) {
    int s = src.indexOf(prefix);
    if (s < 0) return null;
    s += prefix.length();
    int e = src.indexOf(until, s);
    if (e < 0) return null;
    return src.substring(s, e);
  }
}
