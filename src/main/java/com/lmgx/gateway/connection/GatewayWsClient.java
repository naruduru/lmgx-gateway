package com.lmgx.gateway.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

  private final ConcurrentMap<String, WebSocketSession> chatSessions = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, WebSocketSession> emailSessions = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AtomicReference<CompletableFuture<Void>>> pendingChat = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AtomicReference<CompletableFuture<Void>>> pendingEmail = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Integer> haStates = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Long> nextConnectAllowedAt = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, AtomicInteger> connectFailures = new ConcurrentHashMap<>();
  private final java.util.Set<String> connectingChannels = ConcurrentHashMap.newKeySet();

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
    connectChat(wsUrl);
    connectEmail(wsUrl);
  }

  public void connectForce(String wsUrl) {
    connectChatForce(wsUrl);
    connectEmailForce(wsUrl);
  }

  public void connectChat(String wsUrl) {
    connectChannelInternal(wsUrl, MessageSender.Channel.CHAT, false);
  }

  public void connectEmail(String wsUrl) {
    connectChannelInternal(wsUrl, MessageSender.Channel.EMAIL, false);
  }

  public void connectChatForce(String wsUrl) {
    connectChannelInternal(wsUrl, MessageSender.Channel.CHAT, true);
  }

  public void connectEmailForce(String wsUrl) {
    connectChannelInternal(wsUrl, MessageSender.Channel.EMAIL, true);
  }

  public void connectAll(String... urls) {
    if (urls == null) {
      return;
    }
    for (String url : urls) {
      connect(url);
    }
  }

  private void connectChannelInternal(String wsUrl, MessageSender.Channel channel, boolean force) {
    if (wsUrl == null || wsUrl.isBlank() || channel == null) {
      return;
    }
    if (force || currentUrl == null || currentUrl.isBlank()) {
      this.currentUrl = wsUrl;
    }

    if (!force && isChannelOpen(wsUrl, channel)) {
      return;
    }

    String key = connectKey(wsUrl, channel);
    long now = System.currentTimeMillis();
    long allowedAt = nextConnectAllowedAt.getOrDefault(key, 0L);
    if (!force && now < allowedAt) {
      return;
    }
    if (!connectingChannels.add(key)) {
      return;
    }

    connector.submit(() -> {
      try {
        if (!force && isChannelOpen(wsUrl, channel)) {
          return;
        }

        WebSocketSession old = sessionOf(wsUrl, channel);
        if (force || old == null || !old.isOpen()) {
          closeQuiet(old);
          sessionMap(channel).remove(wsUrl, old);
        }

        try {
          log.info("connect: url={}, channel={}", wsUrl, channel);
          AtomicReference<CompletableFuture<Void>> pendingRef = pendingRefOf(wsUrl, channel);
          WebSocketSession opened = open(wsUrl, channelType(channel), pendingRef, channel);
          WebSocketSession previous = sessionMap(channel).put(wsUrl, opened);
          if (previous != null && previous != opened) {
            closeQuiet(previous);
          }
          connectFailures.computeIfAbsent(key, k -> new AtomicInteger()).set(0);
          nextConnectAllowedAt.put(key, 0L);
        } catch (Exception e) {
          int failures = connectFailures.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
          long backoff = calcBackoffMs(failures);
          nextConnectAllowedAt.put(key, System.currentTimeMillis() + backoff);
          log.warn("connect failed: url={}, channel={}, cause={}", wsUrl, channel, describe(e), e);
        }
      } finally {
        connectingChannels.remove(key);
      }
    });
  }

  private long calcBackoffMs(int failures) {
    return 1000L;
  }

  public String currentUrl() { return currentUrl; }

  public long lastPingAt() { return lastPingAt; }
  public boolean lastPingOk() { return lastPingOk; }
  public int pingFailures() { return pingFailures.get(); }
  public boolean isConnecting() { return !connectingChannels.isEmpty(); }

  public String readinessDebug() {
    long age = lastPingAt <= 0 ? -1 : (System.currentTimeMillis() - lastPingAt);
    return "chatOpen=" + isChatOpen()
        + ", emailOpen=" + isEmailOpen()
        + ", connecting=" + isConnecting()
        + ", currentUrl=" + currentUrl
        + ", currentHaState=" + haStateOf(currentUrl)
        + ", lastPingOk=" + lastPingOk
        + ", pingFailures=" + pingFailures.get()
        + ", pingAgeMs=" + age;
  }

  public boolean isChatOpen() { return isChatOpen(currentUrl); }
  public boolean isEmailOpen() { return isEmailOpen(currentUrl); }
  public boolean isReady() { return isReady(currentUrl); }
  public boolean isHealthy() { return isHealthy(currentUrl); }

  public boolean isChatOpen(String url) {
    return isChannelOpen(url, MessageSender.Channel.CHAT);
  }

  public boolean isEmailOpen(String url) {
    return isChannelOpen(url, MessageSender.Channel.EMAIL);
  }

  public boolean isReady(String url) {
    return isChatOpen(url) && isEmailOpen(url);
  }

  public boolean isHealthy(String url) {
    if (!isCommandRoutable(url)) {
      return false;
    }
    if (url != null && url.equals(currentUrl)) {
      return isPingHealthy();
    }
    return true;
  }

  public int haStateOf(String url) {
    if (url == null) {
      return DEFAULT_HA_STATE;
    }
    return haStates.getOrDefault(url, DEFAULT_HA_STATE);
  }

  public boolean isHaActive(String url) {
    return haStateOf(url) == 1;
  }

  public boolean isCommandRoutable(String url) {
    return isReady(url) && isHaActive(url);
  }

  public boolean isPingHealthy() {
    long age = System.currentTimeMillis() - lastPingAt;
    if (age > PING_STALE_MS) {
      return false;
    }
    return lastPingOk || pingFailures.get() < PING_FAIL_THRESHOLD;
  }

  public void disconnectAll() {
    for (WebSocketSession session : chatSessions.values()) {
      closeQuiet(session);
    }
    for (WebSocketSession session : emailSessions.values()) {
      closeQuiet(session);
    }
    chatSessions.clear();
    emailSessions.clear();
    haStates.clear();
    pendingChat.values().forEach(ref -> ref.set(null));
    pendingEmail.values().forEach(ref -> ref.set(null));
  }

  public void disconnectChat(String url) {
    disconnectChannel(url, MessageSender.Channel.CHAT);
  }

  public void disconnectEmail(String url) {
    disconnectChannel(url, MessageSender.Channel.EMAIL);
  }

  public boolean pingChat() {
    return pingChat(currentUrl);
  }

  public boolean pingEmail() {
    return pingEmail(currentUrl);
  }

  public boolean pingBoth() {
    return pingBoth(currentUrl);
  }

  public boolean pingChat(String url) {
    boolean ok = pingChannel(url, MessageSender.Channel.CHAT);
    updatePingStateFor(url, ok && isEmailOpen(url));
    return ok;
  }

  public boolean pingEmail(String url) {
    boolean ok = pingChannel(url, MessageSender.Channel.EMAIL);
    updatePingStateFor(url, ok && isChatOpen(url));
    return ok;
  }

  public boolean pingBoth(String url) {
    boolean chatOk = pingChannel(url, MessageSender.Channel.CHAT);
    boolean emailOk = pingChannel(url, MessageSender.Channel.EMAIL);
    boolean ok = chatOk && emailOk;
    updatePingStateFor(url, ok);
    log.debug("ping both: url={}, chatOk={}, emailOk={}, overallOk={}", url, chatOk, emailOk, ok);
    return ok;
  }

  public String sendChat(Map<String, Object> payload) throws Exception {
    return sendChat(currentUrl, payload);
  }

  public String sendEmail(Map<String, Object> payload) throws Exception {
    return sendEmail(currentUrl, payload);
  }

  public String sendChat(String url, Map<String, Object> payload) throws Exception {
    return send(sessionOf(url, MessageSender.Channel.CHAT), payload, url);
  }

  public String sendEmail(String url, Map<String, Object> payload) throws Exception {
    return send(sessionOf(url, MessageSender.Channel.EMAIL), payload, url);
  }

  private String send(WebSocketSession s, Map<String, Object> payload, String url) throws Exception {
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
    data.put("Command", normalizeCommand(command));

    log.debug("msg send: command={}, url={}", data.get("Command"), url);
    synchronized (s) {
      s.sendMessage(new TextMessage(om.writeValueAsString(data)));
    }
    log.debug("msg sent: command={}, url={}", data.get("Command"), url);
    return "REQ-" + UUID.randomUUID();
  }

  private WebSocketSession open(String url, String type,
                                AtomicReference<CompletableFuture<Void>> pendingRef,
                                MessageSender.Channel channel) {
    CompletableFuture<Void> initDone = new CompletableFuture<>();

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
              haStates.put(url, haState);
              log.debug("init recv: hbPeriodSec={}, haState={}, type={}", hbPeriodSec, haState, type);
              if (!sendJson(session, Map.of(
                  "Command", 2,
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
            haState = ackHaState;
            haStates.put(url, ackHaState);
            Object nodeRole1 = msg.getOrDefault("NodeRole1", msg.get("nodeRole1"));
            Object nodeRole2 = msg.getOrDefault("NodeRole2", msg.get("nodeRole2"));
            log.debug("heartbeat ack: type={}, haState={}, nodeRole1={}, nodeRole2={}", type, ackHaState, nodeRole1, nodeRole2);
            CompletableFuture<Void> hb = pendingRef.getAndSet(null);
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

  private boolean pingChannel(String url, MessageSender.Channel channel) {
    return pingSession(
        sessionOf(url, channel),
        pendingRefOf(url, channel),
        channelType(channel),
        url
    );
  }

  private boolean pingSession(WebSocketSession session,
                              AtomicReference<CompletableFuture<Void>> pendingRef,
                              String type,
                              String url) {
    try {
      if (session == null || !session.isOpen()) {
        return false;
      }
      CompletableFuture<Void> hb = new CompletableFuture<>();
      if (!pendingRef.compareAndSet(null, hb)) {
        log.debug("heartbeat already pending: type={}, url={}", type, url);
        return false;
      }
      log.debug("hb send: command=3, type={}, haState={}, url={}", type, haState, url);
      if (!sendJson(session, Map.of(
          "Command", 3,
          "HaState", haState
      ))) {
        throw new IllegalStateException("session closed while sending heartbeat");
      }
      log.debug("hb sent: command=3, type={}, url={}", type, url);
      hb.get(ackTimeoutMs, TimeUnit.MILLISECONDS);
      return true;
    } catch (TimeoutException e) {
      log.warn("heartbeat timeout waiting command=4 after {}ms, type={}, url={}", ackTimeoutMs, type, url);
      return false;
    } catch (Exception e) {
      log.debug("ping session failed: type={}, url={}, cause={}", type, url, e.getMessage());
      return false;
    } finally {
      pendingRef.set(null);
    }
  }

  private void updatePingStateFor(String url, boolean ok) {
    if (url == null || !url.equals(currentUrl)) {
      return;
    }
    lastPingOk = ok;
    lastPingAt = System.currentTimeMillis();
    if (ok) {
      pingFailures.set(0);
    } else {
      pingFailures.incrementAndGet();
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

  private boolean isChannelOpen(String url, MessageSender.Channel channel) {
    WebSocketSession session = sessionOf(url, channel);
    return session != null && session.isOpen();
  }

  private void disconnectChannel(String url, MessageSender.Channel channel) {
    if (url == null) {
      return;
    }
    WebSocketSession session = sessionMap(channel).remove(url);
    closeQuiet(session);
    pendingRefOf(url, channel).set(null);
  }

  private WebSocketSession sessionOf(String url, MessageSender.Channel channel) {
    if (url == null) {
      return null;
    }
    return sessionMap(channel).get(url);
  }

  private ConcurrentMap<String, WebSocketSession> sessionMap(MessageSender.Channel channel) {
    return channel == MessageSender.Channel.CHAT ? chatSessions : emailSessions;
  }

  private AtomicReference<CompletableFuture<Void>> pendingRefOf(String url, MessageSender.Channel channel) {
    ConcurrentMap<String, AtomicReference<CompletableFuture<Void>>> map =
        channel == MessageSender.Channel.CHAT ? pendingChat : pendingEmail;
    return map.computeIfAbsent(url, key -> new AtomicReference<>());
  }

  private static String connectKey(String url, MessageSender.Channel channel) {
    return url + "|" + channel.name();
  }

  private static String channelType(MessageSender.Channel channel) {
    return channel == MessageSender.Channel.EMAIL ? "I" : "C";
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

  private static int normalizeCommand(Object command) {
    if (command instanceof Number n) {
      return n.intValue();
    }
    try {
      return Integer.parseInt(String.valueOf(command).trim());
    } catch (Exception e) {
      throw new IllegalArgumentException("payload.Command must be numeric");
    }
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
