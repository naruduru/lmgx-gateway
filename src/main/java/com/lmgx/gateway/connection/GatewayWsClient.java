package com.lmgx.gateway.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmgx.gateway.instance.InstanceControlStore;
import jakarta.websocket.WebSocketContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
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
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class GatewayWsClient {

  private static final Logger log = LoggerFactory.getLogger(GatewayWsClient.class);

  private final ObjectMapper om = new ObjectMapper();
  private final StandardWebSocketClient client;
  private final IncomingMessageDispatcher dispatcher;
  private final String sourceIp;
  private final Map<String, String> targetIdByUrl;
  private final ExecutorService connector = Executors.newSingleThreadExecutor(r -> {
    Thread t = new Thread(r, "gw-ws-connector");
    t.setDaemon(true);
    return t;
  });

  // 타겟 URL별 chat 채널 WebSocket 세션 저장소다.
  private final ConcurrentMap<String, WebSocketSession> chatSessions = new ConcurrentHashMap<>();
  // 타겟 URL별 email 채널 WebSocket 세션 저장소다.
  private final ConcurrentMap<String, WebSocketSession> emailSessions = new ConcurrentHashMap<>();
  // 타겟 URL별 chat heartbeat ACK 마지막 수신 시각이다.
  private final ConcurrentMap<String, Long> lastChatAckAt = new ConcurrentHashMap<>();
  // 타겟 URL별 email heartbeat ACK 마지막 수신 시각이다.
  private final ConcurrentMap<String, Long> lastEmailAckAt = new ConcurrentHashMap<>();
  // 타겟 URL별 chat heartbeat 요청 후 ACK 대기 시작 시각이다.
  private final ConcurrentMap<String, Long> pendingChatSinceAt = new ConcurrentHashMap<>();
  // 타겟 URL별 email heartbeat 요청 후 ACK 대기 시작 시각이다.
  private final ConcurrentMap<String, Long> pendingEmailSinceAt = new ConcurrentHashMap<>();
  // 타겟 URL별 peer HA 상태값이다. 1은 active, 2는 standby다.
  private final ConcurrentMap<String, Integer> haStates = new ConcurrentHashMap<>();
  // 타겟/채널별 다음 연결 재시도 가능 시각이다.
  private final ConcurrentMap<String, Long> nextConnectAllowedAt = new ConcurrentHashMap<>();
  // 타겟/채널별 연속 연결 실패 횟수다.
  private final ConcurrentMap<String, AtomicInteger> connectFailures = new ConcurrentHashMap<>();
  // 현재 비동기 연결 시도 중인 타겟/채널 키 집합이다.
  private final java.util.Set<String> connectingChannels = ConcurrentHashMap.newKeySet();

  // 현재 업무 command 기준으로 선택된 타겟 URL이다.
  private volatile String currentUrl;
  // 현재 업무 타겟에 대한 마지막 heartbeat 성공 여부다.
  private volatile boolean lastPingOk = false;
  // 현재 업무 타겟에 대한 마지막 heartbeat 확인 시각이다.
  private volatile long lastPingAt = 0L;
  // 현재 업무 타겟에 대한 연속 heartbeat 실패 횟수다.
  private final AtomicInteger pingFailures = new AtomicInteger(0);

  // 마지막 heartbeat 결과가 이 시간보다 오래되면 현재 ping 상태를 오래된 값으로 본다.
  private static final long PING_STALE_MS = 12000;
  // heartbeat 실패가 이 횟수 이상 누적되면 현재 타겟 ping 상태를 불량으로 본다.
  private static final int PING_FAIL_THRESHOLD = 3;
  // 타겟 init 메시지에 HBPeriod가 없을 때 사용할 기본 heartbeat 주기다.
  private static final int DEFAULT_HB_PERIOD_SEC = 10;
  // HA 상태값이 없을 때는 active로 보는 기본값이다.
  private static final int DEFAULT_HA_STATE = 1;

  private enum HeartbeatResult {
    SEND_FAILED(false, false),
    ACK_PENDING(true, false),
    ACK_OK(true, true),
    ACK_TIMEOUT(false, false);

    private final boolean acceptable;
    private final boolean ackConfirmed;

    HeartbeatResult(boolean acceptable, boolean ackConfirmed) {
      this.acceptable = acceptable;
      this.ackConfirmed = ackConfirmed;
    }

    boolean acceptable() {
      return acceptable;
    }

    boolean ackConfirmed() {
      return ackConfirmed;
    }
  }

  // 타겟이 init에서 알려준 heartbeat 주기다.
  private volatile int hbPeriodSec = DEFAULT_HB_PERIOD_SEC;
  // 이 게이트웨이 인스턴스가 타겟에 광고할 local HA 상태다.
  private volatile int localHaState = DEFAULT_HA_STATE;
  private final long ackTimeoutMs;
  private final long connectBackoffInitialMs;
  private final long connectBackoffMaxMs;
  private final double connectBackoffMultiplier;

  public GatewayWsClient(IncomingMessageDispatcher dispatcher, StandardWebSocketClient client, long ackTimeoutMs) {
    this(dispatcher, client, ackTimeoutMs, "127.0.0.1", Map.of(), 1000L, 1000L, 1.0d);
  }

  @Autowired
  public GatewayWsClient(
      IncomingMessageDispatcher dispatcher,
      ObjectProvider<WebSocketContainer> webSocketContainerProvider,
      InstanceControlStore instanceControlStore,
      @Value("${gateway.ws.ack-timeout-ms:1000}") long ackTimeoutMs,
      @Value("${gateway.ws.ha-state:1}") int haState,
      @Value("${gateway.source-ip:}") String configuredSourceIp,
      @Value("${gateway.targets.U1:}") String targetU1,
      @Value("${gateway.targets.U2:}") String targetU2,
      @Value("${gateway.targets.A1:}") String targetA1,
      @Value("${gateway.targets.A2:}") String targetA2,
      @Value("${gateway.ws.backoff.initial-ms:5000}") long connectBackoffInitialMs,
      @Value("${gateway.ws.backoff.max-ms:1800000}") long connectBackoffMaxMs,
      @Value("${gateway.ws.backoff.multiplier:2.0}") double connectBackoffMultiplier
  ) {
    WebSocketContainer webSocketContainer = webSocketContainerProvider.getIfAvailable();
    StandardWebSocketClient client = webSocketContainer == null
        ? new StandardWebSocketClient()
        : new StandardWebSocketClient(webSocketContainer);
    String sourceIp = configuredSourceIp == null || configuredSourceIp.isBlank()
        ? resolveSourceIp()
        : configuredSourceIp.trim();
    Map<String, String> targetIdByUrl = new java.util.HashMap<>();
    putTarget(targetIdByUrl, "U1", targetU1);
    putTarget(targetIdByUrl, "U2", targetU2);
    putTarget(targetIdByUrl, "A1", targetA1);
    putTarget(targetIdByUrl, "A2", targetA2);

    this.dispatcher = dispatcher;
    this.client = Objects.requireNonNull(client, "StandardWebSocketClient must not be null");
    this.ackTimeoutMs = Math.max(1, ackTimeoutMs);
    this.sourceIp = sourceIp;
    this.targetIdByUrl = Map.copyOf(targetIdByUrl);
    this.connectBackoffInitialMs = Math.max(1L, connectBackoffInitialMs);
    this.connectBackoffMaxMs = Math.max(this.connectBackoffInitialMs, connectBackoffMaxMs);
    this.connectBackoffMultiplier = connectBackoffMultiplier < 1.0d ? 1.0d : connectBackoffMultiplier;

    instanceControlStore.setHaState(haState);
    setLocalHaState(instanceControlStore.getHaState());
  }

  // 설정된 타겟 URL이 있으면 WebSocket URL에서 타겟 ID를 찾을 수 있도록 매핑한다.
  private static void putTarget(Map<String, String> targetIdByUrl, String id, String url) {
    // profile별 설정에서 쓰지 않는 endpoint를 생략할 수 있도록 빈 타겟은 무시한다.
    if (url == null || url.isBlank()) {
      return;
    }
    targetIdByUrl.put(url.trim(), id);
  }

  // source IP 설정이 없을 때 OS에서 확인되는 로컬 IP를 사용한다.
  private static String resolveSourceIp() {
    try {
      return java.net.InetAddress.getLocalHost().getHostAddress();
    } catch (Exception e) {
      return "127.0.0.1";
    }
  }

  public GatewayWsClient(IncomingMessageDispatcher dispatcher, StandardWebSocketClient client, long ackTimeoutMs,
                         String sourceIp, Map<String, String> targetIdByUrl,
                         long connectBackoffInitialMs, long connectBackoffMaxMs,
                         double connectBackoffMultiplier) {
    this.dispatcher = dispatcher;
    this.client = Objects.requireNonNull(client, "StandardWebSocketClient must not be null");
    this.ackTimeoutMs = Math.max(1, ackTimeoutMs);
    this.sourceIp = sourceIp == null || sourceIp.isBlank() ? "127.0.0.1" : sourceIp.trim();
    this.targetIdByUrl = Map.copyOf(targetIdByUrl == null ? Map.of() : targetIdByUrl);
    this.connectBackoffInitialMs = Math.max(1L, connectBackoffInitialMs);
    this.connectBackoffMaxMs = Math.max(this.connectBackoffInitialMs, connectBackoffMaxMs);
    this.connectBackoffMultiplier = Math.max(connectBackoffMultiplier, 1.0d);
  }

  public void connect(String wsUrl) {
    // 하나의 타겟은 chat/email 두 논리 채널이 모두 연결되어야 사용할 수 있다.
    connectChat(wsUrl);
    connectEmail(wsUrl);
  }

  public void connectChat(String wsUrl) {
    connectChannelInternal(wsUrl, MessageSender.Channel.CHAT);
  }

  public void connectEmail(String wsUrl) {
    connectChannelInternal(wsUrl, MessageSender.Channel.EMAIL);
  }

  public void connectAll(String... urls) {
    if (urls == null) {
      return;
    }
    // 모든 설정 타겟을 미리 연결해 둔다. 실제 라우팅은 HA active 상태를 별도로 본다.
    for (String url : urls) {
      connect(url);
    }
  }

  // 지정한 타겟/채널 조합에 대해 WebSocket 연결을 비동기로 시도한다.
  private void connectChannelInternal(String wsUrl, MessageSender.Channel channel) {
    if (wsUrl == null || wsUrl.isBlank() || channel == null) {
      return;
    }
    if (currentUrl == null || currentUrl.isBlank()) {
      this.currentUrl = wsUrl;
    }

    if (isChannelOpen(wsUrl, channel)) {
      return;
    }

    String key = connectKey(wsUrl, channel);
    long now = System.currentTimeMillis();
    long allowedAt = nextConnectAllowedAt.getOrDefault(key, 0L);
    if (now < allowedAt) {
      return;
    }
    if (!connectingChannels.add(key)) {
      // 같은 타겟/채널에 대한 중복 비동기 연결 시도를 막는다.
      return;
    }

    connector.submit(() -> {
      try {
        if (isChannelOpen(wsUrl, channel)) {
          return;
        }

        WebSocketSession old = sessionOf(wsUrl, channel);
        if (old != null && !old.isOpen()) {
          sessionMap(channel).remove(wsUrl, old);
        }

        try {
          log.info("connect: url={}, channel={}", wsUrl, channel);
          // 타겟 handshake 프로토콜에서는 CHAT은 C, EMAIL은 I로 보낸다.
          WebSocketSession opened = open(wsUrl, channelType(channel), channel);
          WebSocketSession previous = sessionMap(channel).put(wsUrl, opened);
          if (previous != null && previous != opened) {
            log.warn("unexpected previous session remains: url={}, channel={}, previousSessionId={}, openedSessionId={}",
                wsUrl, channel, previous.getId(), opened.getId());
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

  // 연속 연결 실패 횟수에 따라 다음 연결 대기 시간을 계산한다.
  private long calcBackoffMs(int failures) {
    // backoff는 타겟/채널별로 관리해 한 채널 장애가 다른 채널을 막지 않게 한다.
    if (failures <= 1 || connectBackoffMultiplier == 1.0d) {
      return connectBackoffInitialMs;
    }
    double backoff = connectBackoffInitialMs * Math.pow(connectBackoffMultiplier, failures - 1);
    if (backoff >= connectBackoffMaxMs) {
      return connectBackoffMaxMs;
    }
    return Math.max(connectBackoffInitialMs, (long) backoff);
  }

  public String currentUrl() { return currentUrl; }
  public void setCurrentUrl(String wsUrl) {
    this.currentUrl = wsUrl;
  }
  public String sourceIp() { return sourceIp; }
  public String targetIdOf(String url) { return url == null ? null : targetIdByUrl.get(url); }

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
        + ", pingAgeMs=" + age
        + ", chatHeartbeatState=" + heartbeatStateOf(currentUrl, MessageSender.Channel.CHAT)
        + ", emailHeartbeatState=" + heartbeatStateOf(currentUrl, MessageSender.Channel.EMAIL)
        + ", chatAckAgeMs=" + heartbeatAckAgeMs(currentUrl, MessageSender.Channel.CHAT)
        + ", emailAckAgeMs=" + heartbeatAckAgeMs(currentUrl, MessageSender.Channel.EMAIL)
        + ", chatPendingAgeMs=" + heartbeatPendingAgeMs(currentUrl, MessageSender.Channel.CHAT)
        + ", emailPendingAgeMs=" + heartbeatPendingAgeMs(currentUrl, MessageSender.Channel.EMAIL);
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
    // ready는 두 논리 소켓이 모두 열린 상태이며, HA active 여부는 별도로 판단한다.
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

  public int getLocalHaState() {
    return localHaState;
  }

  public void setLocalHaState(int haState) {
    this.localHaState = normalizeHaState(haState);
  }

  public int haStateOf(String url) {
    if (url == null) {
      return DEFAULT_HA_STATE;
    }
    // 타겟 HA 상태는 init 및 heartbeat 응답에서 받은 값을 사용한다.
    return haStates.getOrDefault(url, DEFAULT_HA_STATE);
  }

  public boolean isHaActive(String url) {
    return haStateOf(url) == 1;
  }

  public boolean isCommandRoutable(String url) {
    // 업무 command는 두 채널이 모두 연결되고 타겟이 active일 때만 보낼 수 있다.
    return isReady(url) && isHaActive(url);
  }

  public boolean isPingHealthy() {
    long age = System.currentTimeMillis() - lastPingAt;
    if (age > PING_STALE_MS) {
      return false;
    }
    return lastPingOk || pingFailures.get() < PING_FAIL_THRESHOLD;
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
    HeartbeatResult result = checkHeartbeat(url, MessageSender.Channel.CHAT);
    updatePingStateFor(url, result, isEmailOpen(url));
    return result.acceptable();
  }

  public boolean pingEmail(String url) {
    HeartbeatResult result = checkHeartbeat(url, MessageSender.Channel.EMAIL);
    updatePingStateFor(url, result, isChatOpen(url));
    return result.acceptable();
  }

  public boolean pingBoth(String url) {
    HeartbeatResult chatResult = checkHeartbeat(url, MessageSender.Channel.CHAT);
    HeartbeatResult emailResult = checkHeartbeat(url, MessageSender.Channel.EMAIL);
    boolean ok = chatResult.acceptable() && emailResult.acceptable();
    updatePingStateFor(url, chatResult, emailResult);
    log.debug("ping both: url={}, chatResult={}, emailResult={}, overallOk={}", url, chatResult, emailResult, ok);
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

  // 열린 세션으로 command payload를 표준 형태로 정리해 전송한다.
  private String send(WebSocketSession s, Map<String, Object> payload, String url) throws Exception {
    if (s == null || !s.isOpen()) throw new IllegalStateException("ws not open");
    if (payload == null) throw new IllegalArgumentException("payload is required");

    // 프로토콜 payload 전송 전에 허용된 command 별칭을 표준 필드로 정규화한다.
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

  // 타겟 WebSocket을 열고 command=1 init 수신 및 command=2 응답까지 완료한다.
  private WebSocketSession open(String url, String type, MessageSender.Channel channel) {
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
        cleanupSession(url, channel, session);
      }

      @Override
      public void handleTransportError(WebSocketSession session, Throwable exception) {
        String sessionId = session != null ? session.getId() : "null";
        log.warn("ws transport error: type={}, sessionId={}, url={}, cause={}, readyState={}",
            type, sessionId, url, describe(exception), readinessDebug(), exception);
        cleanupSession(url, channel, session);
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
              int peerHaState = readInt(msg, "HaState", DEFAULT_HA_STATE);
              haStates.put(url, peerHaState);
              log.debug("init recv: hbPeriodSec={}, peerHaState={}, localHaState={}, type={}",
                  hbPeriodSec, peerHaState, localHaState, type);
              if (!sendJson(session, Map.of(
                  "Command", 2,
                  "HostKind", hostKindOf(type),
                  "HBPeriod", hbPeriodSec,
                  // peer 타겟 상태가 아니라 이 게이트웨이 인스턴스의 상태를 광고한다.
                  "HaState", localHaState,
                  "ResultCode", 1
              ))) {
                throw new IllegalStateException("session closed while sending init response");
              }
              log.debug("init sent: command=2, type={}, hbPeriodSec={}, localHaState={}",
                  type, hbPeriodSec, localHaState);
              initDone.complete(null);
            } catch (Exception e) {
              initDone.completeExceptionally(e);
            }
            return;
          }

          if (isHeartbeatAckCommand(command)) {
            int ackHaState = readInt(msg, "HaState", haStateOf(url));
            // peer의 HA 상태가 이 타겟에 업무 command를 보낼 수 있는지 결정한다.
            haStates.put(url, ackHaState);
            Object nodeRole1 = msg.getOrDefault("NodeRole1", msg.get("nodeRole1"));
            Object nodeRole2 = msg.getOrDefault("NodeRole2", msg.get("nodeRole2"));
            log.debug("heartbeat ack: type={}, haState={}, nodeRole1={}, nodeRole2={}", type, ackHaState, nodeRole1, nodeRole2);
            recordHeartbeatAck(url, channel);
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

  // 채널별 세션을 찾아 heartbeat 송신 및 ACK 상태 확인을 시도한다.
  private HeartbeatResult pingChannel(String url, MessageSender.Channel channel) {
    return pingSession(sessionOf(url, channel), channelType(channel), url, channel);
  }

  // command=4 ACK 대기 중이면 같은 세션에 command=3을 중복 송신하지 않는다.
  private HeartbeatResult pingSession(WebSocketSession session, String type, String url, MessageSender.Channel channel) {
    try {
      if (session == null || !session.isOpen()) {
        return HeartbeatResult.SEND_FAILED;
      }
      long pendingSince = System.currentTimeMillis();
      Long pendingAt = pendingMap(channel).putIfAbsent(url, pendingSince);
      if (pendingAt != null) {
        long pendingAge = pendingSince - pendingAt;
        if (pendingAge > ackTimeoutMs) {
          pendingMap(channel).remove(url, pendingAt);
          log.warn("heartbeat ack pending timeout: command=4 not received within {}ms, type={}, url={}, pendingAgeMs={}",
              ackTimeoutMs, type, url, pendingAge);
          return HeartbeatResult.ACK_TIMEOUT;
        }
        log.debug("heartbeat check skipped duplicate command=3 while waiting command=4: type={}, url={}, pendingAgeMs={}",
            type, url, pendingAge);
        return HeartbeatResult.ACK_PENDING;
      }
      log.debug("hb send: command=3, type={}, localHaState={}, url={}", type, localHaState, url);
      if (!sendJson(session, Map.of(
          "Command", 3,
          // heartbeat도 우리 local 상태를 보낸다. 타겟 상태는 command=4 응답에서 읽는다.
          "HaState", localHaState
      ))) {
        pendingMap(channel).remove(url, pendingSince);
        throw new IllegalStateException("session closed while sending heartbeat");
      }
      log.debug("hb sent: command=3, type={}, url={}", type, url);
      return heartbeatStateOf(url, channel, true);
    } catch (Exception e) {
      log.debug("ping session failed: type={}, url={}, cause={}", type, url, e.getMessage());
      return HeartbeatResult.SEND_FAILED;
    }
  }

  // heartbeat 송신과 ACK 상태 확인을 한 번에 수행한다.
  private HeartbeatResult checkHeartbeat(String url, MessageSender.Channel channel) {
    return pingChannel(url, channel);
  }

  // 현재 업무 타겟에 대한 마지막 ping 결과와 실패 횟수를 갱신한다.
  private void updatePingStateFor(String url, HeartbeatResult result, boolean peerChannelOpen) {
    if (url == null || !url.equals(currentUrl)) {
      return;
    }
    if (result == HeartbeatResult.ACK_PENDING) {
      return;
    }
    updatePingStateFor(url, result.ackConfirmed() && peerChannelOpen);
  }

  // 현재 업무 타겟에 대한 마지막 ping 결과와 실패 횟수를 갱신한다.
  private void updatePingStateFor(String url, HeartbeatResult chatResult, HeartbeatResult emailResult) {
    if (url == null || !url.equals(currentUrl)) {
      return;
    }
    if (chatResult == HeartbeatResult.ACK_PENDING || emailResult == HeartbeatResult.ACK_PENDING) {
      return;
    }
    updatePingStateFor(url, chatResult.ackConfirmed() && emailResult.ackConfirmed());
  }

  // 현재 업무 타겟에 대한 마지막 ping 결과와 실패 횟수를 갱신한다.
  private void updatePingStateFor(String url, boolean ok) {
    // 전역 ping 상태는 현재 업무 타겟에 대해서만 갱신한다.
    lastPingOk = ok;
    lastPingAt = System.currentTimeMillis();
    if (ok) {
      pingFailures.set(0);
    } else {
      pingFailures.incrementAndGet();
    }
  }

  // 세션이 열려 있을 때만 JSON payload를 thread-safe하게 전송한다.
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

  // 특정 타겟/채널의 WebSocket 세션이 열려 있는지 확인한다.
  private boolean isChannelOpen(String url, MessageSender.Channel channel) {
    WebSocketSession session = sessionOf(url, channel);
    return session != null && session.isOpen();
  }

  // 닫힌 세션의 내부 참조만 정리하고 세션 close는 호출하지 않는다.
  private void cleanupSession(String url, MessageSender.Channel channel, WebSocketSession session) {
    if (url == null || channel == null || session == null) {
      return;
    }
    sessionMap(channel).remove(url, session);
    ackMap(channel).remove(url);
    pendingMap(channel).remove(url);
    connectingChannels.remove(connectKey(url, channel));
  }

  // heartbeat ACK를 받은 시각을 채널별로 기록한다.
  private void recordHeartbeatAck(String url, MessageSender.Channel channel) {
    if (url == null || channel == null) {
      return;
    }
    ackMap(channel).put(url, System.currentTimeMillis());
    pendingMap(channel).remove(url);
  }

  // 최근 command=4 ACK가 설정된 timeout 안에 들어왔는지, 아직 timeout 전 ACK 대기 중인지 확인한다.
  private HeartbeatResult heartbeatStateOf(String url, MessageSender.Channel channel) {
    return heartbeatStateOf(url, channel, false);
  }

  // 최근 command=4 ACK가 설정된 timeout 안에 들어왔는지, 아직 timeout 전 ACK 대기 중인지 확인한다.
  private HeartbeatResult heartbeatStateOf(String url, MessageSender.Channel channel, boolean logStale) {
    if (url == null || channel == null) {
      return HeartbeatResult.SEND_FAILED;
    }
    Long pendingAt = pendingMap(channel).get(url);
    if (pendingAt != null) {
      long pendingAge = System.currentTimeMillis() - pendingAt;
      if (pendingAge <= ackTimeoutMs) {
        log.debug("heartbeat waiting for command=4 within timeout: channel={}, url={}, pendingAgeMs={}",
            channel, url, pendingAge);
        return HeartbeatResult.ACK_PENDING;
      }
      return HeartbeatResult.ACK_TIMEOUT;
    }
    Long ackAt = ackMap(channel).get(url);
    if (ackAt == null) {
      return HeartbeatResult.SEND_FAILED;
    }
    long age = System.currentTimeMillis() - ackAt;
    boolean healthy = age <= ackTimeoutMs;
    if (!healthy && logStale) {
      log.warn("heartbeat ack stale: command=4 not received within {}ms, channel={}, url={}, ackAgeMs={}",
          ackTimeoutMs, channel, url, age);
    }
    return healthy ? HeartbeatResult.ACK_OK : HeartbeatResult.ACK_TIMEOUT;
  }

  // 마지막 heartbeat ACK 이후 경과 시간을 로그와 상태 문자열에 표시한다.
  private long heartbeatAckAgeMs(String url, MessageSender.Channel channel) {
    if (url == null || channel == null) {
      return -1L;
    }
    Long ackAt = ackMap(channel).get(url);
    if (ackAt == null) {
      return -1L;
    }
    return System.currentTimeMillis() - ackAt;
  }

  // heartbeat 요청 후 ACK 대기 경과 시간을 로그와 상태 문자열에 표시한다.
  private long heartbeatPendingAgeMs(String url, MessageSender.Channel channel) {
    if (url == null || channel == null) {
      return -1L;
    }
    Long pendingAt = pendingMap(channel).get(url);
    if (pendingAt == null) {
      return -1L;
    }
    return System.currentTimeMillis() - pendingAt;
  }

  // 특정 타겟/채널에 해당하는 현재 WebSocket 세션을 반환한다.
  private WebSocketSession sessionOf(String url, MessageSender.Channel channel) {
    if (url == null) {
      return null;
    }
    return sessionMap(channel).get(url);
  }

  // 채널 종류에 맞는 세션 저장소를 선택한다.
  private ConcurrentMap<String, WebSocketSession> sessionMap(MessageSender.Channel channel) {
    return channel == MessageSender.Channel.CHAT ? chatSessions : emailSessions;
  }

  // 채널 종류에 맞는 heartbeat ACK 시각 저장소를 선택한다.
  private ConcurrentMap<String, Long> ackMap(MessageSender.Channel channel) {
    return channel == MessageSender.Channel.CHAT ? lastChatAckAt : lastEmailAckAt;
  }

  // 채널 종류에 맞는 heartbeat ACK 대기 시각 저장소를 선택한다.
  private ConcurrentMap<String, Long> pendingMap(MessageSender.Channel channel) {
    return channel == MessageSender.Channel.CHAT ? pendingChatSinceAt : pendingEmailSinceAt;
  }

  // 연결 backoff와 중복 연결 방지에 사용할 타겟/채널 키를 만든다.
  private static String connectKey(String url, MessageSender.Channel channel) {
    return url + "|" + channel.name();
  }

  // 내부 채널 enum을 타겟 프로토콜의 채널 타입 값으로 변환한다.
  private static String channelType(MessageSender.Channel channel) {
    return channel == MessageSender.Channel.EMAIL ? "I" : "C";
  }

  // payload에서 숫자 필드를 안전하게 읽고 실패하면 기본값을 반환한다.
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

  // HA 상태는 2만 standby로 인정하고 나머지는 active로 정규화한다.
  private static int normalizeHaState(int value) {
    return value == 2 ? 2 : 1;
  }

  // init 응답에 넣을 HostKind 값을 채널 타입 기준으로 계산한다.
  private static Integer hostKindOf(String type) {
    return "I".equals(type) ? 2 : 1;
  }

  // command 필드를 숫자형 프로토콜 값으로 정규화한다.
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

  // 수신 payload에서 command 필드를 대소문자 호환 형태로 찾는다.
  private static Object commandOf(Map<String, Object> msg) {
    Object c = msg.get("command");
    return c != null ? c : msg.get("Command");
  }

  // 수신 command가 init 요청(command=1)인지 확인한다.
  private static boolean isInitCommand(Object command) {
    return Integer.valueOf(1).equals(command) || "1".equals(String.valueOf(command));
  }

  // 수신 command가 heartbeat 응답(command=4)인지 확인한다.
  private static boolean isHeartbeatAckCommand(Object command) {
    return Integer.valueOf(4).equals(command) || "4".equals(String.valueOf(command));
  }

  // 로그에 남길 예외 원인 메시지를 root cause 기준으로 만든다.
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

}
