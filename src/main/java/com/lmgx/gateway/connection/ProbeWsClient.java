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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ProbeWsClient {
  private static final Logger log = LoggerFactory.getLogger(ProbeWsClient.class);
  private final ObjectMapper om = new ObjectMapper();
  private final StandardWebSocketClient client = new StandardWebSocketClient();

  private static final long TIMEOUT_MS = 1000;
  private static final int DEFAULT_HB_PERIOD_SEC = 10;
  private static final int DEFAULT_HA_STATE = 1;

  public boolean probe(String url) {
    CompletableFuture<Void> initDone = new CompletableFuture<>();
    CompletableFuture<Void> heartbeatDone = new CompletableFuture<>();

    WebSocketHandler h = new TextWebSocketHandler() {
      @Override
      protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
          String p = message.getPayload();
          @SuppressWarnings("unchecked")
          Map<String, Object> msg = om.readValue(p, Map.class);

          Object command = commandOf(msg);
          log.debug("probe recv: command={}, payload={}", command, msg);
          if (isInitCommand(command)) {
            int hbPeriodSec = readInt(msg, "HBPeriod", DEFAULT_HB_PERIOD_SEC);
            int haState = readInt(msg, "HaState", DEFAULT_HA_STATE);
            log.debug("probe init recv: hbPeriodSec={}, haState={}", hbPeriodSec, haState);
            if (!sendJson(session, Map.of(
                "Command", "2",
                "HostKind", 1,
                "HBPeriod", hbPeriodSec,
                "HaState", haState,
                "ResultCode", 1
            ))) {
              throw new IllegalStateException("session closed while sending init response");
            }
            log.debug("probe init sent: command=2, hbPeriodSec={}, haState={}", hbPeriodSec, haState);

            log.debug("probe hb send: command=3, haState={}", haState);
            if (!sendJson(session, Map.of(
                "Command", "3",
                "HaState", haState
            ))) {
              throw new IllegalStateException("session closed while sending heartbeat");
            }
            log.debug("probe hb sent: command=3, haState={}", haState);

            initDone.complete(null);
            return;
          }

          if (isHeartbeatAckCommand(command)) {
            int ackHaState = readInt(msg, "HaState", DEFAULT_HA_STATE);
            Object nodeRole1 = msg.getOrDefault("NodeRole1", msg.get("nodeRole1"));
            Object nodeRole2 = msg.getOrDefault("NodeRole2", msg.get("nodeRole2"));
            log.debug("probe heartbeat ack: haState={}, nodeRole1={}, nodeRole2={}", ackHaState, nodeRole1, nodeRole2);
            heartbeatDone.complete(null);
          }
        } catch (Exception e) {
          log.debug("probe ws message handling error: {}", e.getMessage());
        }
      }
    };

    WebSocketSession s = null;
    try {
      s = client.execute(h, url).get(1, TimeUnit.SECONDS);
      initDone.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      heartbeatDone.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      log.debug("probe ok: {}", url);
      return true;
    } catch (Exception e) {
      log.debug("probe fail: {} ({})", url, e.getMessage());
      return false;
    } finally {
      if (s != null && s.isOpen()) {
        try { s.close(); } catch (Exception ignore) {}
      }
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
}
