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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ProbeWsClient {
  private static final Logger log = LoggerFactory.getLogger(ProbeWsClient.class);
  private final ObjectMapper om = new ObjectMapper();
  private final StandardWebSocketClient client = new StandardWebSocketClient();
  private final AckRegistry ack = new AckRegistry();

  private static final long TIMEOUT_MS = 1000;

  public boolean probe(String url) {
    CompletableFuture<Void> initDone = new CompletableFuture<>();
    CompletableFuture<Void> pingDone = new CompletableFuture<>();
    AtomicReference<String> initAckRef = new AtomicReference<>();
    AtomicReference<String> pingAckRef = new AtomicReference<>();

    WebSocketHandler h = new TextWebSocketHandler() {
      @Override
      protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String p = message.getPayload();

        if (p.contains("\"command\":1")) {
          String initAckId = ack.newId("PI");
          initAckRef.set(initAckId);
          ack.register(initAckId);

          synchronized (session) {
            session.sendMessage(new TextMessage(om.writeValueAsString(Map.of(
                "command", 2,
                "type", "C",
                "ackId", initAckId
            ))));
          }
          return;
        }

        if (p.contains("\"command\":\"ACK\"") && p.contains("\"ackId\"")) {
          String ackId = pick(p, "\"ackId\":\"", "\"");
          if (ackId == null) return;

          ack.ack(ackId);

          if (ackId.equals(initAckRef.get())) {
            initDone.complete(null);

            String pingAckId = ack.newId("PH");
            pingAckRef.set(pingAckId);
            ack.register(pingAckId);

            synchronized (session) {
              session.sendMessage(new TextMessage(om.writeValueAsString(Map.of(
                  "command", "PING",
                  "ackId", pingAckId
              ))));
            }
            return;
          }

          if (ackId.equals(pingAckRef.get())) {
            pingDone.complete(null);
          }
        }
      }
    };

    WebSocketSession s = null;
    try {
      s = client.execute(h, url).get(1, TimeUnit.SECONDS);
      initDone.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      pingDone.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
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

  private static String pick(String src, String prefix, String until) {
    int s = src.indexOf(prefix);
    if (s < 0) return null;
    s += prefix.length();
    int e = src.indexOf(until, s);
    if (e < 0) return null;
    return src.substring(s, e);
  }
}
