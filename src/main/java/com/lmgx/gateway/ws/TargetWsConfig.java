package com.lmgx.gateway.ws;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Configuration
@EnableWebSocket
public class TargetWsConfig implements WebSocketConfigurer {
  private static final Logger log = LoggerFactory.getLogger(TargetWsConfig.class);

  private final TargetToggleStore toggles;

  public TargetWsConfig(TargetToggleStore toggles) {
    this.toggles = toggles;
  }

  @Override
  public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
    registry.addHandler(new TargetHandler("A1", toggles), "/clientws/A1").setAllowedOriginPatterns("*");
    registry.addHandler(new TargetHandler("A2", toggles), "/clientws/A2").setAllowedOriginPatterns("*");
    registry.addHandler(new TargetHandler("E1", toggles), "/clientws/E1").setAllowedOriginPatterns("*");
    registry.addHandler(new TargetHandler("E2", toggles), "/clientws/E2").setAllowedOriginPatterns("*");
    log.info("WebSocket handlers registered for A1/A2/E1/E2");
  }

  static class TargetHandler extends TextWebSocketHandler {
    private final String targetId;
    private final TargetToggleStore toggles;

    TargetHandler(String targetId, TargetToggleStore toggles) {
      this.targetId = targetId;
      this.toggles = toggles;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
      synchronized (session) {
        session.sendMessage(new TextMessage("{\"command\":1}"));
      }
      log.debug("target ws connected: {}", targetId);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
      if (!toggles.isAckEnabled(targetId)) return;

      String p = message.getPayload();
      String ackId = pick(p, "\"ackId\":\"", "\"");
      if (ackId != null) {
        synchronized (session) {
          session.sendMessage(new TextMessage("{\"command\":\"ACK\",\"ackId\":\"" + ackId + "\"}"));
        }
        log.debug("ack sent: target={}, ackId={}", targetId, ackId);
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
}
