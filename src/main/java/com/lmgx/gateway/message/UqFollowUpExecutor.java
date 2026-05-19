package com.lmgx.gateway.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class UqFollowUpExecutor {
  private static final Logger log = LoggerFactory.getLogger(UqFollowUpExecutor.class);

  private final UqSender uqSender;

  public UqFollowUpExecutor(UqSender uqSender) {
    this.uqSender = uqSender;
  }

  @EventListener
  public void handle(UqFollowUpEvent event) {
    Map<String, Object> payload = new LinkedHashMap<>();
    if (event.payload() != null) {
      payload.putAll(event.payload());
    }

    try {
      switch (event.action()) {
        case "sendTransferSecondChatting" -> uqSender.sendTransferSecondChatting(payload);
        case "sendClearChat" -> uqSender.sendClearChat(payload);
        default -> log.info("unhandled follow-up action={}, payload={}", event.action(), payload);
      }
    } catch (Exception e) {
      log.info("follow-up execution failed: action={}, cause={}, payload={}",
          event.action(), e.getMessage(), payload, e);
    }
  }
}
