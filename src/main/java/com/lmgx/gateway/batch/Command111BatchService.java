package com.lmgx.gateway.batch;

import com.lmgx.gateway.connection.MessageSender;
import com.lmgx.gateway.message.UqSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class Command111BatchService {
  private static final Logger log = LoggerFactory.getLogger(Command111BatchService.class);

  private final UqSender uqSender;

  public Command111BatchService(UqSender uqSender) {
    this.uqSender = uqSender;
  }

  @Scheduled(fixedDelay = 10_000L)
  public void run() {
    try {
      Map<String, Object> payload = new LinkedHashMap<>();
      payload.put("Command", 111);
      String requestId = uqSender.send(MessageSender.Channel.CHAT, payload);
      log.info("batch command sent: command=111, channel=CHAT, requestId={}", requestId);
    } catch (Exception e) {
      log.warn("batch command send failed: command=111, channel=CHAT, cause={}", e.getMessage());
    }
  }
}
