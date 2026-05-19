package com.lmgx.gateway.api;

import com.lmgx.gateway.connection.MessageSender;
import com.lmgx.gateway.message.UqSender;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/gateway")
public class GatewayController {

  private final UqSender uqSender;

  public GatewayController(UqSender uqSender) {
    this.uqSender = uqSender;
  }

  @PostMapping("/chat/send")
  public Map<String, Object> chat(@RequestBody Map<String, Object> body) {
    try {
      if (uqSender.isSerializedChatCommand(body)) {
        Map<String, Object> response = uqSender.sendAndAwait(MessageSender.Channel.CHAT, body);
        return Map.of("ok", true, "response", response);
      }
      String id = uqSender.send(MessageSender.Channel.CHAT, body);
      return Map.of("ok", true, "requestId", id);
    } catch (Exception e) {
      return Map.of("ok", false, "message", e.getMessage());
    }
  }

  @PostMapping("/email/send")
  public Map<String, Object> email(@RequestBody Map<String, Object> body) {
    try {
      String id = uqSender.send(MessageSender.Channel.EMAIL, body);
      return Map.of("ok", true, "requestId", id);
    } catch (Exception e) {
      return Map.of("ok", false, "message", e.getMessage());
    }
  }
}
