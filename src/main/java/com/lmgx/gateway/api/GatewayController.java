package com.lmgx.gateway.api;

import com.lmgx.gateway.connection.MessageSender;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/gateway")
public class GatewayController {

  private final MessageSender sender;

  public GatewayController(MessageSender sender) {
    this.sender = sender;
  }

  @PostMapping("/chat/send")
  public Map<String, Object> chat(@RequestBody Map<String, Object> body) {
    try {
      String id = sender.send(MessageSender.Channel.CHAT, body);
      return Map.of("ok", true, "requestId", id);
    } catch (Exception e) {
      return Map.of("ok", false, "message", e.getMessage());
    }
  }

  @PostMapping("/email/send")
  public Map<String, Object> email(@RequestBody Map<String, Object> body) {
    try {
      String id = sender.send(MessageSender.Channel.EMAIL, body);
      return Map.of("ok", true, "requestId", id);
    } catch (Exception e) {
      return Map.of("ok", false, "message", e.getMessage());
    }
  }
}
