package com.lmgx.gateway.api;

import com.lmgx.gateway.ws.GatewayWsClient;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/gateway")
public class GatewayController {

  private final GatewayWsClient ws;

  public GatewayController(GatewayWsClient ws) {
    this.ws = ws;
  }

  @PostMapping("/chat/send")
  public Map<String, Object> chat(@RequestBody Map<String, Object> body) {
    try {
      String id = ws.sendChat(body);
      return Map.of("ok", true, "requestId", id);
    } catch (Exception e) {
      return Map.of("ok", false, "message", e.getMessage());
    }
  }

  @PostMapping("/email/send")
  public Map<String, Object> email(@RequestBody Map<String, Object> body) {
    try {
      String id = ws.sendEmail(body);
      return Map.of("ok", true, "requestId", id);
    } catch (Exception e) {
      return Map.of("ok", false, "message", e.getMessage());
    }
  }
}
