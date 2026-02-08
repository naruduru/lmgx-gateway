package com.lmgx.gateway.api;

import com.lmgx.gateway.connection.FailoverLoop;
import com.lmgx.gateway.connection.GatewayWsClient;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/gateway")
public class GatewayStatusController {

  private final FailoverLoop failover;
  private final GatewayWsClient ws;

  public GatewayStatusController(FailoverLoop failover, GatewayWsClient ws) {
    this.failover = failover;
    this.ws = ws;
  }

  @GetMapping("/status")
  public Map<String, Object> status() {
    return Map.of(
        "group", failover.getActiveGroup(),
        "activeUrl", failover.getActiveUrl(),
        "wsCurrentUrl", ws.currentUrl(),
        "chatOpen", ws.isChatOpen(),
        "emailOpen", ws.isEmailOpen()
    );
  }
}
