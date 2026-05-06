package com.lmgx.gateway.config;

import com.lmgx.gateway.instance.InstanceControlStore;
import com.lmgx.gateway.connection.GatewayWsClient;
import com.lmgx.gateway.connection.ProbeWsClient;
import com.lmgx.gateway.target.TargetToggleStore;
import jakarta.websocket.WebSocketContainer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class GatewayBeans {

  @Bean
  public GatewayWsClient gatewayWsClient(
      com.lmgx.gateway.connection.IncomingMessageDispatcher dispatcher,
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
    Map<String, String> targetIdByUrl = new HashMap<>();
    putTarget(targetIdByUrl, "U1", targetU1);
    putTarget(targetIdByUrl, "U2", targetU2);
    putTarget(targetIdByUrl, "A1", targetA1);
    putTarget(targetIdByUrl, "A2", targetA2);

    instanceControlStore.setHaState(haState);
    GatewayWsClient gatewayWsClient = new GatewayWsClient(dispatcher, client, ackTimeoutMs, sourceIp,
        targetIdByUrl, connectBackoffInitialMs, connectBackoffMaxMs, connectBackoffMultiplier);
    gatewayWsClient.setLocalHaState(instanceControlStore.getHaState());
    return gatewayWsClient;
  }

  @Bean
  public ProbeWsClient probeWsClient() {
    return new ProbeWsClient();
  }

  @Bean
  public TargetToggleStore targetToggleStore() {
    return new TargetToggleStore();
  }

  @Bean
  public InstanceControlStore instanceControlStore() {
    return new InstanceControlStore();
  }

  private static void putTarget(Map<String, String> targetIdByUrl, String id, String url) {
    if (url == null || url.isBlank()) {
      return;
    }
    targetIdByUrl.put(url.trim(), id);
  }

  private static String resolveSourceIp() {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    } catch (Exception e) {
      return "127.0.0.1";
    }
  }
}
