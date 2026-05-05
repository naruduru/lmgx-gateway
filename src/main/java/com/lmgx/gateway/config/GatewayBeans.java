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

@Configuration
public class GatewayBeans {

  @Bean
  public GatewayWsClient gatewayWsClient(
      com.lmgx.gateway.connection.IncomingMessageDispatcher dispatcher,
      ObjectProvider<WebSocketContainer> webSocketContainerProvider,
      InstanceControlStore instanceControlStore,
      @Value("${gateway.ws.ack-timeout-ms:1000}") long ackTimeoutMs,
      @Value("${gateway.ws.ha-state:1}") int haState
  ) {
    WebSocketContainer webSocketContainer = webSocketContainerProvider.getIfAvailable();
    StandardWebSocketClient client = webSocketContainer == null
        ? new StandardWebSocketClient()
        : new StandardWebSocketClient(webSocketContainer);
    instanceControlStore.setHaState(haState);
    GatewayWsClient gatewayWsClient = new GatewayWsClient(dispatcher, client, ackTimeoutMs);
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
}
