package com.lmgx.gateway.config;

import com.lmgx.gateway.monitor.InstanceControlStore;
import com.lmgx.gateway.ws.GatewayWsClient;
import com.lmgx.gateway.ws.ProbeWsClient;
import com.lmgx.gateway.ws.TargetToggleStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GatewayBeans {

  @Bean
  public GatewayWsClient gatewayWsClient() {
    return new GatewayWsClient();
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
