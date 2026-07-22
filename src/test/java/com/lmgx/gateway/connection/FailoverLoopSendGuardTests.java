package com.lmgx.gateway.connection;

import com.lmgx.gateway.instance.InstanceControlStore;
import com.lmgx.gateway.persist.GatewayLogMapper;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FailoverLoopSendGuardTests {

  private static final String U1 = "ws://target/clientws/U1";

  @Test
  void ensureCommandTargetRequiresHeartbeatCheckForCurrentActiveTarget() {
    GatewayWsClient ws = mock(GatewayWsClient.class);
    when(ws.isCommandRoutable(U1)).thenReturn(true);
    when(ws.isHaActive(U1)).thenReturn(true);
    when(ws.isChatOpen(U1)).thenReturn(true);
    when(ws.isEmailOpen(U1)).thenReturn(true);
    when(ws.pingChat(U1)).thenReturn(false);
    when(ws.pingEmail(U1)).thenReturn(true);
    when(ws.haStateOf(U1)).thenReturn(1);

    FailoverLoop loop = new FailoverLoop(
        ws,
        mock(GatewayLogMapper.class),
        new InstanceControlStore(),
        new MockEnvironment()
            .withProperty("gateway.targets.U1", U1)
            .withProperty("gateway.ring.G1", "U1")
            .withProperty("gateway.recover.G1", "U1")
            .withProperty("gateway.prefer.G1", "U1")
    );

    assertThatThrownBy(() -> loop.ensureCommandTarget(MessageSender.Channel.CHAT))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("no command-routable target");
  }
}
