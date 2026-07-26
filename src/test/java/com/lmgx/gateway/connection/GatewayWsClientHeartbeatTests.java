package com.lmgx.gateway.connection;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GatewayWsClientHeartbeatTests {

  private static final String URL = "ws://target/clientws/U1";

  @Test
  void pingClearsPendingWhenAckTimeoutExpires() throws Exception {
    GatewayWsClient client = newClient(10);
    WebSocketSession session = openSession();
    sessionMap(client, MessageSender.Channel.CHAT).put(URL, session);
    client.setCurrentUrl(URL);

    assertThat(client.pingChat(URL)).isTrue();
    assertThat(pendingMap(client, MessageSender.Channel.CHAT)).containsKey(URL);
    assertThat(client.lastPingOk()).isFalse();
    assertThat(client.lastPingAt()).isZero();

    Thread.sleep(20);

    assertThat(client.pingChat(URL)).isFalse();
    assertThat(pendingMap(client, MessageSender.Channel.CHAT)).doesNotContainKey(URL);
  }

  @Test
  void pingRegistersPendingBeforeSendingHeartbeatSoImmediateAckCanClearIt() throws Exception {
    GatewayWsClient client = newClient(1000);
    WebSocketSession session = openSession();
    sessionMap(client, MessageSender.Channel.CHAT).put(URL, session);
    client.setCurrentUrl(URL);

    doAnswer(invocation -> {
      assertThat(pendingMap(client, MessageSender.Channel.CHAT)).containsKey(URL);
      pendingMap(client, MessageSender.Channel.CHAT).remove(URL);
      ackMap(client, MessageSender.Channel.CHAT).put(URL, System.currentTimeMillis());
      return null;
    }).when(session).sendMessage(any(TextMessage.class));

    assertThat(client.pingChat(URL)).isTrue();
    assertThat(pendingMap(client, MessageSender.Channel.CHAT)).doesNotContainKey(URL);
    assertThat(ackMap(client, MessageSender.Channel.CHAT)).containsKey(URL);
  }

  @Test
  void concurrentPingsSendOnlyOneHeartbeatWhileAckIsPending() throws Exception {
    GatewayWsClient client = newClient(1000);
    WebSocketSession session = openSession();
    sessionMap(client, MessageSender.Channel.CHAT).put(URL, session);
    client.setCurrentUrl(URL);

    CountDownLatch sendStarted = new CountDownLatch(1);
    CountDownLatch releaseSend = new CountDownLatch(1);
    AtomicInteger sendCount = new AtomicInteger();
    doAnswer(invocation -> {
      sendCount.incrementAndGet();
      sendStarted.countDown();
      assertThat(releaseSend.await(1, TimeUnit.SECONDS)).isTrue();
      return null;
    }).when(session).sendMessage(any(TextMessage.class));

    Thread first = new Thread(() -> client.pingChat(URL), "first-ping");
    first.start();
    assertThat(sendStarted.await(1, TimeUnit.SECONDS)).isTrue();

    assertThat(client.pingChat(URL)).isTrue();

    releaseSend.countDown();
    first.join(1000);

    assertThat(sendCount.get()).isEqualTo(1);
    assertThat(pendingMap(client, MessageSender.Channel.CHAT)).containsKey(URL);
  }

  @Test
  void initCommandDoesNotRecordHeartbeatAck() {
    StandardWebSocketClient wsClient = mock(StandardWebSocketClient.class);
    GatewayWsClient client = new GatewayWsClient(null, wsClient, 1000);
    WebSocketSession session = openSession();
    when(session.getId()).thenReturn("session-1");
    when(wsClient.execute(any(WebSocketHandler.class), eq(URL))).thenAnswer(invocation -> {
      WebSocketHandler handler = invocation.getArgument(0);
      handler.handleMessage(session, new TextMessage("""
          {"Command":1,"HBPeriod":10,"HaState":1}
          """));
      return CompletableFuture.completedFuture(session);
    });

    ReflectionTestUtils.invokeMethod(client, "open", URL, "C", MessageSender.Channel.CHAT);

    assertThat(ackMap(client, MessageSender.Channel.CHAT)).doesNotContainKey(URL);
  }

  @SuppressWarnings("unchecked")
  private static ConcurrentMap<String, WebSocketSession> sessionMap(GatewayWsClient client, MessageSender.Channel channel) {
    String field = channel == MessageSender.Channel.CHAT ? "chatSessions" : "emailSessions";
    return (ConcurrentMap<String, WebSocketSession>) ReflectionTestUtils.getField(client, field);
  }

  @SuppressWarnings("unchecked")
  private static ConcurrentMap<String, Long> pendingMap(GatewayWsClient client, MessageSender.Channel channel) {
    String field = channel == MessageSender.Channel.CHAT ? "pendingChatSinceAt" : "pendingEmailSinceAt";
    return (ConcurrentMap<String, Long>) ReflectionTestUtils.getField(client, field);
  }

  @SuppressWarnings("unchecked")
  private static ConcurrentMap<String, Long> ackMap(GatewayWsClient client, MessageSender.Channel channel) {
    String field = channel == MessageSender.Channel.CHAT ? "lastChatAckAt" : "lastEmailAckAt";
    return (ConcurrentMap<String, Long>) ReflectionTestUtils.getField(client, field);
  }

  private static GatewayWsClient newClient(long ackTimeoutMs) {
    return new GatewayWsClient(null, mock(StandardWebSocketClient.class), ackTimeoutMs);
  }

  private static WebSocketSession openSession() {
    WebSocketSession session = mock(WebSocketSession.class);
    when(session.isOpen()).thenReturn(true);
    return session;
  }
}
