package com.lmgx.gateway.message;

import com.lmgx.gateway.connection.MessageSender;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class UqSender {
  private final MessageSender sender;
  private final UqRequestTracker requestTracker;
  private final SessionInFlightStore sessionInFlightStore;
  private final Set<Integer> serializedChatCommands;

  public UqSender(
      MessageSender sender,
      UqRequestTracker requestTracker,
      SessionInFlightStore sessionInFlightStore,
      @Value("${gateway.uq.serialized-chat-commands:1045}") String serializedChatCommands
  ) {
    this.sender = sender;
    this.requestTracker = requestTracker;
    this.sessionInFlightStore = sessionInFlightStore;
    this.serializedChatCommands = parseCommands(serializedChatCommands);
  }

  public String send(MessageSender.Channel channel, Map<String, Object> payload) throws Exception {
    return sendInternal(channel, payload).requestId();
  }

  public Map<String, Object> sendAndAwait(MessageSender.Channel channel, Map<String, Object> payload) throws Exception {
    Integer command = commandOf(payload);
    SendResult sent = sendInternal(channel, payload);
    if (sent.response() == null) {
      return Map.of("requestId", sent.requestId());
    }
    try {
      return sent.response().get(requestTracker.timeoutMs() + 1000L, TimeUnit.MILLISECONDS);
    } finally {
      if (channel == MessageSender.Channel.CHAT && command != null && serializedChatCommands.contains(command)) {
        sessionInFlightStore.release(UqSerializationKeys.lockKey(command));
      }
    }
  }

  public boolean isSerializedChatCommand(Map<String, Object> payload) {
    Integer command = commandOf(payload);
    return command != null && serializedChatCommands.contains(command);
  }

  private SendResult sendInternal(MessageSender.Channel channel, Map<String, Object> payload) throws Exception {
    if (channel == null) {
      throw new IllegalArgumentException("channel is required");
    }
    Map<String, Object> data = new LinkedHashMap<>();
    if (payload != null) {
      data.putAll(payload);
    }
    Object command = data.get("Command");
    if (command == null) {
      command = data.remove("command");
    }
    data.remove("cmd");
    if (command == null) {
      throw new IllegalArgumentException("payload.Command is required");
    }
    int normalizedCommand = normalizeCommand(command);
    data.put("Command", normalizedCommand);
    String sessionKey = trackedChatSessionKey(channel, normalizedCommand, data);
    if (sessionKey != null && !sessionInFlightStore.tryAcquire(sessionKey, requestMarker(normalizedCommand, data))) {
      throw new InFlightRequestException("uq request already in-flight: sessionKey=" + sessionKey);
    }
    try {
      String requestId = sender.send(channel, data);
      CompletableFuture<Map<String, Object>> response = null;
      if (shouldTrackResponse(normalizedCommand)) {
        response = serializedChatCommands.contains(normalizedCommand)
            ? requestTracker.armGlobal(normalizedCommand, data)
            : requestTracker.arm(normalizedCommand, data);
      }
      return new SendResult(requestId, response);
    } catch (Exception e) {
      if (sessionKey != null) {
        sessionInFlightStore.release(sessionKey);
      }
      throw e;
    }
  }

  public void sendRoute(Map<String, Object> payload) throws Exception {
    sendCommand(MessageSender.Channel.CHAT, 1537, payload);
  }

  public void sendCancel(Map<String, Object> payload) throws Exception {
    sendCommand(MessageSender.Channel.CHAT, 1539, payload);
  }

  public void sendUpdate(Map<String, Object> payload) throws Exception {
    sendCommand(MessageSender.Channel.CHAT, 1541, payload);
  }

  public void sendNotify(Map<String, Object> payload) throws Exception {
    sendCommand(MessageSender.Channel.CHAT, 1543, payload);
  }

  public void sendComplete(Map<String, Object> payload) throws Exception {
    sendCommand(MessageSender.Channel.CHAT, 1545, payload);
  }

  private void sendCommand(MessageSender.Channel channel, int command, Map<String, Object> payload) throws Exception {
    Map<String, Object> data = new LinkedHashMap<>();
    if (payload != null) {
      data.putAll(payload);
    }
    data.put("Command", command);
    send(channel, data);
  }

  private static int normalizeCommand(Object command) {
    if (command instanceof Number n) {
      return n.intValue();
    }
    try {
      String text = String.valueOf(command).trim();
      if (text.startsWith("0x") || text.startsWith("0X")) {
        return Integer.parseInt(text.substring(2), 16);
      }
      return Integer.parseInt(text);
    } catch (Exception e) {
      throw new IllegalArgumentException("payload.Command must be numeric");
    }
  }

  private static boolean isTrackedCommand(int command) {
    return command == 1537
        || command == 1539
        || command == 1541
        || command == 1543
        || command == 1545;
  }

  private boolean shouldTrackResponse(int command) {
    return isTrackedCommand(command) || serializedChatCommands.contains(command);
  }

  private String trackedChatSessionKey(MessageSender.Channel channel, int command, Map<String, Object> payload) {
    if (channel != MessageSender.Channel.CHAT || !serializedChatCommands.contains(command)) {
      return null;
    }
    return UqSerializationKeys.lockKey(command);
  }

  private static String requestMarker(int command, Map<String, Object> payload) {
    Object ucid = payload.get("UCID");
    if (ucid == null) {
      ucid = payload.get("ucid");
    }
    return command + ":" + (ucid == null ? "" : String.valueOf(ucid));
  }

  private static Integer commandOf(Map<String, Object> payload) {
    if (payload == null) {
      return null;
    }
    Object command = payload.get("Command");
    if (command == null) {
      command = payload.get("command");
    }
    if (command == null) {
      command = payload.get("cmd");
    }
    if (command == null) {
      return null;
    }
    return normalizeCommand(command);
  }

  private static Set<Integer> parseCommands(String commands) {
    if (commands == null || commands.isBlank()) {
      return Collections.emptySet();
    }
    Set<Integer> parsed = new HashSet<>();
    Arrays.stream(commands.split(","))
        .map(String::trim)
        .filter(value -> !value.isEmpty())
        .map(UqSender::normalizeCommand)
        .forEach(parsed::add);
    return Set.copyOf(parsed);
  }

  private record SendResult(String requestId, CompletableFuture<Map<String, Object>> response) {
  }
}
