package com.lmgx.gateway.message;

import com.lmgx.gateway.connection.MessageSender;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class UqSender {
  private final MessageSender sender;
  private final UqRequestTracker requestTracker;

  public UqSender(MessageSender sender, UqRequestTracker requestTracker) {
    this.sender = sender;
    this.requestTracker = requestTracker;
  }

  public String send(MessageSender.Channel channel, Map<String, Object> payload) throws Exception {
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
    String requestId = sender.send(channel, data);
    if (isTrackedCommand(normalizedCommand)) {
      requestTracker.arm(normalizedCommand, data);
    }
    return requestId;
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
    sender.send(channel, data);
    requestTracker.arm(command, data);
  }

  private static int normalizeCommand(Object command) {
    if (command instanceof Number n) {
      return n.intValue();
    }
    try {
      return Integer.parseInt(String.valueOf(command).trim());
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
}
