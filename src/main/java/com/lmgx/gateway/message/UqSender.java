package com.lmgx.gateway.message;

import com.lmgx.gateway.connection.MessageSender;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class UqSender {
  private final MessageSender sender;

  public UqSender(MessageSender sender) {
    this.sender = sender;
  }

  public void sendRoute(Map<String, Object> payload) throws Exception {
    send(MessageSender.Channel.CHAT, "1537", payload);
  }

  public void sendCancel(Map<String, Object> payload) throws Exception {
    send(MessageSender.Channel.CHAT, "1539", payload);
  }

  public void sendUpdate(Map<String, Object> payload) throws Exception {
    send(MessageSender.Channel.CHAT, "1541", payload);
  }

  public void sendNotify(Map<String, Object> payload) throws Exception {
    send(MessageSender.Channel.CHAT, "1543", payload);
  }

  public void sendComplete(Map<String, Object> payload) throws Exception {
    send(MessageSender.Channel.CHAT, "1545", payload);
  }

  private void send(MessageSender.Channel channel, String command, Map<String, Object> payload) throws Exception {
    Map<String, Object> data = new LinkedHashMap<>();
    if (payload != null) {
      data.putAll(payload);
    }
    data.put("Command", String.valueOf(command));
    sender.send(channel, data);
  }
}
