package com.lmgx.gateway.message;

import com.lmgx.gateway.connection.IncomingCommandHandler;
import com.lmgx.gateway.connection.MessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class UqCommandHandler implements IncomingCommandHandler {
  private static final Logger log = LoggerFactory.getLogger(UqCommandHandler.class);
  private final UqMessageService messageService;

  public UqCommandHandler(UqMessageService messageService) {
    this.messageService = messageService;
  }

  @Override
  public List<String> cmds() {
    return List.of("1538", "1540", "1542", "1544", "1546");
  }

  @Override
  public void handle(MessageSender.Channel channel, Map<String, Object> message) {
    String command = commandOf(message);
    if (command == null) {
      return;
    }
    log.debug("uq recv: command={}, channel={}, payload={}", command, channel, message);
    switch (command) {
      case "1538" -> handleRouteRes(message);
      case "1540" -> handleTimeout(message);
      case "1542" -> handleSuccess(message);
      case "1544" -> handleFailure(message);
      case "1546" -> handleComplete(message);
      default -> {
      }
    }
  }

  private String commandOf(Map<String, Object> message) {
    Object cmdObj = message.get("Command");
    if (cmdObj == null) {
      cmdObj = message.get("command");
    }
    if (cmdObj == null) {
      return null;
    }
    return String.valueOf(cmdObj);
  }

  private void handleRouteRes(Map<String, Object> message) {
    String ucid = stringOf(message.get("UCID"));
    String callingUserId = stringOf(message.get("CallingUserId"));
    Object resultCode = message.get("ResultCode");
    log.info("route res: ucid={}, callingUserId={}, resultCode={}", ucid, callingUserId, resultCode);
    messageService.onRouteRes(message);
  }

  private void handleTimeout(Map<String, Object> message) {
    messageService.onTimeout(message);
  }

  private void handleSuccess(Map<String, Object> message) {
    messageService.onSuccess(message);
  }

  private void handleFailure(Map<String, Object> message) {
    messageService.onFailure(message);
  }

  private void handleComplete(Map<String, Object> message) {
    messageService.onComplete(message);
  }

  private String stringOf(Object v) {
    if (v == null) {
      return null;
    }
    return String.valueOf(v);
  }
}
