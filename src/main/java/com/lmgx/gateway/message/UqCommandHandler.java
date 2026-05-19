package com.lmgx.gateway.message;

import com.lmgx.gateway.connection.IncomingCommandHandler;
import com.lmgx.gateway.connection.MessageSender;
import org.springframework.beans.factory.annotation.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class UqCommandHandler implements IncomingCommandHandler {
  private static final Logger log = LoggerFactory.getLogger(UqCommandHandler.class);
  private final UqMessageService messageService;
  private final UqRequestTracker requestTracker;
  private final SessionInFlightStore sessionInFlightStore;
  private final SessionRequestKeyResolver sessionRequestKeyResolver;
  private final List<Integer> serializedChatCommands;
  private final List<String> responseCommands;

  public UqCommandHandler(
      UqMessageService messageService,
      UqRequestTracker requestTracker,
      SessionInFlightStore sessionInFlightStore,
      SessionRequestKeyResolver sessionRequestKeyResolver,
      @Value("${gateway.uq.serialized-chat-commands:1045}") String serializedChatCommands
  ) {
    this.messageService = messageService;
    this.requestTracker = requestTracker;
    this.sessionInFlightStore = sessionInFlightStore;
    this.sessionRequestKeyResolver = sessionRequestKeyResolver;
    this.serializedChatCommands = parseCommands(serializedChatCommands);
    this.responseCommands = responseCommands(serializedChatCommands);
  }

  @Override
  public List<String> cmds() {
    return responseCommands;
  }

  @Override
  public void handle(MessageSender.Channel channel, Map<String, Object> message) {
    String command = commandOf(message);
    if (command == null) {
      return;
    }
    try {
      requestTracker.complete(message);
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
    } finally {
      releaseSessionLock(channel, message);
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

  private void releaseSessionLock(MessageSender.Channel channel, Map<String, Object> message) {
    if (channel != MessageSender.Channel.CHAT) {
      return;
    }
    String sessionKey = sessionRequestKeyResolver.resolve(message);
    if (sessionKey == null) {
      releaseSerializedCommandLock(message);
      return;
    }
    sessionInFlightStore.release(sessionKey);
    releaseSerializedCommandLock(message);
  }

  private void releaseSerializedCommandLock(Map<String, Object> message) {
    String responseCommand = commandOf(message);
    if (responseCommand == null) {
      return;
    }
    int command;
    try {
      command = parseCommand(responseCommand);
    } catch (Exception e) {
      return;
    }
    int requestCommand = command - 1;
    if (serializedChatCommands.contains(requestCommand)) {
      sessionInFlightStore.release(UqSerializationKeys.lockKey(requestCommand));
    }
  }

  private static List<String> responseCommands(String serializedChatCommands) {
    Set<String> commands = new LinkedHashSet<>(List.of("1538", "1540", "1542", "1544", "1546"));
    parseCommands(serializedChatCommands).stream()
        .map(command -> String.valueOf(command + 1))
        .forEach(commands::add);
    return List.copyOf(commands);
  }

  private static List<Integer> parseCommands(String commands) {
    if (commands == null || commands.isBlank()) {
      return List.of();
    }
    List<Integer> parsed = new ArrayList<>();
    Arrays.stream(commands.split(","))
        .map(String::trim)
        .filter(value -> !value.isEmpty())
        .forEach(value -> parsed.add(parseCommand(value)));
    return parsed;
  }

  private static int parseCommand(String value) {
    if (value.startsWith("0x") || value.startsWith("0X")) {
      return Integer.parseInt(value.substring(2), 16);
    }
    return Integer.parseInt(value);
  }
}
