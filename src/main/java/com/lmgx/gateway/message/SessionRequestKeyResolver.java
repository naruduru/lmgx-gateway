package com.lmgx.gateway.message;

import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
public class SessionRequestKeyResolver {
  private final List<String> keyFields;

  public SessionRequestKeyResolver(
      @Value("${gateway.uq.session-key-fields:UCID,ucid,SessionId,sessionId,SESSION_ID,ChatId,chatId,CHAT_ID,RoomId,roomId,ROOM_ID,UserId,userId,USER_ID,CallingUserId,callingUserId}") String keyFields
  ) {
    this.keyFields = parseFields(keyFields);
  }

  public String resolve(Map<String, Object> message) {
    for (String keyField : keyFields) {
      String value = keyValue(message, keyField);
      if (value != null) {
        return keyField + ":" + value;
      }
    }
    return null;
  }

  private static String keyValue(Map<String, Object> message, String expression) {
    if (!expression.contains("+")) {
      return stringValue(message, expression);
    }
    StringBuilder key = new StringBuilder();
    for (String field : expression.split("\\+")) {
      String value = stringValue(message, field.trim());
      if (value == null) {
        return null;
      }
      if (!key.isEmpty()) {
        key.append('|');
      }
      key.append(field.trim()).append('=').append(value);
    }
    return key.toString();
  }

  private static String stringValue(Map<String, Object> message, String key) {
    if (message == null) {
      return null;
    }
    Object value = message.get(key);
    if (value == null) {
      return null;
    }
    String text = String.valueOf(value).trim();
    return text.isEmpty() ? null : text;
  }

  private static List<String> parseFields(String fields) {
    if (fields == null || fields.isBlank()) {
      return List.of();
    }
    return Arrays.stream(fields.split(","))
        .map(String::trim)
        .filter(value -> !value.isEmpty())
        .toList();
  }
}
