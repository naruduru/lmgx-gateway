# MessageSender 사용 예시

서비스/컨트롤러 어디서든 `MessageSender`를 주입 받아 채팅/이메일 전송을 구분해 사용할 수 있습니다.

## Payload 샘플

채팅/이메일 모두 JSON Map 형태로 전달하며 `Command` 필드는 필수입니다.
`Command` 값은 숫자지만 문자열로 전송합니다.

```json
{
  "Command": "601",
  "to": "user01",
  "subject": "Hello",
  "message": "Test message",
  "meta": {
    "priority": "normal",
    "source": "gateway"
  }
}
```

```java
import com.lmgx.gateway.connection.MessageSender;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class NotificationService {
  private final MessageSender sender;

  public NotificationService(MessageSender sender) {
    this.sender = sender;
  }

  public String sendChat(Map<String, Object> payload) throws Exception {
    return sender.send(MessageSender.Channel.CHAT, payload);
  }

  public String sendEmail(Map<String, Object> payload) throws Exception {
    return sender.send(MessageSender.Channel.EMAIL, payload);
  }
}
```

## 외부 패키지 사용 예시

```java
package com.example.orders;

import com.lmgx.gateway.connection.IncomingCommandHandler;
import com.lmgx.gateway.connection.MessageSender;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class OrderCmdHandler implements IncomingCommandHandler {
  @Override
  public List<String> cmds() {
    return List.of("601", "602");
  }

  @Override
  public void handle(MessageSender.Channel channel, Map<String, Object> message) {
    if (channel == MessageSender.Channel.CHAT) {
      // 주문 채팅 처리
    } else {
      // 주문 이메일 처리
    }
  }
}
```

## 수신 Command 핸들러 예시 (멀티 Command)

```java
import com.lmgx.gateway.connection.IncomingCommandHandler;
import com.lmgx.gateway.connection.MessageSender;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class CmdGroupHandler implements IncomingCommandHandler {
  @Override
  public List<String> cmds() {
    return List.of("999", "998", "997");
  }

  @Override
  public void handle(MessageSender.Channel channel, Map<String, Object> message) {
    if (channel == MessageSender.Channel.CHAT) {
      // chat 처리
    } else {
      // email 처리
    }
  }
}
```

```java
import com.lmgx.gateway.connection.MessageSender;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class NotifyController {
  private final MessageSender sender;

  public NotifyController(MessageSender sender) {
    this.sender = sender;
  }

  @PostMapping("/notify/chat")
  public Map<String, Object> notifyChat(@RequestBody Map<String, Object> body) throws Exception {
    return Map.of("ok", true, "requestId",
        sender.send(MessageSender.Channel.CHAT, body));
  }
}
```
