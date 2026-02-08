# MessageSender 사용 예시

서비스/컨트롤러 어디서든 `MessageSender`를 주입 받아 채팅/이메일 전송을 구분해 사용할 수 있습니다.

## Payload 샘플

채팅/이메일 모두 JSON Map 형태로 전달하며 `cmd` 필드는 필수입니다.

```json
{
  "cmd": "CHAT_SEND",
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
