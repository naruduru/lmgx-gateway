# UqSender 사용 가이드

현재 프로젝트의 전송 진입점은 `UqSender` 하나로 통일합니다.
서비스/컨트롤러/배치에서는 `MessageSender`를 직접 호출하지 않고 `UqSender`를 사용하세요.

## 전송 원칙

- `cmd` 필드는 사용하지 않습니다.
- `Command`만 사용합니다.
- `Command` 값은 숫자 타입으로 전송합니다.
- `UqSender.send(channel, payload)`를 공용 진입점으로 사용합니다.

## Payload 샘플

```json
{
  "Command": 601,
  "to": "user01",
  "subject": "Hello",
  "message": "Test message",
  "meta": {
    "priority": "normal",
    "source": "gateway"
  }
}
```

## 기본 사용 예시

```java
import com.lmgx.gateway.connection.MessageSender;
import com.lmgx.gateway.message.UqSender;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class NotificationService {
  private final UqSender uqSender;

  public NotificationService(UqSender uqSender) {
    this.uqSender = uqSender;
  }

  public String sendChat(Map<String, Object> payload) throws Exception {
    return uqSender.send(MessageSender.Channel.CHAT, payload);
  }

  public String sendEmail(Map<String, Object> payload) throws Exception {
    return uqSender.send(MessageSender.Channel.EMAIL, payload);
  }
}
```

## UQ 전용 메서드 사용 예시

`UqSender`에는 업무 커맨드 전용 헬퍼(`sendRoute`, `sendCancel` 등)가 포함되어 있습니다.
고정 커맨드 전송이 필요한 경우 전용 메서드를 우선 사용하세요.

```java
import com.lmgx.gateway.message.UqSender;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class UqClientService {
  private final UqSender uqSender;

  public UqClientService(UqSender uqSender) {
    this.uqSender = uqSender;
  }

  public void sendRoute(String ucid, String userId) throws Exception {
    uqSender.sendRoute(Map.of(
        "UCID", ucid,
        "CallingUserId", userId
    ));
  }
}
```

## 수신 핸들러 예시

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
