# Send 인수인계 매뉴얼

이 문서는 `send` 요청이 게이트웨이 내부에서 어떤 경로를 타는지, 어디를 확인해야 하는지, 장애 시 어떤 순서로 보는지를 정리한 운영/인수인계용 문서다.

## 1. 전체 흐름

업무 전송은 아래 순서로 처리된다.

1. 서비스 계층에서 `UqSender` 호출
2. `UqSender`가 payload 정리 및 요청 추적 처리
3. `GatewayMessageSender`가 현재 전송 가능한 타겟 URL을 선택
4. `FailoverLoop.ensureCommandTarget(...)`가 라우팅 가능한 타겟인지 최종 확인
5. `GatewayWsClient.sendChat(url, payload)` 또는 `sendEmail(url, payload)`로 실제 전송

핵심은 서비스/컨트롤러가 직접 `GatewayWsClient`를 호출하지 않는다는 점이다.

## 2. 주요 클래스 역할

### `UqSender`

- 외부에서 사용하는 전송 진입점이다.
- `Command` 정규화, request tracking, serialized chat command 제어를 담당한다.
- `send(channel, payload)`와 `sendAndAwait(channel, payload)`를 제공한다.

### `GatewayMessageSender`

- `MessageSender` 구현체다.
- send 직전에 `FailoverLoop.ensureCommandTarget(channel)`를 호출한다.
- 채널별 실제 전송은 `GatewayWsClient`에 위임한다.

### `FailoverLoop`

- 현재 타겟이 실제로 업무 전송 가능한지 판단한다.
- `active` 타겟, HA 상태, chat/email 연결 상태를 함께 본다.
- 라우팅 변경, 후보 탐색, health status 갱신을 담당한다.

### `GatewayWsClient`

- 실제 WebSocket 세션 관리와 메시지 송신을 담당한다.
- chat/email 두 채널 세션을 각각 관리한다.
- heartbeat, init handshake, session cleanup을 처리한다.

## 3. send 진입점

서비스/컨트롤러에서는 `UqSender`를 사용한다.

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

## 4. payload 규칙

- `cmd`는 사용하지 않는다.
- `Command`를 사용한다.
- `Command` 값은 숫자여야 한다.
- `UqSender`와 `GatewayWsClient` 모두 내부에서 `Command`를 정규화한다.

예시:

```json
{
  "Command": 601,
  "UCID": "20260813-0001",
  "to": "user01",
  "subject": "Hello",
  "message": "Test message"
}
```

## 5. 채널 규칙

- `CHAT`과 `EMAIL`은 독립 채널이다.
- `GatewayMessageSender`는 채널에 따라 `sendChat(...)` 또는 `sendEmail(...)`을 호출한다.
- `FailoverLoop.ensureCommandTarget(channel)`가 라우팅 가능한 타겟을 찾지 못하면 send 전에 예외가 날 수 있다.

## 6. 라우팅 규칙

전송 가능 판단은 아래 조건을 모두 만족해야 한다.

- chat 세션이 열려 있어야 한다.
- email 세션이 열려 있어야 한다.
- 타겟 HA 상태가 active여야 한다.

즉, 단순히 소켓이 연결되어 있는 것만으로는 부족하다.

`FailoverLoop`는 현재 타겟이 업무 불가로 보이면 ring 순서에 따라 다음 후보를 찾는다.

## 7. heartbeat와 send의 관계

heartbeat는 send의 전제 조건을 확인하는 용도다.

- `command=3`으로 heartbeat를 보낸다.
- `command=4` 응답을 받아야 정상으로 본다.
- 응답이 없으면 해당 타겟은 업무 불가로 판단할 수 있다.

주의:

- heartbeat timeout이 났다고 해서 무조건 세션을 닫지는 않는다.
- 세션을 실제로 닫는 주체는 보통 상대 타겟 시스템 또는 transport error다.
- send 실패와 session close는 분리해서 봐야 한다.

## 8. 장애 시 확인 순서

send 장애가 나면 아래 순서로 본다.

1. `UqSender`에서 `Command`가 정상 정규화됐는지 확인한다.
2. `GatewayMessageSender`가 어떤 URL을 선택했는지 확인한다.
3. `FailoverLoop.ensureCommandTarget(...)`가 어떤 이유로 다른 타겟을 반환했는지 확인한다.
4. `GatewayWsClient.readinessDebug()`를 확인한다.
5. `afterConnectionClosed(...)` 또는 `handleTransportError(...)`가 있었는지 확인한다.

## 9. 자주 보는 로그

### 타겟 선택 로그

- `switch: ...`
- `target not command-routable observed: ...`
- `No command-routable target found in ring, ...`

### 연결 로그

- `connect: url=..., channel=...`
- `ws established: ...`
- `ws closed: ...`
- `ws transport error: ...`

### heartbeat 로그

- `hb send: command=3, ...`
- `heartbeat timeout waiting command=4 ...`
- `heartbeat ack: ...`

## 10. 운영 체크리스트

- 서비스 코드는 `GatewayWsClient`를 직접 호출하지 않고 `UqSender`를 사용한다.
- send 전에 `Command` 값이 숫자인지 확인한다.
- chat 전송과 email 전송을 혼동하지 않는다.
- active 타겟과 standby 타겟을 구분한다.
- heartbeat timeout과 session close를 같은 문제로 보지 않는다.

## 11. 정리

send 경로의 핵심은 다음 한 줄로 정리된다.

> `UqSender`가 payload를 정리하고, `GatewayMessageSender`가 타겟을 고른 뒤, `GatewayWsClient`가 실제 전송을 수행한다.

장애 분석은 항상 이 순서로 좁혀서 보면 된다.

