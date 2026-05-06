# 인스턴스 백엔드 통합 가이드 (상세)

이 문서는 다른 회사 프로젝트에 **lmgx-gateway의 소켓 클라이언트/인스턴스 기능만** 이식할 때의 절차를 설명합니다.
UI는 제외합니다.

## 1) 가져올 최소 파일

아래 패키지 기준으로 복사합니다.

- `com.lmgx.gateway.connection`
  - `FailoverLoop.java`
  - `GatewayWsClient.java`
  - `MessageSender.java`
  - `GatewayMessageSender.java`
  - `IncomingCommandHandler.java`
  - `IncomingMessageDispatcher.java`
- `com.lmgx.gateway.message`
  - `UqSender.java`
- `com.lmgx.gateway.instance` (선택)
  - `InstanceControlStore.java`
  - `InstanceAdminController.java`
  - `InstanceAdminService.java`

서버 상태 API가 필요하면 아래도 같이 복사합니다.

- `com.lmgx.gateway.api`
  - `GatewayController.java`
  - `GatewayStatusController.java`

## 2) 스프링 빈 설정

`GatewayBeans`를 동일하게 옮기거나, 프로젝트에 맞게 아래 빈을 등록합니다.

- `GatewayWsClient`
- `MessageSender` (구현: `GatewayMessageSender`)
- `UqSender`
- `IncomingMessageDispatcher`
- `InstanceControlStore` (선택)

핵심 포인트:
- `gateway.ws.ack-timeout-ms` 값을 반드시 넣습니다.
- `WebSocketContainer`를 사용하는 프로젝트라면 `GatewayWsClient`에 주입하도록 구성합니다.
- 애플리케이션 코드에서는 전송을 `UqSender`로만 호출하는 것을 권장합니다.

## 3) application.yml 필수 설정

```yaml
gateway:
  targets:
    U1: ws://127.0.0.1:6701/clientws/U1
    U2: ws://127.0.0.1:6702/clientws/U2
    A1: ws://127.0.0.1:6703/clientws/A1
    A2: ws://127.0.0.1:6704/clientws/A2

  ring:
    G1: U1,U2,A1,A2
    G2: A1,A2,U1,U2

  recover:
    enabled: true
    G1: U1,U2
    G2: A1,A2

  prefer:
    enabled: true
    G1: U1,U2
    G2: A1,A2

  ws:
    ack-timeout-ms: 3000
```

로컬/개발처럼 일부 타겟만 쓰는 경우:
- `U2`, `A2`를 비워두어도 됩니다. (`U2:` / `A2:`)
- `ring/recover/prefer`에 포함되어 있어도, 코드에서 실제 URL이 없는 타겟은 자동 제외합니다.

## 4) 연결/헬스 모델 (현재 소스 기준)

- 앱 시작 후 `U1/U2/A1/A2` 전체 타겟에 대해 chat/email 세션을 유지 시도합니다.
- 각 타겟은 chat/email 2세션으로 동작합니다. (`HostKind` C/I 분리)
- failover/ring/recover/prefer 판정은 유지 세션 상태(`isReady(url)`, `pingBoth(url)`) 기준으로 수행합니다.

### ring / recover / prefer 의미

- `ring`: 장애 시 순차 탐색 우선순위입니다.
- `recover`: 정상화 시 되돌아갈 수 있는 목표군입니다.
- `prefer`: 현재보다 더 높은 우선순위로 "승급" 가능한 목표군입니다.
- `recover.enabled`: `false`면 recover 복귀 로직을 수행하지 않습니다.
- `prefer.enabled`: `false`면 prefer 업그레이드 로직을 수행하지 않습니다.

기본값 예시:
- `ring.G1=U1,U2,A1,A2`
- `recover.G1=U1,U2`
- `prefer.G1=U1,U2`

동작 예시(G1 기준):
1. 현재 `U1` 장애 시 `ring` 순서대로 `U2 -> A1 -> A2` 탐색 후 첫 정상 타겟으로 전환
2. `A1`에서 운영 중이고 `U1`이 다시 안정되면 `prefer/recover` 조건 충족 시 `U1`로 복귀
3. 같은 그룹 내에서도 `prefer` 앞쪽 타겟이 안정적으로 살아나면 업그레이드 전환

## 5) 전송 규격

- `cmd` 필드는 사용하지 않습니다.
- `Command`만 사용합니다.
- `Command` 값은 **숫자 타입으로 전송**합니다.

예시:

```json
{
  "Command": 601,
  "UCID": "U-20240207-0001",
  "CallingUserId": "user01",
  "UserData": "payload"
}
```

## 6) 송신 흐름

전송은 `UqSender` 하나로 통일합니다.

```java
import com.lmgx.gateway.connection.MessageSender;
import com.lmgx.gateway.message.UqSender;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class UqClientService {
  private final UqSender uqSender;

  public UqClientService(UqSender uqSender) {
    this.uqSender = uqSender;
  }

  public String sendAny(Map<String, Object> payload) throws Exception {
    return uqSender.send(MessageSender.Channel.CHAT, payload);
  }
}
```

## 7) 수신 흐름

수신 핸들러는 `IncomingCommandHandler` 구현으로 처리합니다.

```java
import com.lmgx.gateway.connection.IncomingCommandHandler;
import com.lmgx.gateway.connection.MessageSender;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class UqCommandHandler implements IncomingCommandHandler {
  @Override
  public List<String> cmds() {
    return List.of("1538", "1540", "1542", "1544", "1546");
  }

  @Override
  public void handle(MessageSender.Channel channel, Map<String, Object> message) {
    // channel == CHAT / EMAIL
  }
}
```

## 8) 하트비트/핸드셰이크

1. 서버 -> 클라이언트: `Command=1`
2. 클라이언트 -> 서버: `Command=2` (`HostKind` 포함)
3. 클라이언트 -> 서버: `Command=3`
4. 서버 -> 클라이언트: `Command=4`

`GatewayWsClient`에 반영되어 있습니다.

## 9) 자주 발생하는 이슈

- `TimeoutException`: `gateway.ws.ack-timeout-ms`를 늘려 테스트하세요.
- `payload.Command is required`: `Command` 누락 여부를 확인하세요.
- `payload.Command must be numeric`: 문자열 숫자 포함 비정상 값 여부를 확인하세요.
- 세션 직후 종료: 상대 서버가 init 직후 끊는 경우 초기 응답 타이밍을 확인하세요.

## 10) 최소 점검 체크리스트

- [ ] `application.yml` 설정 완료
- [ ] `GatewayBeans`/빈 등록 완료
- [ ] `UqSender`로 송신 가능
- [ ] `IncomingCommandHandler`로 수신 라우팅 확인
- [ ] 하트비트 `Command=1~4` 정상 동작
- [ ] 전체 타겟(`U1/U2/A1/A2`) 세션 유지 상태 확인
