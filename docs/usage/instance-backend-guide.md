# 인스턴스 백엔드 통합 가이드 (상세)

이 문서는 다른 회사 프로젝트에 **lmgx-gateway의 소켓 클라이언트/인스턴스 기능만** 이식할 때의 절차를 상세히 설명합니다.
UI는 제외합니다.

## 1) 가져올 최소 파일

아래 패키지 기준으로 복사합니다.

- `com.lmgx.gateway.connection`
  - `FailoverLoop.java`
  - `GatewayWsClient.java`
  - `ProbeWsClient.java`
  - `AckRegistry.java`
  - `MessageSender.java`
  - `GatewayMessageSender.java`
  - `IncomingCommandHandler.java`
  - `IncomingMessageDispatcher.java`
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
- `ProbeWsClient`
- `MessageSender` (구현: `GatewayMessageSender`)
- `IncomingMessageDispatcher`
- `InstanceControlStore` (선택)

핵심 포인트:
- `gateway.ws.ack-timeout-ms` 값을 반드시 넣습니다.
- `WebSocketContainer`를 사용하는 프로젝트라면 `GatewayWsClient`에 주입하도록 구성합니다.

## 3) application.yml 필수 설정

최소 설정은 아래 4개 블록입니다.

```yaml
gateway:
  targets:
    A1: ws://127.0.0.1:6701/clientws/A1
    A2: ws://127.0.0.1:6702/clientws/A2
    E1: ws://127.0.0.1:6703/clientws/E1
    E2: ws://127.0.0.1:6704/clientws/E2

  ring:
    G1: A1,A2,E1,E2
    G2: E1,E2,A1,A2

  recover:
    G1: A1,A2
    G2: E1,E2

  prefer:
    G1: A1,A2
    G2: E1,E2

  ws:
    ack-timeout-ms: 3000
```

## 4) 전송 규격 (업체 규격 맞춤)

- `cmd` 필드는 사용하지 않습니다.
- `Command`만 사용합니다.
- `Command` 값은 **숫자이지만 문자열로 전송**합니다.

예시:

```json
{
  "Command": "601",
  "UCID": "U-20240207-0001",
  "CallingUserId": "user01",
  "UserData": "payload"
}
```

## 5) 송신 흐름 (현재 소스 기준)

현재 프로젝트에 추가된 송신 샘플은 `UqSender`입니다.
한 파일에서 송신 5개를 관리하도록 구성되어 있습니다.

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

## 6) 수신 흐름 (현재 소스 기준)

현재 프로젝트에 추가된 수신 샘플은 `UqCommandHandler`입니다.
한 파일에서 수신 5개를 처리하도록 구성되어 있습니다.

```java
import com.lmgx.gateway.message.UqCommandHandler;
import com.lmgx.gateway.connection.MessageSender;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class UqCommandHandler implements com.lmgx.gateway.connection.IncomingCommandHandler {
  @Override
  public List<String> cmds() {
    return List.of("1538", "1540", "1542", "1544", "1546");
  }

  @Override
  public void handle(MessageSender.Channel channel, Map<String, Object> message) {
    // channel == CHAT / EMAIL 구분 가능
    // Command 값에 따라 업무 로직 분기 처리
  }
}
```

## 7) 하트비트/헬스 흐름

규격상 하트비트는 아래 순서로 동작합니다.

1. 서버 -> 클라이언트: `Command=1` (HBPeriod/HaState 포함)
2. 클라이언트 -> 서버: `Command=2` (HostKind/HBPeriod/HaState/ResultCode)
3. 클라이언트 -> 서버: `Command=3` (HaState)
4. 서버 -> 클라이언트: `Command=4` (HaState/NodeRole1/NodeRole2)

`GatewayWsClient`/`ProbeWsClient`에 이미 반영되어 있습니다.

## 8) 자주 발생하는 이슈

- `TimeoutException`: `gateway.ws.ack-timeout-ms`를 늘려서 테스트하세요.
- `Command` 타입: 서버는 숫자를 보내도 클라이언트는 문자열로 처리합니다.
- `cmd` 필드: 사용하지 않습니다. `Command`만 사용하세요.
- `ACK` 기반 메시지: 업체 규격에 없다면 사용하지 않습니다.

## 9) 주의사항

- 동기 전송: 현재 전송은 동기 방식이며, 대량 트래픽 환경에서는 지연이 커질 수 있습니다.
- 세션 종료 타이밍: 서버가 세션을 닫은 직후 전송하면 `session closed` 오류가 날 수 있습니다.
- 하트비트 지연: `HBPeriod`가 길어도 `ack-timeout-ms`는 짧게 유지하세요.
- 필드 대소문자: `Command`, `HBPeriod`, `HaState`는 대소문자 정확히 맞춰야 합니다.
- 채널 분리: CHAT/EMAIL을 반드시 분리해서 보내고, 수신 처리에서도 채널을 구분하세요.

## 10) 최소 점검 체크리스트

- [ ] `application.yml` 설정 완료
- [ ] `GatewayBeans`/빈 등록 완료
- [ ] `MessageSender`로 송신 가능
- [ ] `IncomingCommandHandler`로 수신 라우팅 확인
- [ ] 하트비트 `Command=1~4` 정상 동작
