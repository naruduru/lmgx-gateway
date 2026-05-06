# 인스턴스 백엔드 구현 체크리스트

아래 항목은 다른 프로젝트에서 **인스턴스 기능만 백엔드로 구현**할 때 필요한 최소 구성입니다.
UI는 제외합니다.

상세 내용은 `docs/usage/instance-backend-guide.md`를 참고하세요.

## 1) 핵심 패키지/클래스

- 연결/스위칭
  - `src/main/java/com/lmgx/gateway/connection/FailoverLoop.java`
  - `src/main/java/com/lmgx/gateway/connection/GatewayWsClient.java`
  - `src/main/java/com/lmgx/gateway/connection/MessageSender.java`
  - `src/main/java/com/lmgx/gateway/connection/GatewayMessageSender.java`
- 전송 진입점
  - `src/main/java/com/lmgx/gateway/message/UqSender.java`
- 수신 처리
  - `src/main/java/com/lmgx/gateway/connection/IncomingCommandHandler.java`
  - `src/main/java/com/lmgx/gateway/connection/IncomingMessageDispatcher.java`
- 인스턴스 제어(선택)
  - `src/main/java/com/lmgx/gateway/instance/InstanceControlStore.java`
  - `src/main/java/com/lmgx/gateway/instance/InstanceAdminController.java`
  - `src/main/java/com/lmgx/gateway/instance/InstanceAdminService.java`

## 2) REST API (백엔드)

- 메시지 전송
  - `POST /gateway/chat/send`
  - `POST /gateway/email/send`
- 상태 조회 (선택)
  - `GET /gateway/status`
- 인스턴스 제어 (선택)
  - `POST /admin/instance/{id}/pause?enabled=true|false`
  - `GET /admin/instance/{id}/pause`

## 3) 설정 파일

`src/main/resources/application.yml`

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

로컬/개발에서 일부만 사용할 때:

```yaml
gateway:
  targets:
    U1: ws://127.0.0.1:6701/clientws/U1
    U2:
    A1: ws://127.0.0.1:6703/clientws/A1
    A2:
```

`U2`, `A2`를 비워도 코드에서 자동 제외됩니다.

## 4) 동작 모델

- 시작 후 `U1/U2/A1/A2` 전체 타겟 세션 유지 시도
- 타겟별 chat/email 2세션 유지 (`HostKind` 분리)
- failover는 `ring/recover/prefer` + 세션 상태 기반 판정

`ring/recover/prefer` 요약:
- `ring`: 장애 시 탐색 순서
- `recover`: 안정화 후 복귀 가능한 목표군
- `prefer`: 현재보다 우선순위가 높아 업그레이드 가능한 목표군
- `recover.enabled`: recover 로직 사용 여부
- `prefer.enabled`: prefer 로직 사용 여부

## 5) 메시지 규격

- `Command` 필수
- `Command` 숫자 타입 전송
- `cmd` 필드는 사용하지 않음

## 6) 송신 규칙

- 서비스/컨트롤러/배치는 `UqSender`만 사용
- `MessageSender` 직접 호출은 인프라 레이어(`UqSender` 내부)로 제한

## 7) 체크포인트

- [ ] `GatewayBeans`에 `GatewayWsClient`, `MessageSender`, `UqSender` 등록
- [ ] `gateway.ws.ack-timeout-ms` 설정
- [ ] `UqSender`로 전송 성공
- [ ] `IncomingCommandHandler` 수신 라우팅 확인
- [ ] `Command=1~4` 하트비트 정상 동작
- [ ] `U1/U2/A1/A2` 타겟별 chat/email 세션 오픈 상태 확인
