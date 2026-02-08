# 인스턴스 백엔드 구현 체크리스트

아래 항목은 다른 프로젝트에서 **인스턴스 기능만 백엔드로 구현**할 때 필요한 파일/구성을 정리한 목록입니다.
UI는 제외합니다.

## 1) 핵심 패키지/클래스

- 연결/스위칭
  - `src/main/java/com/lmgx/gateway/connection/FailoverLoop.java`
  - `src/main/java/com/lmgx/gateway/connection/GatewayWsClient.java`
  - `src/main/java/com/lmgx/gateway/connection/ProbeWsClient.java`
  - `src/main/java/com/lmgx/gateway/connection/AckRegistry.java`
- 메시지 전송
  - `src/main/java/com/lmgx/gateway/connection/MessageSender.java`
  - `src/main/java/com/lmgx/gateway/connection/GatewayMessageSender.java`
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

- `src/main/resources/application.yml`
  - 타겟 주소/링/복귀/우선순위 설정
  - 예시
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
    ```

## 4) 스프링 설정

- `src/main/java/com/lmgx/gateway/config/GatewayBeans.java`
  - `GatewayWsClient`
  - `ProbeWsClient`
  - `InstanceControlStore`

## 5) DB 로깅 (선택)

DB를 쓰지 않는 경우 아래는 제외해도 됩니다.

- `src/main/java/com/lmgx/gateway/persist/*`
- `src/main/resources/mapper/GatewayLogMapper.xml`
- `docs/db/postgresql.md`

## 6) 메시지 Payload 필수 필드

`cmd`는 반드시 포함해야 합니다.

```json
{
  "cmd": "CHAT_SEND",
  "to": "user01",
  "subject": "Hello",
  "message": "Test message"
}
```

## 7) 권장 운영 설정

코드에 하드코딩된 값이 있으니, 운영 환경에 맞춰 조정이 필요할 수 있습니다.

- 연결/헬스 체크 관련
  - `FailoverLoop` tick 주기: 1초
  - `GatewayWsClient` ACK 타임아웃: 1초
  - `ProbeWsClient` 타임아웃: 1초
  - `PING_STALE_MS`: 5초
  - `PING_FAIL_THRESHOLD`: 3회
  - `FAIL_THRESHOLD`: 2회
  - `RECOVER_STABLE_THRESHOLD`: 5회
- 스레드 동작
  - 스프링 스케줄러(기본): 헬스/상태 갱신
  - 커넥터 스레드(전용 1개): WebSocket 연결/재연결

운영 특성(지연, 네트워크 품질, 타겟 안정성)에 따라 위 수치를 조정하세요.

## 8) 보안 설정 체크리스트

- 관리 API 보호
  - `/admin/target/*`, `/admin/instance/*`는 인증/권한 필수
  - 최소 IP allowlist 또는 API key 적용
- 모니터링 화면 보호
  - `/monitor`, `/monitor/api/*`는 내부망 전용 권장
  - 사내망 제한 또는 SSO 연동
- WebSocket 보안
  - 운영에서는 `wss` 사용
  - 인증 토큰(헤더/쿼리) 검토
  - Origin 제한 설정 검토
- 데이터 노출 방지
  - activeUrl/인스턴스/로그가 외부로 노출되지 않도록 분리
- DB 계정 권한 최소화
  - 로깅용 계정은 읽기/쓰기 최소 권한만 부여
