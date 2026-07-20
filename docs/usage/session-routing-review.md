# 세션 유지 및 라우팅 전환 검토

이 문서는 현재 `lmgx-gateway`가 우리가 원하는 운영 방식대로 동작하는지 코드 기준으로 점검한 결과를 정리한다.

## 목표

- 한번 맺은 WebSocket 세션은 가능한 한 유지한다.
- `ping/pong` 실패는 세션 종료가 아니라 라우팅 대상 변경 신호로만 사용한다.
- 우선순위, 그룹순위, 복구 정책은 유지한다.
- 세션 종료는 타겟 서버가 실제로 끊거나, 명시적인 pause 같은 운영 제어가 있을 때만 발생한다.

## 현재 동작 요약

### 0. 빈 등록 방식

현재 `GatewayWsClient`는 `@Component`로 스프링이 직접 생성한다.
`GatewayBeans`는 `ProbeWsClient`, `TargetToggleStore`, `InstanceControlStore`만 등록한다.

- `GatewayWsClient` 참조: [GatewayWsClient.java](../../src/main/java/com/lmgx/gateway/connection/GatewayWsClient.java:26)
- `GatewayBeans` 참조: [GatewayBeans.java](../../src/main/java/com/lmgx/gateway/config/GatewayBeans.java:1)

### 1. 초기 연결

애플리케이션 시작 시 설정된 모든 타겟에 대해 chat/email 세션 연결을 시도한다. 현재 active 타겟이 있으면 그 값을 `currentUrl`과 라우팅 기준으로 설정한 뒤 연결을 유지한다.

- `FailoverLoop.onReady()`에서 전체 타겟 연결 후 active 타겟을 연결한다.
- 참조: [FailoverLoop.java](../../src/main/java/com/lmgx/gateway/connection/FailoverLoop.java:121)

### 2. 주기적 점검

매 tick마다 다음 순서로 점검한다.

1. pause 상태면 전체 세션을 닫는다.
2. 일정 주기마다 모든 타겟의 연결 상태를 다시 확인한다.
3. 현재 active 타겟에 대해 chat/email ping과 HA active 여부를 확인한다.
4. 현재 active가 불안정하면 ring 순서대로 다른 타겟을 찾고 active만 교체한다.

- `disconnectAll()`은 pause에서만 사용한다.
- `ensureAllTargetConnections()`는 누락된 연결만 채우는 역할이다.
- `checkAndRepairChannel()`은 ping 결과만 보고 세션을 닫지 않는다.
- 참조: [FailoverLoop.java](../../src/main/java/com/lmgx/gateway/connection/FailoverLoop.java:130), [FailoverLoop.java](../../src/main/java/com/lmgx/gateway/connection/FailoverLoop.java:146), [FailoverLoop.java](../../src/main/java/com/lmgx/gateway/connection/FailoverLoop.java:399)

### 3. 라우팅 전환

라우팅 대상이 바뀌면 `switchTo()`가 수행된다.

- `active`를 새 타겟으로 교체한다.
- `GatewayWsClient.currentUrl`도 새 타겟으로 바꾼다.
- route 전환 중에는 새 세션을 만들지 않는다.
- 기존에 열려 있던 다른 타겟 세션은 닫지 않는다.

- 참조: [FailoverLoop.java](../../src/main/java/com/lmgx/gateway/connection/FailoverLoop.java:311), [FailoverLoop.java](../../src/main/java/com/lmgx/gateway/connection/FailoverLoop.java:319)

### 4. 복귀 정책

`prefer` / `recover` 조건은 그대로 유지된다.

- 더 높은 우선순위 타겟이 여러 tick 동안 연속으로 정상일 때만 `UPGRADE` 복귀한다.
- 현재 그룹의 복구 대상이 안정화되면 `RECOVER` 복귀한다.
- 이 판단은 세션을 닫는 것과 무관하다.

- 참조: [FailoverLoop.java](../../src/main/java/com/lmgx/gateway/connection/FailoverLoop.java:192), [FailoverLoop.java](../../src/main/java/com/lmgx/gateway/connection/FailoverLoop.java:210)

## 세션 종료가 일어나는 경우

현재 코드에서 세션 종료는 다음 경우로 제한된다.

1. 타겟 서버가 실제로 세션을 종료한 경우
   - `afterConnectionClosed()` / `handleTransportError()`로 관측된다.
2. 인스턴스 pause
   - `disconnectAll()`이 전체 세션을 닫는다.
3. 중복/오래된 세션 정리
   - 같은 타겟/채널에 새 세션이 정상적으로 열리면 이전 참조를 정리한다.
4. 연결 실패 후 동일 타겟/채널을 다시 연결하는 경우
   - 현재는 `connect()` 경로만 남아 있어서, 세션이 실제로 끊어진 뒤에만 다시 열린다.

- `disconnectAll()` 참조: [GatewayWsClient.java](../../src/main/java/com/lmgx/gateway/connection/GatewayWsClient.java:351)
- 세션 close 콜백 참조: [GatewayWsClient.java](../../src/main/java/com/lmgx/gateway/connection/GatewayWsClient.java:461)
- 중복 세션 정리 참조: [GatewayWsClient.java](../../src/main/java/com/lmgx/gateway/connection/GatewayWsClient.java:224)

## 요구사항 적합성 검토

### 맞는 부분

- ping/pong 실패가 곧바로 세션 종료로 이어지지 않는다.
- 라우팅 전환은 기존 세션을 유지한 채 active 포인터만 바꾸는 방식이다.
- route 후보 탐색 중에는 세션이 없는 타겟에 새 연결을 만들지 않는다.
- 우선순위와 그룹 복귀 로직은 그대로 동작한다.
- backup 타겟도 미리 열어두고, 필요할 때 그 세션을 재사용할 수 있다.

### 주의할 부분

- pause는 의도적으로 전체 세션을 닫는다. 운영 제어용 예외다.
- `ensureAllTargetConnections()`는 모든 설정 타겟의 세션을 계속 준비 상태로 두는 동작이다.

- 백업 연결 유지 참조: [FailoverLoop.java](../../src/main/java/com/lmgx/gateway/connection/FailoverLoop.java:237)

## 결론

현재 구현은 우리가 정한 운영 방식에 거의 맞는다.

- 세션은 서버가 끊지 않는 한 유지한다.
- 우리 쪽은 ping/pong과 HA 상태를 보고 route target만 바꾼다.
- 우선순위/복구는 유지된다.

남는 명시적 예외는 `pause`에 의한 전체 종료뿐이다. 이 경로까지 없애려면 운영 제어 정책 자체를 다시 정해야 한다.
