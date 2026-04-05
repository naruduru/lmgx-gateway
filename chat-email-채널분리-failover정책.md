# Chat/Email 채널 분리 및 Failover 정책 정리

## 배경

기존 구조에서는 타겟 서버에 대해 채팅(1), 이메일(2) WebSocket 연결을 사실상 한 묶음처럼 취급하고 있었다.

이 때문에 아래와 같은 문제가 발생할 수 있었다.

- 이메일(2)만 라이센스 검증 등으로 끊겼는데 채팅(1)까지 같이 재연결됨
- 한 채널만 장애여도 전체 타겟 장애처럼 처리됨
- 정상인 채널도 함께 끊기거나 다시 붙는 불필요한 동작이 발생함

운영 요구사항은 명확하다.

- 정상인 채널은 어떤 경우에도 유지해야 함
- 죽은 채널만 재연결 시도해야 함
- 둘 다 죽었을 때만 failover 진행

## 정책

정책은 아래 3가지 경우의 수를 기준으로 한다.

### 1. 채팅 연결 끊김 + 이메일 연결 정상

- 이메일 연결은 유지
- 채팅 연결만 재연결 시도
- failover 진행하지 않음

### 2. 채팅 연결 정상 + 이메일 연결 끊김

- 채팅 연결은 유지
- 이메일 연결만 재연결 시도
- failover 진행하지 않음

### 3. 채팅 연결 끊김 + 이메일 연결 끊김

- 두 채널 모두 장애 상태
- 이때만 현재 타겟 장애로 판단
- 현재 타겟 복구 시도 후, 필요 시 failover 진행

즉 최종 정책은 아래 한 줄로 정리된다.

`둘 중 하나라도 살아 있으면 살아 있는 채널은 유지하고 죽은 채널만 복구한다. 둘 다 죽었을 때만 failover한다.`

## 설계 방향

기존 `SessionPair` 중심 구조 대신, 채널별 독립 상태 관리 구조로 변경한다.

### 핵심 원칙

- `chat`, `email` 세션은 완전히 별개로 관리
- reconnect도 채널별로 따로 수행
- 정상인 채널은 절대 건드리지 않음
- failover 판단은 채널 개별 상태를 본 뒤 최종적으로 결정

## 코드 변경 방향

## 1. GatewayWsClient 변경

파일:

- `src/main/java/com/lmgx/gateway/connection/GatewayWsClient.java`

### 변경 전

- `SessionPair`로 chat/email를 한 묶음으로 관리
- 한 채널 reconnect 시 pair 전체를 닫을 수 있는 구조
- email 문제로 chat까지 영향 받을 수 있음

### 변경 후

- `chatSessions`
- `emailSessions`
- `pendingChat`
- `pendingEmail`

위 구조로 채널별 상태를 완전히 분리

### 추가/변경된 주요 메서드

- `connectChat(String wsUrl)`
- `connectEmail(String wsUrl)`
- `connectChatForce(String wsUrl)`
- `connectEmailForce(String wsUrl)`
- `disconnectChat(String url)`
- `disconnectEmail(String url)`
- `pingChat(String url)`
- `pingEmail(String url)`

### 의미

- 채팅 reconnect가 이메일 세션을 건드리지 않음
- 이메일 reconnect가 채팅 세션을 건드리지 않음
- 둘 다 죽었을 때만 `connectForce(url)`로 전체 복구 가능

## 2. FailoverLoop 변경

파일:

- `src/main/java/com/lmgx/gateway/connection/FailoverLoop.java`

### 변경 전 문제

- 사실상 `chat` 또는 pair 기준으로 상태를 보고 failover 여부 판단
- 한 채널만 죽어도 전체 장애로 확대 해석될 여지가 있었음

### 변경 후 정책 C 적용

`tick()`에서 아래 순서로 처리

1. `chat` 채널 상태 확인 및 필요 시 `chat`만 재연결
2. `email` 채널 상태 확인 및 필요 시 `email`만 재연결
3. 둘 다 실패한 경우에만 `notReadyStreak` 증가
4. 둘 다 실패가 일정 횟수 이상 지속되면 failover 대상 탐색
5. 다른 타겟도 둘 다 죽어 있으면 현재 타겟에 대해 전체 reconnect 시도

### 새 판단 기준

- `chatOk && emailOk`
  완전 정상

- `chatOk || emailOk`
  부분 정상
  failover 금지
  죽은 채널만 reconnect

- `!chatOk && !emailOk`
  전체 장애
  이때만 failover 검토

## ring scan / 생존 판정 기준

`findFirstAliveInRing()`에서 사용하는 생존 판정도 변경

기존:

- 사실상 한 채널 또는 pair 기준 판단

변경 후:

- `chatUp || emailUp` 이면 해당 타겟은 살아있는 것으로 판단
- 즉 둘 중 하나라도 살아 있으면 현재 타겟은 유지 가치가 있음

## 기대 동작

### 케이스 A

- chat down
- email up

결과:

- email 유지
- chat만 reconnect
- failover 안 함

### 케이스 B

- chat up
- email down

결과:

- chat 유지
- email만 reconnect
- failover 안 함

### 케이스 C

- chat down
- email down

결과:

- 해당 타겟 전체 장애로 판단
- failover 후보 탐색
- 필요 시 target switch

## 운영상 기대 효과

- 부분 장애를 전체 장애로 오판하지 않음
- 정상 세션을 불필요하게 끊지 않음
- 이메일 라이센스 문제처럼 특정 채널만 끊기는 경우에 영향 범위를 최소화
- 실제 target failover는 정말 필요한 경우에만 발생

## 검증 방법

아래 3가지 시나리오를 운영/개발 환경에서 반드시 확인한다.

### 시나리오 1

- 채팅 끊김
- 이메일 정상

확인 포인트:

- 이메일 세션 유지 여부
- 채팅만 reconnect 되는지
- failover 미발생 여부

### 시나리오 2

- 채팅 정상
- 이메일 끊김

확인 포인트:

- 채팅 세션 유지 여부
- 이메일만 reconnect 되는지
- failover 미발생 여부

### 시나리오 3

- 채팅 끊김
- 이메일 끊김

확인 포인트:

- failover 조건 진입 여부
- ring scan 수행 여부
- 다른 타겟 전환 또는 전체 reconnect 수행 여부

## 현재 반영 파일

- `src/main/java/com/lmgx/gateway/connection/GatewayWsClient.java`
- `src/main/java/com/lmgx/gateway/connection/FailoverLoop.java`

## 빌드 검증

아래 명령으로 컴파일 확인 완료

```bash
./gradlew compileJava
```

컴파일 성공.
