# Gateway HA State / Command Routing 변경내용 수기 공유 문서

git 접근이 어려운 회사 내부망 환경에서 변경사항을 사람 손으로 옮기기 쉽도록 파일 단위로 정리한 문서입니다.

이 문서는 아래 두 단계 변경을 함께 다룹니다.

- `2dc235d`: `Use instance-local HA state for WS init/heartbeat and add per-instance HA control`
- `1a33e1a`: `Route commands only to HA active targets`

---

## 1) 전체 변경 목적

- 소켓 연결 시 `Command=2`(init 응답), `Command=3`(heartbeat)에서 `HaState`를 타겟 서버 값이 아닌 우리 게이트웨이 인스턴스의 상태값으로 보내도록 변경.
- 운영 중 인스턴스별 HA 상태를 조회/변경할 수 있게 구성.
- 타겟 서버에서 수신한 `HaState`를 URL별로 저장.
- 실제 업무 명령은 `CHAT`, `EMAIL` 두 채널이 모두 준비됐고, 타겟 `HaState`가 active인 서버로만 전송.

`HaState` 의미:

- `1` = active
- `2` = standby

---

## 2) `2dc235d` 변경사항

### A. `src/main/java/com/lmgx/gateway/connection/GatewayWsClient.java`

핵심:

- 내부 HA 상태 변수명을 `haState`에서 `localHaState`로 명확화.
- `Command=1` 수신 후 `Command=2` 응답 시:
  - 수신한 peer `HaState`는 로그용으로만 사용.
  - 응답 payload `HaState`는 `localHaState` 사용.
- heartbeat(`Command=3`) 송신 시 payload `HaState`를 `localHaState`로 송신.
- `getLocalHaState()`, `setLocalHaState(int)` 추가.
- `normalizeHaState(int)` 추가.
  - `2`면 `2`
  - 나머지는 `1`

포인트:

- 프로토콜 상으로 우리 게이트웨이 노드가 자기 자신의 HA 상태를 광고하게 됨.

### B. `src/main/java/com/lmgx/gateway/instance/InstanceControlStore.java`

핵심:

- 기존 pause 상태(`AtomicBoolean paused`) 외에 `AtomicInteger haState` 필드 추가.
- 기본값은 `1`.
- `getHaState()`, `setHaState(int)` 추가.
- 내부 `normalizeHaState(int)`로 `1`, `2` 외 값을 방어.

포인트:

- 인스턴스별 HA 상태를 메모리에서 보관 가능.

### C. `src/main/java/com/lmgx/gateway/config/GatewayBeans.java`

핵심:

- `gatewayWsClient` 빈 생성 시 아래 의존/프로퍼티를 추가로 주입:
  - `InstanceControlStore instanceControlStore`
  - `@Value("${gateway.ws.ha-state:1}") int haState`
- 생성 직후 아래 초기화 수행:
  - `instanceControlStore.setHaState(haState)`
  - `gatewayWsClient.setLocalHaState(instanceControlStore.getHaState())`

포인트:

- 설정값으로 인스턴스 초기 HA 상태를 주입하고 WS 클라이언트와 동기화.

### D. `src/main/java/com/lmgx/gateway/instance/InstanceAdminController.java`

신규 API:

- `POST /admin/instance/{id}/ha-state?value=1|2`
- `GET /admin/instance/{id}/ha-state`

포인트:

- 운영 중 인스턴스별 HA 상태 조회/변경 가능.

### E. `src/main/java/com/lmgx/gateway/instance/InstanceAdminService.java`

핵심:

- `GatewayWsClient` 의존성 추가.
- 로컬 인스턴스 처리:
  - `setHaState()` 호출 시 `controlStore`와 `gatewayWsClient` 둘 다 즉시 반영.
- 원격 인스턴스 처리:
  - 기존 pause 처리 방식처럼 HTTP 프록시 호출로 전달.
- 추가 메서드:
  - `setHaState(String instanceId, int value)`
  - `getHaState(String instanceId)`
  - `getHaStateValue(String instanceId)`
  - `normalizeHaState(int value)`

포인트:

- 로컬/원격 공통 운영 인터페이스로 HA 상태 제어 가능.

---

## 3) `1a33e1a` 추가 변경사항

### 변경 목적

- 기존에는 세션이 살아 있으면 명령 전송 후보가 될 수 있었음.
- 이제는 타겟 서버가 heartbeat/init에서 알려준 `HaState`까지 확인해서 active 타겟에만 명령을 보냄.
- standby 타겟은 소켓이 연결돼 있고 heartbeat가 살아 있어도 명령 전송 대상에서 제외.

### A. `src/main/java/com/lmgx/gateway/connection/GatewayWsClient.java`

핵심:

- URL별 타겟 HA 상태 저장소 추가:
  - `ConcurrentMap<String, Integer> haStates`
- readiness 로그에 현재 타겟 HA 상태 추가:
  - `currentHaState=...`
- `isHealthy(String url)` 기준 변경:
  - 기존: `isReady(url)`
  - 변경: `isCommandRoutable(url)`
- 신규 메서드 추가:
  - `haStateOf(String url)`
  - `isHaActive(String url)`
  - `isCommandRoutable(String url)`
- `disconnectAll()` 호출 시 `haStates.clear()` 추가.
- init 수신(`Command=1`) 시 타겟 `HaState`를 `haStates.put(url, haState)`로 저장.
- heartbeat ack 수신(`Command=4` 또는 기존 판별 메서드가 인정하는 ACK) 시:
  - `ackHaState`를 읽음.
  - 기존 `haState` 값 갱신.
  - `haStates.put(url, ackHaState)`로 URL별 상태 저장.

포인트:

- `GatewayWsClient`가 각 타겟 URL의 최신 HA 상태를 기억함.
- `isCommandRoutable(url)`은 `isReady(url) && isHaActive(url)` 조건을 만족해야 true.
- `haStateOf(null)` 또는 아직 상태가 없는 URL은 기본값 `1`로 봄.

### B. `src/main/java/com/lmgx/gateway/connection/FailoverLoop.java`

핵심:

- tick 로그에 `haActive` 추가.
- 기존 판단:
  - `chatOk`, `emailOk` 중심.
- 변경 판단:
  - `chatOk && emailOk && haActive`를 모두 만족해야 command-routable.
- 기존 `findFirstAliveInRing(...)` 개념을 `findFirstCommandRoutableInRing(...)`로 변경.
- 기존 `isAliveAndLog(...)` 개념을 `isCommandRoutableAndLog(...)`로 변경.
- ring scan, prefer, recover 후보 선택 기준도 모두 command-routable 기준으로 변경.
- 신규 public 메서드 추가:
  - `ensureCommandTarget(MessageSender.Channel channel)`

`ensureCommandTarget(...)` 동작:

- 현재 active URL이 `ws.isCommandRoutable(current)`이면 그대로 반환.
- 현재 active가 전송 불가이면 ring에서 command-routable 타겟을 찾음.
- 다른 타겟을 찾으면 `switchTo(found, "SEND_GUARD_SWITCH")` 후 해당 URL 반환.
- 찾지 못하면 `IllegalStateException` 발생.

standby 처리:

- 현재 active가 standby이면 무조건 재접속을 반복하지 않음.
- command-routable 타겟이 없고 현재 active가 standby이면 active 타겟이 나타날 때까지 대기 로그를 남김.
- standby 때문에 전환하는 경우 reason은 `HA_STANDBY_RING_SCAN`.

포인트:

- failover 판단 기준이 단순 생존 여부에서 "명령을 보내도 되는 상태"로 강화됨.
- 두 채널이 모두 열려 있고 HA active인 타겟만 정상 후보가 됨.

### C. `src/main/java/com/lmgx/gateway/connection/GatewayMessageSender.java`

핵심:

- `FailoverLoop` 의존성 추가.
- 생성자 변경:
  - 기존: `GatewayMessageSender(GatewayWsClient ws)`
  - 변경: `GatewayMessageSender(GatewayWsClient ws, FailoverLoop failover)`
- `send(...)`에서 전송 전 `failover.ensureCommandTarget(channel)` 호출.
- 반환된 URL로 명시 전송:
  - `ws.sendChat(url, payload)`
  - `ws.sendEmail(url, payload)`

포인트:

- 명령 전송 직전에 마지막으로 command-routable 타겟을 보장함.
- Failover tick 주기 사이에 HA 상태가 바뀌어도 send 시점에서 방어 가능.

---

## 4) 수기 반영 순서 권장

1. `2dc235d` 변경사항을 먼저 반영.
2. `GatewayWsClient`에 로컬 HA 상태(`localHaState`) 관련 변경이 들어간 상태인지 확인.
3. `1a33e1a`의 URL별 타겟 HA 상태 저장소(`haStates`)를 추가.
4. `GatewayWsClient`에 `haStateOf`, `isHaActive`, `isCommandRoutable` 메서드 추가.
5. init/heartbeat ack 수신부에서 URL별 `haStates` 갱신 추가.
6. `FailoverLoop`의 alive 기준 메서드를 command-routable 기준으로 변경.
7. `FailoverLoop.ensureCommandTarget(...)` 추가.
8. `GatewayMessageSender`에서 `FailoverLoop`를 주입받고 send 전에 `ensureCommandTarget(...)` 호출.

---

## 5) 최종 체크리스트

- [ ] `GatewayWsClient`에서 init/heartbeat 송신 `HaState`가 우리 인스턴스의 `localHaState`를 사용하는지 확인.
- [ ] `GatewayWsClient`가 수신한 타겟 `HaState`를 URL별 `haStates`에 저장하는지 확인.
- [ ] `GatewayWsClient.isCommandRoutable(url)`이 `isReady(url) && isHaActive(url)` 조건인지 확인.
- [ ] `GatewayWsClient.disconnectAll()`에서 `haStates.clear()`가 호출되는지 확인.
- [ ] `InstanceControlStore`에 `haState` 저장 필드 및 getter/setter가 있는지 확인.
- [ ] `GatewayBeans`에 `gateway.ws.ha-state` 초기화 로직이 있는지 확인.
- [ ] `InstanceAdminController`에 `/ha-state` GET/POST 엔드포인트가 있는지 확인.
- [ ] `InstanceAdminService`에 HA 상태 set/get 및 원격 프록시 로직이 있는지 확인.
- [ ] `FailoverLoop`가 active 판단 시 `chatOk && emailOk && haActive`를 모두 확인하는지 확인.
- [ ] `FailoverLoop`의 ring/prefer/recover 후보 탐색이 command-routable 기준인지 확인.
- [ ] `FailoverLoop.ensureCommandTarget(...)`가 있고, 전송 직전 대상 URL을 보장하는지 확인.
- [ ] `GatewayMessageSender`가 `ws.sendChat(payload)`, `ws.sendEmail(payload)` 대신 URL 지정 메서드를 호출하는지 확인.
- [ ] 입력값 정규화 규칙이 동일한지 확인.
  - 로컬 인스턴스 HA 상태: `2`면 `2`, 나머지는 `1`
  - 타겟 HA 상태 판정: `1`만 active

---

## 6) 참고 커밋

- `2dc235d`: `Use instance-local HA state for WS init/heartbeat and add per-instance HA control`
- `1a33e1a`: `Route commands only to HA active targets`
