# 회사망 수기 적용 가이드: `1a33e1a` 이후 변경분

회사 내부망에서 GitHub 접근이 안 되는 상황을 기준으로, `1a33e1a`부터 최신 `aa104f6`까지의 변경을 손으로 옮기는 순서를 정리한 문서입니다.

적용 대상 커밋:

- `1a33e1a`: `Route commands only to HA active targets`
- `aa104f6`: `Separate local and target HA state handling`

---

## 1) 적용 목적

현재 운영 문제:

- A1이 standby(`HaState=2`)이고 A2가 active(`HaState=1`)인 상황에서,
- 우리 게이트웨이가 A1 소켓 연결만 보고 A1을 정상으로 판단함.
- 그 결과 heartbeat 이후 업무 command도 A1로 전송될 수 있음.

변경 후 기준:

- 소켓 연결만으로 정상 타겟으로 보지 않음.
- 업무 command 전송 가능 조건은 아래 세 가지를 모두 만족해야 함.
  - CHAT 소켓 연결 정상
  - EMAIL 소켓 연결 정상
  - 타겟 서버 `HaState=1`
- 우리 게이트웨이가 `Command=2`, `Command=3`에 담아 보내는 `HaState`는 타겟에서 받은 값이 아니라 우리 로컬 상태값을 사용함.
- 우리 서버는 기본 active이므로 기본 송신값은 `HaState=1`.

---

## 2) 변경 파일 목록

`1a33e1a` 변경 파일:

- `src/main/java/com/lmgx/gateway/connection/FailoverLoop.java`
- `src/main/java/com/lmgx/gateway/connection/GatewayMessageSender.java`
- `src/main/java/com/lmgx/gateway/connection/GatewayWsClient.java`

`aa104f6` 변경 파일:

- `src/main/java/com/lmgx/gateway/config/GatewayBeans.java`
- `src/main/java/com/lmgx/gateway/connection/GatewayWsClient.java`
- `src/main/java/com/lmgx/gateway/instance/InstanceAdminController.java`
- `src/main/java/com/lmgx/gateway/instance/InstanceAdminService.java`
- `src/main/java/com/lmgx/gateway/instance/InstanceControlStore.java`
- `docs/usage/gateway-ha-state-manual-share.md`

실제 운영 소스 반영에 필요한 Java 파일:

- `GatewayWsClient.java`
- `FailoverLoop.java`
- `GatewayMessageSender.java`
- `GatewayBeans.java`
- `InstanceControlStore.java`
- `InstanceAdminController.java`
- `InstanceAdminService.java`

---

## 3) 권장 적용 순서

### 1. `GatewayWsClient.java` 먼저 반영

가장 중요합니다.

반영해야 하는 핵심:

- 타겟별 HA 상태 저장용 필드 추가:

```java
private final ConcurrentMap<String, Integer> haStates = new ConcurrentHashMap<>();
```

- 우리 로컬 HA 상태 필드 추가 또는 기존 `haState` 분리:

```java
private volatile int localHaState = DEFAULT_HA_STATE;
```

- init 수신 시 타겟 `HaState`는 URL별로 저장만 함:

```java
int peerHaState = readInt(msg, "HaState", DEFAULT_HA_STATE);
haStates.put(url, peerHaState);
```

- init 응답 `Command=2`에는 우리 상태를 보냄:

```java
"HaState", localHaState
```

- heartbeat ack 수신 시 타겟 `HaState` 저장:

```java
int ackHaState = readInt(msg, "HaState", haStateOf(url));
haStates.put(url, ackHaState);
```

- heartbeat 송신 `Command=3`에는 우리 상태를 보냄:

```java
"HaState", localHaState
```

- 업무 command 라우팅 판정 메서드 추가:

```java
public int haStateOf(String url) {
  if (url == null) {
    return DEFAULT_HA_STATE;
  }
  return haStates.getOrDefault(url, DEFAULT_HA_STATE);
}

public boolean isHaActive(String url) {
  return haStateOf(url) == 1;
}

public boolean isCommandRoutable(String url) {
  return isReady(url) && isHaActive(url);
}
```

- 로컬 상태 setter/getter 추가:

```java
public int getLocalHaState() {
  return localHaState;
}

public void setLocalHaState(int haState) {
  this.localHaState = normalizeHaState(haState);
}
```

- 정규화 규칙 추가:

```java
private static int normalizeHaState(int value) {
  return value == 2 ? 2 : 1;
}
```

확인 포인트:

- `Command=2`, `Command=3` 송신 payload에 타겟에서 받은 `peerHaState`나 `ackHaState`를 넣으면 안 됨.
- 송신은 `localHaState`, 타겟 판정은 `haStates`를 사용해야 함.

### 2. `FailoverLoop.java` 반영

반영해야 하는 핵심:

- tick에서 현재 active 타겟의 HA active 여부 확인:

```java
boolean haActive = ws.isHaActive(active);
```

- 정상 조건을 아래처럼 강화:

```java
if (!(chatOk && emailOk && haActive)) {
```

- ring scan 기준을 alive가 아니라 command-routable로 변경:

```java
String found = findFirstCommandRoutableInRing(activeGroup, active);
```

- 후보 판정 메서드는 아래 조건을 사용:

```java
boolean routable = chatUp && emailUp && haActive;
```

- 업무 command 전송 직전 방어 메서드 추가:

```java
public synchronized String ensureCommandTarget(MessageSender.Channel channel) {
  String current = this.active;
  if (ws.isCommandRoutable(current)) {
    return current;
  }

  String found = findFirstCommandRoutableInRing(activeGroup, current);
  if (found != null) {
    if (!found.equals(current)) {
      switchTo(found, "SEND_GUARD_SWITCH");
    }
    return found;
  }

  throw new IllegalStateException("no command-routable target: group=" + activeGroup
      + ", channel=" + channel + ", active=" + current + ", haState=" + ws.haStateOf(current));
}
```

확인 포인트:

- A1 소켓이 살아 있어도 `ws.isHaActive(A1)`이 false이면 정상 타겟으로 인정하면 안 됨.
- prefer/recover/ring 후보 탐색도 `isCommandRoutableAndLog(...)` 기준이어야 함.

### 3. `GatewayMessageSender.java` 반영

반영해야 하는 핵심:

- `FailoverLoop` 의존성 추가:

```java
private final FailoverLoop failover;
```

- 생성자에서 주입:

```java
public GatewayMessageSender(GatewayWsClient ws, FailoverLoop failover) {
    this.ws = ws;
    this.failover = failover;
}
```

- send 직전에 command-routable URL 확보:

```java
String url = failover.ensureCommandTarget(channel);
```

- 전송은 currentUrl 기본값이 아니라 확보한 URL로 명시:

```java
return switch (channel) {
    case CHAT -> ws.sendChat(url, payload);
    case EMAIL -> ws.sendEmail(url, payload);
};
```

확인 포인트:

- `ws.sendChat(payload)`, `ws.sendEmail(payload)`를 그대로 쓰면 현재 URL로 나갈 수 있음.
- 반드시 `ws.sendChat(url, payload)`, `ws.sendEmail(url, payload)` 형태여야 함.

### 4. `InstanceControlStore.java` 반영

반영해야 하는 핵심:

- 로컬 인스턴스 HA 상태 저장 필드 추가:

```java
private final AtomicInteger haState = new AtomicInteger(1);
```

- getter/setter 추가:

```java
public int getHaState() {
    return haState.get();
}

public void setHaState(int value) {
    haState.set(normalizeHaState(value));
}
```

- 정규화 규칙:

```java
private static int normalizeHaState(int value) {
    return value == 2 ? 2 : 1;
}
```

확인 포인트:

- 기본값은 `1`.
- 우리 서버가 모두 active이면 별도 설정 없이 `1`로 동작.

### 5. `GatewayBeans.java` 반영

반영해야 하는 핵심:

- `gatewayWsClient(...)` 빈 생성 시 `InstanceControlStore`와 설정값 주입:

```java
InstanceControlStore instanceControlStore,
@Value("${gateway.ws.ha-state:1}") int haState
```

- 생성 직후 로컬 HA 상태 초기화:

```java
instanceControlStore.setHaState(haState);
GatewayWsClient gatewayWsClient = new GatewayWsClient(dispatcher, client, ackTimeoutMs);
gatewayWsClient.setLocalHaState(instanceControlStore.getHaState());
return gatewayWsClient;
```

확인 포인트:

- `gateway.ws.ha-state` 미설정 시 기본값은 `1`.
- 회사 운영 서버가 모두 active로 광고해야 하면 설정 추가 없이도 동작.

### 6. `InstanceAdminController.java` 반영

운영 중 로컬 HA 상태 확인/변경 API입니다.

추가 API:

```java
@PostMapping("/{id}/ha-state")
public Map<String, Object> setHaState(@PathVariable String id, @RequestParam int value) {
    return service.setHaState(id, value);
}

@GetMapping("/{id}/ha-state")
public Map<String, Object> getHaState(@PathVariable String id) {
    return service.getHaState(id);
}
```

확인 포인트:

- 기존 pause API는 유지.
- URL은 `/admin/instance/{id}/ha-state`.

### 7. `InstanceAdminService.java` 반영

반영해야 하는 핵심:

- `GatewayWsClient` 의존성 추가.
- 로컬 인스턴스 변경 시 `controlStore`와 `gatewayWsClient` 둘 다 반영:

```java
controlStore.setHaState(normalized);
gatewayWsClient.setLocalHaState(controlStore.getHaState());
```

- 원격 인스턴스는 기존 pause 방식처럼 HTTP proxy 호출.
- 추가 메서드:
  - `setHaState(String instanceId, int value)`
  - `getHaState(String instanceId)`
  - `getHaStateValue(String instanceId)`
  - `normalizeHaState(int value)`

확인 포인트:

- API로 `value=2`를 넣으면 standby로 광고.
- 그 외 값은 `1`로 정규화.
- 현재 운영 방침이 모두 active이면 기본값 `1`을 유지.

---

## 4) 적용 후 필수 확인

### 빌드 확인

회사망에서 Gradle 실행이 가능하면:

```bash
./gradlew test
```

테스트가 무겁거나 DB 연결 문제가 있으면 최소 컴파일이라도 확인:

```bash
./gradlew compileJava
```

### 설정 확인

기본값으로 사용하면 별도 설정 불필요:

```yaml
gateway:
  ws:
    ha-state: 1
```

명시하고 싶으면 운영 profile yml에 위 값을 추가.

### 운영 동작 확인 시나리오

상황:

- A1: standby, `HaState=2`
- A2: active, `HaState=1`
- A1/A2 모두 소켓 연결 가능

기대 동작:

- A1 heartbeat는 가능할 수 있음.
- A1은 command-routable이 아님.
- 업무 command 전송 시 A2가 선택됨.
- 로그에 A1은 `haState=2`, `routable=false`로 보여야 함.
- A2는 `haState=1`, `routable=true`로 보여야 함.

확인할 로그 키워드:

- `target not command-routable observed`
- `haState=2`
- `HA_STANDBY_RING_SCAN`
- `SEND_GUARD_SWITCH`
- `session-check`
- `routable=true`

---

## 5) 최종 체크리스트

- [ ] `GatewayWsClient`에 `localHaState`와 `haStates`가 분리되어 있음.
- [ ] `Command=2` 송신 `HaState`는 `localHaState`.
- [ ] `Command=3` 송신 `HaState`는 `localHaState`.
- [ ] 타겟에서 받은 `HaState`는 `haStates.put(url, value)`로 저장.
- [ ] `isCommandRoutable(url)` 조건이 `isReady(url) && isHaActive(url)`.
- [ ] `FailoverLoop` 정상 판단 조건이 `chatOk && emailOk && haActive`.
- [ ] ring/prefer/recover 후보 탐색이 command-routable 기준.
- [ ] `GatewayMessageSender`가 send 직전 `ensureCommandTarget(...)` 호출.
- [ ] `GatewayMessageSender`가 URL 지정 send 메서드를 호출.
- [ ] `InstanceControlStore` 기본 HA 상태가 `1`.
- [ ] `GatewayBeans`에서 `gateway.ws.ha-state:1` 기본값으로 초기화.
- [ ] `/admin/instance/{id}/ha-state` GET/POST API가 추가됨.

---

## 6) 한 줄 결론

소켓 연결은 생존 확인일 뿐이고, 업무 command 전송 기준은 반드시 `소켓 연결 + 타겟 HaState=1`입니다. 반대로 우리 게이트웨이가 heartbeat로 보내는 `HaState`는 타겟 상태가 아니라 우리 로컬 상태이며 기본값은 `1`입니다.
