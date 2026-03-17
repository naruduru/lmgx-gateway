# UqSender 고도화 작업 정리

## 배경

현재 `UqSender`를 통해 UQ 요청을 전송한 뒤, 응답 처리 중 다시 `UqSender`의 다른 함수를 호출하는 흐름이 발생할 수 있다.

기존 구조에서는 WebSocket 수신 콜백 안에서 바로 `IncomingMessageDispatcher -> UqCommandHandler -> UqMessageService`가 실행되었기 때문에,
응답 처리 중 다시 `UqSender`를 호출하면 같은 수신 콜스택 안에서 재진입이 발생할 수 있었다.

이 상황에서 세션 상태가 변하거나 닫히는 시점과 겹치면 `GatewayWsClient.send()`에서 `ws not open` 예외가 발생할 수 있다.

추가로, `UqSender` 요청 후 응답이 장시간 오지 않는 경우를 감지해서 종료 처리할 수 있는 timeout 메커니즘도 필요하다.

## 작업 목표

1. 수신 처리 중 `UqSender` 재호출 시 직접 재귀/재진입이 발생하지 않도록 구조를 분리한다.
2. `UqSender`로 보낸 UQ 요청에 대해 5분 응답 timeout을 감지할 수 있도록 한다.
3. timeout 발생 시 서비스 레벨에서 종료 처리 로직을 넣을 수 있는 확장 포인트를 제공한다.
4. 폐쇄망 환경에서도 확인 가능하도록 관련 최종 소스를 한 문서로 정리한다.

## 적용 내용 요약

### 1. 수신 처리 비동기 분리

`IncomingMessageDispatcher`에서 핸들러를 즉시 실행하지 않고 단일 스레드 executor에 넣어 처리하도록 변경했다.

효과:

- WebSocket 수신 콜백과 업무 핸들러 실행이 분리된다.
- 응답 처리 중 `UqSender` 재호출이 발생해도 같은 수신 스택에서 재진입하지 않는다.
- 처리 순서는 단일 스레드 큐로 유지된다.

### 2. UQ 요청 timeout 추적기 추가

`UqRequestTracker` 신규 컴포넌트를 추가했다.

동작 방식:

- `UqSender`가 `1537/1539/1541/1543/1545` 요청을 전송하면 `UCID` 기준으로 timeout 감시를 등록한다.
- `UqCommandHandler`가 `1538/1540/1542/1544/1546` 응답을 받으면 같은 `UCID` 기준으로 timeout 감시를 해제한다.
- 기본 5분(`300000ms`) 안에 응답이 없으면 `UqMessageService.onRequestTimeout(...)`을 호출한다.

설정값:

- `gateway.uq.response-timeout-ms`
- 기본값: `300000`

### 3. 서비스 확장 포인트 추가

`UqMessageService`에 아래 메서드를 추가했다.

```java
default void onRequestTimeout(Map<String, Object> message) {
}
```

서비스 구현체에서 이 메서드를 오버라이드하면, 5분 응답 미수신 시 종료 처리나 상태 정리 로직을 넣을 수 있다.

## 주의사항

현재 timeout 추적 키는 `UCID` 하나다.

즉, 같은 `UCID`로 여러 요청을 동시에 날리면 마지막 요청 기준으로 timeout 정보가 덮어써질 수 있다.
만약 동일 `UCID`로 병렬 요청을 여러 개 보낼 가능성이 있으면 다음 단계에서 추적 키를 아래처럼 확장하는 것이 맞다.

- `UCID + Command`
- 또는 별도 `requestKey`

## 수정 파일 목록

1. `src/main/java/com/lmgx/gateway/connection/IncomingMessageDispatcher.java`
2. `src/main/java/com/lmgx/gateway/message/UqSender.java`
3. `src/main/java/com/lmgx/gateway/message/UqCommandHandler.java`
4. `src/main/java/com/lmgx/gateway/message/UqMessageService.java`
5. `src/main/java/com/lmgx/gateway/message/DefaultUqMessageService.java`
6. `src/main/java/com/lmgx/gateway/message/UqRequestTracker.java`

## 파일별 최종 코드

### 1. IncomingMessageDispatcher.java

```java
package com.lmgx.gateway.connection;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class IncomingMessageDispatcher {
    private static final Logger log = LoggerFactory.getLogger(IncomingMessageDispatcher.class);

    private final Map<String, IncomingCommandHandler> handlers;
    private final ExecutorService dispatcherExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "gw-incoming-dispatcher");
        t.setDaemon(true);
        return t;
    });

    public IncomingMessageDispatcher(List<IncomingCommandHandler> handlerList) {
        Map<String, IncomingCommandHandler> map = new HashMap<>();
        for (IncomingCommandHandler handler : handlerList) {
            if (handler == null || handler.cmds() == null) {
                continue;
            }
            for (String cmd : handler.cmds()) {
                if (cmd != null && !cmd.isBlank()) {
                    map.put(cmd, handler);
                }
            }
        }
        this.handlers = map;
    }

    public void dispatch(MessageSender.Channel channel, Map<String, Object> message) {
        if (message == null) {
            return;
        }
        String command = commandOf(message);
        if (command == null || command.isBlank()) {
            return;
        }
        IncomingCommandHandler handler = handlers.get(command);
        if (handler == null) {
            return;
        }
        dispatcherExecutor.execute(() -> {
            try {
                handler.handle(channel, message);
            } catch (Exception e) {
                log.warn("incoming handler failed: command={}, channel={}, cause={}",
                    command, channel, e.getMessage(), e);
            }
        });
    }

    private static String commandOf(Map<String, Object> message) {
        Object cmdObj = message.get("Command");
        if (cmdObj == null) {
            cmdObj = message.get("command");
        }
        if (cmdObj == null) {
            return null;
        }
        return String.valueOf(cmdObj);
    }

    @PreDestroy
    public void shutdown() {
        dispatcherExecutor.shutdownNow();
    }
}
```

### 2. UqSender.java

```java
package com.lmgx.gateway.message;

import com.lmgx.gateway.connection.MessageSender;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class UqSender {
  private final MessageSender sender;
  private final UqRequestTracker requestTracker;

  public UqSender(MessageSender sender, UqRequestTracker requestTracker) {
    this.sender = sender;
    this.requestTracker = requestTracker;
  }

  public String send(MessageSender.Channel channel, Map<String, Object> payload) throws Exception {
    if (channel == null) {
      throw new IllegalArgumentException("channel is required");
    }
    Map<String, Object> data = new LinkedHashMap<>();
    if (payload != null) {
      data.putAll(payload);
    }
    Object command = data.get("Command");
    if (command == null) {
      command = data.remove("command");
    }
    data.remove("cmd");
    if (command == null) {
      throw new IllegalArgumentException("payload.Command is required");
    }
    int normalizedCommand = normalizeCommand(command);
    data.put("Command", normalizedCommand);
    String requestId = sender.send(channel, data);
    if (isTrackedCommand(normalizedCommand)) {
      requestTracker.arm(normalizedCommand, data);
    }
    return requestId;
  }

  public void sendRoute(Map<String, Object> payload) throws Exception {
    sendCommand(MessageSender.Channel.CHAT, 1537, payload);
  }

  public void sendCancel(Map<String, Object> payload) throws Exception {
    sendCommand(MessageSender.Channel.CHAT, 1539, payload);
  }

  public void sendUpdate(Map<String, Object> payload) throws Exception {
    sendCommand(MessageSender.Channel.CHAT, 1541, payload);
  }

  public void sendNotify(Map<String, Object> payload) throws Exception {
    sendCommand(MessageSender.Channel.CHAT, 1543, payload);
  }

  public void sendComplete(Map<String, Object> payload) throws Exception {
    sendCommand(MessageSender.Channel.CHAT, 1545, payload);
  }

  private void sendCommand(MessageSender.Channel channel, int command, Map<String, Object> payload) throws Exception {
    Map<String, Object> data = new LinkedHashMap<>();
    if (payload != null) {
      data.putAll(payload);
    }
    data.put("Command", command);
    sender.send(channel, data);
    requestTracker.arm(command, data);
  }

  private static int normalizeCommand(Object command) {
    if (command instanceof Number n) {
      return n.intValue();
    }
    try {
      return Integer.parseInt(String.valueOf(command).trim());
    } catch (Exception e) {
      throw new IllegalArgumentException("payload.Command must be numeric");
    }
  }

  private static boolean isTrackedCommand(int command) {
    return command == 1537
        || command == 1539
        || command == 1541
        || command == 1543
        || command == 1545;
  }
}
```

### 3. UqCommandHandler.java

```java
package com.lmgx.gateway.message;

import com.lmgx.gateway.connection.IncomingCommandHandler;
import com.lmgx.gateway.connection.MessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class UqCommandHandler implements IncomingCommandHandler {
  private static final Logger log = LoggerFactory.getLogger(UqCommandHandler.class);
  private final UqMessageService messageService;
  private final UqRequestTracker requestTracker;

  public UqCommandHandler(UqMessageService messageService, UqRequestTracker requestTracker) {
    this.messageService = messageService;
    this.requestTracker = requestTracker;
  }

  @Override
  public List<String> cmds() {
    return List.of("1538", "1540", "1542", "1544", "1546");
  }

  @Override
  public void handle(MessageSender.Channel channel, Map<String, Object> message) {
    String command = commandOf(message);
    if (command == null) {
      return;
    }
    requestTracker.complete(message);
    log.debug("uq recv: command={}, channel={}, payload={}", command, channel, message);
    switch (command) {
      case "1538" -> handleRouteRes(message);
      case "1540" -> handleTimeout(message);
      case "1542" -> handleSuccess(message);
      case "1544" -> handleFailure(message);
      case "1546" -> handleComplete(message);
      default -> {
      }
    }
  }

  private String commandOf(Map<String, Object> message) {
    Object cmdObj = message.get("Command");
    if (cmdObj == null) {
      cmdObj = message.get("command");
    }
    if (cmdObj == null) {
      return null;
    }
    return String.valueOf(cmdObj);
  }

  private void handleRouteRes(Map<String, Object> message) {
    String ucid = stringOf(message.get("UCID"));
    String callingUserId = stringOf(message.get("CallingUserId"));
    Object resultCode = message.get("ResultCode");
    log.info("route res: ucid={}, callingUserId={}, resultCode={}", ucid, callingUserId, resultCode);
    messageService.onRouteRes(message);
  }

  private void handleTimeout(Map<String, Object> message) {
    messageService.onTimeout(message);
  }

  private void handleSuccess(Map<String, Object> message) {
    messageService.onSuccess(message);
  }

  private void handleFailure(Map<String, Object> message) {
    messageService.onFailure(message);
  }

  private void handleComplete(Map<String, Object> message) {
    messageService.onComplete(message);
  }

  private String stringOf(Object v) {
    if (v == null) {
      return null;
    }
    return String.valueOf(v);
  }
}
```

### 4. UqMessageService.java

```java
package com.lmgx.gateway.message;

import java.util.Map;

public interface UqMessageService {
  void onRouteRes(Map<String, Object> message);

  void onTimeout(Map<String, Object> message);

  void onSuccess(Map<String, Object> message);

  void onFailure(Map<String, Object> message);

  void onComplete(Map<String, Object> message);

  default void onRequestTimeout(Map<String, Object> message) {
  }
}
```

### 5. DefaultUqMessageService.java

```java
package com.lmgx.gateway.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class DefaultUqMessageService implements UqMessageService {
  private static final Logger log = LoggerFactory.getLogger(DefaultUqMessageService.class);

  @Override
  public void onRouteRes(Map<String, Object> message) {
    log.info("service route res: {}", message);
  }

  @Override
  public void onTimeout(Map<String, Object> message) {
    log.info("service timeout: {}", message);
  }

  @Override
  public void onSuccess(Map<String, Object> message) {
    log.info("service success: {}", message);
  }

  @Override
  public void onFailure(Map<String, Object> message) {
    log.info("service failure: {}", message);
  }

  @Override
  public void onComplete(Map<String, Object> message) {
    log.info("service complete: {}", message);
  }

  @Override
  public void onRequestTimeout(Map<String, Object> message) {
    log.warn("service request timeout: {}", message);
  }
}
```

### 6. UqRequestTracker.java

```java
package com.lmgx.gateway.message;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Component
public class UqRequestTracker {
  private static final Logger log = LoggerFactory.getLogger(UqRequestTracker.class);

  private final UqMessageService messageService;
  private final long timeoutMs;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "uq-request-timeout");
    t.setDaemon(true);
    return t;
  });
  private final ConcurrentMap<String, TrackedRequest> requests = new ConcurrentHashMap<>();

  public UqRequestTracker(
      UqMessageService messageService,
      @Value("${gateway.uq.response-timeout-ms:300000}") long timeoutMs
  ) {
    this.messageService = messageService;
    this.timeoutMs = Math.max(1L, timeoutMs);
  }

  public void arm(int command, Map<String, Object> payload) {
    String ucid = ucidOf(payload);
    if (ucid == null) {
      return;
    }

    Map<String, Object> snapshot = new LinkedHashMap<>();
    if (payload != null) {
      snapshot.putAll(payload);
    }
    snapshot.put("Command", command);

    TrackedRequest next = new TrackedRequest(command, ucid, snapshot);
    TrackedRequest prev = requests.put(ucid, next);
    if (prev != null) {
      prev.cancel();
    }

    ScheduledFuture<?> future = scheduler.schedule(() -> onTimeout(next), timeoutMs, TimeUnit.MILLISECONDS);
    next.future = future;
    log.debug("uq request armed: ucid={}, command={}, timeoutMs={}", ucid, command, timeoutMs);
  }

  public void complete(Map<String, Object> message) {
    String ucid = ucidOf(message);
    if (ucid == null) {
      return;
    }
    TrackedRequest tracked = requests.remove(ucid);
    if (tracked == null) {
      return;
    }
    tracked.cancel();
    log.debug("uq request completed: ucid={}, requestCommand={}, responseCommand={}",
        ucid, tracked.command, commandOf(message));
  }

  private void onTimeout(TrackedRequest tracked) {
    if (!requests.remove(tracked.ucid, tracked)) {
      return;
    }

    Map<String, Object> timeoutMessage = new LinkedHashMap<>(tracked.payload);
    timeoutMessage.put("TimedOut", true);
    timeoutMessage.put("TimeoutMs", timeoutMs);
    timeoutMessage.put("TimeoutCommand", tracked.command);
    log.warn("uq request timeout: ucid={}, command={}, timeoutMs={}", tracked.ucid, tracked.command, timeoutMs);
    try {
      messageService.onRequestTimeout(timeoutMessage);
    } catch (Exception e) {
      log.warn("uq timeout handler failed: ucid={}, command={}, cause={}",
          tracked.ucid, tracked.command, e.getMessage(), e);
    }
  }

  private static String ucidOf(Map<String, Object> message) {
    if (message == null) {
      return null;
    }
    Object ucid = message.get("UCID");
    if (ucid == null) {
      ucid = message.get("ucid");
    }
    if (ucid == null) {
      return null;
    }
    String value = String.valueOf(ucid).trim();
    return value.isEmpty() ? null : value;
  }

  private static Object commandOf(Map<String, Object> message) {
    if (message == null) {
      return null;
    }
    Object command = message.get("Command");
    return command != null ? command : message.get("command");
  }

  @PreDestroy
  public void shutdown() {
    scheduler.shutdownNow();
  }

  private static final class TrackedRequest {
    private final int command;
    private final String ucid;
    private final Map<String, Object> payload;
    private volatile ScheduledFuture<?> future;

    private TrackedRequest(int command, String ucid, Map<String, Object> payload) {
      this.command = command;
      this.ucid = ucid;
      this.payload = payload;
    }

    private void cancel() {
      ScheduledFuture<?> current = future;
      if (current != null) {
        current.cancel(false);
      }
    }
  }
}
```
