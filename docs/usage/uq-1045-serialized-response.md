# UQ 1045/1046 전역 직렬화 응답 처리

## 배경

`FEATURE_FNC_GET_ROUTABLE_REQ` 요청 command는 `0x0415`이며, decimal 값은 `1045`다.
응답 command는 `0x0416`이며, decimal 값은 `1046`이다.

현재 `1045` 요청과 `1046` 응답 전문에는 요청별로 유니크하게 매칭할 수 있는 `UCID` 같은 correlation 값이 없다.
`TenantId + ReqUserId`도 여러 사용자가 같은 값으로 동시에 요청할 수 있어 안전한 매칭 키로 사용할 수 없다.

게이트웨이와 UQ 서버 사이의 WebSocket session id도 프론트 사용자별 세션이 아니라 공유 연결 단위이므로,
동시에 여러 `1045` 요청이 나가면 들어온 `1046` 응답이 어느 프론트 요청의 응답인지 구분할 수 없다.

## 현재 처리 방식

잘못된 사용자에게 응답을 전달하지 않기 위해 `1045`는 전역 직렬화한다.

- `gateway.uq.serialized-chat-commands`에 등록된 command는 한 번에 하나만 전송한다.
- 기본값은 `1045`다.
- 두 번째 `1045` 요청이 첫 번째 `1046` 응답 또는 timeout 전에 들어오면 `InFlightRequestException`이 발생한다.
- `1046` 응답이 들어오면 현재 대기 중인 단일 `1045` 요청의 응답으로 간주하고 프론트에 반환한다.
- timeout 발생 시 대기 요청을 timeout payload로 완료하고 전역 락을 해제한다.

## 설정

```yaml
gateway:
  uq:
    response-timeout-ms: 300000
    session-inflight-ttl-ms: 300000
    serialized-chat-commands: 1045
```

## API 응답

`/gateway/chat/send`로 `Command: 1045`를 전송하면 컨트롤러는 즉시 `requestId`만 반환하지 않고 `1046` 응답을 기다린다.

성공 시:

```json
{
  "ok": true,
  "response": {
    "Command": 1046
  }
}
```

동일 command가 이미 진행 중인 경우:

```json
{
  "ok": false,
  "message": "uq request already in-flight: sessionKey=serialized-command:1045"
}
```

## UCID 도입 시 변경 방향

외부 시스템에서 `1045` 요청에 `UCID`를 받고 `1046` 응답에 같은 `UCID`를 내려줄 수 있게 되면 전역 직렬화를 제거할 수 있다.

그 경우:

- `gateway.uq.serialized-chat-commands`에서 `1045`를 제거하거나 비운다.
- `SessionRequestKeyResolver`가 `UCID` 기준으로 요청/응답을 매칭한다.
- 동일 시간대에 여러 사용자의 `1045` 요청을 병렬 처리할 수 있다.

## 관련 소스

- `src/main/java/com/lmgx/gateway/api/GatewayController.java`
- `src/main/java/com/lmgx/gateway/message/UqSender.java`
- `src/main/java/com/lmgx/gateway/message/UqRequestTracker.java`
- `src/main/java/com/lmgx/gateway/message/UqCommandHandler.java`
- `src/main/java/com/lmgx/gateway/message/SessionInFlightStore.java`
- `src/main/java/com/lmgx/gateway/message/UqSerializationKeys.java`
