# gateway-project (Spring Boot)

## Run
- `./gradlew bootRun`

## Local target WS endpoints
- `/clientws/U1`
- `/clientws/U2`
- `/clientws/A1`
- `/clientws/A2`

## APIs
- Status: `GET /gateway/status`
- Send chat: `POST /gateway/chat/send`  (JSON body)
- Send email: `POST /gateway/email/send` (JSON body)
- Toggle target ACK: `POST /admin/target/{U1|U2|A1|A2}/ack?enabled=false`

## Oracle/MyBatis
- Create tables: `GW_TARGET_HEALTH`, `GW_FAILOVER_EVENT`
- Add Oracle JDBC driver (ojdbc) to runtime.
