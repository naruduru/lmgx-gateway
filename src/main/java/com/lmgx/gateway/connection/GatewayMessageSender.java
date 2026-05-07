package com.lmgx.gateway.connection;

import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class GatewayMessageSender implements MessageSender {
    private final GatewayWsClient ws;
    private final FailoverLoop failover;

    public GatewayMessageSender(GatewayWsClient ws, FailoverLoop failover) {
        this.ws = ws;
        this.failover = failover;
    }

    @Override
    public String send(Channel channel, Map<String, Object> payload) throws Exception {
        if (channel == null) {
            throw new IllegalArgumentException("channel is required");
        }
        // 전송 직전에 다시 타겟을 확인해 standby 또는 반쪽 연결 타겟으로 보내지 않게 한다.
        String url = failover.ensureCommandTarget(channel);
        return switch (channel) {
            case CHAT -> ws.sendChat(url, payload);
            case EMAIL -> ws.sendEmail(url, payload);
        };
    }
}
