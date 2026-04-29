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
        String url = failover.ensureCommandTarget(channel);
        return switch (channel) {
            case CHAT -> ws.sendChat(url, payload);
            case EMAIL -> ws.sendEmail(url, payload);
        };
    }
}
