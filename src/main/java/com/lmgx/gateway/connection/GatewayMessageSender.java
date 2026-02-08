package com.lmgx.gateway.connection;

import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class GatewayMessageSender implements MessageSender {
    private final GatewayWsClient ws;

    public GatewayMessageSender(GatewayWsClient ws) {
        this.ws = ws;
    }

    @Override
    public String send(Channel channel, Map<String, Object> payload) throws Exception {
        if (channel == null) {
            throw new IllegalArgumentException("channel is required");
        }
        return switch (channel) {
            case CHAT -> ws.sendChat(payload);
            case EMAIL -> ws.sendEmail(payload);
        };
    }
}
