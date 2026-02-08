package com.lmgx.gateway.connection;

import java.util.Map;

public interface MessageSender {
    enum Channel { CHAT, EMAIL }

    String send(Channel channel, Map<String, Object> payload) throws Exception;
}
