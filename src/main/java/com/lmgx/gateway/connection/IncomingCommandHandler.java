package com.lmgx.gateway.connection;

import java.util.Map;

public interface IncomingCommandHandler {
    java.util.List<String> cmds();

    void handle(MessageSender.Channel channel, Map<String, Object> message);
}
