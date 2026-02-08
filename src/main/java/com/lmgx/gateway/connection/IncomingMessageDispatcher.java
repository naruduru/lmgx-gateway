package com.lmgx.gateway.connection;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class IncomingMessageDispatcher {
    private final Map<String, IncomingCommandHandler> handlers;

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
        Object cmdObj = message.get("cmd");
        if (!(cmdObj instanceof String cmd)) {
            return;
        }
        IncomingCommandHandler handler = handlers.get(cmd);
        if (handler == null) {
            return;
        }
        handler.handle(channel, message);
    }
}
