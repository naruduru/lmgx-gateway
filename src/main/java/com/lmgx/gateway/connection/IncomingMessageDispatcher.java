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
