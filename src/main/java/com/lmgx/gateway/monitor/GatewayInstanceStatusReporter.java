package com.lmgx.gateway.monitor;

import com.lmgx.gateway.failover.FailoverLoop;
import com.lmgx.gateway.persist.GatewayLogMapper;
import com.lmgx.gateway.persist.InstanceStatus;
import com.lmgx.gateway.ws.GatewayWsClient;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class GatewayInstanceStatusReporter {
    private final GatewayLogMapper logMapper;
    private final GatewayWsClient ws;
    private final FailoverLoop failover;
    private final Environment env;
    private final InstanceControlStore controlStore;

    public GatewayInstanceStatusReporter(GatewayLogMapper logMapper, GatewayWsClient ws, FailoverLoop failover,
                                         InstanceControlStore controlStore, Environment env) {
        this.logMapper = logMapper;
        this.ws = ws;
        this.failover = failover;
        this.controlStore = controlStore;
        this.env = env;
    }

    @Scheduled(fixedDelay = 2000)
    public void report() {
        if (logMapper == null) {
            return;
        }
        String instanceId = env.getProperty("gateway.instance.id", "unknown");
        if (instanceId == null || instanceId.isBlank() || "unknown".equalsIgnoreCase(instanceId)) {
            return;
        }
        InstanceStatus status = new InstanceStatus();
        status.instanceId = instanceId;
        status.groupId = failover.getActiveGroup();
        status.serverPort = parseInt(env.getProperty("server.port"));
        if (controlStore.isPaused()) {
            status.activeUrl = null;
            status.activeTarget = null;
            status.chatOpen = false;
            status.emailOpen = false;
            status.ready = false;
        } else {
            status.activeUrl = failover.getActiveUrl();
            status.activeTarget = targetIdOf(status.activeUrl);
            status.chatOpen = ws.isChatOpen();
            status.emailOpen = ws.isEmailOpen();
            status.ready = ws.isHealthy();
        }
        try {
            logMapper.upsertInstanceStatus(status);
        } catch (Exception ignore) {
        }
    }

    private Integer parseInt(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String targetIdOf(String url) {
        if (url == null) return null;
        int i = url.lastIndexOf('/');
        if (i < 0 || i == url.length() - 1) return null;
        return url.substring(i + 1);
    }
}
