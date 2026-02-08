package com.lmgx.gateway.api;

import com.lmgx.gateway.monitor.InstanceControlStore;
import com.lmgx.gateway.persist.GatewayLogMapper;
import com.lmgx.gateway.persist.InstanceStatus;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

@Service
public class InstanceAdminService {
    private final InstanceControlStore controlStore;
    private final Environment env;
    private final GatewayLogMapper logMapper;
    private final RestTemplate restTemplate = new RestTemplate();

    public InstanceAdminService(InstanceControlStore controlStore, Environment env, GatewayLogMapper logMapper) {
        this.controlStore = controlStore;
        this.env = env;
        this.logMapper = logMapper;
    }

    public Map<String, Object> setPaused(String instanceId, boolean paused) {
        if (isLocalInstance(instanceId)) {
            controlStore.setPaused(paused);
            return Map.of("ok", true, "instance", instanceId, "paused", paused, "mode", "local");
        }

        InstanceStatus row = findInstance(instanceId);
        if (row == null || row.serverPort == null) {
            return Map.of("ok", false, "instance", instanceId, "message", "instance not found");
        }

        String host = env.getProperty("gateway.instance-hosts." + instanceId, "127.0.0.1");
        String url = "http://" + host + ":" + row.serverPort + "/admin/instance/" + instanceId + "/pause?enabled=" + paused;
        try {
            restTemplate.postForEntity(url, null, String.class);
            return Map.of("ok", true, "instance", instanceId, "paused", paused, "mode", "proxy", "url", url);
        } catch (Exception e) {
            return Map.of("ok", false, "instance", instanceId, "message", e.getMessage(), "url", url);
        }
    }

    public Map<String, Object> getPaused(String instanceId) {
        Boolean paused = getPausedValue(instanceId);
        if (paused == null) {
            return Map.of("ok", false, "instance", instanceId, "message", "pause status unavailable");
        }
        return Map.of("ok", true, "instance", instanceId, "paused", paused);
    }

    public Boolean getPausedValue(String instanceId) {
        if (isLocalInstance(instanceId)) {
            return controlStore.isPaused();
        }
        InstanceStatus row = findInstance(instanceId);
        if (row == null || row.serverPort == null) {
            return null;
        }
        String host = env.getProperty("gateway.instance-hosts." + instanceId, "127.0.0.1");
        String url = "http://" + host + ":" + row.serverPort + "/admin/instance/" + instanceId + "/pause";
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> body = restTemplate.getForObject(url, Map.class);
            if (body != null && body.get("paused") instanceof Boolean b) {
                return b;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private boolean isLocalInstance(String instanceId) {
        String localId = env.getProperty("gateway.instance.id", "unknown");
        return localId != null && localId.equalsIgnoreCase(instanceId);
    }

    private InstanceStatus findInstance(String instanceId) {
        if (logMapper == null) {
            return null;
        }
        List<InstanceStatus> rows = logMapper.selectInstanceStatus();
        if (rows == null) {
            return null;
        }
        for (InstanceStatus row : rows) {
            if (row != null && row.instanceId != null && row.instanceId.equalsIgnoreCase(instanceId)) {
                return row;
            }
        }
        return null;
    }
}
