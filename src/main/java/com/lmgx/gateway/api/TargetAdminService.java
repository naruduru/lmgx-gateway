package com.lmgx.gateway.api;

import com.lmgx.gateway.ws.TargetToggleStore;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
public class TargetAdminService {
    private final TargetToggleStore toggles;
    private final Environment env;
    private final RestTemplate restTemplate = new RestTemplate();

    public TargetAdminService(TargetToggleStore toggles, Environment env) {
        this.toggles = toggles;
        this.env = env;
    }

    public Map<String, Object> setAck(String id, boolean enabled) {
        if (isLocalTarget(id)) {
            toggles.setAckEnabled(id, enabled);
            return Map.of("ok", true, "target", id, "ackEnabled", enabled, "mode", "local");
        }

        String host = env.getProperty("gateway.target-hosts." + id, "127.0.0.1");
        String port = env.getProperty("gateway.target-ports." + id);
        if (port == null || port.isBlank()) {
            toggles.setAckEnabled(id, enabled);
            return Map.of("ok", true, "target", id, "ackEnabled", enabled, "mode", "local-fallback",
                "message", "target port not configured, applied locally");
        }

        String url = "http://" + host + ":" + port + "/admin/target/" + id + "/ack?enabled=" + enabled;
        try {
            restTemplate.postForEntity(url, null, String.class);
            return Map.of("ok", true, "target", id, "ackEnabled", enabled, "mode", "proxy", "url", url);
        } catch (Exception e) {
            return Map.of("ok", false, "target", id, "message", e.getMessage(), "url", url);
        }
    }

    public Map<String, Object> getAck(String id) {
        Boolean enabled = getAckEnabled(id);
        if (enabled == null) {
            return Map.of("ok", false, "target", id, "message", "ack status unavailable");
        }
        return Map.of("ok", true, "target", id, "ackEnabled", enabled);
    }

    public Boolean getAckEnabled(String id) {
        if (isLocalTarget(id)) {
            return toggles.isAckEnabled(id);
        }

        String host = env.getProperty("gateway.target-hosts." + id, "127.0.0.1");
        String port = env.getProperty("gateway.target-ports." + id);
        if (port == null || port.isBlank()) {
            return toggles.isAckEnabled(id);
        }

        String url = "http://" + host + ":" + port + "/admin/target/" + id + "/ack";
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> body = restTemplate.getForObject(url, Map.class);
            if (body != null && body.get("ackEnabled") instanceof Boolean b) {
                return b;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private boolean isLocalTarget(String id) {
        String host = env.getProperty("gateway.target-hosts." + id, "127.0.0.1");
        String port = env.getProperty("gateway.target-ports." + id);
        String serverPort = env.getProperty("server.port");
        if (port == null || serverPort == null) {
            return false;
        }
        boolean hostLocal = "127.0.0.1".equals(host) || "localhost".equalsIgnoreCase(host);
        return hostLocal && port.trim().equals(serverPort.trim());
    }
}
