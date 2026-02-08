package com.lmgx.gateway.monitor.react;

import com.lmgx.gateway.monitor.core.GatewayMonitorService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/monitor/api")
public class GatewayMonitorApiController {
    private final GatewayMonitorService service;

    public GatewayMonitorApiController(GatewayMonitorService service) {
        this.service = service;
    }

    @GetMapping("/summary")
    public Map<String, Object> summary() {
        return Map.of(
            "group", service.activeGroup(),
            "activeUrl", service.activeUrl(),
            "activeTarget", service.activeId(),
            "ready", service.isReady(),
            "refreshMs", service.uiRefreshMs()
        );
    }

    @GetMapping("/instances")
    public Object instances() {
        return service.instanceStatuses();
    }

    @GetMapping("/targets")
    public Object targets() {
        return service.targets();
    }

    @GetMapping("/logs")
    public Map<String, Object> logs() {
        return Map.of(
            "health", service.recentHealth(10),
            "events", service.recentEvents(10)
        );
    }
}
