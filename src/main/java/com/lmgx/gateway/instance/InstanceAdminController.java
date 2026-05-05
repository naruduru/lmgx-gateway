package com.lmgx.gateway.instance;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/admin/instance")
public class InstanceAdminController {
    private final InstanceAdminService service;

    public InstanceAdminController(InstanceAdminService service) {
        this.service = service;
    }

    @PostMapping("/{id}/pause")
    public Map<String, Object> setPaused(@PathVariable String id, @RequestParam boolean enabled) {
        return service.setPaused(id, enabled);
    }

    @GetMapping("/{id}/pause")
    public Map<String, Object> getPaused(@PathVariable String id) {
        return service.getPaused(id);
    }

    @PostMapping("/{id}/ha-state")
    public Map<String, Object> setHaState(@PathVariable String id, @RequestParam int value) {
        return service.setHaState(id, value);
    }

    @GetMapping("/{id}/ha-state")
    public Map<String, Object> getHaState(@PathVariable String id) {
        return service.getHaState(id);
    }
}
