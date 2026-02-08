package com.lmgx.gateway.target;

import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/admin/target")
public class TargetAdminController {
  private final TargetAdminService adminService;

  public TargetAdminController(TargetAdminService adminService) {
    this.adminService = adminService;
  }

  @PostMapping("/{id}/ack")
  public Map<String, Object> setAck(@PathVariable String id, @RequestParam boolean enabled) {
    return adminService.setAck(id, enabled);
  }

  @GetMapping("/{id}/ack")
  public Map<String, Object> getAck(@PathVariable String id) {
    return adminService.getAck(id);
  }
}
