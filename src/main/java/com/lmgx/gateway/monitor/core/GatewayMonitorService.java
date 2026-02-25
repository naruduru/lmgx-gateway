package com.lmgx.gateway.monitor.core;

import com.lmgx.gateway.target.TargetAdminService;
import com.lmgx.gateway.connection.FailoverLoop;
import com.lmgx.gateway.persist.FailoverEventLog;
import com.lmgx.gateway.persist.GatewayLogMapper;
import com.lmgx.gateway.persist.InstanceStatus;
import com.lmgx.gateway.persist.TargetHealthLog;
import com.lmgx.gateway.connection.GatewayWsClient;
import com.lmgx.gateway.target.TargetToggleStore;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class GatewayMonitorService {
    private static final List<String> TARGET_IDS = List.of("A1", "A2", "E1", "E2");

    private final Environment env;
    private final TargetToggleStore toggles;
    private final TargetAdminService adminService;
    private final GatewayWsClient ws;
    private final FailoverLoop failover;
    private final GatewayLogMapper logMapper;
    // DB integration note: logMapper can be replaced per site (optional monitoring storage).
    private final Map<String, Boolean> serverUpCache = new ConcurrentHashMap<>();
    private final Map<String, Boolean> ackCache = new ConcurrentHashMap<>();
    private final AtomicBoolean probing = new AtomicBoolean(false);
    private final AtomicLong lastLogRefresh = new AtomicLong(0);
    private volatile List<TargetHealthLog> healthCache = Collections.emptyList();
    private volatile List<FailoverEventLog> eventCache = Collections.emptyList();
    private final AtomicLong lastInstanceRefresh = new AtomicLong(0);
    private volatile List<InstanceStatus> instanceCache = Collections.emptyList();

    public GatewayMonitorService(Environment env, TargetToggleStore toggles, TargetAdminService adminService,
                                 GatewayWsClient ws, FailoverLoop failover,
                                 GatewayLogMapper logMapper) {
        this.env = env;
        this.toggles = toggles;
        this.adminService = adminService;
        this.ws = ws;
        this.failover = failover;
        this.logMapper = logMapper;
    }

    public String activeGroup() {
        return failover.getActiveGroup();
    }

    public String activeUrl() {
        String url = failover.getActiveUrl();
        if (url == null || url.isBlank()) {
            url = ws.currentUrl();
        }
        return url;
    }

    public boolean isReady() {
        return ws.isHealthy();
    }

    public int uiRefreshMs() {
        return ws.isReady() ? 2000 : 500;
    }

    public List<TargetStatus> targets() {
        List<TargetStatus> list = new ArrayList<>();
        String activeUrl = activeUrl();
        String[] ring = ringForGroup(activeGroup());
        java.util.Map<String, String> rankMap = buildRankMap(ring);
        java.util.Map<String, List<String>> instanceMap = buildInstanceTargetMap();
        java.util.Map<String, Boolean> upMap = new java.util.HashMap<>();
        for (String id : TARGET_IDS) {
            String url = env.getProperty("gateway.targets." + id);
            boolean ackEnabled = ackCache.getOrDefault(id, toggles.isAckEnabled(id));
            boolean active = url != null && url.equals(activeUrl);
            boolean ready = url != null && ws.isHealthy(url);
            boolean chatOpen = url != null && ws.isChatOpen(url);
            boolean emailOpen = url != null && ws.isEmailOpen(url);
            boolean serverUp = serverUpCache.getOrDefault(id, false);
            boolean localConnected = url != null && ws.isReady(url);
            upMap.put(id, serverUp);
            list.add(new TargetStatus(id, url, ackEnabled, active, ready, serverUp, localConnected, false,
                chatOpen, emailOpen, null, null, rankMap.getOrDefault(id, "UNKNOWN"),
                instanceMap.getOrDefault(id, List.of())));
        }

        for (int i = 0; i < list.size(); i++) {
            TargetStatus t = list.get(i);
            String[] status = statusFor(t.id, upMap);
            list.set(i, t.withStatus(status[0], status[1]));
        }
        return list;
    }

    public void setAckEnabled(String targetId, boolean enabled) {
        toggles.setAckEnabled(targetId, enabled);
    }

    public List<TargetHealthLog> recentHealth(int limit) {
        return healthCache;
    }

    public List<FailoverEventLog> recentEvents(int limit) {
        return eventCache;
    }

    public List<InstanceStatus> instanceStatuses() {
        if (instanceCache.isEmpty()) {
            return instanceCache;
        }
        List<InstanceStatus> filtered = new ArrayList<>();
        for (InstanceStatus row : instanceCache) {
            if (row == null || row.instanceId == null) {
                continue;
            }
            String id = row.instanceId.trim().toUpperCase();
            if (id.startsWith("TA") || id.startsWith("TE") || "MONITOR".equals(id)) {
                continue;
            }
            filtered.add(row);
        }
        return filtered;
    }

    public String activeId() {
        return targetIdOf(activeUrl());
    }

    private String targetIdOf(String url) {
        if (url == null) return null;
        int i = url.lastIndexOf('/');
        if (i < 0 || i == url.length() - 1) return null;
        return url.substring(i + 1);
    }

    public record TargetStatus(String id, String url, boolean ackEnabled, boolean active, boolean ready,
                               boolean serverUp, boolean localConnected, boolean primary,
                               boolean chatOpen, boolean emailOpen, String statusLabel, String statusTone,
                               String rankLabel, List<String> connectedInstances) {
        public TargetStatus withStatus(String label, String tone) {
            return new TargetStatus(id, url, ackEnabled, active, ready, serverUp, localConnected, primary,
                chatOpen, emailOpen, label, tone, rankLabel, connectedInstances);
        }
    }

    private String[] ringForGroup(String group) {
        String key = "gateway.ring." + ("G2".equalsIgnoreCase(group) ? "G2" : "G1");
        String csv = env.getProperty(key, "A1,A2,E1,E2");
        String[] parts = csv.split(",");
        for (int i = 0; i < parts.length; i++) {
            parts[i] = parts[i].trim();
        }
        return parts;
    }

    private java.util.Map<String, String> buildRankMap(String[] ring) {
        java.util.Map<String, String> map = new java.util.HashMap<>();
        if (ring == null) {
            return map;
        }
        String[] labels = new String[] {"PRIMARY", "SECONDARY", "THIRD", "FOURTH"};
        for (int i = 0; i < ring.length && i < labels.length; i++) {
            map.put(ring[i], labels[i]);
        }
        return map;
    }

    private java.util.Map<String, List<String>> buildInstanceTargetMap() {
        java.util.Map<String, List<String>> map = new java.util.HashMap<>();
        for (InstanceStatus row : instanceStatuses()) {
            if (row == null || row.activeTarget == null || row.instanceId == null) {
                continue;
            }
            map.computeIfAbsent(row.activeTarget, key -> new ArrayList<>()).add(row.instanceId);
        }
        return map;
    }

    private String[] statusFor(String id, java.util.Map<String, Boolean> upMap) {
        if ("A1".equals(id) || "A2".equals(id)) {
            boolean a1Up = upMap.getOrDefault("A1", false);
            boolean a2Up = upMap.getOrDefault("A2", false);
            if ("A1".equals(id)) {
                return a1Up ? new String[] {"ACTIVE", "success"} : new String[] {"DOWN", "danger"};
            }
            if (a1Up) {
                return a2Up ? new String[] {"STANDBY", "warning"} : new String[] {"DOWN", "danger"};
            }
            return a2Up ? new String[] {"ACTIVE", "success"} : new String[] {"DOWN", "danger"};
        }

        boolean e1Up = upMap.getOrDefault("E1", false);
        boolean e2Up = upMap.getOrDefault("E2", false);
        if ("E1".equals(id)) {
            return e1Up ? new String[] {"ACTIVE", "success"} : new String[] {"DOWN", "danger"};
        }
        if (e1Up) {
            return e2Up ? new String[] {"STANDBY", "warning"} : new String[] {"DOWN", "danger"};
        }
        return e2Up ? new String[] {"ACTIVE", "success"} : new String[] {"DOWN", "danger"};
    }

    @Scheduled(fixedDelay = 1000)
    public void refreshServerUp() {
        if (!probing.compareAndSet(false, true)) {
            return;
        }
        try {
            for (String id : TARGET_IDS) {
                String url = env.getProperty("gateway.targets." + id);
                boolean up = url != null && ws.isReady(url);
                serverUpCache.put(id, up);
                Boolean ackEnabled = adminService.getAckEnabled(id);
                if (ackEnabled != null) {
                    ackCache.put(id, ackEnabled);
                }
            }
        } finally {
            probing.set(false);
        }
    }

    @Scheduled(fixedDelay = 1000)
    public void refreshLogCache() {
        // DB integration note: replace log reads with site-specific storage if needed.
        if (logMapper == null) {
            return;
        }
        if (System.currentTimeMillis() - lastLogRefresh.get() < 900) {
            return;
        }
        try {
            healthCache = logMapper.selectRecentHealth(10);
            eventCache = logMapper.selectRecentEvents(10);
            lastLogRefresh.set(System.currentTimeMillis());
        } catch (Exception e) {
            // keep last cache
        }
    }

    @Scheduled(fixedDelay = 1000)
    public void refreshInstanceCache() {
        // DB integration note: replace instance status reads with site-specific storage if needed.
        if (logMapper == null) {
            return;
        }
        if (System.currentTimeMillis() - lastInstanceRefresh.get() < 900) {
            return;
        }
        try {
            instanceCache = logMapper.selectInstanceStatus();
            lastInstanceRefresh.set(System.currentTimeMillis());
        } catch (Exception e) {
            // keep last cache
        }
    }
}
