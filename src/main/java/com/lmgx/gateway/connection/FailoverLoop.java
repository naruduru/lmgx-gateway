package com.lmgx.gateway.connection;

import com.lmgx.gateway.instance.InstanceControlStore;
import com.lmgx.gateway.persist.FailoverEventLog;
import com.lmgx.gateway.persist.GatewayLogMapper;
import com.lmgx.gateway.persist.HealthStatus;
import com.lmgx.gateway.connection.GatewayWsClient;
import com.lmgx.gateway.connection.ProbeWsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.core.env.Environment;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.InetAddress;

@Component
public class FailoverLoop {

  private static final Logger log = LoggerFactory.getLogger(FailoverLoop.class);

  private final GatewayWsClient ws;
  private final ProbeWsClient probe;
  private final GatewayLogMapper logMapper;
  // DB integration note: logMapper can be swapped per site (optional logging).
  private final InstanceControlStore controlStore;

  private final String A1;
  private final String A2;
  private final String E1;
  private final String E2;

  private final String[] ringG1;
  private final String[] ringG2;
  private final String[] recoverG1;
  private final String[] recoverG2;
  private final String[] preferG1;
  private final String[] preferG2;

  private volatile String activeGroup = "G1";
  private volatile String active;
  private final String sourceIp;

  private int failCount = 0;
  private int recoverStable = 0;

  private static final int FAIL_THRESHOLD = 2;
  private static final int RECOVER_STABLE_THRESHOLD = 5;
  private static final int NOT_READY_THRESHOLD = 3;
  private static final long SAME_TARGET_RECONNECT_COOLDOWN_MS = 30_000L;

  private int notReadyStreak = 0;
  private volatile long lastSameTargetReconnectAt = 0L;

  public FailoverLoop(GatewayWsClient ws, ProbeWsClient probe, GatewayLogMapper logMapper,
                      InstanceControlStore controlStore, Environment env) {
    this.ws = ws;
    this.probe = probe;
    this.logMapper = logMapper;
    this.controlStore = controlStore;

    this.A1 = env.getProperty("gateway.targets.A1");
    this.A2 = env.getProperty("gateway.targets.A2");
    this.E1 = env.getProperty("gateway.targets.E1");
    this.E2 = env.getProperty("gateway.targets.E2");

    this.ringG1 = parseIds(env.getProperty("gateway.ring.G1", "A1,A2,E1,E2"));
    this.ringG2 = parseIds(env.getProperty("gateway.ring.G2", "E1,E2,A1,A2"));
    this.recoverG1 = parseIds(env.getProperty("gateway.recover.G1", "A1,A2"));
    this.recoverG2 = parseIds(env.getProperty("gateway.recover.G2", "E1,E2"));
    this.preferG1 = parseIds(env.getProperty("gateway.prefer.G1", "A1,A2"));
    this.preferG2 = parseIds(env.getProperty("gateway.prefer.G2", "E1,E2"));

    String overrideGroup = env.getProperty("gateway.group.override", "G1");
    if ("G2".equalsIgnoreCase(overrideGroup)) {
      this.activeGroup = "G2";
    } else {
      this.activeGroup = "G1";
    }

    this.sourceIp = resolveSourceIp(env.getProperty("gateway.source-ip"));
    this.active = urlOf(ringOf(activeGroup)[0]);
    log.info("FailoverLoop init: group={}, active={}", activeGroup, active);
  }

  @EventListener(ApplicationReadyEvent.class)
  public void onReady() {
    if (this.active != null) {
      ws.connect(this.active);
    }
  }

  private volatile long lastTickAt = 0L;

  @Scheduled(fixedDelay = 1000)
  public void tick() {
    try {
      long now = System.currentTimeMillis();
      boolean ready = ws.isReady();
      long interval = 1000;
      if (now - lastTickAt < interval) {
        return;
      }
      lastTickAt = now;

      log.debug("tick: group={}, active={}, ready={}", activeGroup, active, ready);
      if (controlStore.isPaused()) {
        notReadyStreak = 0;
        ws.disconnectAll();
        return;
      }
      if (!ready) {
        notReadyStreak++;
        log.warn("ready=false observed: group={}, active={}, streak={}, wsState={}",
            activeGroup, active, notReadyStreak, ws.readinessDebug());
        String found = findFirstAliveInRing(activeGroup, active);
        if (found != null) {
          if (!found.equals(active)) {
            notReadyStreak = 0;
            switchTo(found, "NOT_READY_RING_SCAN");
          } else {
            if (notReadyStreak < NOT_READY_THRESHOLD) {
              log.info("not ready but alive, waiting threshold: active={}, streak={}/{}",
                  active, notReadyStreak, NOT_READY_THRESHOLD);
            } else {
              long cooldownLeft = SAME_TARGET_RECONNECT_COOLDOWN_MS - (now - lastSameTargetReconnectAt);
              if (cooldownLeft > 0) {
                log.info("not ready but alive, cooldown active: active={}, waitMs={}", active, cooldownLeft);
              } else {
                log.info("not ready but alive, reconnecting current: {}, streak={}, wsState={}",
                  active, notReadyStreak, ws.readinessDebug());
                ws.connectForce(active);
                lastSameTargetReconnectAt = now;
                notReadyStreak = 0;
              }
            }
          }
        } else {
          notReadyStreak = 0;
          log.warn("No alive target found in ring, reconnecting current: {}", active);
          ws.connect(active);
        }
        return;
      }

      notReadyStreak = 0;

      long t0 = System.currentTimeMillis();
      boolean ok = ws.pingBoth();
      long ms = System.currentTimeMillis() - t0;

      updateHealthStatus(active, ok);
      log.debug("ping: ok={}, ms={}, active={}", ok, ms, active);

      if (!ok) {
        failCount++;
        if (failCount >= FAIL_THRESHOLD) {
          String found = findFirstAliveInRing(activeGroup, active);
          if (found != null) {
            switchTo(found, "PING_FAIL_RING_SCAN");
          } else {
            log.warn("PING fail, no alive target found in ring, reconnecting: {}", active);
            ws.connect(active);
          }
          resetCounters();
        }
        return;
      }
      failCount = 0;

      // upgrade (A2->A1 / E2->E1)
      String[] higher = higherPreferCandidates(activeGroup, active);
      if (higher.length > 0) {
        String upUrl = probeFirstAliveByIds(higher);
        if (upUrl != null) {
          recoverStable++;
          if (recoverStable >= RECOVER_STABLE_THRESHOLD) {
            switchTo(upUrl, "UPGRADE_STABLE");
            resetCounters();
          }
        } else {
          recoverStable = 0;
        }
        return;
      }

      // recover (other group -> primary group)
      if (!isInRecover(activeGroup, targetIdOf(active))) {
        String recoverTo = probeFirstAliveByIds(recoverOf(activeGroup));
        if (recoverTo != null) {
          recoverStable++;
          if (recoverStable >= RECOVER_STABLE_THRESHOLD) {
            switchTo(recoverTo, "RECOVER_STABLE");
            resetCounters();
          }
        } else {
          recoverStable = 0;
        }
        return;
      }

      recoverStable = 0;

    } catch (Exception e) {
      log.warn("Failover tick error", e);
    }
  }

  public String getActiveUrl() { return active; }
  public String getActiveGroup() { return activeGroup; }

  private void resetCounters() {
    failCount = 0;
    recoverStable = 0;
  }

  private String findFirstAliveInRing(String group, String curUrl) {
    String[] ring = ringOf(group);

    if (probeAndLog(curUrl)) return curUrl;

    int idx = indexOf(ring, targetIdOf(curUrl));
    if (idx < 0) idx = 0;

    for (int i = 1; i < ring.length; i++) {
      String url = urlOf(ring[(idx + i) % ring.length]);
      if (probeAndLog(url)) return url;
    }
    return null;
  }

  private synchronized void switchTo(String toUrl, String reason) {
    String fromUrl = this.active;
    if (fromUrl != null && fromUrl.equals(toUrl)) return;

    String eventKind = decideEventKind(activeGroup, fromUrl, toUrl);
    log.info("switch: {} -> {} (kind={}, reason={})", fromUrl, toUrl, eventKind, reason);

    this.active = toUrl;
    ws.connectForce(toUrl);

    FailoverEventLog ev = new FailoverEventLog();
    ev.serverGroup = activeGroup;
    ev.fromTarget = targetIdOf(fromUrl);
    ev.toTarget = targetIdOf(toUrl);
    ev.fromUrl = fromUrl;
    ev.toUrl = toUrl;
    ev.eventKind = eventKind;
    ev.triggerReason = reason;
    ev.failCount = failCount;
    ev.recoverStable = recoverStable;

    safeInsertEvent(ev);
  }

  private String decideEventKind(String group, String fromUrl, String toUrl) {
    String from = targetIdOf(fromUrl);
    String to = targetIdOf(toUrl);

    if (isInPrefer(group, from) && isInPrefer(group, to)) {
      if (indexOf(preferOf(group), to) < indexOf(preferOf(group), from)) {
        return "UPGRADE";
      }
    }

    if (!isInRecover(group, from) && isInRecover(group, to)) {
      return "RECOVER";
    }

    return "SWITCH";
  }

  private String[] higherPreferCandidates(String group, String curUrl) {
    String[] prefer = preferOf(group);
    String curId = targetIdOf(curUrl);

    int idx = indexOf(prefer, curId);
    if (idx <= 0) return new String[0];

    String[] higher = new String[idx];
    System.arraycopy(prefer, 0, higher, 0, idx);
    return higher;
  }

  private String probeFirstAliveByIds(String[] ids) {
    for (String id : ids) {
      String url = urlOf(id);
      if (url != null && probeAndLog(url)) return url;
    }
    return null;
  }

  private boolean probeAndLog(String url) {
    long t0 = System.currentTimeMillis();
    boolean up = probe.probe(url);
    long ms = System.currentTimeMillis() - t0;

    updateHealthStatus(url, up);
    log.debug("probe: url={}, up={}, ms={}", url, up, ms);

    return up;
  }

  private String[] ringOf(String g) { return "G2".equals(g) ? ringG2 : ringG1; }
  private String[] recoverOf(String g) { return "G2".equals(g) ? recoverG2 : recoverG1; }
  private String[] preferOf(String g) { return "G2".equals(g) ? preferG2 : preferG1; }

  private boolean isInRecover(String g, String id) { return indexOf(recoverOf(g), id) >= 0; }
  private boolean isInPrefer(String g, String id) { return indexOf(preferOf(g), id) >= 0; }

  private String[] parseIds(String csv) {
    String[] parts = csv.split(",");
    for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
    return parts;
  }

  private int indexOf(String[] arr, String v) {
    if (v == null) return -1;
    for (int i = 0; i < arr.length; i++) if (v.equals(arr[i])) return i;
    return -1;
  }

  private String urlOf(String id) {
    if (id == null) return null;
    switch (id.trim()) {
      case "A1": return A1;
      case "A2": return A2;
      case "E1": return E1;
      case "E2": return E2;
      default: return null;
    }
  }

  private String targetIdOf(String url) {
    if (url == null) return null;
    int i = url.lastIndexOf('/');
    if (i < 0 || i == url.length() - 1) return null;
    return url.substring(i + 1);
  }

  private void updateHealthStatus(String targetUrl, boolean up) {
    String targetIp = hostOf(targetUrl);
    if (targetIp == null || targetIp.isBlank()) {
      return;
    }
    HealthStatus status = new HealthStatus();
    status.srcIp = sourceIp;
    status.targetIp = targetIp;
    status.isUp = up ? "Y" : "N";
    try { logMapper.upsertHealthStatus(status); } catch (Exception ignore) {}
  }

  private void safeInsertEvent(FailoverEventLog log) {
    // DB integration note: replace with site-specific persistence if needed.
    try { logMapper.insertFailoverEvent(log); } catch (Exception ignore) {}
  }

  private static String hostOf(String url) {
    if (url == null || url.isBlank()) {
      return null;
    }
    try {
      return java.net.URI.create(url).getHost();
    } catch (Exception e) {
      return null;
    }
  }

  private static String resolveSourceIp(String configuredIp) {
    if (configuredIp != null && !configuredIp.isBlank()) {
      return configuredIp.trim();
    }
    try {
      return InetAddress.getLocalHost().getHostAddress();
    } catch (Exception e) {
      return "127.0.0.1";
    }
  }
}
