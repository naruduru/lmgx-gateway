package com.lmgx.gateway.connection;

import com.lmgx.gateway.instance.InstanceControlStore;
import com.lmgx.gateway.persist.FailoverEventLog;
import com.lmgx.gateway.persist.GatewayLogMapper;
import com.lmgx.gateway.persist.HealthStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class FailoverLoop {

  private static final Logger log = LoggerFactory.getLogger(FailoverLoop.class);

  private final GatewayWsClient ws;
  private final GatewayLogMapper logMapper;
  private final InstanceControlStore controlStore;

  private final String U1;
  private final String U2;
  private final String A1;
  private final String A2;

  private final String[] ringG1;
  private final String[] ringG2;
  private final String[] recoverG1;
  private final String[] recoverG2;
  private final String[] preferG1;
  private final String[] preferG2;
  private final boolean recoverEnabled;
  private final boolean preferEnabled;
  private final Map<String, String> targetIdByUrl;

  private volatile String activeGroup = "G1";
  private volatile String active;
  private final String sourceIp;

  private int failCount = 0;
  private int recoverStable = 0;

  private static final int FAIL_THRESHOLD = 3;
  private static final int RECOVER_STABLE_THRESHOLD = 5;
  private static final int NOT_READY_THRESHOLD = 3;
  private static final long SAME_TARGET_RECONNECT_COOLDOWN_MS = 60_000L;
  private static final long ENSURE_INTERVAL_MS = 10_000L;
  private static final long ALL_TARGET_HEALTHCHECK_INTERVAL_MS = 5_000L;
  private static final long STANDBY_HEALTHCHECK_INTERVAL_MS = 5_000L;

  private int notReadyStreak = 0;
  private volatile long lastChatReconnectAt = 0L;
  private volatile long lastEmailReconnectAt = 0L;
  private volatile long lastSameTargetReconnectAt = 0L;
  private volatile long lastEnsureAt = 0L;
  private volatile long lastAllTargetHealthcheckAt = 0L;
  private final Map<String, Long> lastStandbyHealthcheckAt = new ConcurrentHashMap<>();

  public FailoverLoop(GatewayWsClient ws, GatewayLogMapper logMapper,
                      InstanceControlStore controlStore, Environment env) {
    this.ws = ws;
    this.logMapper = logMapper;
    this.controlStore = controlStore;

    this.U1 = env.getProperty("gateway.targets.U1");
    this.U2 = env.getProperty("gateway.targets.U2");
    this.A1 = env.getProperty("gateway.targets.A1");
    this.A2 = env.getProperty("gateway.targets.A2");

    Map<String, String> idMap = new HashMap<>();
    putIfPresent(idMap, U1, "U1");
    putIfPresent(idMap, U2, "U2");
    putIfPresent(idMap, A1, "A1");
    putIfPresent(idMap, A2, "A2");
    this.targetIdByUrl = Collections.unmodifiableMap(idMap);

    this.ringG1 = configuredIds(parseIds(env.getProperty("gateway.ring.G1", "U1,U2,A1,A2")));
    this.ringG2 = configuredIds(parseIds(env.getProperty("gateway.ring.G2", "A1,A2,U1,U2")));
    this.recoverG1 = configuredIds(parseIds(env.getProperty("gateway.recover.G1", "U1,U2")));
    this.recoverG2 = configuredIds(parseIds(env.getProperty("gateway.recover.G2", "A1,A2")));
    this.preferG1 = configuredIds(parseIds(env.getProperty("gateway.prefer.G1", "U1,U2")));
    this.preferG2 = configuredIds(parseIds(env.getProperty("gateway.prefer.G2", "A1,A2")));
    this.recoverEnabled = env.getProperty("gateway.recover.enabled", Boolean.class, true);
    this.preferEnabled = env.getProperty("gateway.prefer.enabled", Boolean.class, true);

    String overrideGroup = env.getProperty("gateway.group.override", "G1");
    if ("G2".equalsIgnoreCase(overrideGroup)) {
      this.activeGroup = "G2";
    } else {
      this.activeGroup = "G1";
    }

    this.sourceIp = resolveSourceIp(env.getProperty("gateway.source-ip"));
    String[] initialRing = ringOf(activeGroup);
    this.active = initialRing.length == 0 ? null : urlOf(initialRing[0]);
    log.info("FailoverLoop init: group={}, active={}, recoverEnabled={}, preferEnabled={}",
        activeGroup, active, recoverEnabled, preferEnabled);
  }

  @EventListener(ApplicationReadyEvent.class)
  public void onReady() {
    ensureAllTargetConnections();
    long now = System.currentTimeMillis();
    lastEnsureAt = now;
    healthcheckAllTargets(now);
    if (this.active != null) {
      ws.connectForce(this.active);
    }
  }

  private volatile long lastTickAt = 0L;

  @Scheduled(fixedDelay = 1000)
  public void tick() {
    try {
      long now = System.currentTimeMillis();
      long interval = 1000;
      if (now - lastTickAt < interval) {
        return;
      }
      lastTickAt = now;

      if (controlStore.isPaused()) {
        notReadyStreak = 0;
        ws.disconnectAll();
        return;
      }

      if (now - lastEnsureAt >= ENSURE_INTERVAL_MS) {
        ensureAllTargetConnections();
        lastEnsureAt = now;
      }

      if (now - lastAllTargetHealthcheckAt >= ALL_TARGET_HEALTHCHECK_INTERVAL_MS) {
        healthcheckAllTargets(now);
        lastAllTargetHealthcheckAt = now;
      }

      boolean chatOk = checkAndRepairChannel(active, MessageSender.Channel.CHAT, now);
      boolean emailOk = checkAndRepairChannel(active, MessageSender.Channel.EMAIL, now);
      boolean haActive = ws.isHaActive(active);
      log.debug("tick: group={}, active={}, chatOk={}, emailOk={}, haActive={}",
          activeGroup, active, chatOk, emailOk, haActive);

      if (!(chatOk && emailOk && haActive)) {
        notReadyStreak++;
        log.warn("target not command-routable observed: group={}, active={}, streak={}, haState={}, wsState={}",
            activeGroup, active, notReadyStreak, ws.haStateOf(active), ws.readinessDebug());

        String found = findFirstCommandRoutableInRing(activeGroup, active);
        if (found != null) {
          if (!found.equals(active)) {
            notReadyStreak = 0;
            switchTo(found, haActive ? "NOT_READY_RING_SCAN" : "HA_STANDBY_RING_SCAN");
          } else {
            if (notReadyStreak < NOT_READY_THRESHOLD) {
              log.info("target not command-routable yet, waiting threshold: active={}, streak={}/{}, haState={}",
                  active, notReadyStreak, NOT_READY_THRESHOLD, ws.haStateOf(active));
            } else {
              if (!haActive) {
                log.info("target heartbeat alive but standby, holding connections: active={}, haState={}, streak={}",
                    active, ws.haStateOf(active), notReadyStreak);
              } else if (shouldReconnectSameTarget(now)) {
                log.info("target not ready but alive, reconnecting both channels: {}, streak={}, wsState={}",
                    active, notReadyStreak, ws.readinessDebug());
                ws.connectForce(active);
                lastSameTargetReconnectAt = now;
              } else {
                log.info("target not ready but same-target reconnect cooldown active: active={}, waitMs={}",
                    active, sameTargetReconnectCooldownLeft(now));
              }
              notReadyStreak = 0;
            }
          }
        } else {
          notReadyStreak = 0;
          if (haActive && shouldReconnectSameTarget(now)) {
            log.warn("No command-routable target found in ring, reconnecting both channels on current: {}", active);
            ws.connect(active);
            lastSameTargetReconnectAt = now;
          } else if (haActive) {
            log.warn("No command-routable target found in ring, same-target reconnect cooldown active: active={}, waitMs={}",
                active, sameTargetReconnectCooldownLeft(now));
          } else {
            log.warn("No command-routable target found in ring, waiting for HA active target: group={}, active={}, haState={}",
                activeGroup, active, ws.haStateOf(active));
          }
        }
        return;
      }

      notReadyStreak = 0;
      failCount = 0;
      updateHealthStatus(active, true);

      if (!(chatOk && emailOk)) {
        recoverStable = 0;
        return;
      }

      if (preferEnabled) {
        String[] higher = higherPreferCandidates(activeGroup, active);
        if (higher.length > 0) {
          String upUrl = firstAliveByIds(higher);
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
      }

      if (recoverEnabled) {
        if (!isInRecover(activeGroup, targetIdOf(active))) {
          String recoverTo = firstAliveByIds(recoverOf(activeGroup));
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
      }

      recoverStable = 0;

    } catch (Exception e) {
      log.warn("Failover tick error", e);
    }
  }

  public String getActiveUrl() { return active; }
  public String getActiveGroup() { return activeGroup; }

  private void ensureAllTargetConnections() {
    ws.connectAll(U1, U2, A1, A2);
  }

  private void healthcheckAllTargets(long now) {
    healthcheckTarget("U1", U1, now);
    healthcheckTarget("U2", U2, now);
    healthcheckTarget("A1", A1, now);
    healthcheckTarget("A2", A2, now);
  }

  private void healthcheckTarget(String id, String url, long now) {
    if (url == null || url.isBlank()) {
      return;
    }

    boolean haActive = ws.isHaActive(url);
    boolean checkSession = haActive || shouldHealthcheckStandby(url, now);
    boolean chatUp = checkSession && evaluateChannelAvailability(url, MessageSender.Channel.CHAT);
    boolean emailUp = checkSession && evaluateChannelAvailability(url, MessageSender.Channel.EMAIL);
    boolean commandRoutable = chatUp && emailUp && haActive;

    updateHealthStatus(url, commandRoutable);
    log.debug("all-target-healthcheck: target={}, url={}, chatUp={}, emailUp={}, haState={}, commandRoutable={}",
        id, url, chatUp, emailUp, ws.haStateOf(url), commandRoutable);
  }

  private void resetCounters() {
    failCount = 0;
    recoverStable = 0;
  }

  public synchronized String ensureCommandTarget(MessageSender.Channel channel) {
    String current = this.active;
    if (ws.isCommandRoutable(current)) {
      return current;
    }

    String found = findFirstCommandRoutableInRing(activeGroup, current);
    if (found != null) {
      if (!found.equals(current)) {
        switchTo(found, "SEND_GUARD_SWITCH");
      }
      return found;
    }

    throw new IllegalStateException("no command-routable target: group=" + activeGroup
        + ", channel=" + channel + ", active=" + current + ", haState=" + ws.haStateOf(current));
  }

  private String findFirstCommandRoutableInRing(String group, String curUrl) {
    String[] ring = ringOf(group);

    if (isCommandRoutableAndLog(curUrl)) return curUrl;

    int idx = indexOf(ring, targetIdOf(curUrl));
    if (idx < 0) idx = 0;

    for (int i = 1; i < ring.length; i++) {
      String url = urlOf(ring[(idx + i) % ring.length]);
      if (isCommandRoutableAndLog(url)) return url;
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

    if (preferEnabled && isInPrefer(group, from) && isInPrefer(group, to)) {
      if (indexOf(preferOf(group), to) < indexOf(preferOf(group), from)) {
        return "UPGRADE";
      }
    }

    if (recoverEnabled && !isInRecover(group, from) && isInRecover(group, to)) {
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

  private String firstAliveByIds(String[] ids) {
    for (String id : ids) {
      String url = urlOf(id);
      if (isCommandRoutableAndLog(url)) return url;
    }
    return null;
  }

  private boolean isCommandRoutableAndLog(String url) {
    if (url == null) {
      return false;
    }

    boolean haActive = ws.isHaActive(url);
    if (!haActive && !shouldHealthcheckStandby(url, System.currentTimeMillis())) {
      log.debug("session-check skipped for standby cooldown: url={}, haState={}", url, ws.haStateOf(url));
      return false;
    }

    boolean chatUp = evaluateChannelAvailability(url, MessageSender.Channel.CHAT);
    boolean emailUp = evaluateChannelAvailability(url, MessageSender.Channel.EMAIL);
    boolean routable = chatUp && emailUp && haActive;
    updateHealthStatus(url, routable);
    log.debug("session-check: url={}, chatUp={}, emailUp={}, haState={}, routable={}",
        url, chatUp, emailUp, ws.haStateOf(url), routable);
    return routable;
  }

  private boolean checkAndRepairChannel(String url, MessageSender.Channel channel, long now) {
    if (url == null) {
      return false;
    }

    boolean open = channel == MessageSender.Channel.CHAT ? ws.isChatOpen(url) : ws.isEmailOpen(url);
    if (open) {
      boolean pingOk = channel == MessageSender.Channel.CHAT ? ws.pingChat(url) : ws.pingEmail(url);
      if (pingOk) {
        return true;
      }
    }

    long lastReconnectAt = channel == MessageSender.Channel.CHAT ? lastChatReconnectAt : lastEmailReconnectAt;
    long cooldownLeft = SAME_TARGET_RECONNECT_COOLDOWN_MS - (now - lastReconnectAt);
    if (cooldownLeft > 0) {
      log.info("{} not ready but cooldown active: active={}, waitMs={}",
          channel.name().toLowerCase(), url, cooldownLeft);
      return false;
    }

    log.warn("{} not ready observed: active={}, wsState={}",
        channel.name().toLowerCase(), url, ws.readinessDebug());
    if (channel == MessageSender.Channel.CHAT) {
      ws.connectChatForce(url);
      lastChatReconnectAt = now;
    } else {
      ws.connectEmailForce(url);
      lastEmailReconnectAt = now;
    }
    return false;
  }

  private boolean evaluateChannelAvailability(String url, MessageSender.Channel channel) {
    boolean open = channel == MessageSender.Channel.CHAT ? ws.isChatOpen(url) : ws.isEmailOpen(url);
    if (!open) {
      if (channel == MessageSender.Channel.CHAT) {
        ws.connectChat(url);
      } else {
        ws.connectEmail(url);
      }
      return false;
    }
    return channel == MessageSender.Channel.CHAT ? ws.pingChat(url) : ws.pingEmail(url);
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

  private String[] configuredIds(String[] ids) {
    java.util.List<String> valid = new java.util.ArrayList<>();
    for (String id : ids) {
      if (id == null || id.isBlank()) {
        continue;
      }
      String url = urlOf(id);
      if (url != null && !url.isBlank()) {
        valid.add(id.trim());
      }
    }
    return valid.toArray(new String[0]);
  }

  private int indexOf(String[] arr, String v) {
    if (v == null) return -1;
    for (int i = 0; i < arr.length; i++) if (v.equals(arr[i])) return i;
    return -1;
  }

  private String urlOf(String id) {
    if (id == null) return null;
    switch (id.trim()) {
      case "U1": return U1;
      case "U2": return U2;
      case "A1": return A1;
      case "A2": return A2;
      default: return null;
    }
  }

  private String targetIdOf(String url) {
    if (url == null) return null;
    String mapped = targetIdByUrl.get(url);
    if (mapped != null) return mapped;
    mapped = ws.targetIdOf(url);
    if (mapped != null) return mapped;
    int i = url.lastIndexOf('/');
    if (i < 0 || i == url.length() - 1) return null;
    return url.substring(i + 1);
  }

  private boolean shouldReconnectSameTarget(long now) {
    return sameTargetReconnectCooldownLeft(now) <= 0;
  }

  private long sameTargetReconnectCooldownLeft(long now) {
    return SAME_TARGET_RECONNECT_COOLDOWN_MS - (now - lastSameTargetReconnectAt);
  }

  private boolean shouldHealthcheckStandby(String url, long now) {
    Long lastAt = lastStandbyHealthcheckAt.get(url);
    if (lastAt != null && now - lastAt < STANDBY_HEALTHCHECK_INTERVAL_MS) {
      return false;
    }
    lastStandbyHealthcheckAt.put(url, now);
    return true;
  }

  private static void putIfPresent(Map<String, String> targetIdByUrl, String url, String id) {
    if (url == null || url.isBlank()) {
      return;
    }
    targetIdByUrl.put(url.trim(), id);
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
