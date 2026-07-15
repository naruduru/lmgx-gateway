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

  // 현재 이 인스턴스가 따르는 그룹 정책이다. 기본값은 G1이다.
  private volatile String activeGroup = "G1";
  // 현재 업무 command를 보낼 대표 타겟 URL이다.
  private volatile String active;
  private final String sourceIp;

  // 장애 전환 이벤트 로그에 남길 실패 관측 카운터다.
  private int failCount = 0;
  // 복구/우선순위 복귀 후보가 연속으로 정상 확인된 횟수다.
  private int recoverStable = 0;

  // 복구/우선순위 복귀 대상이 이 횟수만큼 연속 정상일 때 실제 전환한다.
  private static final int RECOVER_STABLE_THRESHOLD = 5;
  // 현재 active 타겟이 업무 불가로 관측되어도 즉시 재연결하지 않고 기다리는 횟수다.
  private static final int NOT_READY_THRESHOLD = 3;
  // 같은 타겟에 대해 chat/email 전체 재연결을 반복하지 않기 위한 cooldown이다.
  private static final long SAME_TARGET_RECONNECT_COOLDOWN_MS = 60_000L;
  // 모든 타겟 소켓 연결 유지를 다시 시도하는 주기다.
  private static final long ENSURE_INTERVAL_MS = 10_000L;
  // DB 헬스 상태를 모든 타겟 기준으로 갱신하는 주기다.
  private static final long ALL_TARGET_HEALTHCHECK_INTERVAL_MS = 5_000L;
  // standby 타겟 heartbeat 확인을 제한하는 타겟별 최소 간격이다.
  private static final long STANDBY_HEALTHCHECK_INTERVAL_MS = 5_000L;

  // 현재 active 타겟이 연속으로 업무 불가 상태였던 횟수다.
  private int notReadyStreak = 0;
  // chat 채널을 마지막으로 강제 재연결한 시각이다.
  private volatile long lastChatReconnectAt = 0L;
  // email 채널을 마지막으로 강제 재연결한 시각이다.
  private volatile long lastEmailReconnectAt = 0L;
  // 같은 타겟의 chat/email 전체 재연결을 마지막으로 시도한 시각이다.
  private volatile long lastSameTargetReconnectAt = 0L;
  // 전체 타겟 연결 유지 작업을 마지막으로 수행한 시각이다.
  private volatile long lastEnsureAt = 0L;
  // 전체 타겟 헬스체크를 마지막으로 수행한 시각이다.
  private volatile long lastAllTargetHealthcheckAt = 0L;
  // standby 타겟별 마지막 헬스체크 시각이다.
  private final Map<String, Long> lastStandbyHealthcheckAt = new ConcurrentHashMap<>();

  /**
   * 설정된 타겟과 그룹 순서를 기준으로 장애 전환 정책을 구성한다.
   * G1은 기본적으로 U 계열을, G2는 기본적으로 A 계열을 우선 사용한다.
   */
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
    // 장애 전환 전에 standby/backup 서버 상태도 볼 수 있도록 모든 타겟 소켓을 먼저 연결한다.
    ensureAllTargetConnections();
    long now = System.currentTimeMillis();
    lastEnsureAt = now;
    healthcheckAllTargets(now);
    if (this.active != null) {
      ws.setCurrentUrl(this.active);
      ws.connect(this.active);
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
        // 인스턴스 pause는 로컬 차단 스위치이므로 resume 전까지 모든 소켓을 닫는다.
        notReadyStreak = 0;
        ws.disconnectAll();
        return;
      }

      if (now - lastEnsureAt >= ENSURE_INTERVAL_MS) {
        // 현재 active 타겟뿐 아니라 설정된 모든 타겟 소켓을 계속 준비 상태로 유지한다.
        ensureAllTargetConnections();
        lastEnsureAt = now;
      }

      if (now - lastAllTargetHealthcheckAt >= ALL_TARGET_HEALTHCHECK_INTERVAL_MS) {
        // 모든 타겟의 상태를 갱신한다. standby 타겟은 확인하되 업무 전송 가능으로 보지 않는다.
        healthcheckAllTargets(now);
        lastAllTargetHealthcheckAt = now;
      }

      // 업무 command는 chat/email 두 채널과 타겟 HA active 상태가 모두 필요하다.
      boolean chatOk = checkAndRepairChannel(active, MessageSender.Channel.CHAT, now);
      boolean emailOk = checkAndRepairChannel(active, MessageSender.Channel.EMAIL, now);
      boolean haActive = ws.isHaActive(active);
      log.debug("tick: group={}, active={}, chatOk={}, emailOk={}, haActive={}",
          activeGroup, active, chatOk, emailOk, haActive);

      if (!(chatOk && emailOk && haActive)) {
        notReadyStreak++;
        log.warn("target not command-routable observed: group={}, active={}, streak={}, haState={}, wsState={}",
            activeGroup, active, notReadyStreak, ws.haStateOf(active), ws.readinessDebug());

        // 연결은 됐지만 standby인 타겟에 업무 command가 가지 않도록 ring을 즉시 탐색한다.
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
                log.info("target not ready but alive, keeping existing sessions and rechecking later: {}, streak={}, wsState={}",
                    active, notReadyStreak, ws.readinessDebug());
                ws.connect(active);
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
            log.warn("No command-routable target found in ring, keeping existing sessions on current: {}", active);
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
        // 더 높은 우선순위 타겟은 여러 tick 동안 안정적으로 업무 가능할 때만 복귀한다.
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
          // 반대 그룹으로 넘어간 상태라면 안정성 확인 후 현재 그룹의 주 타겟으로 복구한다.
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

  // 설정된 모든 타겟에 대해 chat/email 소켓 연결을 시도한다.
  private void ensureAllTargetConnections() {
    ws.connectAll(U1, U2, A1, A2);
  }

  /**
   * 현재 업무 라우팅 대상과 별개로 설정된 모든 타겟의 상태를 주기적으로 확인한다.
   */
  private void healthcheckAllTargets(long now) {
    healthcheckTarget("U1", U1, now);
    healthcheckTarget("U2", U2, now);
    healthcheckTarget("A1", A1, now);
    healthcheckTarget("A2", A2, now);
  }

  // 개별 타겟의 chat/email 연결과 HA 상태를 평가해 업무 가능 여부를 DB에 반영한다.
  private void healthcheckTarget(String id, String url, long now) {
    if (url == null || url.isBlank()) {
      return;
    }

    // standby 타겟은 제한된 주기로 ping하지만 DB 상태는 업무 불가로 기록한다.
    boolean haActive = ws.isHaActive(url);
    boolean checkSession = haActive || shouldHealthcheckStandby(url, now);
    boolean chatUp = checkSession && evaluateChannelAvailability(url, MessageSender.Channel.CHAT);
    boolean emailUp = checkSession && evaluateChannelAvailability(url, MessageSender.Channel.EMAIL);
    boolean commandRoutable = chatUp && emailUp && haActive;

    updateHealthStatus(url, commandRoutable);
    log.debug("all-target-healthcheck: target={}, url={}, chatUp={}, emailUp={}, haState={}, commandRoutable={}",
        id, url, chatUp, emailUp, ws.haStateOf(url), commandRoutable);
  }

  // 장애/복구 판단에 쓰는 연속 카운터들을 초기화한다.
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
        // 전송 직전 마지막 방어 로직이다. 스케줄러가 아직 전환하지 못했다면 여기서 전환한다.
        switchTo(found, "SEND_GUARD_SWITCH");
      }
      return found;
    }

    throw new IllegalStateException("no command-routable target: group=" + activeGroup
        + ", channel=" + channel + ", active=" + current + ", haState=" + ws.haStateOf(current));
  }

  // 현재 타겟부터 ring 순서대로 업무 command를 보낼 수 있는 첫 타겟을 찾는다.
  private String findFirstCommandRoutableInRing(String group, String curUrl) {
    String[] ring = ringOf(group);

    if (isCommandRoutableAndLog(curUrl)) return curUrl;

    int idx = indexOf(ring, targetIdOf(curUrl));
    if (idx < 0) idx = 0;

    // 현재 타겟 다음부터 설정된 ring을 한 바퀴 돌며 업무 가능한 첫 타겟을 찾는다.
    for (int i = 1; i < ring.length; i++) {
      String url = urlOf(ring[(idx + i) % ring.length]);
      if (isCommandRoutableAndLog(url)) return url;
    }
    return null;
  }

  // active 타겟을 교체하고 이벤트 로그를 남긴다.
  private synchronized void switchTo(String toUrl, String reason) {
    String fromUrl = this.active;
    if (fromUrl != null && fromUrl.equals(toUrl)) return;

    String eventKind = decideEventKind(activeGroup, fromUrl, toUrl);
    log.info("switch: {} -> {} (kind={}, reason={})", fromUrl, toUrl, eventKind, reason);

    this.active = toUrl;
    // 라우팅 대상만 교체하고, 기존 세션은 닫지 않는다.
    ws.setCurrentUrl(toUrl);
    ws.connect(toUrl);

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

  // 전환 전후 타겟의 위치를 기준으로 SWITCH/RECOVER/UPGRADE 이벤트 종류를 결정한다.
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

  // 현재 타겟보다 우선순위가 높은 prefer 후보 목록만 잘라낸다.
  private String[] higherPreferCandidates(String group, String curUrl) {
    String[] prefer = preferOf(group);
    String curId = targetIdOf(curUrl);

    int idx = indexOf(prefer, curId);
    if (idx <= 0) return new String[0];

    String[] higher = new String[idx];
    System.arraycopy(prefer, 0, higher, 0, idx);
    return higher;
  }

  // 주어진 타겟 ID 목록 중 업무 가능 상태인 첫 타겟 URL을 찾는다.
  private String firstAliveByIds(String[] ids) {
    for (String id : ids) {
      String url = urlOf(id);
      if (isCommandRoutableAndLog(url)) return url;
    }
    return null;
  }

  // 타겟이 chat/email 연결과 HA active 조건을 모두 만족하는지 확인하고 상태를 기록한다.
  private boolean isCommandRoutableAndLog(String url) {
    if (url == null) {
      return false;
    }

    // 연결된 standby 타겟은 헬스체크 대상이지만 업무 라우팅에서는 제외한다.
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

  // 현재 active 타겟의 특정 채널을 heartbeat로 확인하고 필요하면 강제 재연결한다.
  private boolean checkAndRepairChannel(String url, MessageSender.Channel channel, long now) {
    if (url == null) {
      return false;
    }

    boolean open = channel == MessageSender.Channel.CHAT ? ws.isChatOpen(url) : ws.isEmailOpen(url);
    if (open) {
      // 소켓이 열려 있어도 사용할 수 없을 수 있으므로 heartbeat 성공을 준비 상태로 본다.
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

  // 후보 타겟의 특정 채널이 열려 있고 heartbeat까지 성공하는지 확인한다.
  private boolean evaluateChannelAvailability(String url, MessageSender.Channel channel) {
    boolean open = channel == MessageSender.Channel.CHAT ? ws.isChatOpen(url) : ws.isEmailOpen(url);
    if (!open) {
      // 수동 확인 중에도 재연결을 걸어 backup 타겟이 빠르게 업무 가능 상태가 되게 한다.
      if (channel == MessageSender.Channel.CHAT) {
        ws.connectChat(url);
      } else {
        ws.connectEmail(url);
      }
      return false;
    }
    return channel == MessageSender.Channel.CHAT ? ws.pingChat(url) : ws.pingEmail(url);
  }

  // activeGroup에 맞는 장애 전환 ring을 반환한다.
  private String[] ringOf(String g) { return "G2".equals(g) ? ringG2 : ringG1; }
  // activeGroup에 맞는 복구 대상 목록을 반환한다.
  private String[] recoverOf(String g) { return "G2".equals(g) ? recoverG2 : recoverG1; }
  // activeGroup에 맞는 우선순위 대상 목록을 반환한다.
  private String[] preferOf(String g) { return "G2".equals(g) ? preferG2 : preferG1; }

  // 타겟 ID가 현재 그룹의 복구 대상에 포함되는지 확인한다.
  private boolean isInRecover(String g, String id) { return indexOf(recoverOf(g), id) >= 0; }
  // 타겟 ID가 현재 그룹의 우선순위 대상에 포함되는지 확인한다.
  private boolean isInPrefer(String g, String id) { return indexOf(preferOf(g), id) >= 0; }

  // 쉼표로 구분된 타겟 ID 설정을 배열로 변환한다.
  private String[] parseIds(String csv) {
    String[] parts = csv.split(",");
    for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
    return parts;
  }

  // 설정에 존재하는 URL을 가진 타겟 ID만 필터링한다.
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

  // 배열 안에서 특정 타겟 ID의 위치를 찾는다.
  private int indexOf(String[] arr, String v) {
    if (v == null) return -1;
    for (int i = 0; i < arr.length; i++) if (v.equals(arr[i])) return i;
    return -1;
  }

  // 타겟 ID를 실제 WebSocket URL로 변환한다.
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

  // WebSocket URL에 대응하는 타겟 ID를 찾는다.
  private String targetIdOf(String url) {
    if (url == null) return null;
    // 환경별 endpoint suffix가 달라질 수 있으므로 설정 기반 URL 매핑을 우선한다.
    String mapped = targetIdByUrl.get(url);
    if (mapped != null) return mapped;
    mapped = ws.targetIdOf(url);
    if (mapped != null) return mapped;
    int i = url.lastIndexOf('/');
    if (i < 0 || i == url.length() - 1) return null;
    return url.substring(i + 1);
  }

  // 같은 타겟에 대한 전체 재연결 cooldown이 끝났는지 확인한다.
  private boolean shouldReconnectSameTarget(long now) {
    return sameTargetReconnectCooldownLeft(now) <= 0;
  }

  // 같은 타겟에 대한 전체 재연결 cooldown 남은 시간을 계산한다.
  private long sameTargetReconnectCooldownLeft(long now) {
    return SAME_TARGET_RECONNECT_COOLDOWN_MS - (now - lastSameTargetReconnectAt);
  }

  // standby 타겟 헬스체크를 너무 자주 하지 않도록 타겟별 주기를 제한한다.
  private boolean shouldHealthcheckStandby(String url, long now) {
    Long lastAt = lastStandbyHealthcheckAt.get(url);
    if (lastAt != null && now - lastAt < STANDBY_HEALTHCHECK_INTERVAL_MS) {
      return false;
    }
    lastStandbyHealthcheckAt.put(url, now);
    return true;
  }

  // URL이 설정된 경우에만 URL -> 타겟 ID 매핑에 추가한다.
  private static void putIfPresent(Map<String, String> targetIdByUrl, String url, String id) {
    if (url == null || url.isBlank()) {
      return;
    }
    targetIdByUrl.put(url.trim(), id);
  }

  // 타겟 host 기준 헬스 상태를 DB에 upsert한다.
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

  // 장애 전환 이벤트 저장 실패가 루프를 중단하지 않도록 보호한다.
  private void safeInsertEvent(FailoverEventLog log) {
    try { logMapper.insertFailoverEvent(log); } catch (Exception ignore) {}
  }

  // WebSocket URL에서 host 부분만 추출한다.
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

  // 설정값이 있으면 사용하고 없으면 로컬 host IP를 source IP로 사용한다.
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
