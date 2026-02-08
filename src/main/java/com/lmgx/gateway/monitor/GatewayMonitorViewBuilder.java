package com.lmgx.gateway.monitor;

import com.lmgx.core.ui.component.Badge;
import com.lmgx.core.ui.component.Button;
import com.lmgx.core.ui.component.Card;
import com.lmgx.core.ui.component.StatusPanel;
import com.lmgx.core.ui.component.StyleBlock;
import com.lmgx.core.ui.component.Table;
import com.lmgx.core.ui.component.TableRow;
import com.lmgx.core.ui.component.Text;
import com.lmgx.core.ui.event.LmgxUiEventRegistry;
import com.lmgx.core.ui.event.LmgxUiEvents;
import com.lmgx.core.ui.layout.Div;
import com.lmgx.core.ui.layout.Grid;
import com.lmgx.core.ui.layout.Page;
import com.lmgx.core.ui.layout.Row;
import com.lmgx.core.ui.layout.Span;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class GatewayMonitorViewBuilder {
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int LOG_LIMIT = 10;

    private final GatewayMonitorService service;
    private final LmgxUiEventRegistry eventRegistry;

    public GatewayMonitorViewBuilder(GatewayMonitorService service, LmgxUiEventRegistry eventRegistry) {
        this.service = service;
        this.eventRegistry = eventRegistry;
    }

    public com.lmgx.core.ui.component.UiComponent buildPage() {
        Page page = new Page();
        page.addClass("gw-monitor-page");
        page.add(buildStyles());

        Div header = new Div();
        header.addClass("gw-monitor-header");
        header.add(
            Text.h1("Gateway Monitor"),
            Text.p("서버 상태를 모니터링하고 필요 시 강제로 중단/복구합니다.")
        );
        page.add(header);

        page.add(buildRootComponent());
        return page;
    }

    public Div buildRootComponent() {
        Div root = new Div();
        root.id("monitor-root");
        root.addClass("gw-monitor-root");

        Div auto = new Div();
        auto.id("monitor-auto");
        auto.attr("data-id", "monitor-auto");
        auto.attr("data-evt", "click");
        LmgxUiEvents.on(auto, eventRegistry).onAuto("monitor-root", service.uiRefreshMs(), this::buildRootComponent);

        Div left = new Div();
        left.addClass("gw-monitor-col");
        left.add(buildInstanceGrid(), buildTargetGrid());

        Div right = new Div();
        right.addClass("gw-monitor-col");
        right.addClass("gw-monitor-col--right");
        right.add(buildEventLog(), buildHealthLog());

        Div split = new Div();
        split.addClass("gw-monitor-split");
        split.add(left, right);

        root.add(auto, split);
        return root;
    }


    private Card buildInstanceGrid() {
        Card card = new Card();
        card.addClass("gw-monitor-card");
        card.add(Text.h3("인스턴스 연결 상태"));

        var rows = service.instanceStatuses();
        if (rows.isEmpty()) {
            card.add(StatusPanel.empty("인스턴스 없음", "등록된 인스턴스 상태가 없습니다."));
            return card;
        }

        Grid grid = new Grid();
        grid.addClass("gw-monitor-grid");
        for (var row : rows) {
            grid.add(buildInstanceCard(row));
        }
        card.add(grid);
        return card;
    }

    private Card buildInstanceCard(com.lmgx.gateway.persist.InstanceStatus row) {
        Card card = new Card();
        card.addClass("gw-monitor-card");

        if (row.serverPort != null) {
            Text url = Text.small("http://127.0.0.1:" + row.serverPort);
            url.addClass("gw-monitor-url");
            card.add(url);
        }

        Span dot = new Span();
        dot.addClass("gw-monitor-dot");
        dot.addClass(row.ready != null && row.ready ? "gw-monitor-dot--ok" : "gw-monitor-dot--down");

        Text title = Text.h3(safe(row.instanceId));
        title.addClass("gw-monitor-title");
        Badge status = badge(row.ready != null && row.ready ? "READY" : "NOT READY",
            row.ready != null && row.ready ? "success" : "danger");
        status.addClass("gw-monitor-status-pill");

        Row titleRow = new Row();
        titleRow.addClass("gw-monitor-title-row");
        Row titleLeft = new Row();
        titleLeft.addClass("gw-monitor-title-left");
        titleLeft.add(dot, title);
        Row titleMeta = new Row();
        titleMeta.addClass("gw-monitor-title-meta");
        titleMeta.add(status);
        titleRow.add(titleLeft, titleMeta);
        card.add(titleRow);

        Row meta = new Row();
        meta.addClass("gw-monitor-row");
        meta.add(badge("GROUP " + safe(row.groupId), "info"));
        meta.add(badge("PORT " + (row.serverPort == null ? "-" : row.serverPort.toString()), "warning"));
        meta.add(badge("ACTIVE " + safe(row.activeTarget), "success"));
        card.add(meta);

        Row details = new Row();
        details.addClass("gw-monitor-row");
        details.addClass("gw-monitor-status");
        details.add(badge("CHAT " + boolLabel(row.chatOpen), row.chatOpen != null && row.chatOpen ? "success" : "danger"));
        details.add(badge("EMAIL " + boolLabel(row.emailOpen), row.emailOpen != null && row.emailOpen ? "success" : "danger"));
        details.add(badge("READY " + boolLabel(row.ready), row.ready != null && row.ready ? "success" : "danger"));
        card.add(details);

        Row actions = new Row();
        actions.addClass("gw-monitor-actions");
        Button kill = new Button("Kill").tone("danger").variant("outline").size("sm");
        kill.id("kill-inst-" + safe(row.instanceId));
        kill.attr("data-click-loading", "true");
        kill.attr("data-ajax-url", "/admin/instance/" + safe(row.instanceId) + "/pause?enabled=true");
        kill.attr("data-ajax-method", "POST");
        kill.attr("data-ajax-refresh", "/monitor/fragment");
        kill.attr("data-ajax-target", "monitor-root");
        LmgxUiEvents.on(kill, eventRegistry).onClick("monitor-root", this::buildRootComponent);

        Button restore = new Button("Restore").tone("success").variant("outline").size("sm");
        restore.id("restore-inst-" + safe(row.instanceId));
        restore.attr("data-click-loading", "true");
        restore.attr("data-ajax-url", "/admin/instance/" + safe(row.instanceId) + "/pause?enabled=false");
        restore.attr("data-ajax-method", "POST");
        restore.attr("data-ajax-refresh", "/monitor/fragment");
        restore.attr("data-ajax-target", "monitor-root");
        LmgxUiEvents.on(restore, eventRegistry).onClick("monitor-root", this::buildRootComponent);

        actions.add(kill, restore);
        card.add(actions);

        String time = row.updatedAt == null ? "-" : TIME_FMT.format(row.updatedAt);
        Text updated = Text.small("Updated: " + time);
        updated.addClass("gw-monitor-url");
        card.add(updated);

        return card;
    }

    private Card buildTargetGrid() {
        Card card = new Card();
        card.addClass("gw-monitor-card");
        card.add(Text.h3("서버 연결 상태"));

        Grid grid = new Grid();
        grid.addClass("gw-monitor-grid");
        for (GatewayMonitorService.TargetStatus target : service.targets()) {
            grid.add(buildTargetCard(target));
        }
        card.add(grid);
        return card;
    }

    private Card buildEventLog() {
        Card card = new Card();
        card.addClass("gw-monitor-card");
        card.addClass("gw-monitor-log");
        card.add(Text.h3("최근 Failover 이벤트"));

        Table table = new Table();
        table.addHeaderRow("시간", "그룹", "FROM", "TO", "종류", "사유");

        var rows = service.recentEvents(LOG_LIMIT);
        if (rows.isEmpty()) {
            card.add(StatusPanel.empty("이벤트 없음", "최근 이벤트가 없습니다."));
            return card;
        }

        for (var row : rows) {
            String time = row.createdAt == null ? "-" : TIME_FMT.format(row.createdAt);
            TableRow tr = new TableRow();
            tr.addCell(time)
              .addCell(safe(row.serverGroup))
              .addCell(safe(row.fromTarget))
              .addCell(safe(row.toTarget))
              .addCell(safe(row.eventKind))
              .addCell(safe(row.triggerReason));
            table.add(tr);
        }
        card.add(table);
        return card;
    }

    private Card buildHealthLog() {
        Card card = new Card();
        card.addClass("gw-monitor-card");
        card.addClass("gw-monitor-log");
        card.add(Text.h3("최근 Health 로그"));

        Table table = new Table();
        table.addHeaderRow("시간", "그룹", "타겟", "종류", "UP", "지연(ms)", "사유");

        var rows = service.recentHealth(LOG_LIMIT);
        if (rows.isEmpty()) {
            card.add(StatusPanel.empty("헬스 로그 없음", "최근 헬스 체크 로그가 없습니다."));
            return card;
        }

        for (var row : rows) {
            String time = row.createdAt == null ? "-" : TIME_FMT.format(row.createdAt);
            TableRow tr = new TableRow();
            tr.addCell(time)
              .addCell(safe(row.serverGroup))
              .addCell(safe(row.targetId))
              .addCell(safe(row.checkKind))
              .addCell(safe(row.isUp))
              .addCell(row.latencyMs == null ? "-" : Long.toString(row.latencyMs))
              .addCell(safe(row.failReason));
            table.add(tr);
        }
        card.add(table);
        return card;
    }

    private Card buildTargetCard(GatewayMonitorService.TargetStatus target) {
        Card card = new Card();
        card.addClass("gw-monitor-card");
        if (target.url() != null) {
            Text url = Text.small(target.url());
            url.addClass("gw-monitor-url");
            card.add(url);
        }
        Span connDot = new Span();
        connDot.addClass("gw-monitor-dot");
        connDot.addClass(connectionDotClass(target));

        Text title = Text.h3(target.id());
        title.addClass("gw-monitor-title");
        Badge status = badge(target.statusLabel(), target.statusTone());
        status.addClass("gw-monitor-status-pill");
        Badge ack = badge(target.ackEnabled() ? "ACK ON" : "ACK OFF", target.ackEnabled() ? "success" : "danger");
        status.addClass("gw-monitor-badge--fixed");
        ack.addClass("gw-monitor-badge--fixed");
        Row titleRow = new Row();
        titleRow.addClass("gw-monitor-title-row");
        Row titleMeta = new Row();
        titleMeta.addClass("gw-monitor-title-meta");
        titleMeta.addClass("gw-monitor-title-meta--stack");
        titleMeta.add(status, ack);
        Row titleLeft = new Row();
        titleLeft.addClass("gw-monitor-title-left");
        titleLeft.add(connDot, title);
        titleRow.add(titleLeft, titleMeta);
        card.add(titleRow);

        Row primaryRow = new Row();
        primaryRow.addClass("gw-monitor-row");
        primaryRow.addClass("gw-monitor-badge-stack");
        Badge rank = badge(target.rankLabel(), "info");
        rank.addClass("gw-monitor-badge--fixed");
        primaryRow.add(rank);
        card.add(primaryRow);

        Row instanceRow = new Row();
        instanceRow.addClass("gw-monitor-row");
        Badge instanceBadge;
        if (!target.connectedInstances().isEmpty()) {
            instanceBadge = badge("INSTANCE CONNECTED: " + String.join(", ", target.connectedInstances()), "info");
        } else {
            instanceBadge = badge("INSTANCE IDLE", "warning");
        }
        instanceBadge.addClass("gw-monitor-badge--tall");
        instanceRow.add(instanceBadge);
        card.add(instanceRow);

        Row row = new Row();
        row.addClass("gw-monitor-row");
        row.addClass("gw-monitor-status");
        Badge chat = badge(target.active() ? (target.chatOpen() ? "CHAT READY" : "CHAT NOT READY") : "CHAT N/A",
            target.active() ? (target.chatOpen() ? "success" : "danger") : "warning");
        Badge email = badge(target.active() ? (target.emailOpen() ? "EMAIL READY" : "EMAIL NOT READY") : "EMAIL N/A",
            target.active() ? (target.emailOpen() ? "success" : "danger") : "warning");
        chat.addClass("gw-monitor-badge--fixed");
        email.addClass("gw-monitor-badge--fixed");
        row.add(chat);
        row.add(email);
        card.add(row);

        Row actions = new Row();
        actions.addClass("gw-monitor-actions");
        Button kill = new Button("Kill").tone("danger").variant("outline").size("sm");
        kill.id("kill-" + target.id());
        kill.attr("data-click-loading", "true");
        kill.attr("data-ajax-url", "/admin/target/" + target.id() + "/ack?enabled=false");
        kill.attr("data-ajax-method", "POST");
        kill.attr("data-ajax-refresh", "/monitor/fragment");
        kill.attr("data-ajax-target", "monitor-root");
        LmgxUiEvents.on(kill, eventRegistry).onClick("monitor-root", this::buildRootComponent);

        Button restore = new Button("Restore").tone("success").variant("outline").size("sm");
        restore.id("restore-" + target.id());
        restore.attr("data-click-loading", "true");
        restore.attr("data-ajax-url", "/admin/target/" + target.id() + "/ack?enabled=true");
        restore.attr("data-ajax-method", "POST");
        restore.attr("data-ajax-refresh", "/monitor/fragment");
        restore.attr("data-ajax-target", "monitor-root");
        LmgxUiEvents.on(restore, eventRegistry).onClick("monitor-root", this::buildRootComponent);

        actions.add(kill, restore);
        card.add(actions);

        Text updated = Text.small("Updated: " + TIME_FMT.format(LocalDateTime.now()));
        updated.addClass("gw-monitor-url");
        card.add(updated);
        return card;
    }

    private Badge badge(String text, String tone) {
        return new Badge(text).tone(tone).size("sm");
    }

    private StyleBlock buildStyles() {
        return new StyleBlock().css(
            ".gw-monitor-page{height:100vh;display:flex;flex-direction:column;padding:12px;background:linear-gradient(140deg,#f5f2ea 0%,#eef1f6 55%,#f7f1ee 100%);overflow:hidden;}" +
            ".gw-monitor-header{margin-bottom:4px;color:#1c2430;}" +
            ".gw-monitor-header h1{color:#1c2430;font-size:22px;line-height:1.1;margin:0 0 2px;}" +
            ".gw-monitor-header p{color:#1c2430;font-size:11px;line-height:1.2;margin:0;}" +
            ".gw-monitor-root{flex:1;min-height:0;}" +
            ".gw-monitor-split{display:grid;grid-template-columns:1.4fr 0.6fr;gap:8px;height:100%;}" +
            ".gw-monitor-col{display:flex;flex-direction:column;gap:8px;min-height:0;}" +
            ".gw-monitor-col--right{height:100%;}" +
            ".gw-monitor-grid{display:grid;gap:8px;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));}" +
            ".gw-monitor-card{display:flex;flex-direction:column;gap:6px;}" +
            ".gw-monitor-title{letter-spacing:0.2px;}" +
            ".gw-monitor-title-row{display:flex;align-items:center;justify-content:space-between;gap:8px;padding-bottom:6px;border-bottom:1px solid #e5e7eb;}" +
            ".gw-monitor-title-left{display:flex;align-items:center;gap:8px;}" +
            ".gw-monitor-status-pill{white-space:nowrap;}" +
            ".gw-monitor-title-meta{display:flex;gap:6px;align-items:center;}" +
            ".gw-monitor-title-meta--stack{flex-direction:column;align-items:stretch;gap:4px;width:max-content;}" +
            ".gw-monitor-badge--fixed{width:100%;justify-content:center;height:20px;}" +
            ".gw-monitor-badge--tall{height:20px;}" +
            ".gw-monitor-dot{width:14px;height:14px;border-radius:50%;display:inline-block;border:2px solid #0f172a;}" +
            ".gw-monitor-dot--ok{background:#22c55e;}" +
            ".gw-monitor-dot--warn{background:#facc15;}" +
            ".gw-monitor-dot--down{background:#ef4444;}" +
            ".gw-monitor-row{display:flex;flex-wrap:wrap;gap:8px;align-items:center;}" +
            ".gw-monitor-status{flex-direction:column;align-items:stretch;width:max-content;}" +
            ".gw-monitor-actions{display:flex;flex-wrap:wrap;gap:8px;margin-top:4px;}" +
            ".gw-monitor-url{color:#445267;word-break:break-all;}" +
            ".gw-monitor-meta{padding:10px;background:#fbfbfd;border:1px solid #e5e7eb;color:#1c2430;}" +
            ".gw-monitor-card .lmgx-button{min-width:96px;}" +
            ".gw-monitor-log{flex:1;min-height:0;font-size:12px;}" +
            ".gw-monitor-log .lmgx-table{display:block;flex:1;max-height:none;overflow:auto;border:1px solid #e5e7eb;border-radius:10px;}" +
            ".gw-monitor-badge-stack{align-items:stretch;width:max-content;}"
        );
    }

    private String safe(String value) {
        return value == null || value.isBlank() ? "-" : value;
    }

    private String boolLabel(Boolean value) {
        if (value == null) return "-";
        return value ? "ON" : "OFF";
    }

    private String connectionDotClass(GatewayMonitorService.TargetStatus target) {
        if (!target.connectedInstances().isEmpty()) {
            return "gw-monitor-dot--ok";
        }
        if (target.active()) {
            return target.localConnected() ? "gw-monitor-dot--ok" : "gw-monitor-dot--down";
        }
        return target.serverUp() ? "gw-monitor-dot--warn" : "gw-monitor-dot--down";
    }
}
