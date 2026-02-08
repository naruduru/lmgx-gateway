package com.lmgx.gateway.monitor.lmgx;

import com.lmgx.core.ui.component.UiComponent;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GatewayMonitorController {
    private final GatewayMonitorViewBuilder viewBuilder;

    public GatewayMonitorController(GatewayMonitorViewBuilder viewBuilder) {
        this.viewBuilder = viewBuilder;
    }

    @GetMapping(value = "/monitor", produces = MediaType.TEXT_HTML_VALUE)
    public String monitor() {
        UiComponent root = viewBuilder.buildPage();
        return root.renderHtml();
    }

    @GetMapping(value = "/monitor/fragment", produces = MediaType.TEXT_HTML_VALUE)
    public String fragment() {
        UiComponent root = viewBuilder.buildRootComponent();
        return root.renderHtml();
    }
}
