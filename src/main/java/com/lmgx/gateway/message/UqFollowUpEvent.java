package com.lmgx.gateway.message;

import java.util.Map;

public record UqFollowUpEvent(
    String action,
    Map<String, Object> payload
) {
}
