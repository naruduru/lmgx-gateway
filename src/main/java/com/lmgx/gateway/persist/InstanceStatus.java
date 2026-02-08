package com.lmgx.gateway.persist;

import java.time.LocalDateTime;

public class InstanceStatus {
    public String instanceId;
    public String groupId;
    public Integer serverPort;
    public String activeUrl;
    public String activeTarget;
    public Boolean chatOpen;
    public Boolean emailOpen;
    public Boolean ready;
    public LocalDateTime updatedAt;
}
