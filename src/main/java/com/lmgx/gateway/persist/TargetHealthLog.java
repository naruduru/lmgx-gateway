package com.lmgx.gateway.persist;

public class TargetHealthLog {
    public java.time.LocalDateTime createdAt;
    public String serverGroup;
    public String targetId;
    public String targetUrl;
  public String checkKind;
  public String isUp;
  public Long latencyMs;
  public String failReason;
  public String detailMsg;
}
