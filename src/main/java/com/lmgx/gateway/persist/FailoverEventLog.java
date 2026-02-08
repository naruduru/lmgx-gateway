package com.lmgx.gateway.persist;

public class FailoverEventLog {
    public java.time.LocalDateTime createdAt;
    public String serverGroup;
    public String fromTarget;
  public String toTarget;
  public String fromUrl;
  public String toUrl;

  public String eventKind;
  public String triggerReason;

  public Integer failCount;
  public Integer recoverStable;
}
