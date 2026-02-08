package com.lmgx.gateway.persist;

import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface GatewayLogMapper {
  int insertTargetHealth(TargetHealthLog log);
  int insertFailoverEvent(FailoverEventLog log);
  java.util.List<TargetHealthLog> selectRecentHealth(int limit);
  java.util.List<FailoverEventLog> selectRecentEvents(int limit);
  int upsertInstanceStatus(InstanceStatus status);
  java.util.List<InstanceStatus> selectInstanceStatus();
}
