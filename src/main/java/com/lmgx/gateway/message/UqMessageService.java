package com.lmgx.gateway.message;

import java.util.Map;

public interface UqMessageService {
  void onRouteRes(Map<String, Object> message);

  void onTimeout(Map<String, Object> message);

  void onSuccess(Map<String, Object> message);

  void onFailure(Map<String, Object> message);

  void onComplete(Map<String, Object> message);
}
