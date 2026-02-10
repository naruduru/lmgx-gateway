package com.lmgx.gateway.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class DefaultUqMessageService implements UqMessageService {
  private static final Logger log = LoggerFactory.getLogger(DefaultUqMessageService.class);

  @Override
  public void onRouteRes(Map<String, Object> message) {
    log.info("service route res: {}", message);
  }

  @Override
  public void onTimeout(Map<String, Object> message) {
    log.info("service timeout: {}", message);
  }

  @Override
  public void onSuccess(Map<String, Object> message) {
    log.info("service success: {}", message);
  }

  @Override
  public void onFailure(Map<String, Object> message) {
    log.info("service failure: {}", message);
  }

  @Override
  public void onComplete(Map<String, Object> message) {
    log.info("service complete: {}", message);
  }
}
