package com.lmgx.gateway.message;

public class InFlightRequestException extends RuntimeException {
  public InFlightRequestException(String message) {
    super(message);
  }
}
