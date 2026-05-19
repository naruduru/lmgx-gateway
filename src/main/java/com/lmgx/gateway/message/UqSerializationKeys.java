package com.lmgx.gateway.message;

final class UqSerializationKeys {
  private static final String PREFIX = "serialized-command:";

  private UqSerializationKeys() {
  }

  static String lockKey(int requestCommand) {
    return PREFIX + requestCommand;
  }
}
