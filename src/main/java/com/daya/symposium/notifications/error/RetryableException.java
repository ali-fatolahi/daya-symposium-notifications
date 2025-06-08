package com.daya.symposium.notifications.error;

public class RetryableException extends RuntimeException {
  public RetryableException(final String message) {
    super(message);
  }

  public RetryableException(final Throwable cause) {
    super(cause);
  }
}
