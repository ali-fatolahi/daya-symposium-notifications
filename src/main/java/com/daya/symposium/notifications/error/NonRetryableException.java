package com.daya.symposium.notifications.error;

public class NonRetryableException extends RuntimeException {
  public NonRetryableException(final String message) {
    super(message);
  }

  public NonRetryableException(final Throwable cause) {
    super(cause);
  }
}
