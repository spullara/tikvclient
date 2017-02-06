package com.sampullara.pingcap;

/**
 * Created by sam on 2/6/17.
 */
public class TiException extends RuntimeException {
  public TiException() {
    super();
  }

  public TiException(String message) {
    super(message);
  }

  public TiException(String message, Throwable cause) {
    super(message, cause);
  }

  public TiException(Throwable cause) {
    super(cause);
  }

  protected TiException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
