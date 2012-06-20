package com.senseidb.search.req;

import java.io.Serializable;

public class SenseiError implements Serializable {
  private final String message;
  private final ErrorType errorType;
  private final int errorCode;

  public SenseiError(String message, ErrorType errorType) {
    this.message = message;
    this.errorType = errorType;
    this.errorCode = errorType.getDefaultErrorCode();
  }
  public SenseiError(String message, ErrorType errorType, int errorCode) {
    this.message = message;
    this.errorType = errorType;
    this.errorCode = errorCode;
  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((errorType == null) ? 0 : errorType.hashCode());
    result = prime * result + ((message == null) ? 0 : message.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SenseiError other = (SenseiError) obj;
    if (errorType != other.errorType)
      return false;
    if (message == null) {
      if (other.message != null)
        return false;
    } else if (!message.equals(other.message))
      return false;
    return true;
  }
  public String getMessage() {
    return message;
  }
  public ErrorType getErrorType() {
    return errorType;
  }
  public int getErrorCode() {
    return errorCode;
  }

  @Override
  public String toString() {
    ErrorType et = errorType;
    if (et == null)
      et = ErrorType.UnknownError;
    return String.format("%s(%d): %s", et.name(), errorCode, message);
  }
}
