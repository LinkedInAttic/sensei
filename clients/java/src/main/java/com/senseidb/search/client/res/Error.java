package com.senseidb.search.client.res;

public class Error {
  private String message;
  private String errorType;
  public String getMessage() {
    return message;
  }
  public void setMessage(String message) {
    this.message = message;
  }
  public String getErrorType() {
    return errorType;
  }
  public void setErrorType(String errorType) {
    this.errorType = errorType;
  }
  
}
