package com.senseidb.search.req;

public class SenseiError {
  private final String message;
  private final ErrorType errorType;

  public SenseiError(String message, ErrorType errorType) {
    this.message = message;
    this.errorType = errorType;
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
  
  
  
}