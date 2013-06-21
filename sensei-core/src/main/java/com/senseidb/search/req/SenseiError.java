/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
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
