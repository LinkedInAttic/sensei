package com.senseidb.search.req;

public enum ErrorType {
  JsonParsingError(100),
  JsonCompilationError(101),
  BQLParsingError(150), 
  BoboExecutionError(200), 
  ExecutionTimeout(250), 
  BrokerGatherError(300), 
  PartitionCallError(350), 
  BrokerTimeout(400), 
  InternalError(450), 
  MergePartitionError(500),
  FederatedBrokerUnavailable(550),
  UnknownError(1000);
  
  private final int defaultErrorCode;

  private ErrorType(int defaultErrorCode) {
    this.defaultErrorCode = defaultErrorCode;
  }
  public int getDefaultErrorCode() {
    return defaultErrorCode;
  }
}
