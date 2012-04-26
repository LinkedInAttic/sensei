package com.senseidb.search.client.json.req;


import java.util.Map;

public class MapReduce {
  private String function;
  private Map<String, Object> parameters;
  public String getFunction() {
    return function;
  }
  public void setFunction(String function) {
    this.function = function;
  }
  public Map<String, Object> getParameters() {
    return parameters;
  }
  public void setParameters(Map<String, Object> parameters) {
    this.parameters = parameters;
  }
  public MapReduce(String function, Map<String, Object> parameters) {
    super();
    this.function = function;
    this.parameters = parameters;
  }
  
}
