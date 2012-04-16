package com.senseidb.search.client.json.req;

import java.util.HashMap;
import java.util.Map;

public class MapReduce {
  String function;
  Map<String, String> parameters;
  
  private MapReduce(String function){
    this.function = function;
    parameters = new HashMap<String, String>();
  }
  
  private void addProperties(String key, String value){
    parameters.put(key, value);
  }
  
  public static class Builder {
    private MapReduce mapReduce;
    public Builder(String function){
      mapReduce = new MapReduce(function);
    }

    
    public Builder addParams(String key, String value){
      mapReduce.addProperties(key, value);
      return this;
    }
    
    public MapReduce build(){
      return mapReduce;
    }
    
  }
  public static Builder builder(String string){
    return new Builder(string);
  }
}

