package com.senseidb.search.req.mapred.impl;

import java.util.List;

import com.senseidb.search.req.AbstractSenseiResult;

public class MapReduceResult implements AbstractSenseiResult {  
  private static final long serialVersionUID = 1L;
  
  private List mapResults;

  private Object reduceResult;
  
  private long time;
  
  public List getMapResults() {
    return mapResults;
  }

  public MapReduceResult setMapResults(List mapResults) {
    this.mapResults = mapResults;
    return this;
  }



  public Object getReduceResult() {
    return reduceResult;
   
  }

  public MapReduceResult setReduceResult(Object reduceResult) {
    this.reduceResult = reduceResult;
    return this;
  }

  @Override
  public long getTime() {    
    return time;
  }
  @Override
  public void setTime(long searchTimeMillis) {
    this.time = searchTimeMillis;    
    
  }
}
