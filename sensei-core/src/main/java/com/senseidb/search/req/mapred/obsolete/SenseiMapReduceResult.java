package com.senseidb.search.req.mapred.obsolete;

import java.util.List;

import com.linkedin.bobo.mapred.MapReduceResult;
import com.senseidb.search.req.AbstractSenseiResult;

public class SenseiMapReduceResult extends MapReduceResult implements AbstractSenseiResult {  
  private static final long serialVersionUID = 1L;
  
 
  
  private long time;
  
  public List getMapResults() {
    return mapResults;
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
