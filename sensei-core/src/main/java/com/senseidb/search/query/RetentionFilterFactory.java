package com.senseidb.search.query;

import org.apache.lucene.search.Filter;

public abstract class RetentionFilterFactory{

  public RetentionFilterFactory(){
    
  }
  
  public abstract Filter buildRetentionFilter(int nDays);
}
