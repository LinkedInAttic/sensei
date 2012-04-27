package com.senseidb.search.req.mapred;

import java.util.Arrays;
import java.util.List;

import org.json.JSONObject;

import com.senseidb.search.req.mapred.obsolete.MapReduceJob;

import com.linkedin.zoie.api.DocIDMapper;

public class MaxTestMapReduce implements MapReduceJob<Long, Long> {
  private static final long serialVersionUID = 1L;
  
  private final String fieldName;

  public MaxTestMapReduce(String fieldName) {
    this.fieldName = fieldName;   
  }
 
 

  @Override
  public Long map(long[] uids, DocIDMapper docIDMapper, FieldAccessor fieldAccessor) {
    long max = Long.MIN_VALUE;
    for (long uid : uids) {
      if (max < fieldAccessor.getLongByUID(fieldName, uid)) {
        max = fieldAccessor.getLongByUID(fieldName, uid);
      }
    }
    return max;
  }

  @Override
  public List<Long> combine(List<Long> mapResults) {
    long max = Long.MIN_VALUE;
    for (long mapResult : mapResults) {
      if (max < mapResult) {
        max = mapResult;
      }
    }
    return Arrays.asList(max);
  }

  @Override
  public Long reduce(List<Long> combineResults) {
    long max = Long.MIN_VALUE;
    for (long mapResult : combineResults) {
      if (max < mapResult) {
        max = mapResult;
      }
    }
    return max;
  }

  @Override
  public JSONObject render(Long reduceResult) {
    return null;
  }
}
