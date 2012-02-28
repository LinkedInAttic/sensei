package com.senseidb.search.req.mapred.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.browseengine.bobo.mapred.MapReduceResult;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.mapred.SenseiMapReduce;

public class SenseiReduceFunctionWrapper {
 
  public static MapReduceResult combine(SenseiMapReduce mapReduceFunction, List<MapReduceResult> results) {
    MapReduceResult ret = null;
    if (results.isEmpty()) {
      return null;
    }
    ret =  results.get(0);
    for (int i = 1; i < results.size(); i++) {
      ret.getMapResults().addAll(results.get(i).getMapResults());
    }
    ret.setMapResults(mapReduceFunction.combine(ret.getMapResults()));
    return ret;
  }
 
  
  public static MapReduceResult reduce(SenseiMapReduce mapReduceFunction, List<MapReduceResult> results) {
    MapReduceResult ret = null;
    if (results.isEmpty()) {
      return ret;
    }
    ret =  results.get(0);
    for (int i = 1; i < results.size(); i++) {
      ret.getMapResults().addAll(results.get(i).getMapResults());
    }    
    ret.setReduceResult(mapReduceFunction.reduce(ret.getMapResults())) ;
    ret.setMapResults(null);
    return ret;
  }
  public static List<MapReduceResult> extractMapReduceResults(Collection<SenseiResult> senseiResults) {
    List<MapReduceResult> ret = new ArrayList<MapReduceResult>(senseiResults.size());
    for (SenseiResult senseiResult : senseiResults) {
      if (senseiResult.getMapReduceResult()!= null) {
        ret.add(senseiResult.getMapReduceResult());
      }
    }
    return ret;
  }
  
}
