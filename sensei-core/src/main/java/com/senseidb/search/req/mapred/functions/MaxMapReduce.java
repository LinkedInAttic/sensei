package com.senseidb.search.req.mapred.functions;

import java.io.Serializable;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import scala.actors.threadpool.Arrays;

import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class MaxMapReduce implements SenseiMapReduce<MaxResult, MaxResult> {

  private String column;

  @Override
  public MaxResult map(int[] docIds, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountAccessor) {
    double max = Double.MIN_VALUE;
    double tmp = 0;
    long uid = 0l;
    for (int i =0; i < docIdCount; i++) {
      tmp = accessor.getDouble(column, docIds[i]);
      if (max < tmp) {       
        max = tmp;
        uid = uids[docIds[i]];
      }
    }
    return new MaxResult(max, uid);
  }

  @Override
  public List<MaxResult> combine(List<MaxResult> mapResults, CombinerStage combinerStage) {
    if (mapResults.isEmpty()) {
      return mapResults;
    }
    MaxResult ret = mapResults.get(0);
    for (int i = 1; i < mapResults.size(); i++) {
      if (ret.value < mapResults.get(i).value) {
        ret = mapResults.get(i);
      }
    } 
    return java.util.Arrays.asList(ret);
  }

  @Override
  public MaxResult reduce(List<MaxResult> combineResults) {
    if (combineResults.isEmpty()) {
      return null;
    }
    MaxResult ret = combineResults.get(0);
    for (int i = 1; i < combineResults.size(); i++) {
      if (ret.value < combineResults.get(i).value) {
        ret = combineResults.get(i);
      }
    }
    return ret;
  }

  @Override
  public JSONObject render(MaxResult reduceResult) {
    
    try {
      return new FastJSONObject().put("max", reduceResult.value).put("uid", reduceResult.uid);
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void init(JSONObject params) {
     column = params.optString("column");
    if (column == null) {
      throw new IllegalStateException("Column parameter shouldn't be null");
    }
  }
 
}
class MaxResult implements Serializable {
  public double value;
  public long uid;
  public MaxResult(double value, long uid) {
    super();
    this.value = value;
    this.uid = uid;
  }
  
}
