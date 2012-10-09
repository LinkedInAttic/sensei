package com.senseidb.search.req.mapred.functions;

import java.io.Serializable;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class AvgMapReduce implements SenseiMapReduce<AvgResult, AvgResult> {
  private String column;

  @Override
  public void init(JSONObject params) {
    column = params.optString("column");
    if (column == null) {
      throw new IllegalStateException("Column parameter shouldn't be null");
    }
    
  }

  @Override
  public AvgResult map(int[] docId, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountAccessor) {
    double ret = 0;
    for (int i = 0; i < docIdCount; i++) {
      ret+= accessor.getDouble(column, docId[i]);
    }  
    return new AvgResult(ret / docIdCount, docIdCount);
  }

  @Override
  public List<AvgResult> combine(List<AvgResult> mapResults, CombinerStage combinerStage) {
    AvgResult avgResult = reduce(mapResults);
    mapResults.clear();
    mapResults.add(avgResult);
    return mapResults;
  }

  @Override
  public AvgResult reduce(List<AvgResult> combineResults) {
    if (combineResults.isEmpty()) {
      return null;
    }   
    int minCount = Integer.MAX_VALUE;
    for (AvgResult avgResult : combineResults) {
      if (avgResult == null || avgResult.count == 0) {
        continue;
      }
      if (minCount > avgResult.count) {
        minCount = avgResult.count;
      }
    }
    if (minCount == Integer.MAX_VALUE) {
      return null;
    }    
    double accumulatedValue = 0;
    int accumulatedCount = 0;
    for (AvgResult avgResult : combineResults) {
      if (avgResult == null || avgResult.count == 0) {
        continue;
      }
      accumulatedValue += avgResult.value / minCount * avgResult.count;
      accumulatedCount += avgResult.count;
    }
    double ret = accumulatedValue / ((double) accumulatedCount / minCount);
    return new AvgResult(ret, accumulatedCount);
  }
  
 
  @Override
  public JSONObject render(AvgResult reduceResult) {
   
    try {
      return new FastJSONObject().put("avg", reduceResult.value).put("count", reduceResult.count);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  
  
  
}
class AvgResult implements Serializable {
  public double value;
  public int count;
  public AvgResult(double value, int count) {
    super();
    this.value = value;
    this.count = count;
  }
}
