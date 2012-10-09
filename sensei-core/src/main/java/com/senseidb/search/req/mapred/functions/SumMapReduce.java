package com.senseidb.search.req.mapred.functions;

import java.util.List;

import javax.management.RuntimeErrorException;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class SumMapReduce implements SenseiMapReduce<Double, Double> {
  private String column;

  @Override
  public void init(JSONObject params) {
    column = params.optString("column");
    if (column == null) {
      throw new IllegalStateException("Column parameter shouldn't be null");
    }
    
  }

  @Override
  public Double map(int[] docIds, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountAccessor) {
    double ret = 0;
    for (int i = 0; i < docIdCount; i++) {
      ret += accessor.getDouble(column, docIds[i]);
    }
    return ret;
  }

  @Override
  public List<Double> combine(List<Double> mapResults, CombinerStage combinerStage) {
    double ret = 0;
    for (Double count : mapResults) {
      ret += count;
    }
    mapResults.clear();
    mapResults.add(ret);
    return mapResults;
  }

  @Override
  public Double reduce(List<Double> combineResults) {
    double ret = 0;
    for (Double count : combineResults) {
      ret += count;
    }
    return ret;
  }

  @Override
  public JSONObject render(Double reduceResult) {
   
    try {
      return new FastJSONObject().put("sum", reduceResult);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }
  
}
