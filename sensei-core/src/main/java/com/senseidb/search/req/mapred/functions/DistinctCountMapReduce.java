package com.senseidb.search.req.mapred.functions;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class DistinctCountMapReduce implements SenseiMapReduce<LongOpenHashSet, Integer> {

  private String column;

  @Override
  public void init(JSONObject params) {
    column = params.optString("column");
    if (column == null) {
      throw new IllegalStateException("Column parameter shouldn't be null");
    }
    
  }

  @Override
  public LongOpenHashSet map(int[] docId, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountAccessor) {
    LongOpenHashSet hashSet = new LongOpenHashSet(docIdCount);
    for (int i =0; i < docIdCount; i++) {
      hashSet.add(accessor.getLong(column, docId[i]));
    }
    
    return hashSet;
  }

  @Override
  public List<LongOpenHashSet> combine(List<LongOpenHashSet> mapResults, CombinerStage combinerStage) {
    if (mapResults.isEmpty()) {
      return mapResults;
    }
    LongOpenHashSet ret = mapResults.get(0);
    for (int i = 1; i < mapResults.size(); i++) {
     ret.addAll(mapResults.get(i));
    }
    mapResults.clear();
    mapResults.add(ret);
    return mapResults;
  }

  @Override
  public Integer reduce(List<LongOpenHashSet> combineResults) {
    if (combineResults.isEmpty()) {
      return 0;
    }
    LongOpenHashSet ret = combineResults.get(0);
    for (int i = 1; i < combineResults.size(); i++) {
     ret.addAll(combineResults.get(i));
    }
    
    return ret.size();
  }

  @Override
  public JSONObject render(Integer reduceResult) {
    // TODO Auto-generated method stub
    try {
      return new FastJSONObject().put("distinctCount", reduceResult);
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
  }

}
