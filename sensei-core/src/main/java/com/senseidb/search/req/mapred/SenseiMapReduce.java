package com.senseidb.search.req.mapred;

import java.io.Serializable;
import java.util.List;

import org.json.JSONObject;


import com.linkedin.zoie.api.DocIDMapper;

public interface SenseiMapReduce<MapResult extends Serializable, ReduceResult extends Serializable> extends Serializable {
  public void init(JSONObject params);
  public MapResult map(int[] docIds, int docIdCount, long[] uids, FieldAccessor accessor);
  public List<MapResult>  combine(List<MapResult> mapResults);
  public ReduceResult  reduce(List<MapResult> combineResults);
  public JSONObject  render(ReduceResult reduceResult);
}

