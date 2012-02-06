package com.senseidb.search.req.mapred.v2;

import java.io.Serializable;
import java.util.List;

import org.json.JSONObject;

import com.senseidb.search.req.mapred.FieldAccessor;

import proj.zoie.api.DocIDMapper;

public interface SenseiMapReduce<MapResult extends Serializable, ReduceResult extends Serializable> {
  public MapResult map(int[] docId, int docIdCount, long[] uids, FieldAccessor accessor);
  public List<MapResult>  combine(List<MapResult> mapResults);
  public ReduceResult  reduce(List<MapResult> combineResults);
  public JSONObject  render(ReduceResult reduceResult);
  public void init(JSONObject params);
}

