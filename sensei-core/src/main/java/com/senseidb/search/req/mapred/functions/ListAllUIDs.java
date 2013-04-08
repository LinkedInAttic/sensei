package com.senseidb.search.req.mapred.functions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import org.json.JSONObject;

public class ListAllUIDs implements SenseiMapReduce<Serializable, Serializable> {

  @Override
  public void init(JSONObject params) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Serializable map(int[] docIds, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountAccessor) {
    ArrayList<Long> collectedUids = new ArrayList<Long>(docIdCount);
    for (int i = 0; i < docIdCount; i++) {
      collectedUids.add(uids[docIds[i]]);
    }
    System.out.println("!!!" + collectedUids);
    return collectedUids;
  }

  @Override
  public List<Serializable> combine(List<Serializable> mapResults, CombinerStage combinerStage) {
    // TODO Auto-generated method stub
    return mapResults;
  }

  @Override
  public Serializable reduce(List<Serializable> combineResults) {
    // TODO Auto-generated method stub
    return new ArrayList<Serializable>(combineResults);
  }

  @Override
  public JSONObject render(Serializable reduceResult) {
    // TODO Auto-generated method stub
    return new JSONObject();
  }

  @Override
  public String[] getColumns()
  {
    return new String[0];
  }
}
