package com.senseidb.search.req.mapred;

import java.io.Serializable;
import java.util.List;

import org.json.JSONObject;

import proj.zoie.api.DocIDMapper;

public interface MapReduceJob<MapResult extends Serializable, ReduceResult extends Serializable> extends Serializable{
    public MapResult map(long[] uids, DocIDMapper docIDMapper, FieldAccessor fieldAccessor);
    public List<MapResult>  combine(List<MapResult> mapResults);
    public ReduceResult  reduce(List<MapResult> combineResults);
    public JSONObject  render(ReduceResult reduceResult);
}
