package com.senseidb.search.req.mapred;

import java.io.Serializable;
import java.util.List;

import org.json.JSONObject;

/**
 * By extending this interface, one can access the Sensei segment data, produce intermediate results, aggregate those results on 
 * the partition, node and cluster level. This is much simple than to implement your own facet handler. 
 * Also this allows to enhance the bobo/Sensei merging logic
 *
 * @param <MapResult>
 * @param <ReduceResult>
 */
public interface SenseiMapReduce<MapResult extends Serializable, ReduceResult extends Serializable> extends Serializable {
  /**
   * "mapReduce":{"function":"com.senseidb.search.req.mapred.functions.MaxMapReduce","parameters":{"column":"groupid"}} 
   * the argument corresponds to the parameters object in Json request. It is used to initialize the mapred job
   * 
   */
  public void init(JSONObject params);
  /**
   * The map function. It can get the docId  from the docIds array containing value from 0 to docIdCount. 
   * All the docIds with array indexes >= docIdCount should be ignored
   * One can simply get the document's uid by calling uids[docId]
   * @param docIds
   * @param docIdCount
   * @param uids
   * @param accessor is used to get field's values 
   * @return arbitrary map function results
   */
  public MapResult map(int[] docIds, int docIdCount, long[] uids, FieldAccessor accessor);
  /**
   * Merge map results objects to reduce memory and serialization costs. If this method will not merge map results, there is a high chance, that you'd get 
   * outOfMemory in case there is a significant number of documents indexed
   * @param mapResults
   * @return
   */
  public List<MapResult>  combine(List<MapResult> mapResults, CombinerStage combinerStage);
  /**
   * Reduce the merged map results
   * @param combineResults
   * @return
   */
  public ReduceResult  reduce(List<MapResult> combineResults);
  /**
   * Converts the result of the reduce function into JsonObject, so that it can be sent back to the client
   * @param reduceResult
   * @return
   */
  public JSONObject  render(ReduceResult reduceResult);
}

