/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
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
   * @param facetCountsAccessor 
   * @return arbitrary map function results
   */
  public MapResult map(int[] docIds, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountsAccessor);
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

