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
