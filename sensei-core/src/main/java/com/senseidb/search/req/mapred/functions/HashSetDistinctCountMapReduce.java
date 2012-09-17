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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.SenseiMapReduce;

public class HashSetDistinctCountMapReduce implements SenseiMapReduce<HashSet, Integer> {

  private String column;

  @Override
  public void init(JSONObject params) {
    column = params.optString("column");
    if (column == null) {
      throw new IllegalStateException("Column parameter shouldn't be null");
    }
    
  }

  @Override
  public HashSet map(int[] docId, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountAccessor) {
    HashSet hashSet = new HashSet(docIdCount);
    for (int i =0; i < docIdCount; i++) {
      hashSet.add(accessor.getLong(column, docId[i]));
    }
    
    return hashSet;
  }

  @Override
  public List<HashSet> combine(List<HashSet> mapResults, CombinerStage combinerStage) {
    if (mapResults.isEmpty()) {
      return mapResults;
    }
    HashSet ret = mapResults.get(0);

    for (int i = 1; i < mapResults.size(); i++) {
     ret.addAll(mapResults.get(i));
    }
    return Arrays.asList(ret);
  }

  @Override
  public Integer reduce(List<HashSet> combineResults) {
    if (combineResults.isEmpty()) {
      return 0;
    }
    HashSet ret = combineResults.get(0);
    for (int i = 1; i < combineResults.size(); i++) {
     ret.addAll(combineResults.get(i));
    }
    
    return ret.size();
  }

  @Override
  public JSONObject render(Integer reduceResult) {
    // TODO Auto-generated method stub
    try {
      return new JSONObject().put("distinctCount", reduceResult);
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
  }
}
