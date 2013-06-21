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

  @Override
  public String[] getColumns() {
    return new String[]{column};
  }
}
