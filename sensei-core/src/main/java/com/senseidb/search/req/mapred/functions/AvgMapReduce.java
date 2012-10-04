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

import java.io.Serializable;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.SenseiMapReduce;

public class AvgMapReduce implements SenseiMapReduce<AvgResult, AvgResult> {
  private String column;

  @Override
  public void init(JSONObject params) {
    column = params.optString("column");
    if (column == null) {
      throw new IllegalStateException("Column parameter shouldn't be null");
    }
    
  }

  @Override
  public AvgResult map(int[] docId, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountAccessor) {
    double ret = 0;
    for (int i = 0; i < docIdCount; i++) {
      ret+= accessor.getDouble(column, docId[i]);
    }  
    return new AvgResult(ret / docIdCount, docIdCount);
  }

  @Override
  public List<AvgResult> combine(List<AvgResult> mapResults, CombinerStage combinerStage) {
    AvgResult avgResult = reduce(mapResults);
    mapResults.clear();
    mapResults.add(avgResult);
    return mapResults;
  }

  @Override
  public AvgResult reduce(List<AvgResult> combineResults) {
    if (combineResults.isEmpty()) {
      return null;
    }   
    int minCount = Integer.MAX_VALUE;
    for (AvgResult avgResult : combineResults) {
      if (avgResult == null || avgResult.count == 0) {
        continue;
      }
      if (minCount > avgResult.count) {
        minCount = avgResult.count;
      }
    }
    if (minCount == Integer.MAX_VALUE) {
      return null;
    }    
    double accumulatedValue = 0;
    int accumulatedCount = 0;
    for (AvgResult avgResult : combineResults) {
      if (avgResult == null || avgResult.count == 0) {
        continue;
      }
      accumulatedValue += avgResult.value / minCount * avgResult.count;
      accumulatedCount += avgResult.count;
    }
    double ret = accumulatedValue / ((double) accumulatedCount / minCount);
    return new AvgResult(ret, accumulatedCount);
  }
  
 
  @Override
  public JSONObject render(AvgResult reduceResult) {
   
    try {
      return new JSONObject().put("avg", reduceResult.value).put("count", reduceResult.count);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  
  
  
}
class AvgResult implements Serializable {
  public double value;
  public int count;
  public AvgResult(double value, int count) {
    super();
    this.value = value;
    this.count = count;
  }
}
