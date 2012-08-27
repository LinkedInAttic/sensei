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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.facets.data.TermValueList;

public class FacetCountsMapReduce implements SenseiMapReduce<HashMap<String, IntContainer>, ArrayList<GroupedValue>> {
  private static final long serialVersionUID = 1L;  
  private String column;
  
  public void init(JSONObject params) {
    try {
       column = params.getString("column");     
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
  }
  public HashMap<String, IntContainer> map(int[] docIds, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountAccessor) {
    if (!facetCountAccessor.areFacetCountsPresent()) {
      return null;
    }
    int[] countDistribution = facetCountAccessor.getFacetCollector(column).getCountDistribution();
    TermValueList termValueList = accessor.getTermValueList(column);
    HashMap<String, IntContainer> ret = new HashMap<String, IntContainer>(countDistribution.length);
    for (int i = 0; i < countDistribution.length; i++) {
      ret.put(termValueList.get(i), new IntContainer(countDistribution[i]));
    }
    return ret;
  }
 

  private String getKey(String[] columns, FieldAccessor fieldAccessor, int docId) {
    StringBuilder key = new StringBuilder(fieldAccessor.get(columns[0], docId).toString());
    for (int i = 1; i < columns.length; i++) {
      key.append(":").append(fieldAccessor.get(columns[i], docId).toString());
    }
    return key.toString();
  }

  @Override
  public List<HashMap<String, IntContainer>> combine(List<HashMap<String, IntContainer>> mapResults, CombinerStage combinerStage) {
    
    if (mapResults == null || mapResults.isEmpty()) return mapResults;
    HashMap<String, IntContainer> ret = new HashMap<String, IntContainer>();
    for (int i = 0; i < mapResults.size(); i++) {
      Map<String, IntContainer> map = mapResults.get(i);
      if (map == null) {
        continue;
      }
      for (String key : map.keySet()) {
        IntContainer count = ret.get(key);
        if (count != null) {
          count.add(map.get(key).value);
        } else {
          ret.put(key, map.get(key));
        }
      }
    }  
    return java.util.Arrays.asList(ret);
  }

  @Override
  public ArrayList<GroupedValue> reduce(List<HashMap<String, IntContainer>> combineResults) {
    if (combineResults == null || combineResults.isEmpty()) return new ArrayList<GroupedValue>();
    Map<String, IntContainer> retMap = new HashMap<String, IntContainer>();
    for (int i = 0; i < combineResults.size(); i++) {
      Map<String, IntContainer> map = combineResults.get(i);
      for (String key : map.keySet()) {
        IntContainer count = retMap.get(key);
        if (count != null) {
          count.add(map.get(key).value);
        } else {
          retMap.put(key, map.get(key));
        }
      }
    }
    ArrayList<GroupedValue> ret = new ArrayList<GroupedValue>();
    for (Map.Entry<String, IntContainer> entry : retMap.entrySet()) {
      ret.add(new GroupedValue(entry.getKey(), entry.getValue().value));
    }
    Collections.sort(ret);
    return ret;
  }

  public JSONObject render(ArrayList<GroupedValue> reduceResult) {
    try {
      JSONObject ret = new JSONObject();
      for (GroupedValue grouped : reduceResult) {
        ret.put(grouped.key, grouped.value);
      }
      return new JSONObject().put("facetCounts", ret);
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
  }
}

 class IntContainer implements Serializable {
  public int value;

  public IntContainer(int value) {
    super();
    this.value = value;
  }

  public IntContainer add(int value) {
    this.value += value;
    return this;
  }
}
 


