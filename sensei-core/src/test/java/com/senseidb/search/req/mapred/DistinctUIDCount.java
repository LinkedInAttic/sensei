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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import scala.actors.threadpool.Arrays;

@SuppressWarnings("unchecked")
public class DistinctUIDCount implements SenseiMapReduce<HashSet<Long>, Integer> {
  private static final long serialVersionUID = 1L;

  public void init(JSONObject params) {
  
  }

  @Override
  public HashSet<Long> map(int[] docIds, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountAccessor) {
    HashSet<Long> ret = new HashSet<Long>(docIdCount);
    for (int i = 0; i < docIdCount; i++) {
      ret.add(uids[docIds[i]]);
    }
    return ret;
  }

  @Override
  public List<HashSet<Long>> combine(List<HashSet<Long>> mapResults, CombinerStage combinerStage) {
    HashSet<Long> ret = new HashSet<Long>();
    for (HashSet<Long> mapResult : mapResults) {
      ret.addAll(mapResult);
    }
    return java.util.Arrays.asList(ret);
  }

  @Override
  public Integer reduce(List<HashSet<Long>> combineResults) {
    HashSet<Long> ret = new HashSet<Long>();
    for (HashSet<Long> mapResult : combineResults) {
      ret.addAll(mapResult);
    }
    return ret.size();
  }

  @Override
  public JSONObject render(Integer reduceResult) {
    
    try {
      return new JSONObject().put("distinctUidCount", reduceResult);
    } catch (JSONException e) {      
      throw new RuntimeException(e);
    }
  }
  
}
