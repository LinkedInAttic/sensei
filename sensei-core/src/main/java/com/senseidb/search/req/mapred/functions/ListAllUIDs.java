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
