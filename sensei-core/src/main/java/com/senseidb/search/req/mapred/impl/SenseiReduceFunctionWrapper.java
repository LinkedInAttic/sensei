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
package com.senseidb.search.req.mapred.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.browseengine.bobo.mapred.MapReduceResult;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.SenseiMapReduce;

/**
 * Is the part of the SenseiRequest, keeps the logic of merging  result, that were got on the map/combine phase
 *
 */
public class SenseiReduceFunctionWrapper {
 
  /**
   * Combine callback
   * @param mapReduceFunction
   * @param results
   * @return
   */
  public static MapReduceResult combine(SenseiMapReduce mapReduceFunction, List<MapReduceResult> results) {
    MapReduceResult ret = null;
    if (results.isEmpty()) {
      return null;
    }
    ret =  results.get(0);
    for (int i = 1; i < results.size(); i++) {
      ret.getMapResults().addAll(results.get(i).getMapResults());
    }
    ret.setMapResults(new ArrayList(mapReduceFunction.combine(ret.getMapResults(), CombinerStage.nodeLevel)));
    return ret;
  }
 
  
  /** Reduce callback
   * @param mapReduceFunction
   * @param results
   * @return
   */
  public static MapReduceResult reduce(SenseiMapReduce mapReduceFunction, List<MapReduceResult> results) {
    MapReduceResult ret = null;
    if (results.isEmpty()) {
      return ret;
    }
    ret =  results.get(0);
    for (int i = 1; i < results.size(); i++) {
      ret.getMapResults().addAll(results.get(i).getMapResults());
    }    
    ret.setReduceResult(mapReduceFunction.reduce(ret.getMapResults())) ;
    ret.setMapResults(null);
    return ret;
  }
  public static List<MapReduceResult> extractMapReduceResults(Collection<SenseiResult> senseiResults) {
    List<MapReduceResult> ret = new ArrayList<MapReduceResult>(senseiResults.size());
    for (SenseiResult senseiResult : senseiResults) {
      if (senseiResult.getMapReduceResult()!= null) {
        ret.add(senseiResult.getMapReduceResult());
      }
    }
    return ret;
  }
  
}
