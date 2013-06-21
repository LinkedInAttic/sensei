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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.search.req.mapred.functions.AvgMapReduce;
import com.senseidb.search.req.mapred.functions.DistinctCountMapReduce;
import com.senseidb.search.req.mapred.functions.MaxMapReduce;
import com.senseidb.search.req.mapred.functions.MinMapReduce;
import com.senseidb.search.req.mapred.functions.SumMapReduce;

/**
 * Registry, that is used to register map reduce functions with the nickname, so that it can be easily referred from the Json api
 * @author vzhabiuk
 *
 */
@SuppressWarnings("rawtypes")
public class MapReduceRegistry {

  private static Map<String, Class<? extends SenseiMapReduce>> keyToFunction = new ConcurrentHashMap<String, Class<? extends SenseiMapReduce>>();
  static {
    keyToFunction.put("sensei.max", MaxMapReduce.class);
    keyToFunction.put("sensei.distinctCount", DistinctCountMapReduce.class);
    keyToFunction.put("sensei.distinctCountHashSet", DistinctCountMapReduce.class);
    keyToFunction.put("sensei.min", MinMapReduce.class);
    keyToFunction.put("sensei.avg", AvgMapReduce.class);
    keyToFunction.put("sensei.sum", SumMapReduce.class);
  }
  

  public static void register(String mapReduceKey, Class<? extends SenseiMapReduce> mapReduceClass) {
    keyToFunction.put(mapReduceKey, mapReduceClass);
  }

  public static SenseiMapReduce get(String mapReduceKey) {
    try {
    Class<? extends SenseiMapReduce>  cls = keyToFunction.get(mapReduceKey);
    if (cls != null) {
      return (SenseiMapReduce) cls.newInstance();
    }
    cls = (Class<? extends SenseiMapReduce>) Class.forName(mapReduceKey);
    keyToFunction.put(mapReduceKey,  cls);   
    return  cls.newInstance();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
