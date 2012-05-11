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
