package com.senseidb.search.req.mapred.v2.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.senseidb.search.req.mapred.v2.SenseiMapReduce;

@SuppressWarnings("rawtypes")
public class MapReduceRegistry {

  private static Map<String, Class<? extends SenseiMapReduce>> keyToFunction = new ConcurrentHashMap<String, Class<? extends SenseiMapReduce>>();
  

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
