package com.senseidb.search.relevance;

import java.util.HashMap;
import java.util.HashSet;

/**
 * @author sguo
 * Objects initialized as SenseiPlugins for relevance are stored here. They are used to serve as static data for relevance computing inside the function.
 * e.g., a big static hashmap with large amount of data, a connector to an off-heap memory key-value store.
 *
 */
public class FacilityDataStorage
{
  
  private static HashMap<String, Object> hmObjs;
  private static HashMap<String, String> hmClsNames;

  static{
    hmObjs = new HashMap<String, Object>();
    hmClsNames = new HashMap<String, String>();
    
    //put in some testing object;
    HashSet<String> hs = new HashSet<String>();
    hs.add("red");
    putObj("test_obj", hs);
  }
  
  public static Object getObj(String name)
  {
    return hmObjs.get(name);
  }
  
  public static String getObjClsName(String name)
  {
    return hmClsNames.get(name);
  }
  
  public static void putObj(String name, Object obj)
  {
    hmObjs.put(name, obj);
    hmClsNames.put(name, obj.getClass().getName());
  }
}
