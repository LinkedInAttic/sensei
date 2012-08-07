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
    putObj("test_obj", new ExampleExternalObj());
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
  
  

  
  /**
   * This class has to be public to be used inside the relevance model;
   *
   */
  public static class ExampleExternalObj {

    public boolean contains(String color)
    {
      return ExampleExternalExternalObj.hs.contains(color);
    }
  }
  
  public static class ExampleExternalExternalObj {
    
    public static HashSet<String> hs = new HashSet<String>();
    static {
      hs.add("red");
    }
  }
  
  
  
}
