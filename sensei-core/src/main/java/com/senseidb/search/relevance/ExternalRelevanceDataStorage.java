package com.senseidb.search.relevance;

import java.util.HashMap;
import java.util.HashSet;

import com.senseidb.plugin.SenseiPlugin;

/**
 * @author sguo
 * Objects initialized as SenseiPlugins for relevance are stored here. They are used to serve as static data for relevance computing inside the function.
 * e.g., a big static hashmap with large amount of data, a connector to an off-heap memory key-value store.
 *
 */
public class ExternalRelevanceDataStorage
{
  
  private static HashMap<String, RelevanceObjPlugin> hmObjs;
  private static HashMap<String, String> hmClsNames;

  static{
    hmObjs = new HashMap<String, RelevanceObjPlugin>();
    hmClsNames = new HashMap<String, String>();
    
    // put in another testing object;
    // we already use senseiplugin to inject one external object, named as "test_obj", and it has an hashset, which contains "red";
    // This one is injected here not in the plugin, it has different name and different hashset;
    ExternalRelevanceDataExample example = new ExternalRelevanceDataExample();
    HashSet<String> hsColor = new HashSet<String>();
    hsColor.add("green");
    example.setHashSet(hsColor);
    example.setName("test_obj2");
    putObj(example);
  }
  
  public static Object getObj(String name)
  {
    return hmObjs.get(name);
  }
  
  public static String getObjClsName(String name)
  {
    return hmClsNames.get(name);
  }
  
  public static void putObj(RelevanceObjPlugin obj)
  {
    String name = obj.getName();
    hmObjs.put(name, obj);
    hmClsNames.put(name, obj.getClass().getName());

  }
  
  

  public interface RelevanceObjPlugin extends SenseiPlugin{
    
    /**
     * @return the name of the external object. This is used as a variable name inside relevance model;
     */
    public String getName();
  } 
  
}
