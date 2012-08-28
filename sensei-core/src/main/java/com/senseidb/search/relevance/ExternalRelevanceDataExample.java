package com.senseidb.search.relevance;

import java.util.HashSet;
import java.util.Map;

import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.relevance.ExternalRelevanceDataStorage.RelevanceObjPlugin;


/**
 * This class has to be public to be used inside the relevance model;
 *
 */
public class ExternalRelevanceDataExample implements RelevanceObjPlugin
{
  String name = "test_obj";
  HashSet<String> hsColor = ExampleExternalExternalObj.hs;
  
  public boolean contains(String color)
  {
    return hsColor.contains(color);
  }
  
  public void setName(String testName)
  {
    name = testName;
  }
  
  public void setHashSet(HashSet<String> hs)
  {
    hsColor = hs;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void start()
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void stop()
  {
    // TODO Auto-generated method stub
    
  }
  
  
  
  public static class ExampleExternalExternalObj {
    
    public static HashSet<String> hs = new HashSet<String>();
    static {
      hs.add("red");
    }
  }
}
