package com.senseidb.search.relevance;

import java.util.HashMap;
import java.util.Map;

import com.senseidb.search.relevance.CustomRelevanceFunction.CustomRelevanceFunctionFactory;
import com.senseidb.search.relevance.RuntimeRelevanceFunction.RuntimeRelevanceFunctionFactory;

public class ModelStorage
{

  /**
   * 
   *   A model is a relevance function factory, which can build() a CustomRelevanceFunction Object;
   * 
   * 
   * **/
  
  
  // preloaded models; custom relevance function factory
  private static Map<String, CustomRelevanceFunctionFactory> preloadedCRFMap = new HashMap<String, CustomRelevanceFunctionFactory>();
  
  
  // runtime models; runtime relevance function factory
  private static Map<String, RuntimeRelevanceFunctionFactory> runtimeCRFMap = new HashMap<String, RuntimeRelevanceFunctionFactory>();
  
  
  
  public static void injectPreloadedModel(String name, CustomRelevanceFunctionFactory crf)
  {
    preloadedCRFMap.put(name, crf);
  }
  
  public static void injectRuntimeModel(String name, RuntimeRelevanceFunctionFactory rrf)
  {
    runtimeCRFMap.put(name, rrf);
  }
  
  public static boolean hasRuntimeModel(String modelName)
  {
    if(runtimeCRFMap.containsKey(modelName))
      return true;
    else
      return false;
  }
  
  public static boolean hasPreloadedModel(String modelName)
  {
    if(preloadedCRFMap.containsKey(modelName))
      return true;
    else
      return false;
  }
  
  public static RuntimeRelevanceFunctionFactory getRuntimeModel(String modelName)
  {
    if(runtimeCRFMap.containsKey(modelName))
      return runtimeCRFMap.get(modelName);
    else
      return null;
  }
  
  public static CustomRelevanceFunctionFactory getPreloadedModel(String modelName)
  {
    if(preloadedCRFMap.containsKey(modelName))
      return preloadedCRFMap.get(modelName);
    else
      return null;
  }
  
  
}
