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
