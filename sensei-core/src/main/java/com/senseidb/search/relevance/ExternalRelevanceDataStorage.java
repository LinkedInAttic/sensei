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
