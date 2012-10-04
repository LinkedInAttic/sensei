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
